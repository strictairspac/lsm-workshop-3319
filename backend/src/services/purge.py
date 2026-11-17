"""Permanent, key-scoped purge of raw telemetry values.

Accepting a purge request is the barrier: from that commit on, raw values for the
selected keys are hidden from every read path, new trials for the batch are
paused, and a slow running trial may not write the values back.  The worker then
physically removes the keys from the batch, from every associated trial snapshot
and result, and deletes the on-disk evidence files.
"""
import copy, hashlib, logging
from datetime import datetime
from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from fastapi import HTTPException
from ..models.dataset import Dataset
from ..models.trial import Trial
from ..models.purge import PurgeRequest
from .artifacts import remove_artifact
from .queue import queue

PURGE_QUEUE='lsm:purges'

def normalize_keys(keys):
    clean=[]
    for key in keys or []:
        if not isinstance(key,str) or not 1<=len(key)<=64:
            raise HTTPException(422,'清理键必须是1到64个字符的字符串')
        if key not in clean:clean.append(key)
    if not clean:raise HTTPException(422,'至少选择一个要永久清理的键')
    return sorted(clean)

def fingerprint(dataset_id,keys):
    payload=('\0'.join([str(dataset_id),*sorted(keys)])).encode()
    return hashlib.sha256(payload).hexdigest()

def keys_for(db,dataset_id):
    keys=set()
    for row in db.scalars(select(PurgeRequest).where(PurgeRequest.dataset_id==dataset_id)):
        keys.update(row.keys or [])
    return keys

def status_by_dataset(db):
    result={}
    for row in db.scalars(select(PurgeRequest).order_by(PurgeRequest.id)):
        item=result.setdefault(row.dataset_id,{'purged_keys':set(),'purge_active':False,'purge_status':None,'purge_id':None})
        item['purged_keys'].update(row.keys or [])
        item['purge_id']=row.id
        item['purge_status']=row.status
        # The barrier survives a failed run: values stay hidden until a retry completes.
        if row.status!='completed':item['purge_active']=True
    for item in result.values():item['purged_keys']=sorted(item['purged_keys'])
    return result

def paused(db,dataset_id):
    return db.scalar(select(PurgeRequest.id).where(PurgeRequest.dataset_id==dataset_id,PurgeRequest.status!='completed')) is not None

def scrub_entries(entries,keys):
    return [e for e in entries or [] if e.get('key') not in keys]

def snapshot_mentions(snapshot,keys):
    return any(e.get('key') in keys for e in (snapshot or {}).get('entries',[]) or [])

def result_mentions(result,keys):
    if not result:return False
    if keys & set(result.get('actual') or {}):return True
    if keys & set(result.get('oracle') or {}):return True
    for side in ('before_levels','after_levels'):
        for level in result.get(side) or []:
            for table in level.get('tables') or []:
                if table.get('min_key') in keys or table.get('max_key') in keys:return True
    return False

def scrub_result(result,keys):
    """Drop raw values for the purged keys but keep historical metrics untouched.

    Checksums that digest a dict containing a purged value are withheld: a stable
    hash of a short reading is a cheap oracle for guessing the value back.
    """
    if not isinstance(result,dict):return result
    out=copy.deepcopy(result)
    withheld=[]
    for field in ('actual','oracle'):
        rows=out.get(field)
        if isinstance(rows,dict):
            if keys & set(rows):withheld.append(field+'_checksum')
            out[field]={k:v for k,v in rows.items() if k not in keys}
    for side in ('before_levels','after_levels'):
        for level in out.get(side) or []:
            for table in level.get('tables') or []:
                if table.get('min_key') in keys:table['min_key']=None
                if table.get('max_key') in keys:table['max_key']=None
    for field in withheld:out[field]=None
    if withheld:out['checksums_withheld']='永久清理已撤回覆盖原值的校验和：'+'、'.join(withheld)
    out['purged_keys']=sorted(set(out.get('purged_keys') or [])|set(keys))
    return out

def scrub_snapshot(snapshot,keys):
    if not isinstance(snapshot,dict):return snapshot
    out=copy.deepcopy(snapshot)
    out['entries']=scrub_entries(out.get('entries'),keys)
    return out

def serialize(row):
    return {'id':row.id,'dataset_id':row.dataset_id,'keys':row.keys,'status':row.status,
            'attempts':row.attempts or 0,'progress':row.progress or {},'error':row.error,
            'created_at':str(row.created_at),'updated_at':str(row.updated_at),
            'completed_at':str(row.completed_at) if row.completed_at else None}

def listing(db,dataset_id=None):
    stmt=select(PurgeRequest).order_by(PurgeRequest.id.desc())
    if dataset_id is not None:stmt=stmt.where(PurgeRequest.dataset_id==dataset_id)
    return [serialize(x) for x in db.scalars(stmt)]

def detail(db,ident):
    row=db.get(PurgeRequest,ident)
    if not row:raise HTTPException(404,'清理任务不存在')
    return serialize(row)

def enqueue(ident):
    # Redis is only a wake-up hint; the worker also recovers pending rows from MySQL.
    try:queue.rpush(PURGE_QUEUE,ident)
    except Exception:logging.warning('purge enqueue failed; worker will recover from database',exc_info=True)

def request(db,dataset_id,keys):
    keys=normalize_keys(keys)
    dataset=db.execute(select(Dataset).where(Dataset.id==dataset_id).with_for_update()).scalar_one_or_none()
    if not dataset:raise HTTPException(404,'写入批次不存在')
    mark=fingerprint(dataset_id,keys)
    existing=db.scalar(select(PurgeRequest).where(PurgeRequest.fingerprint==mark))
    if existing:return existing,False
    present={e.get('key') for e in dataset.entries or []}|keys_for(db,dataset_id)
    unknown=sorted(set(keys)-present)
    if unknown:raise HTTPException(422,'以下键不在该批次中：'+'、'.join(unknown))
    row=PurgeRequest(dataset_id=dataset_id,keys=keys,fingerprint=mark,status='pending',
                     progress={'trials_total':None,'trials_processed':0,'files_deleted':0})
    db.add(row)
    try:db.commit()
    except IntegrityError:
        # Another identical request won the race; reuse it instead of registering twice.
        db.rollback()
        return db.scalar(select(PurgeRequest).where(PurgeRequest.fingerprint==mark)),False
    enqueue(row.id)
    return row,True

def retry(db,ident):
    row=db.get(PurgeRequest,ident)
    if not row:raise HTTPException(404,'清理任务不存在')
    if row.status=='completed':raise HTTPException(409,'清理已完成，无需重试')
    if row.status!='pending':
        row.status='pending';row.error=None;db.commit();enqueue(row.id)
    return serialize(row)

def _affected_trial_ids(trials,keys):
    ids=[]
    for trial in trials:
        if snapshot_mentions(trial.input_snapshot,keys) or result_mentions(trial.result,keys):
            ids.append(trial.id)
    return ids

def run(db,ident,remove_file=None):
    """Idempotent purge execution. Safe to re-run after an interruption."""
    remove_file=remove_file or remove_artifact
    row=db.get(PurgeRequest,ident)
    if not row or row.status=='completed':return row
    row.status='running';row.attempts=(row.attempts or 0)+1;row.error=None;db.commit()
    try:
        # Lock the batch so a trial finishing concurrently cannot commit a raw
        # snapshot between our scrub and our state transition.
        dataset=db.execute(select(Dataset).where(Dataset.id==row.dataset_id).with_for_update()).scalar_one()
        keys=set(row.keys or [])
        trials=list(db.scalars(select(Trial).where(Trial.dataset_id==row.dataset_id)))
        affected=_affected_trial_ids(trials,keys)
        prior=row.progress or {}
        pending=set(prior.get('files_pending') or [])|set(affected)
        deleted_before=prior.get('files_deleted') or 0
        dataset.entries=scrub_entries(dataset.entries,keys)
        for trial in trials:
            trial.input_snapshot=scrub_snapshot(trial.input_snapshot,keys)
            if trial.result is not None:trial.result=scrub_result(trial.result,keys)
        row.progress={'trials_total':len(trials),'trials_processed':len(trials),
                      'trials_affected':len(affected),'files_pending':sorted(pending),'files_deleted':deleted_before,'keys':row.keys}
        db.commit()
        # File removal runs after the metadata is safe; an interruption here leaves
        # the row 'running' and the pending file list lets a retry finish the job.
        deleted=deleted_before
        for trial_id in sorted(pending):
            if remove_file(trial_id):deleted+=1
        row=db.get(PurgeRequest,ident)
        row.progress={**(row.progress or {}),'files_pending':[],'files_deleted':deleted}
        row.status='completed';row.completed_at=datetime.now();row.error=None
        db.commit();return row
    except Exception as exc:
        db.rollback()
        row=db.get(PurgeRequest,ident)
        row.status='failed';row.error=str(exc)[:1000];db.commit()
        raise
