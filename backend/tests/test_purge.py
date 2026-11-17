import copy,json
import pytest
from sqlalchemy import select
from src.config.database import Session
from src.models.dataset import Dataset
from src.models.trial import Trial
from src.models.purge import PurgeRequest
from src.services import purge
from src.services import trial as trial_service
from src.services.artifacts import artifact_path,write_artifact
from src.worker import run_one,run_purge,recover
from conftest import make_dataset,make_config,make_trial

PURGED={'sensor-01'}

def purge_keys(db,dataset,keys=PURGED):
    row,_=purge.request(db,dataset.id,list(keys))
    purge.run(db,row.id)
    return row

def test_accept_hides_values_and_pauses_only_that_batch(client,tokens,db):
    dataset=make_dataset(db)
    other=make_dataset(db,'批次乙')
    config=make_config(db)
    trial=make_trial(db,dataset,config)

    response=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01']},headers=tokens['operator'])
    assert response.status_code==202,response.text
    assert response.json()['status']=='pending'

    # Barrier: reads are scrubbed immediately, before the worker runs.
    listing=client.get('/datasets',headers=tokens['viewer']).json()
    first=next(x for x in listing if x['id']==dataset.id)
    assert 'sensor-01' in first['purged_keys'] and first['purge_active'] is True
    assert all(e['key']!='sensor-01' for e in first['entries'])
    second=next(x for x in listing if x['id']==other.id)
    assert second['purged_keys']==[] and second['purge_active'] is False
    assert len(second['entries'])==len(other.entries)

    detail=client.get(f'/trials/{trial.id}',headers=tokens['viewer']).json()
    body=json.dumps(detail,ensure_ascii=False)
    assert '18.2' not in body and '18.8' not in body
    assert all(e['key']!='sensor-01' for e in detail['input_snapshot']['entries'])
    assert 'sensor-01' not in detail['result']['actual']
    assert 'sensor-01' not in detail['result']['oracle']
    # A checksum over the purged value would let a guess be verified.
    assert detail['result']['oracle_checksum'] is None
    assert detail['result']['actual_checksum'] is None
    assert 'checksums_withheld' in detail['result']
    # Historical metrics that contain no raw values survive.
    for field in ('read_amplification_before','read_amplification_after','write_amplification',
                  'operation_count','live_keys','flushes','compactions','timeline'):
        assert field in detail['result']

    # New trials paused for this batch, fine elsewhere.
    blocked=client.post('/trials',json={'name':'清理中试验','dataset_id':dataset.id,'config_id':config.id},headers=tokens['operator'])
    assert blocked.status_code==409
    allowed=client.post('/trials',json={'name':'无关试验','dataset_id':other.id,'config_id':config.id},headers=tokens['operator'])
    assert allowed.status_code==202,allowed.text

    # Fixture trial's artifact is still present until the worker runs.
    assert artifact_path(trial.id).exists()


def test_worker_removes_batch_snapshots_results_and_files(client,tokens,db,fake_queue):
    dataset=make_dataset(db)
    config=make_config(db)
    trial=make_trial(db,dataset,config)
    other=make_dataset(db,'批次乙')
    other_trial=make_trial(db,other,config)
    other_entries=copy.deepcopy(other.entries)
    other_result=copy.deepcopy(other_trial.result)

    response=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01','sensor-02']},headers=tokens['admin'])
    ident=response.json()['id']
    run_purge(ident)

    db.expire_all()
    row=db.get(PurgeRequest,ident)
    assert row.status=='completed' and row.error is None
    assert row.progress['files_deleted']==1
    batch=db.get(Dataset,dataset.id)
    assert {e['key'] for e in batch.entries}=={'sensor-03'}
    saved=db.get(Trial,trial.id)
    assert {e['key'] for e in saved.input_snapshot['entries']}=={'sensor-03'}
    assert saved.result['actual']=={'sensor-03':'22.4'}
    assert saved.result['oracle']=={'sensor-03':'22.4'}
    assert saved.result['operation_count']==5  # metric kept
    assert saved.name==trial.name  # trial identity kept
    assert not artifact_path(trial.id).exists()
    # Unrelated batch untouched, file kept.
    assert db.get(Dataset,other.id).entries==other_entries
    assert db.get(Trial,other_trial.id).result==other_result
    assert db.get(Trial,other_trial.id).input_snapshot['entries']==other_entries
    assert artifact_path(other_trial.id).exists()


def test_completed_purge_status_and_page_visible(client,tokens,db,fake_queue):
    dataset=make_dataset(db);config=make_config(db)
    ident=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-03']},headers=tokens['operator']).json()['id']
    run_purge(ident)
    rows=client.get('/purges',headers=tokens['viewer']).json()
    assert rows[0]['status']=='completed'
    assert rows[0]['progress']['trials_total']==0
    # viewer cannot start or retry
    assert client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01']},headers=tokens['viewer']).status_code==403


def test_retry_interrupted_file_deletion(client,tokens,db,monkeypatch):
    dataset=make_dataset(db);config=make_config(db)
    trial=make_trial(db,dataset,config)
    ident=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01']},headers=tokens['operator']).json()['id']

    calls={'n':0}
    def flaky(trial_id):
        calls['n']+=1
        if calls['n']==1:raise OSError('模拟删文件时中断')
        return artifact_path(trial_id).unlink(missing_ok=False)
    monkeypatch.setattr(purge,'remove_artifact',flaky)
    with pytest.raises(OSError):purge.run(db,ident)

    db.expire_all()
    row=db.get(PurgeRequest,ident)
    assert row.status=='failed' and '删文件' in row.error
    # Data already scrubbed before the interruption; nothing re-exposed.
    assert all(e['key']!='sensor-01' for e in db.get(Trial,trial.id).input_snapshot['entries'])
    assert artifact_path(trial.id).exists()

    # Values remain hidden and the batch stays paused while failed.
    listing=client.get('/datasets',headers=tokens['viewer']).json()
    assert next(x for x in listing if x['id']==dataset.id)['purge_active'] is True

    client.post(f'/purges/{ident}/retry',headers=tokens['operator'])
    run_purge(ident)
    db.expire_all()
    row=db.get(PurgeRequest,ident)
    assert row.status=='completed' and row.attempts==2
    assert not artifact_path(trial.id).exists()


def test_retry_after_metadata_interruption_is_idempotent(client,tokens,db,monkeypatch):
    dataset=make_dataset(db);config=make_config(db)
    trial=make_trial(db,dataset,config)
    ident=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01']},headers=tokens['operator']).json()['id']

    original=purge.scrub_result
    def boom(result,keys):raise RuntimeError('模拟数据库提交前中断')
    monkeypatch.setattr(purge,'scrub_result',boom)
    with pytest.raises(RuntimeError):purge.run(db,ident)
    monkeypatch.setattr(purge,'scrub_result',original)

    db.expire_all()
    assert db.get(PurgeRequest,ident).status=='failed'
    # Rolled back: snapshot still has the key in storage, but API hides it.
    assert any(e['key']=='sensor-01' for e in db.get(Trial,trial.id).input_snapshot['entries'])
    assert all(e['key']!='sensor-01' for e in client.get(f'/trials/{trial.id}',headers=tokens['viewer']).json()['input_snapshot']['entries'])

    client.post(f'/purges/{ident}/retry',headers=tokens['operator'])
    run_purge(ident)
    db.expire_all()
    assert db.get(PurgeRequest,ident).status=='completed'
    assert all(e['key']!='sensor-01' for e in db.get(Trial,trial.id).input_snapshot['entries'])


def test_recover_requeues_pending_after_worker_restart(db,fake_queue):
    dataset=make_dataset(db);config=make_config(db)
    row,_=purge.request(db,dataset.id,['sensor-01'])
    fake_queue.items.clear()  # simulate Redis loss on restart
    recover()
    assert ('lsm:purges',row.id) in fake_queue.items


def test_repeated_same_key_does_not_register_twice(client,tokens,db,fake_queue):
    dataset=make_dataset(db)
    first=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01','sensor-02']},headers=tokens['operator'])
    second=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-02','sensor-01','sensor-01']},headers=tokens['operator'])
    assert first.status_code==202 and second.status_code==202
    assert first.json()['id']==second.json()['id']
    assert db.query(PurgeRequest).count()==1
    assert len([x for x in fake_queue.items if x[0]=='lsm:purges'])==1
    # A later superset is a distinct request.
    third=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01','sensor-03']},headers=tokens['operator'])
    assert third.json()['id']!=first.json()['id']
    assert db.query(PurgeRequest).count()==2


def test_overlapping_purges_do_not_delete_other_keys(client,tokens,db,fake_queue):
    dataset=make_dataset(db);config=make_config(db)
    trial=make_trial(db,dataset,config)
    a=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01']},headers=tokens['operator']).json()['id']
    b=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-02','sensor-03']},headers=tokens['operator']).json()['id']
    # Re-running the first purge must not resurrect or drop the second's keys.
    run_purge(a);run_purge(b);run_purge(a)
    db.expire_all()
    assert db.get(Dataset,dataset.id).entries==[]
    saved=db.get(Trial,trial.id)
    assert saved.result['actual']=={} and saved.result['oracle']=={}
    assert set(saved.result['purged_keys'])=={'sensor-01','sensor-02','sensor-03'}
    assert json.dumps(saved.result,ensure_ascii=False).count('"18.8"')==0


def test_unknown_or_empty_keys_rejected(client,tokens,db):
    dataset=make_dataset(db)
    assert client.post('/purges',json={'dataset_id':dataset.id,'keys':['nope']},headers=tokens['operator']).status_code==422
    assert client.post('/purges',json={'dataset_id':dataset.id,'keys':[]},headers=tokens['operator']).status_code==422
    assert client.post('/purges',json={'dataset_id':9999,'keys':['sensor-01']},headers=tokens['operator']).status_code==404
    assert client.post('/purges',json={'dataset_id':dataset.id,'keys':['x'*65]},headers=tokens['operator']).status_code==422


def test_trial_completing_concurrently_with_purge_stores_no_raw_values(db,fake_queue):
    dataset=make_dataset(db);config=make_config(db)
    from src.models.config import Config
    snapshot={'entries':dataset.entries,'config':{'memtable_limit':2,'l0_trigger':2,'max_levels':3,'final_compaction':True}}
    trial=Trial(name='并发试验',dataset_id=dataset.id,config_id=config.id,status='running',input_snapshot=snapshot)
    db.add(trial);db.commit()
    # Purge is accepted first (barrier), the trial finishes afterwards.
    row,_=purge.request(db,dataset.id,['sensor-01'])
    assert db.get(Trial,trial.id).status=='running'
    run_one(trial.id)
    db.expire_all()
    saved=db.get(Trial,trial.id)
    assert saved.status=='passed'
    assert 'sensor-01' not in saved.result['actual'] and 'sensor-01' not in saved.result['oracle']
    assert any(k!='sensor-01' for k in saved.result['actual'])
    # The purge is still in flight, so no fresh evidence file may appear behind it.
    assert not artifact_path(trial.id).exists()
    run_purge(row.id)
    assert not artifact_path(trial.id).exists()
    db.expire_all()
    assert db.get(PurgeRequest,row.id).status=='completed'


def test_trial_started_after_completed_purge_writes_scrubbed_artifact(db,fake_queue):
    dataset=make_dataset(db);config=make_config(db)
    snapshot={'entries':dataset.entries,'config':{'memtable_limit':2,'l0_trigger':2,'max_levels':3,'final_compaction':True}}
    trial=Trial(name='清理后试验',dataset_id=dataset.id,config_id=config.id,status='queued',input_snapshot=snapshot)
    db.add(trial);db.commit()
    row,_=purge.request(db,dataset.id,['sensor-01'])
    purge.run(db,row.id)
    run_one(trial.id)
    db.expire_all()
    saved=db.get(Trial,trial.id)
    assert saved.status=='passed'
    assert 'sensor-01' not in saved.result['actual']
    text=artifact_path(trial.id).read_text()
    assert '18.8' not in text  # raw value gone; the key name remains as purge metadata
    assert '"purged_keys"' in text and 'sensor-01' in json.loads(text)['purged_keys']


def test_dataset_listing_after_purge_has_no_raw_values(client,tokens,db,fake_queue):
    dataset=make_dataset(db)
    ident=client.post('/purges',json={'dataset_id':dataset.id,'keys':['sensor-01']},headers=tokens['operator']).json()['id']
    run_purge(ident)
    body=json.dumps(client.get('/datasets',headers=tokens['viewer']).json(),ensure_ascii=False)
    assert '18.2' not in body and '18.8' not in body
    assert 'sensor-01' in body  # key name recorded as purged metadata


def test_trial_create_on_fully_purged_batch_is_422_not_crash(client,tokens,db,fake_queue):
    dataset=make_dataset(db,entries=[{'op':'put','key':'solo','value':'1'}])
    config=make_config(db)
    ident=client.post('/purges',json={'dataset_id':dataset.id,'keys':['solo']},headers=tokens['operator']).json()['id']
    run_purge(ident)
    response=client.post('/trials',json={'name':'空批次试验','dataset_id':dataset.id,'config_id':config.id},headers=tokens['operator'])
    assert response.status_code==422
    assert db.query(Trial).count()==0
