from sqlalchemy import select
from fastapi import HTTPException
from ..models.trial import Trial
from ..models.dataset import Dataset
from ..models.config import Config
from .config import serialize as config_dict
from .queue import queue
from . import purge

def serialize(x,purged=None):
    keys=set(purged or ())
    snapshot=purge.scrub_snapshot(x.input_snapshot,keys) if keys else x.input_snapshot
    result=purge.scrub_result(x.result,keys) if keys else x.result
    return {'id':x.id,'name':x.name,'dataset_id':x.dataset_id,'config_id':x.config_id,'status':x.status,
            'created_at':str(x.created_at),'input_snapshot':snapshot,'result':result,
            'purged_keys':sorted(keys)}

def listing(db):
    cache={}
    return [serialize(x,cache.setdefault(x.dataset_id,purge.keys_for(db,x.dataset_id))) for x in db.scalars(select(Trial).order_by(Trial.id.desc()))]

def detail(db,ident):
    x=db.get(Trial,ident)
    if not x:raise HTTPException(404,'试验不存在')
    return serialize(x,purge.keys_for(db,x.dataset_id))

def create(db,data):
    dataset=db.execute(select(Dataset).where(Dataset.id==data.dataset_id).with_for_update()).scalar_one_or_none()
    config=db.get(Config,data.config_id)
    if not dataset or not config:raise HTTPException(404,'写入批次或存储配置不存在')
    if purge.paused(db,dataset.id):raise HTTPException(409,'该批次正在永久清理，暂停发起新试验')
    if not dataset.entries:raise HTTPException(422,'该批次没有可用于试验的操作')
    if db.scalar(select(Trial).where(Trial.name==data.name)):raise HTTPException(409,'试验名称已存在')
    if len(list(db.scalars(select(Trial).where(Trial.status.in_(['queued','running'])))))>=20:raise HTTPException(429,'待运行试验上限20个')
    x=Trial(**data.model_dump(),input_snapshot={'entries':dataset.entries,'config':config_dict(config)})
    db.add(x);db.commit();queue.rpush('lsm:jobs',x.id);return serialize(x,purge.keys_for(db,x.dataset_id))
