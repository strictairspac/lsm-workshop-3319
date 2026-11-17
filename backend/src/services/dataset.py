from sqlalchemy import select
from fastapi import HTTPException
from ..models.dataset import Dataset
from . import purge

def serialize(x,purged=None,state=None):
    purged=set(purged or ())
    entries=[e for e in (x.entries or []) if e.get('key') not in purged]
    state=state or {}
    return {'id':x.id,'name':x.name,'description':x.description,'entries':entries,
            'operation_count':len(entries),'distinct_keys':len({e['key'] for e in entries}),
            'purged_keys':sorted(purged),'purge_active':bool(state.get('purge_active')),
            'purge_status':state.get('purge_status'),'purge_id':state.get('purge_id')}

def listing(db):
    states=purge.status_by_dataset(db)
    return [serialize(x,states.get(x.id,{}).get('purged_keys'),states.get(x.id)) for x in db.scalars(select(Dataset).order_by(Dataset.id))]

def create(db,data):
    if db.scalar(select(Dataset).where(Dataset.name==data.name)):raise HTTPException(409,'写入批次名称已存在')
    x=Dataset(**data.model_dump());db.add(x);db.commit();return serialize(x)
