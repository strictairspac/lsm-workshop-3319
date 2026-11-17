import os, sys, tempfile
from pathlib import Path

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))

TMP=Path(tempfile.mkdtemp(prefix='lsm-tests-'))
os.environ['DATABASE_URL']='sqlite:///'+str(TMP/'test.db')
os.environ['REDIS_URL']='redis://127.0.0.1:6399/0'
os.environ['JWT_SECRET']='test-secret'
os.environ['SEED_DEMO']='false'
os.environ['ARTIFACTS_DIR']=str(TMP/'artifacts')

import pytest
from sqlalchemy import event
from fastapi.testclient import TestClient
from src.config.database import Base,engine,Session
from src.models.user import User
from src.models.dataset import Dataset
from src.models.config import Config
from src.models.trial import Trial
from src.models.purge import PurgeRequest
from src.models.audit import Audit
from src.utils.password import hash_password

@event.listens_for(engine,'connect')
def _sqlite_pragmas(conn,record):
    cur=conn.cursor();cur.execute('PRAGMA journal_mode=WAL');cur.execute('PRAGMA busy_timeout=30000');cur.close()

class FakeQueue:
    def __init__(self):self.items=[]
    def rpush(self,name,*values):self.items.extend((name,int(v)) for v in values);return len(self.items)
    def blpop(self,names,timeout=0):
        for i,(name,value) in enumerate(self.items):
            if name in names:return name.encode(),str(value).encode()
        return None
    def lrem(self,name,count,value):self.items=[x for x in self.items if not (x[0]==name and x[1]==int(value))]
    def ping(self):return True

@pytest.fixture()
def fake_queue(monkeypatch):
    import src.services.queue as qmod
    import src.services.trial as trial_service
    import src.services.purge as purge_service
    import src.worker as worker
    fake=FakeQueue()
    monkeypatch.setattr(qmod,'queue',fake)
    monkeypatch.setattr(trial_service,'queue',fake)
    monkeypatch.setattr(purge_service,'queue',fake)
    monkeypatch.setattr(worker,'queue',fake)
    return fake

@pytest.fixture()
def db():
    Base.metadata.drop_all(engine);Base.metadata.create_all(engine)
    # Trial ids restart per test, so stale evidence files from a previous test
    # must not be mistaken for this one's output.
    for stale in (TMP/'artifacts').glob('lsm-trial-*.json'):stale.unlink()
    with Session() as session:
        for role in ('admin','operator','viewer'):
            session.add(User(email=role+'@beiling.test',role=role,password_hash=hash_password('EdgeStore!17')))
        session.commit()
        yield session

@pytest.fixture()
def client(db,fake_queue):
    from src.main import app
    return TestClient(app)

def token(client,role='operator'):
    response=client.post('/auth/login',json={'email':role+'@beiling.test','password':'EdgeStore!17'})
    assert response.status_code==200,response.text
    return {'Authorization':'Bearer '+response.json()['token']}

@pytest.fixture()
def tokens(client):return {role:token(client,role) for role in ('admin','operator','viewer')}

def make_dataset(db,name='批次甲',entries=None):
    entries=entries or [
        {'op':'put','key':'sensor-01','value':'18.2'},
        {'op':'put','key':'sensor-02','value':'19.1'},
        {'op':'put','key':'sensor-01','value':'18.8'},
        {'op':'delete','key':'sensor-02'},
        {'op':'put','key':'sensor-03','value':'22.4'},
    ]
    row=Dataset(name=name,description='测试批次',entries=entries)
    db.add(row);db.commit();return row

def make_config(db,name='小内存-完整合并'):
    row=Config(name=name,memtable_limit=2,l0_trigger=2,max_levels=3,final_compaction=True)
    db.add(row);db.commit();return row

def make_trial(db,dataset,config,status='passed'):
    import json
    from src.services.lsm_engine import simulate
    from src.services.config import serialize as config_dict
    from src.services.artifacts import write_artifact
    snapshot={'entries':dataset.entries,'config':config_dict(config)}
    result=simulate(snapshot['entries'],snapshot['config']) if status in ('passed','failed') else None
    row=Trial(name=f'试验-{dataset.id}-{status}',dataset_id=dataset.id,config_id=config.id,
              status=status,input_snapshot=snapshot,result=result)
    db.add(row);db.commit()
    if result is not None:write_artifact(row.id,result)
    return row
