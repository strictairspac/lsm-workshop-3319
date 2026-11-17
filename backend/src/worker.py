import time,traceback
from pathlib import Path
from sqlalchemy import select
from .config.database import Session
from .models.trial import Trial
from .models.dataset import Dataset
from .models.purge import PurgeRequest
from .services.queue import queue
from .services.lsm_engine import simulate
from .services import purge
from .services.artifacts import write_artifact
heartbeat=Path('/artifacts/worker.heartbeat')

def run_one(ident):
    with Session() as db:
        x=db.get(Trial,ident)
        if not x or x.result is not None:return
        x.status='running';db.commit()
        try:result=simulate(x.input_snapshot['entries'],x.input_snapshot['config']);state='passed'
        except Exception as exc:result={'error':str(exc)[:500]};state='failed'
        # Serialize with purge acceptance: either the purge is visible here and the
        # result is scrubbed, or it is accepted after our commit and its own run
        # rewrites the trial and deletes the artifact.
        db.execute(select(Dataset).where(Dataset.id==x.dataset_id).with_for_update()).scalar_one()
        purged=purge.keys_for(db,x.dataset_id)
        if purged:result=purge.scrub_result(result,purged)
        # While a purge is still in flight its file sweep may already have run;
        # do not create a fresh evidence file behind its back.
        if not purge.paused(db,x.dataset_id):
            try:write_artifact(ident,result)
            except Exception:traceback.print_exc()
        x.result=result;x.status=state;db.commit()

def run_purge(ident):
    with Session() as db:
        try:purge.run(db,ident)
        except Exception:traceback.print_exc()

def recover():
    with Session() as db:
        for ident in list(db.scalars(select(PurgeRequest.id).where(PurgeRequest.status.in_(('pending','running'))))):
            try:queue.rpush('lsm:purges',ident)
            except Exception:traceback.print_exc()
        for ident in list(db.scalars(select(Trial.id).where(Trial.status.in_(['queued','running'])))):
            try:queue.rpush('lsm:jobs',ident)
            except Exception:traceback.print_exc()

def main():
    while True:
        try:
            heartbeat.parent.mkdir(parents=True,exist_ok=True);heartbeat.write_text(str(time.time()))
            item=queue.blpop(['lsm:purges','lsm:jobs'],timeout=2)
            if item:
                name,raw=item;ident=int(raw)
                if name in (b'lsm:purges','lsm:purges'):run_purge(ident)
                else:run_one(ident)
            else:recover()
        except Exception:traceback.print_exc();time.sleep(2)

if __name__=='__main__':main()
