from fastapi import APIRouter,Depends
from ..config.database import get_db
from ..schemas.purge import PurgeInput
from ..services.auth import current_user,writer
from ..services import purge
router=APIRouter(prefix='/purges')
@router.get('')
def get(dataset_id:int|None=None,db=Depends(get_db),user=Depends(current_user)):return purge.listing(db,dataset_id)
@router.post('',status_code=202)
def add(data:PurgeInput,db=Depends(get_db),user=Depends(writer)):
    row,created=purge.request(db,data.dataset_id,data.keys)
    return purge.serialize(row)
@router.get('/{ident}')
def one(ident:int,db=Depends(get_db),user=Depends(current_user)):return purge.detail(db,ident)
@router.post('/{ident}/retry',status_code=202)
def retry(ident:int,db=Depends(get_db),user=Depends(writer)):return purge.retry(db,ident)
