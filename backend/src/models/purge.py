from datetime import datetime
from sqlalchemy.orm import Mapped,mapped_column
from sqlalchemy import String,JSON,ForeignKey,DateTime,Integer,func
from ..config.database import Base
class PurgeRequest(Base):
    __tablename__='purge_requests'
    id:Mapped[int]=mapped_column(primary_key=True)
    dataset_id:Mapped[int]=mapped_column(ForeignKey('datasets.id'),index=True)
    keys:Mapped[list]=mapped_column(JSON)
    fingerprint:Mapped[str]=mapped_column(String(64),unique=True)
    status:Mapped[str]=mapped_column(String(20),default='pending',index=True)
    attempts:Mapped[int]=mapped_column(Integer,default=0)
    progress:Mapped[dict]=mapped_column(JSON,default=dict)
    error:Mapped[str|None]=mapped_column(String(1000),nullable=True)
    created_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now())
    updated_at:Mapped[datetime]=mapped_column(DateTime,server_default=func.now(),onupdate=func.now())
    completed_at:Mapped[datetime|None]=mapped_column(DateTime,nullable=True)
