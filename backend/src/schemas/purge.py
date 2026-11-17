from pydantic import BaseModel,Field
class PurgeInput(BaseModel):
    dataset_id:int=Field(gt=0)
    keys:list[str]=Field(min_length=1,max_length=200)
