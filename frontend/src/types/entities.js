/** @typedef {{id:number,name:string,description:string,entries:Array}} Dataset */
/** @typedef {{id:number,name:string,memtable_limit:number,l0_trigger:number,max_levels:number,final_compaction:boolean}} Config */
/** @typedef {{id:number,name:string,dataset_id:number,config_id:number,status:string,result:Object}} Trial */
export const entityNames={dataset:'键值批次',config:'存储配置',trial:'合并试验'};
/** @typedef {{id:number,dataset_id:number,keys:string[],status:string,attempts:number,progress:Object,error:string|null}} PurgeRequest */
