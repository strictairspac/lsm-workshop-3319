import{list}from'../api/purge.js';export const purges={items:[],async load(){this.items=await list();return this.items}};
