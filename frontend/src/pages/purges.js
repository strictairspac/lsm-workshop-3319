import{datasets}from'../stores/dataset.js';import{purges}from'../stores/purge.js';import{create,retry}from'../api/purge.js';import{header}from'../components/common/header.js';import{table}from'../components/common/table.js';import{esc}from'../utils/html.js';import{submit,notify}from'../hooks/feedback.js';import{session}from'../stores/session.js';import{PURGE_STATUSES}from'../constants/enums.js';
function purgeBadge(s){return '<span class="badge '+esc(s)+'">'+esc(PURGE_STATUSES[s]||s)+'</span>'}
function progress(p){if(!p||p.trials_total==null)return '等待 worker 领取';return `${p.trials_processed??0}/${p.trials_total} 试验已处理 · ${p.files_deleted??0} 个落盘文件已删除`}
function keyChoices(d){const live=[...new Set((d.entries||[]).map(e=>e.key))].sort();const gone=new Set(d.purged_keys||[]);return live.map(k=>`<label class="checkline"><input type="checkbox" name="key" value="${esc(k)}"> <span class="mono">${esc(k)}</span></label>`).join('')+(gone.size?`<p class="muted">已永久清理：${[...gone].sort().map(esc).join('、')}</p>`:'')||'<p class="muted">该批次没有可清理的键</p>'}
export async function purgesPage(el){const ds=await datasets.load();const ps=await purges.load();const writable=session.get().role!=='viewer';el.innerHTML=header('05','永久清掉原值，留下证据骨架','按批次选择键，永久删除原值：批次操作、关联试验的输入快照与结果，以及落盘文件。受理后立即屏蔽查看与下载、暂停该批次新试验；试验标识与不含原值的历史指标保留。')+`<div class="split"><section class="panel"><h2>清理任务</h2><div class="actions" style="margin:0 0 20px"><button id="refresh">刷新进度</button><span class="muted">${ps.length} 个任务</span></div>${table(['任务','批次 / 键','状态与进度','失败原因 / 操作'],ps.map(x=>{const d=ds.find(y=>y.id===x.dataset_id);const ops=x.status!=='completed'&&writable?`<button data-retry="${x.id}">重试</button>`:'';return [`#${x.id}<br><small>${esc(x.created_at)}<br>第 ${x.attempts||0} 次执行</small>`,`#${x.dataset_id} ${esc(d?.name||'已移除')}<br><small>${(x.keys||[]).map(esc).join('、')}</small>`,purgeBadge(x.status)+'<br><small>'+esc(progress(x.progress))+'</small>',(x.error?'<span class="error">'+esc(x.error)+'</span><br>':'')+ops||'<span class="muted">—</span>']}))}</section><aside class="panel"><h2>${writable?'发起永久清理':'清理只读'}</h2>${writable?`<form id="purge-form"><label>键值写入批次</label><select name="dataset_id">${ds.map(x=>`<option value="${x.id}">${esc(x.name)} / ${x.operation_count}条</option>`).join('')}</select><label>选择要永久清理的键</label><div id="keybox"></div><label>也可粘贴键名（逗号或换行分隔）</label><input name="extra" placeholder="sensor-01, sensor-02"><div class="actions"><button class="primary" type="submit">受理永久清理 →</button></div></form><p class="helper">受理即屏蔽该批次相关原值的查看与下载并暂停新试验。只清理勾选的键；同键重复发起不会重复登记，其他批次与试验不受影响。</p>`:'<p class="helper">观察员可以查看清理进度与失败原因，但只有管理员或操作员可以发起或重试。</p>'}</aside></div>`;
const form=el.querySelector('#purge-form');
if(form){
  const select=form.querySelector('select');const box=el.querySelector('#keybox');
  const loadKeys=()=>{const d=ds.find(x=>x.id===Number(select.value));box.innerHTML=d?keyChoices(d):'<p class="muted">没有可用批次</p>'};
  loadKeys();select.onchange=loadKeys;
  form.onsubmit=e=>{e.preventDefault();submit(form,async()=>{
    const picked=[...form.querySelectorAll('input[name=key]:checked')].map(x=>x.value);
    const typed=form.extra.value.split(/[,，\n]/).map(s=>s.trim()).filter(Boolean);
    const keys=[...new Set([...picked,...typed])];
    if(!keys.length)throw Error('请至少选择一个要清理的键');
    await create({dataset_id:Number(select.value),keys});notify('清理已受理，相关原值即刻屏蔽');await purgesPage(el)
  })}
}
el.querySelector('#refresh').onclick=()=>purgesPage(el);
el.querySelectorAll('[data-retry]').forEach(b=>b.onclick=()=>submit(b,async()=>{await retry(b.dataset.retry);notify('已重新排入清理队列');await purgesPage(el)}));
}
