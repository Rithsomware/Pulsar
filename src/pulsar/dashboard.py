"""PULSAR Web Dashboard — Real-time GPU process monitoring."""
from typing import Dict, List

STYLES = """
:root {
  --bg: #0a0a0a; --surface: #141414; --border: #222; --text: #d4d4d4;
  --muted: #737373; --accent: #a78bfa; --green: #4ade80;
  --yellow: #fbbf24; --red: #f87171;
  --font: -apple-system, 'Segoe UI', system-ui, sans-serif;
  --mono: 'SF Mono', 'Fira Code', 'Consolas', monospace;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body { font-family: var(--font); background: var(--bg); color: var(--text);
       padding: 24px 32px; max-width: 1400px; margin: 0 auto; line-height: 1.5; }

header { display: flex; align-items: baseline; gap: 12px; margin-bottom: 24px;
         border-bottom: 1px solid var(--border); padding-bottom: 16px; flex-wrap: wrap; }
header h1 { font-size: 1.25rem; font-weight: 600; letter-spacing: -0.02em; }
.tag { background: var(--surface); border: 1px solid var(--border);
       padding: 2px 8px; border-radius: 4px; font-size: 0.75rem;
       font-family: var(--mono); color: var(--accent); }
.meta { color: var(--muted); font-size: 0.8rem; }

.layout { display: grid; grid-template-columns: 1fr 380px; gap: 20px; }
@media (max-width: 900px) { .layout { grid-template-columns: 1fr; } }

section { margin-bottom: 20px; }
section h2 { font-size: 0.8rem; font-weight: 500; text-transform: uppercase;
             letter-spacing: 0.06em; color: var(--muted); margin-bottom: 10px;
             display: flex; align-items: baseline; gap: 10px; }
.inline-stat { font-family: var(--mono); font-size: 0.85rem; }

.stat-grid { display: grid; grid-template-columns: repeat(4, 1fr); gap: 10px; margin-bottom: 12px; }
@media (max-width: 900px) { .stat-grid { grid-template-columns: repeat(2, 1fr); } }
.card { background: var(--surface); border: 1px solid var(--border);
        border-radius: 8px; padding: 14px; }
.card-label { font-size: 0.7rem; color: var(--muted); text-transform: uppercase;
              letter-spacing: 0.04em; margin-bottom: 2px; }
.card-value { font-size: 1.6rem; font-weight: 600; font-family: var(--mono);
              letter-spacing: -0.03em; }
.card-sub { font-size: 0.75rem; color: var(--muted); margin-top: 2px; }

.gauge-track { height: 6px; background: var(--surface); border-radius: 3px;
               border: 1px solid var(--border); overflow: hidden; }
.gauge-bar { height: 100%; border-radius: 3px; transition: width 0.6s ease; }
.gauge-labels { display: flex; justify-content: space-between;
                font-size: 0.7rem; color: var(--muted); margin-top: 3px; }

table { width: 100%; border-collapse: collapse; font-size: 0.8rem; }
th { text-align: left; padding: 7px 10px; color: var(--muted); font-weight: 500;
     font-size: 0.7rem; text-transform: uppercase; letter-spacing: 0.04em;
     border-bottom: 1px solid var(--border); }
td { padding: 7px 10px; border-bottom: 1px solid #1a1a1a; }
tr:hover td { background: rgba(167, 139, 250, 0.03); }
td.empty { color: var(--muted); font-style: italic; }

code { font-family: var(--mono); font-size: 0.75rem; background: #1a1a1a;
       padding: 2px 5px; border-radius: 3px; }

.badge { padding: 2px 7px; border-radius: 4px; font-size: 0.65rem;
         font-weight: 600; font-family: var(--mono); text-transform: uppercase; }
.badge.running { background: rgba(74, 222, 128, 0.12); color: var(--green); }
.badge.queued { background: rgba(251, 191, 36, 0.12); color: var(--yellow); }
.badge.completed { background: rgba(167, 139, 250, 0.12); color: var(--accent); }
.badge.failed, .badge.rejected { background: rgba(248, 113, 113, 0.12); color: var(--red); }
.badge.cancelled { background: rgba(115, 115, 115, 0.12); color: var(--muted); }

.bar-track { background: #1a1a1a; border-radius: 4px; height: 18px; overflow: hidden; }
.bar-fill { background: var(--accent); height: 100%; border-radius: 4px;
            display: flex; align-items: center; justify-content: center;
            font-size: 0.65rem; font-weight: 600; color: #fff; min-width: 22px;
            font-family: var(--mono); transition: width 0.5s ease; }

.panel { background: var(--surface); border: 1px solid var(--border);
         border-radius: 8px; padding: 16px; }
.panel h2 { margin-bottom: 12px; }
.panel label { display: block; font-size: 0.75rem; color: var(--muted);
               margin-bottom: 3px; margin-top: 10px; text-transform: uppercase;
               letter-spacing: 0.03em; }
.panel label:first-of-type { margin-top: 0; }
.panel input, .panel select { width: 100%; padding: 7px 10px; background: var(--bg);
        border: 1px solid var(--border); border-radius: 5px; color: var(--text);
        font-family: var(--font); font-size: 0.85rem; }
.panel input:focus, .panel select:focus { outline: none; border-color: var(--accent); }
.panel button { width: 100%; padding: 9px; margin-top: 14px; background: var(--accent);
        color: #fff; border: none; border-radius: 6px; font-weight: 600;
        font-size: 0.85rem; cursor: pointer; transition: opacity 0.2s; }
.panel button:hover { opacity: 0.85; }
.panel button:active { opacity: 0.7; }
.panel button.secondary { background: transparent; border: 1px solid var(--border);
        color: var(--text); margin-top: 8px; }
.panel button.secondary:hover { border-color: var(--accent); color: var(--accent); }
.panel .msg { font-size: 0.75rem; margin-top: 8px; padding: 6px 8px;
              border-radius: 4px; font-family: var(--mono); }
.panel .msg.ok { background: rgba(74,222,128,0.1); color: var(--green); }
.panel .msg.err { background: rgba(248,113,113,0.1); color: var(--red); }

.gpu-hw { background: var(--surface); border: 1px solid var(--border);
          border-radius: 8px; padding: 12px; margin-bottom: 16px; font-size: 0.8rem; }
.gpu-hw .hw-label { font-size: 0.7rem; color: var(--muted); text-transform: uppercase; }
.gpu-hw .hw-value { font-family: var(--mono); font-size: 0.9rem; font-weight: 600; }

.event-log { max-height: 150px; overflow-y: auto; font-size: 0.75rem;
             font-family: var(--mono); color: var(--muted); }
.event-log div { padding: 2px 0; border-bottom: 1px solid #111; }

.mini-gauge { margin-bottom: 10px; }
.mini-gauge .mg-label { display: flex; justify-content: space-between; font-size: 0.7rem; margin-bottom: 3px; }
.mini-gauge .mg-label span:first-child { color: var(--muted); text-transform: uppercase; letter-spacing: 0.03em; }
.mini-gauge .mg-label span:last-child { font-family: var(--mono); font-weight: 600; }
.mini-gauge .mg-track { height: 5px; background: #1a1a1a; border-radius: 3px; overflow: hidden; }
.mini-gauge .mg-bar { height: 100%; border-radius: 3px; transition: width 0.6s ease; }
.gpu-stat-row { display: flex; justify-content: space-between; padding: 3px 0; font-size: 0.75rem; border-bottom: 1px solid #1a1a1a; }
.gpu-stat-row span:first-child { color: var(--muted); }
.gpu-stat-row span:last-child { font-family: var(--mono); font-weight: 500; }

footer { text-align: center; color: var(--muted); font-size: 0.7rem;
         margin-top: 24px; padding-top: 12px; border-top: 1px solid var(--border); }
footer a { color: var(--accent); text-decoration: none; }
"""

SCRIPT_FILE = "/pulsar_dashboard.js"

def _build_script():
    lines = []
    lines.append("let refreshInterval=null, completedHistory=[], prevJobIds=new Set();")
    lines.append("""
async function api(m,p,b){const o={method:m,headers:{'Content-Type':'application/json'}};if(b)o.body=JSON.stringify(b);return(await fetch(p,o)).json();}
function badge(s){return '<span class="badge '+(s||'').toLowerCase()+'">'+s+'</span>';}
function bar(f){const w=Math.max(2,Math.round(f*100));return '<div class="bar-track"><div class="bar-fill" style="width:'+w+'%">'+Math.round(f*100)+'%</div></div>';}
function gc(p){return p<60?'#4ade80':p<85?'#fbbf24':'#f87171';}
function elapsed(t){if(!t)return'';const s=Math.floor((new Date()-new Date(t))/1000);return s<60?s+'s':Math.floor(s/60)+'m '+(s%60)+'s';}

async function refresh(){try{const d=await api('GET','/api/v1/dashboard');rCluster(d);rNvidia(d);rFairness(d);rJobs(d);rProcs(d);rQueues(d);rDetect(d);}catch(e){console.error(e);}}

function rDetect(d){
  const cur=new Set(Object.keys(d.active_jobs||{}));
  const termLog=d.terminated_log||[];
  const seen=new Set();
  for(const id of prevJobIds)if(!cur.has(id)){
    // Look up termination reason from backend log
    const entry=termLog.slice().reverse().find(e=>e.job_id===id);
    if(entry){
      const label=entry.status==='CANCELLED'?'killed':entry.status.toLowerCase();
      const reason=entry.reason?' ('+entry.reason+')':'';
      if(!seen.has(id)){addLog(label+' '+id+reason);seen.add(id);}
    }else{
      if(!seen.has(id)){addLog('completed '+id+' (process exited)');seen.add(id);}
    }
    completedHistory.unshift({id,time:new Date().toLocaleTimeString(),status:(entry&&entry.status)||'COMPLETED'});rCompleted();
  }
  prevJobIds=cur;
}

function rCluster(d){
  const c=d.cluster||{},m=d.metrics||{},t=c.total_gpus||0,u=c.used_gpus||0,p=t>0?Math.round(u/t*100):0,co=gc(p);
  document.getElementById('stat-gpus').innerHTML='<div class="card-label">GPUs</div><div class="card-value" style="color:'+co+'">'+u+'/'+t+'</div><div class="card-sub">'+(t-u)+' available</div>';
  document.getElementById('stat-running').innerHTML='<div class="card-label">Running</div><div class="card-value">'+(d.active_job_count||0)+'</div><div class="card-sub">'+(d.completed_jobs||0)+' completed</div>';
  document.getElementById('stat-submitted').innerHTML='<div class="card-label">Submitted</div><div class="card-value">'+(m.jobs_submitted_total||0)+'</div><div class="card-sub">'+Math.round((m.admission_rate||0)*100)+'% admitted</div>';
  document.getElementById('stat-rejected').innerHTML='<div class="card-label">Rejected</div><div class="card-value">'+(m.jobs_rejected_total||0)+'</div><div class="card-sub">'+(m.jobs_preempted_total||0)+' preempted</div>';
  const dgpuTotal=m.dgpu_jobs_total||0,igpuTotal=m.igpu_jobs_total||0,fbTotal=m.fallback_total||0;
  document.getElementById('stat-dgpu').innerHTML='<div class="card-label">dGPU Jobs</div><div class="card-value" style="color:#60a5fa">'+dgpuTotal+'</div><div class="card-sub">'+(d.dgpu_name||'NVIDIA')+'</div>';
  document.getElementById('stat-igpu').innerHTML='<div class="card-label">iGPU Jobs</div><div class="card-value" style="color:#4ade80">'+igpuTotal+'</div><div class="card-sub">'+(d.igpu_name||'fallback')+'</div>';
  document.getElementById('stat-fallback').innerHTML='<div class="card-label">Fallbacks</div><div class="card-value" style="color:#fbbf24">'+fbTotal+'</div><div class="card-sub">dGPU &rarr; iGPU</div>';
  document.getElementById('gauge').innerHTML='<div class="gauge-track"><div class="gauge-bar" style="width:'+p+'%;background:'+co+'"></div></div><div class="gauge-labels"><span>'+u+' used</span><span>'+(t-u)+' free</span></div>';
}

function miniGauge(label,val,max,unit,color){
  var pct=max>0?Math.round(val/max*100):0;
  var c=color||gc(pct);
  return '<div class="mini-gauge"><div class="mg-label"><span>'+label+'</span><span style="color:'+c+'">'+val+(unit||'')+(max?' / '+max+(unit||''):'')+'</span></div><div class="mg-track"><div class="mg-bar" style="width:'+pct+'%;background:'+c+'"></div></div></div>';
}
function statRow(l,v){return '<div class="gpu-stat-row"><span>'+l+'</span><span>'+v+'</span></div>';}

function rNvidia(d){
  var nv=d.nvidia_smi||{},el=document.getElementById('nvidia-smi');if(!el)return;
  if(!nv.available){
    el.innerHTML='<div style="color:var(--muted)">nvidia-smi unavailable</div>';
    return;
  }

  var g=nv.gpu||{},gp=g.gpu_util||0,mp=g.mem_util||0,tp=g.temperature_c,pw=g.power_w||0,pl=g.power_limit_w||0;
  var ppct=pl>0?Math.round(pw/pl*100):0;
  var tpDisp=(tp===0||tp==="N/A"||tp===undefined)?"N/A":tp+"°C";
  var tColor=(tpDisp==="N/A")?'var(--muted)':(tp<65?'#4ade80':tp<80?'#fbbf24':'#f87171');
  var cg=g.clock_graphics_mhz||0,cm=g.clock_memory_mhz||0,cmg=g.clock_max_graphics_mhz||0,cmm=g.clock_max_memory_mhz||0;
  var pcieGen=g.pcie_gen||0,pcieWidth=g.pcie_width||0;
  var h='<div style="font-family:var(--mono);font-size:0.8rem;font-weight:600;margin-bottom:8px;color:var(--accent)">'+(g.name||'GPU')+'</div>';
  h+=miniGauge('GPU Util',gp,100,'%');
  h+=miniGauge('VRAM Used',g.mem_used_mb||0,g.mem_total_mb||0,' MB');
  h+=miniGauge('Power',Math.round(pw)||0,Math.round(pl)||150,' W','#818cf8');
  h+='<div style="display:grid;grid-template-columns:1fr 1fr;gap:8px;margin-top:12px">';
  h+=statRow('Temp','<span style="color:'+tColor+'">'+tpDisp+'</span>');
  h+=statRow('Driver',g.driver_version||'N/A');
  h+=statRow('PCIe',pcieGen?'Gen '+pcieGen+' x'+pcieWidth:'N/A');
  h+=statRow('PCI Bus',g.pci_bus||'N/A');
  h+='</div>';
  if(d.has_igpu){
    h+='<div style="margin-top:10px;padding:8px 10px;background:rgba(74,222,128,0.06);border:1px solid rgba(74,222,128,0.15);border-radius:6px">';
    h+='<div style="display:flex;align-items:center;gap:8px;margin-bottom:3px"><span style="display:inline-block;width:6px;height:6px;border-radius:50%;background:var(--green);box-shadow:0 0 6px var(--green)"></span><span style="font-size:0.7rem;text-transform:uppercase;color:var(--green);font-weight:600;letter-spacing:0.04em">iGPU Standby</span></div>';
    h+='<div style="font-family:var(--mono);font-size:0.78rem;font-weight:500">'+(d.igpu_name||'Integrated GPU')+'</div>';
    h+='<div style="font-size:0.7rem;color:var(--muted);margin-top:2px">Fallback device &middot; dGPU &rarr; iGPU</div>';
    h+='</div>';
  }
  el.innerHTML=h;
}

function rFairness(d){
  const f=d.fairness||{},fi=d.fairness_index||1.0,fc=fi>0.85?'#4ade80':fi>0.6?'#fbbf24':'#f87171';
  let r='';for(const[u,x]of Object.entries(f)){
    const drf=(x.drf_dominant_share||0).toFixed(4);
    const drfRes=x.drf_dominant_resource||'-';
    r+='<tr><td><strong>'+u+'</strong></td><td>'+bar(x.usage_share||0)+'</td><td>'+(x.active_gpus||0)+'</td><td>'+(x.cumulative_gpu_usage||0).toFixed(0)+'</td><td>'+(x.weight||1)+'</td><td>'+(x.fairness_priority||0).toFixed(4)+'</td><td style="font-family:var(--mono);font-size:0.75rem">'+drfRes+' '+drf+'</td></tr>';
  }
  if(!r)r='<tr><td colspan="7" class="empty">no usage data yet</td></tr>';
  document.getElementById('fairness-header').innerHTML="Fairness <span class='inline-stat' style='color:"+fc+"'>Jain's Index: "+fi.toFixed(3)+"</span>";
  document.getElementById('fairness-body').innerHTML=r;
}

function rJobs(d){
  const jobs=d.active_jobs||{},pids=d.active_pids||{};let r='';
  for(const[jid,j]of Object.entries(jobs)){const a=elapsed(j.started_at),pid=pids[j.job_id||jid]||'-';
    const lane=(j.assigned_gpu_class||j.preferred_gpu_class||'dgpu').toUpperCase();
    const fb=j.fallback_applied?'<div style="margin-top:2px"><span class="badge" style="background:rgba(251,191,36,0.12);color:var(--yellow);font-size:0.6rem" title="'+(j.fallback_reason||'fallback')+'">FALLBACK</span></div>':'';
    r+='<tr><td><code>'+(j.job_id||jid)+'</code></td><td>'+(j.user||'')+'</td><td>'+(j.gpu_required||0)+'</td><td style="font-family:var(--mono);color:var(--green);font-weight:600">'+pid+'</td><td>'+badge(j.status||'RUNNING')+'</td><td>'+(j.priority||'NORMAL')+'</td><td>'+(j.workload_type||'')+'</td><td style="font-family:var(--mono);font-size:0.75rem;text-align:center">'+lane+fb+'</td><td style="font-family:var(--mono);font-size:0.75rem;color:var(--accent)">'+a+'</td><td><button onclick="completeJob(\\''+(j.job_id||jid)+'\\')" style="background:none;border:1px solid var(--border);color:var(--muted);padding:2px 8px;border-radius:4px;cursor:pointer;font-size:0.7rem">kill</button></td></tr>';}
  if(!r)r='<tr><td colspan="10" class="empty">no running jobs</td></tr>';
  document.getElementById('jobs-body').innerHTML=r;
}

function rProcs(d){
  const nv=d.nvidia_smi||{},el=document.getElementById('gpu-proc-body');if(!el)return;
  const ps=nv.processes||[];let r='';
  for(const p of ps){const jl=p.pulsar_job_id?'<code>'+p.pulsar_job_id+'</code>':'<span style="color:var(--muted)">external</span>';
    r+='<tr><td style="font-family:var(--mono);font-weight:600;color:var(--green)">'+p.pid+'</td><td>'+p.gpu_mem_mb+' MB</td><td>'+p.process_name+'</td><td>'+jl+'</td></tr>';}
  if(!r)r='<tr><td colspan="4" class="empty">no GPU processes</td></tr>';
  el.innerHTML=r;
}

function rQueues(d){
  const q=d.queues||{};let r='';
  for(const[u,x]of Object.entries(q))r+='<tr><td><strong>'+u+'</strong></td><td>'+(x.depth||0)+'</td><td>'+(x.total_gpus_queued||0)+'</td><td><span style="color:#60a5fa">'+(x.dgpu_queued||0)+'</span></td><td><span style="color:#4ade80">'+(x.igpu_queued||0)+'</span></td></tr>';
  if(!r)r='<tr><td colspan="5" class="empty">queues empty</td></tr>';
  document.getElementById('queues-body').innerHTML=r;
}

function rCompleted(){
  const el=document.getElementById('completed-body');if(!el)return;let r='';
  for(const c of completedHistory){
    const st=(c.status||'COMPLETED');
    const sc=st==='COMPLETED'?'var(--accent)':st==='CANCELLED'?'var(--red)':'var(--yellow)';
    r+='<tr><td><code>'+c.id+'</code></td><td style="color:'+sc+'">'+st.toLowerCase()+'</td><td>'+c.time+'</td></tr>';
  }
  if(!r)r='<tr><td colspan="3" class="empty">none yet</td></tr>';el.innerHTML=r;
}

async function submitJob(){
  const msg=document.getElementById('submit-msg'),user=document.getElementById('f-user').value.trim(),gpus=parseInt(document.getElementById('f-gpus').value),type=document.getElementById('f-type').value,prio=document.getElementById('f-priority').value,fw=document.getElementById('f-framework').value,dur=parseFloat(document.getElementById('f-duration').value)||1;
  if(!user){msg.className='msg err';msg.textContent='team name required';return;}
  if(!gpus||gpus<1){msg.className='msg err';msg.textContent='GPU count required';return;}
  try{const r=await api('POST','/api/v1/jobs',{user:user,gpu_required:gpus,workload_type:type,priority:prio,framework:fw,estimated_duration_minutes:dur});msg.className='msg ok';msg.textContent=r.job?r.job.job_id:'submitted';addLog('submit '+(r.job?r.job.job_id:'?')+' -> '+user+' ('+gpus+' GPU)');refresh();}catch(e){msg.className='msg err';msg.textContent='failed: '+e.message;}
}

async function addTeam(){
  const msg=document.getElementById('submit-msg'),user=document.getElementById('f-user').value.trim(),maxGpus=parseInt(document.getElementById('f-gpus').value)||4,weight=parseFloat(document.getElementById('f-weight').value)||1.0;
  if(!user){msg.className='msg err';msg.textContent='team name required';return;}
  try{await api('PUT','/api/v1/quotas/'+user,{max_gpus:maxGpus,max_jobs:10,weight:weight});msg.className='msg ok';msg.textContent='team '+user+' created (quota: '+maxGpus+' GPUs)';addLog('team '+user+' added');refresh();}catch(e){msg.className='msg err';msg.textContent='failed';}
}

async function completeJob(id){try{await api('POST','/api/v1/jobs/'+id+'/complete');refresh();}catch(e){console.error(e);}}

function addLog(t){const l=document.getElementById('event-log'),d=document.createElement('div');d.textContent=new Date().toLocaleTimeString()+'  '+t;l.prepend(d);while(l.children.length>30)l.removeChild(l.lastChild);}

async function detectGPU(){try{const d=await api('GET','/api/v1/gpu'),el=document.getElementById('gpu-hw');if(d.detected&&d.devices&&d.devices.length>0){const dev=d.devices[0];const mem=(dev.memory_mb>0)?dev.memory_mb+' MB':'VRAM unknown';el.innerHTML='<div class="hw-label">detected gpu</div><div class="hw-value">'+dev.name+'</div><div style="color:var(--muted);font-size:0.75rem">'+mem+'</div>';}else{el.innerHTML='<div class="hw-label">gpu</div><div style="color:var(--muted)">none detected</div>';}}catch(e){}}

detectGPU();refresh();refreshInterval=setInterval(refresh,2000);
""")
    return "\n".join(lines)

SCRIPT = _build_script()

def render_dashboard(data: dict) -> str:
    policy = data.get("scheduling_policy", "fair_share")
    mode = data.get("execution_mode", "standalone")
    preemption_tag = '<span class="tag">preemption</span>' if data.get("preemption_enabled") else ""

    before_script = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>PULSAR</title>
<style>{STYLES}</style>
</head>
<body>

<header>
  <h1>PULSAR</h1>
  <span class="tag">{policy}</span>
  <span class="tag">{mode}</span>
  {preemption_tag}
  <span class="meta" style="margin-left:auto">live</span>
</header>

<div class="layout">
<div class="main">

<section>
  <h2>Cluster</h2>
  <div class="stat-grid">
    <div class="card" id="stat-gpus"></div>
    <div class="card" id="stat-running"></div>
    <div class="card" id="stat-submitted"></div>
    <div class="card" id="stat-dgpu"></div>
    <div class="card" id="stat-igpu"></div>
    <div class="card" id="stat-fallback"></div>
    <div class="card" id="stat-rejected"></div>
  </div>
  <div id="gauge"></div>
</section>

<section>
  <h2 id="fairness-header">Fairness</h2>
  <table>
    <tr><th>Team</th><th>Share</th><th>Active</th><th>Cumulative</th><th>Weight</th><th>Priority</th><th>DRF Dominant</th></tr>
    <tbody id="fairness-body"></tbody>
  </table>
</section>

<section>
  <h2>Running Jobs</h2>
  <table>
    <tr><th>Job</th><th>Team</th><th>GPUs</th><th>PID</th><th>Status</th><th>Priority</th><th>Type</th><th>Lane</th><th>Elapsed</th><th></th></tr>
    <tbody id="jobs-body"></tbody>
  </table>
</section>

<section>
  <h2>GPU Processes (nvidia-smi)</h2>
  <table>
    <tr><th>PID</th><th>VRAM</th><th>Process</th><th>PULSAR Job</th></tr>
    <tbody id="gpu-proc-body"></tbody>
  </table>
</section>

<section>
  <h2>Queue</h2>
  <table>
    <tr><th>Team</th><th>Queued</th><th>GPUs Pending</th><th>dGPU (Req)</th><th>iGPU (Req)</th></tr>
    <tbody id="queues-body"></tbody>
  </table>
</section>

<section>
  <h2>Completed</h2>
  <table>
    <tr><th>Job</th><th>Status</th><th>Time</th></tr>
    <tbody id="completed-body"><tr><td colspan="3" class="empty">none yet</td></tr></tbody>
  </table>
</section>

</div>

<div class="sidebar">
  <div id="gpu-hw" class="gpu-hw">
    <div class="hw-label">gpu</div>
    <div style="color:var(--muted)">detecting...</div>
  </div>

  <div class="gpu-hw">
    <div class="hw-label">nvidia-smi (live)</div>
    <div id="nvidia-smi" style="margin-top:4px">loading...</div>
  </div>

  <div class="panel">
    <h2 style="font-size:0.8rem;text-transform:uppercase;letter-spacing:0.06em;color:var(--muted)">Submit Job</h2>

    <label>Team</label>
    <input id="f-user" type="text" placeholder="e.g. ml-research" value="">

    <label>GPUs</label>
    <input id="f-gpus" type="number" min="1" max="256" value="1">

    <label>Duration (minutes)</label>
    <input id="f-duration" type="number" min="0.5" max="1440" step="0.5" value="2">

    <label>Type</label>
    <select id="f-type">
      <option>Training</option>
      <option>Inference</option>
      <option>FineTuning</option>
      <option>DataPreprocessing</option>
    </select>

    <label>Priority</label>
    <select id="f-priority">
      <option>NORMAL</option>
      <option>LOW</option>
      <option>HIGH</option>
      <option>CRITICAL</option>
    </select>

    <label>Framework</label>
    <select id="f-framework">
      <option>PyTorch</option>
      <option>TensorFlow</option>
      <option>JAX</option>
      <option>Triton</option>
    </select>

    <label>Weight (for new team)</label>
    <input id="f-weight" type="number" min="0.1" max="10" step="0.1" value="1.0">

    <button onclick="submitJob()">Submit Job</button>
    <button class="secondary" onclick="addTeam()">Create Team</button>

    <div id="submit-msg"></div>
  </div>

  <div class="panel" style="margin-top:12px">
    <h2 style="font-size:0.8rem;text-transform:uppercase;letter-spacing:0.06em;color:var(--muted)">Activity</h2>
    <div id="event-log" class="event-log">
      <div>ready</div>
    </div>
  </div>
</div>
</div>

<footer>
  v3.0 &middot;
  <a href="/api/v1/metrics">metrics</a> &middot;
  <a href="/api/v1/gpu">gpu info</a> &middot;
  <a href="/api/v1/gpu/nvidia-smi">nvidia-smi</a> &middot;
  <a href="/api/v1/gpu/processes">processes</a> &middot;
  <a href="/docs">api docs</a> &middot;
  <a href="/healthz">health</a>
</footer>

<script>"""

    after_script = """</script>
</body>
</html>"""

    return before_script + SCRIPT + after_script
