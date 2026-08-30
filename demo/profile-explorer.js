/* The same canvas nodes feed an illustrative table at VENUE_PROFILE lookup. */
(function(root){
  'use strict';
  const M=typeof module!=='undefined'&&module.exports?require('./feature-model.js'):root.TurboFeatureModel;
  const esc=s=>String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const colors=['#6a1b9a','#1565c0','#00796b','#a85b00','#ad1457'];
  function tableHTML(s){
    const venue=s.venue;
    if(!venue)return '<div class="empty-result">Select a venue in the graph to inspect its intermediate rows.</div>';
    const profiles=Array.from({length:5},(_,i)=>M.profile(i,venue.id));
    const rowhead=(p,i)=>`<th scope="row"><button type="button" data-profile-record="${i}" aria-label="Inspect ${esc(p.section)} venue profile" style="--profile-color:${colors[i]}">${esc(p.section)}</button></th>`;
    return `<div class="intermediate${s.packed?' is-packed':''}" data-venue="${venue.id}">
      <div class="intermediate-head"><div><strong>After VENUE_PROFILE lookup <span>· before RETURN</span></strong><small>${esc(venue.label)} → 5 venue profile sections · illustrative values</small></div>
        <div class="intermediate-switch" role="group" aria-label="Intermediate format"><button type="button" data-profile-format="column" aria-pressed="${!s.packed}">Columnar</button><button type="button" data-profile-format="row" aria-pressed="${s.packed}">SSRF</button></div></div>
      <div class="intermediate-tables">
        <div class="intermediate-wide" aria-hidden="${s.packed}"><table aria-label="Columnar intermediate with NULL padding"><thead><tr><th>Section</th>${[0,1,2,3,4].map(i=>`<th>Populated property ${i+1}</th>`).join('')}<th>Empty union slots</th></tr></thead><tbody>${profiles.map((p,i)=>`<tr data-record-row="${i}">${rowhead(p,i)}${p.entries.map(e=>`<td class="ivalue" style="--profile-color:${colors[i]}"><small>${esc(e.key)}</small>${esc(e.value)}</td>`).join('')}<td class="inull imore">NULL × 195</td></tr>`).join('')}</tbody></table></div>
        <div class="intermediate-packed" aria-hidden="${!s.packed}"><table aria-label="SSRF intermediate without NULL padding"><thead><tr><th>Section</th>${[0,1,2,3,4].map(i=>`<th>Property ${i+1}</th>`).join('')}</tr></thead><tbody>${profiles.map((p,i)=>`<tr data-record-row="${i}">${rowhead(p,i)}${p.entries.map(e=>`<td class="ivalue" style="--profile-color:${colors[i]}"><small>${esc(e.key)}</small>${esc(e.value)}</td>`).join('')}</tr>`).join('')}</tbody></table></div>
      </div>
      <div class="intermediate-reason" aria-live="polite"><span class="wide-reason"><b>5 properties + 195 NULL slots per section.</b> Showing 25 of the 200-column union.</span><span class="packed-reason"><b>Same properties, no NULL padding.</b> Each section carries only its five key-value pairs.</span></div>
    </div>`;
  }
  function updateTable(container,s){
    let box=container.querySelector('.intermediate');
    if(!box||+box.dataset.venue!==s.venue?.id){container.innerHTML=tableHTML(s);box=container.querySelector('.intermediate');}
    if(!box)return;
    box.classList.toggle('is-packed',s.packed);
    box.querySelectorAll('[data-profile-format]').forEach(b=>b.setAttribute('aria-pressed',String((b.dataset.profileFormat==='row')===s.packed)));
    box.querySelector('.intermediate-wide').setAttribute('aria-hidden',String(s.packed));
    box.querySelector('.intermediate-packed').setAttribute('aria-hidden',String(!s.packed));
    box.querySelectorAll('[data-record-row]').forEach(row=>row.classList.toggle('selected',+row.dataset.recordRow===s.record));
  }
  function mount(host,{anchors,guided=false,enabled=false,onDone=()=>{},onClose=()=>{},onChange=()=>{},onInspect=()=>{}}){
    const abort=new AbortController(),signal=abort.signal,stage=host.parentElement;
    const reduced=matchMedia('(prefers-reduced-motion: reduce)').matches;
    let selected=anchors()[0]?.id??null,record=0,packed=enabled,timers=[],disposed=false;
    host.classList.add('profile-explorer');host.dataset.feature='ssrf';
    host.innerHTML=`<svg class="profile-tether" aria-hidden="true"></svg><div class="profile-anchors"></div>
      <div class="attached-profiles" role="group" aria-label="VENUE_PROFILE sections of the selected venue"></div>
      <div class="profile-bar"><strong>SSRF</strong><span>VENUE_PROFILE lookup → intermediate table below</span><button type="button" data-action="guide" aria-label="Replay venue profile walkthrough"><span class="ms">replay</span></button><button type="button" data-action="close" aria-label="Return to graph"><span class="ms">close</span></button></div>`;
    const pins=host.querySelector('.profile-anchors'),records=host.querySelector('.attached-profiles'),tethers=host.querySelector('.profile-tether');
    function cancel(){timers.forEach(clearTimeout);timers=[];guided=false;}
    function state(){return {venue:anchors().find(a=>a.id===selected)||null,record,packed};}
    function layout(){
      if(disposed)return;
      const all=anchors(),w=host.clientWidth,h=host.clientHeight,active=all.find(a=>a.id===selected);
      pins.innerHTML=all.map(a=>`<button type="button" class="profile-pin${a.id===selected?' selected':''}" data-venue="${a.id}" aria-label="Inspect ${esc(a.label)} profiles" style="left:${a.x}px;top:${a.y}px"><i></i><span>${esc(a.label)}</span></button>`).join('');
      records.innerHTML=Array.from({length:5},(_,i)=>{const p=M.profile(i,selected);return `<button type="button" data-record="${i}" aria-label="Inspect ${esc(p.section)} venue profile" aria-pressed="${i===record}" style="--profile-color:${colors[i]}"><i>${i+1}</i><span>${esc(p.section)}</span></button>`;}).join('');
      records.style.bottom='53px';
      if(active){
        const cr=host.getBoundingClientRect();
        tethers.setAttribute('viewBox',`0 0 ${w} ${h}`);
        tethers.innerHTML=[...records.children].map((el,i)=>{const r=el.getBoundingClientRect(),x=r.left-cr.left+r.width/2,y=r.top-cr.top+10;
          return `<path d="M${active.x} ${active.y} Q${active.x} ${y-18} ${x} ${y}" stroke="${colors[i]}" class="${i===record?'selected':''}"/>`;
        }).join('');
      }
    }
    function notify(){onChange(state());}
    function format(value){cancel();packed=value;notify();}
    function chooseRecord(i){cancel();record=i;layout();notify();}
    function guide(auto){
      cancel();guided=auto;packed=false;record=0;layout();notify();
      if(reduced)return;
      const later=(fn,ms)=>timers.push(setTimeout(()=>{if(!disposed)fn();},ms));
      later(()=>{packed=true;notify();},3000);
      if(auto)later(()=>{cancel();onDone();},5600);
    }
    host.addEventListener('click',e=>{
      const b=e.target.closest('button');if(!b)return;cancel();
      if(b.dataset.action==='close'){onClose();return;}
      if(b.dataset.action==='guide'){onInspect();guide(false);return;}
      if(b.dataset.venue!=null){onInspect();selected=+b.dataset.venue;record=0;layout();notify();return;}
      if(b.dataset.record!=null){onInspect();chooseRecord(+b.dataset.record);}
    },{signal});
    const resize=new ResizeObserver(()=>requestAnimationFrame(layout));resize.observe(host);
    layout();notify();if(guided)guide(true);
    const dispose=()=>{disposed=true;cancel();abort.abort();resize.disconnect();host.classList.remove('profile-explorer');host.innerHTML='';};
    dispose.setFormat=format;dispose.selectRecord=chooseRecord;
    return dispose;
  }
  const api={mount,tableHTML,updateTable};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;else root.TurboProfileExplorer=api;
})(typeof window!=='undefined'?window:this);
