/* In-place SI/GEM controls. The original canvas remains the explanation surface. */
(function(root){
  'use strict';
  const labels=['{title}','{noise}','{junk0}','{junk1}','{bot0}','{bot1}','{bot2}','{bot3}','{bot4}','{bot5}'];
  const button=(value,label,on)=>`<button type="button" data-value="${value}" aria-pressed="${on===value}">${label}</button>`;
  function mount(host,{mode,guided=false,onDone=()=>{},onClose=()=>{},onGraphChange=()=>{}}){
    const abort=new AbortController(),signal=abort.signal,reduced=matchMedia('(prefers-reduced-motion: reduce)').matches;
    let enabled=mode==='si',order='gem',timers=[],disposed=false;
    host.classList.add('graph-feature');host.dataset.feature=mode;
    host.innerHTML=`<svg class="graph-feature-overlay" viewBox="0 0 640 360" preserveAspectRatio="none" aria-hidden="true"></svg>
      <div class="graph-feature-bar"><strong>${mode.toUpperCase()}</strong><span class="graph-feature-summary"></span><div class="graph-feature-options"></div>
      <button type="button" data-action="replay" aria-label="Replay ${mode.toUpperCase()} walkthrough"><span class="ms">replay</span></button>
      <button type="button" data-action="close" aria-label="Return to graph"><span class="ms">close</span></button></div>`;
    const overlay=host.querySelector('svg'),summary=host.querySelector('.graph-feature-summary'),options=host.querySelector('.graph-feature-options');
    function cancel(){timers.forEach(clearTimeout);timers=[];guided=false;}
    function render(){
      if(mode==='si'){
        host.classList.toggle('si-on',enabled);
        options.innerHTML=button('off','SI off',enabled?'on':'off')+button('on','SI on',enabled?'on':'off');
        summary.innerHTML=enabled?'<b>4 / 14</b> anchor areas remain':'<b>14 / 14</b> anchor areas are scanned';
        const chips=labels.map((l,i)=>{const x=278+(i%2)*54,y=65+Math.floor(i/2)*42;return `<g class="si-rejected"><rect x="${x}" y="${y}" width="48" height="27" rx="6"/><text x="${x+24}" y="${y+18}" text-anchor="middle">${l}</text><path d="M${x+(i%2?0:48)} ${y+13} Q${i%2?520:120} ${y} ${i%2?430:125} 170"/></g>`;}).join('');
        overlay.innerHTML=`<g class="si-keep music"><ellipse cx="123" cy="170" rx="76" ry="67"/><ellipse cx="123" cy="170" rx="57" ry="49"/><text x="123" y="93" text-anchor="middle">2 Music person schemas</text></g>
          <g class="si-keep oldtown"><ellipse cx="517" cy="170" rx="76" ry="67"/><ellipse cx="517" cy="170" rx="57" ry="49"/><text x="517" y="93" text-anchor="middle">2 Old Town person schemas</text></g>${chips}`;
        onGraphChange({mode:'si',enabled});
      }else{
        options.innerHTML=button('follows','FOLLOWS first',order)+button('visits','VISITS first',order)+button('gem','GEM',order);
        const left=order==='follows'?'Wide frontier':'Small frontier',right=order==='visits'?'Wide frontier':'Small frontier';
        summary.innerHTML=order==='gem'?'<b>Per group:</b> both start small':'<b>One shared order:</b> one side expands';
        overlay.innerHTML=`<g class="gem-frontier ${left.startsWith('Wide')?'wide':'small'}"><rect x="29" y="270" width="130" height="30" rx="7"/><text x="94" y="290" text-anchor="middle">${left}</text></g>
          <g class="gem-frontier ${right.startsWith('Wide')?'wide':'small'}"><rect x="481" y="270" width="130" height="30" rx="7"/><text x="546" y="290" text-anchor="middle">${right}</text></g>`;
        onGraphChange({mode:'gem',order});
      }
    }
    function guide(auto){
      cancel();guided=auto;
      const later=(fn,ms)=>timers.push(setTimeout(()=>{if(!disposed)fn();},ms));
      if(mode==='si'){
        enabled=false;render();if(!reduced)later(()=>{enabled=true;render();},2600);
      }else{
        order='follows';render();if(!reduced){later(()=>{order='visits';render();},1800);later(()=>{order='gem';render();},3600);}
      }
      if(auto&&!reduced)later(()=>{cancel();onDone();},5600);
    }
    host.addEventListener('click',e=>{
      const b=e.target.closest('button');if(!b)return;
      cancel();
      if(b.dataset.action==='close'){onGraphChange(null);onClose();return;}
      if(b.dataset.action==='replay'){guide(false);return;}
      if(b.dataset.value){if(mode==='si')enabled=b.dataset.value==='on';else order=b.dataset.value;render();}
    },{signal});
    render();if(guided)guide(true);
    return ()=>{disposed=true;cancel();abort.abort();onGraphChange(null);host.classList.remove('graph-feature','si-on');host.innerHTML='';};
  }
  root.TurboFeatureExplorer={mount};
})(window);
