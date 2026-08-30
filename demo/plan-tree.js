/* SVG cards, subtree layout and curved links adapted from
 * app/components/scenes/S3_Plan.tsx. No React runtime is needed by ui.html.
 * The operator facts below are the same trace-backed facts as the former list.
 */
(function(root){
  'use strict';
  const CW=200, CH=72, GAP=28, STEP=32, PAD=24;
  const COLORS={ProduceResults:'#5f6368',IdSeek:'#087f8c',AdjIdxJoin:'#7139bd',
    HashAgg:'#9a5b00',UnionAll:'#b21866',TableScan:'#0b57d0',NodeScan:'#0b57d0'};
  const esc=s=>String(s??'').replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
  const fmt=n=>n==null?'':Number(n).toLocaleString('en-US');
  const node=(op,detail,rows=null,tone='',flag='',unit='rows')=>({op,detail,rows,tone,flag,unit,children:[]});
  const chain=(items,children=[])=>items.reduceRight((next,n)=>({...n,children:next?[next]:children}),null);

  function model(mode,d){
    const split=mode==='gem'||mode==='ssrf', packed=mode==='ssrf';
    let tail;
    if(split){
      const music=chain([
        node('AdjIdxJoin','[FOLLOWS] INTO a→b · check',86000,'cheap'),
        node('IdSeek','b · vpart rebind'),
        node('AdjIdxJoin','[RECOMMENDS] backward c→b · ×4'),
        node('IdSeek','c · fetch c.title'),
        node('AdjIdxJoin','[VISITS] a→c · fanout 1',null,'cheap'),
        node('NodeScan','music anchors · graphlets {586, 546}',21500)
      ]);
      const oldtown=chain([
        node('AdjIdxJoin','[VISITS] INTO a→c · check',86000,'cheap'),
        node('IdSeek','c · fetch c.title'),
        node('AdjIdxJoin','[RECOMMENDS] b→c · ×4'),
        node('IdSeek','b · vpart rebind'),
        node('AdjIdxJoin','[FOLLOWS] a→b · fanout 1',null,'cheap'),
        node('NodeScan','oldtown anchors · graphlets {550, 590}',21500)
      ]);
      music.branch='Music District'; music.branchColor='#1a73e8';
      oldtown.branch='Old Town'; oldtown.branchColor='#e8710a';
      tail={...node('UnionAll','2 GEM branches · 2 × 86,000',d.paths),children:[music,oldtown]};
    }else if(mode==='si'){
      tail=chain([
        node('AdjIdxJoin','[FOLLOWS] INTO a→b · check'),
        node('IdSeek','b · vpart rebind'),
        node('AdjIdxJoin','[RECOMMENDS] backward c→b · ×4 recommenders',d.peak_rows,'flood','flood','Σ peak'),
        node('IdSeek','c · fetch c.title'),
        node('AdjIdxJoin','[VISITS] forward a→c · locals fan out 3.8M',null,'flood'),
        node('TableScan','a · 4 kind-graphlets — schema-pruned',43000,'cheap','14 → 4 graphlets')
      ]);
    }else{
      tail=chain([
        node('AdjIdxJoin','[VISITS] backward c→a · rides 4.48M filler edges',d.peak_rows,'flood','flood','Σ peak'),
        node('IdSeek','a · 14-graphlet vpart + filter kind IS NOT NULL',null,'flood'),
        node('AdjIdxJoin','[FOLLOWS] backward b→a'),
        node('IdSeek','c · fetch c.title'),
        node('IdSeek','b · unpruned vpart · 172,000 rebinds'),
        node('TableScan','RECOMMENDS edge relation · anchor',d.paths)
      ]);
    }
    return chain([
      node('ProduceResults','202 columns · venue · reach · 200 venue-profile properties',d.result_rows),
      node('IdSeek','vp · [VENUE_PROFILE] attach · 5 of 200 props (40 section graphlets)',d.result_rows,packed?'cheap':'',packed?'fmt=ROW':'columnar union'),
      node('AdjIdxJoin','[PROFILE_OF] backward c→vp · 5 sections per venue'),
      node('HashAgg','GROUP BY venue · 172,000 → 33,452',d.venues,'','5.1× collapse')
    ],[tail]);
  }

  // The original recursive subtree-width algorithm, with an axis switch for
  // the wide console panel. Parent links and card dimensions are preserved.
  function layout(tree,vertical=false){
    const result=[], cache=new Map(), cross=vertical?CW:CH;
    const size=n=>{
      if(!cache.has(n)) cache.set(n,Math.max(cross,
        n.children.reduce((sum,c)=>sum+size(c),0)+Math.max(0,n.children.length-1)*GAP));
      return cache.get(n);
    };
    function walk(n,start,depth,parentIdx,branchColor=''){
      const span=size(n), center=start+(span-cross)/2, index=result.length;
      const color=n.branchColor||branchColor;
      result.push({...n,id:index,parentIdx,depth,branchColor:color,
        x:PAD+(vertical?center:depth*(CW+STEP)),
        y:PAD+(vertical?depth*(CH+STEP):center)});
      const total=n.children.reduce((s,c)=>s+size(c),0)+Math.max(0,n.children.length-1)*GAP;
      let cursor=start+(span-total)/2;
      n.children.forEach(c=>{walk(c,cursor,depth+1,index,color);cursor+=size(c)+GAP;});
    }
    walk(tree,0,0,-1);
    return {nodes:result,width:Math.max(...result.map(n=>n.x+CW))+PAD,
      height:Math.max(...result.map(n=>n.y+CH))+PAD,vertical};
  }

  // Collapse only consecutive unary operators. A group is an explicit view
  // of the physical operators, never an extra operator or an invented fork.
  function overview(tree){
    const shared=[]; let next=tree;
    while(next && shared.length<4){shared.push(next);next=next.children[0];}
    const group=(op,members,detail,w=440,h=64)=>{
      const peak=members.find(n=>n.unit==='Σ peak'), first=members[0];
      return {...node(op,detail,peak?peak.rows:first.rows,peak?'flood':'',
        `${members.length} operators`,peak?'Σ peak':'rows'),
        members,w,h,group:true,branchColor:first.branchColor||''};
    };
    const root=group('Results · venue profiles · aggregate',shared,
      `${shared.length} operators${shared[1].flag==='fmt=ROW'?' · fmt=ROW':''}`,440,52);
    if(next.op==='UnionAll'){
      root.children=[{...next,w:184,h:32,children:next.children.map(start=>{
        const members=[];for(let n=start;n;n=n.children[0])members.push(n);
        const order=members.filter(n=>n.op==='AdjIdxJoin').reverse()
          .map(n=>n.detail.match(/\[(.*?)\]/)[1]).join(' → ');
        return group(start.branch,members,order,400,64);
      })}];
    }else{
      const members=[];for(let n=next;n;n=n.children[0])members.push(n);
      const scan=members.pop();
      const order=members.filter(n=>n.op==='AdjIdxJoin').reverse()
        .map(n=>n.detail.match(/\[(.*?)\]/)[1]);
      if(scan.detail.startsWith('RECOMMENDS'))order.unshift('RECOMMENDS');
      const match=group('Match traversal',members,order.join(' → '));
      match.children=[{...scan,w:400,h:42}];root.children=[match];
    }
    return root;
  }

  function compactLayout(tree,maxCardWidth=440){
    const root=overview(tree), nodes=[], spans=new Map(), heights=[];
    function measure(n,depth){
      n.w=Math.min(n.w,Math.max(280,maxCardWidth));
      heights[depth]=Math.max(heights[depth]||0,n.h);
      const children=n.children.map(c=>measure(c,depth+1));
      const span=Math.max(n.w,children.reduce((a,b)=>a+b,0)+Math.max(0,children.length-1)*28);
      spans.set(n,span);return span;
    }
    const width=measure(root,0)+16, levels=[];let height=8;
    for(const h of heights){levels.push(height);height+=h+18;}
    function walk(n,start,depth,parentIdx,branchColor=''){
      const index=nodes.length,span=spans.get(n),color=n.branchColor||branchColor;
      nodes.push({...n,id:index,parentIdx,depth,x:8+start+(span-n.w)/2,y:levels[depth],branchColor:color});
      const total=n.children.reduce((sum,c)=>sum+spans.get(c),0)+Math.max(0,n.children.length-1)*28;
      let cursor=start+(span-total)/2;
      n.children.forEach(c=>{walk(c,cursor,depth+1,index,color);cursor+=spans.get(c)+28;});
    }
    walk(root,0,0,-1);
    return {nodes,width,height:height-10,vertical:true,compact:true};
  }

  function svg(scene){
    const links=scene.nodes.filter(n=>n.parentIdx>=0).map(n=>{
      const p=scene.nodes[n.parentIdx]; let path;
      if(scene.vertical){
        const pw=p.w||CW,ph=p.h||CH,nw=n.w||CW,mid=(p.y+ph+n.y)/2;
        path=`M${p.x+pw/2},${p.y+ph} C${p.x+pw/2},${mid} ${n.x+nw/2},${mid} ${n.x+nw/2},${n.y}`;
      }else{
        const mid=(p.x+CW+n.x)/2;
        path=`M${p.x+CW},${p.y+CH/2} C${mid},${p.y+CH/2} ${mid},${n.y+CH/2} ${n.x},${n.y+CH/2}`;
      }
      return `<path class="plan-link" d="${path}" stroke="${n.branchColor||'#bdc1c6'}"/>`;
    }).join('');
    const cards=scene.nodes.map(n=>{
      const color=n.tone==='flood'?'#b3261e':(scene.compact?n.branchColor:'')||COLORS[n.op]||'#5f6368';
      const fill=n.tone==='flood'?'#fdeceb':n.tone==='cheap'?'#e6f4ea':'#fff';
      const summary=n.detail.length>25?n.detail.slice(0,23)+'…':n.detail;
      const description=[n.op,n.detail,n.detail.includes(n.flag)?'':n.flag,n.rows==null?'':n.unit+' '+fmt(n.rows)].filter(Boolean).join(' · ');
      if(scene.compact){
        const w=n.w,h=n.h,short=h<=42,sub=h===32?'':n.detail;
        const detail=sub.length>Math.floor((w-26)/7.3)?sub.slice(0,Math.floor((w-26)/7.3)-1)+'…':sub;
        return `<g class="plan-node plan-summary" data-node="${n.id}" role="button" tabindex="0" aria-label="${esc(description)}" aria-pressed="false" transform="translate(${n.x} ${n.y})">
          <title>${esc(description)}</title>
          <rect class="node-card" width="${w}" height="${h}" rx="7" fill="${fill}" stroke="${color}"/>
          <rect y="6" width="4" height="${h-12}" rx="2" fill="${color}"/>
          <text x="13" y="${h===42?18:22}" class="node-op" fill="${color}">${esc(n.op)}</text>
          ${sub?`<text x="13" y="${h===42?35:42}" class="node-detail">${esc(detail)}</text>`:''}
          ${h===64?`<text x="13" y="57" class="node-flag" fill="${color}">${esc(n.flag)}</text>`:''}
          ${n.rows==null?'':`<text x="${w-12}" y="${short?(h===42?18:22):h===64?57:42}" class="node-rows" text-anchor="end">${n.unit==='Σ peak'?'Σ peak ':''}${fmt(n.rows)}</text>`}
        </g>`;
      }
      return `<g class="plan-node" data-node="${n.id}" role="button" tabindex="0" aria-label="${esc(description)}" aria-pressed="false" transform="translate(${n.x} ${n.y})">
        <title>${esc(description)}</title>
        <rect class="node-card" width="${CW}" height="${CH}" rx="8" fill="${fill}" stroke="${color}"/>
        <rect x="0" y="7" width="4" height="${CH-14}" rx="2" fill="${color}"/>
        <text x="13" y="23" class="node-op" fill="${color}">${esc(n.op)}</text>
        <text x="13" y="43" class="node-detail">${esc(summary)}</text>
        <text x="13" y="62" class="node-flag" fill="${n.tone==='flood'?'#b3261e':n.tone==='cheap'||n.op==='HashAgg'?'#146c2e':'#5f6368'}">${esc(n.flag)}</text>
        ${n.rows==null?'':`<text x="${CW-12}" y="62" class="node-rows" text-anchor="end">${esc(n.unit==='Σ peak'?'Σ ':'')}${fmt(n.rows)}</text>`}
        ${n.branch?`<text x="0" y="-8" class="node-branch" fill="${n.branchColor}">${esc(n.branch)}</text>`:''}
      </g>`;
    }).join('');
    return `<svg class="plan-svg${scene.compact?' compact':''}" xmlns="http://www.w3.org/2000/svg" viewBox="0 0 ${scene.width} ${scene.height}" role="group" aria-label="${scene.compact?'Grouped query plan tree':'Query operator tree'}">${links}${cards}</svg>`;
  }

  function html(){
    const button=(action,label,icon)=>`<button type="button" class="plan-control" data-plan-action="${action}" aria-label="${label}" title="${label}"><span class="ms" aria-hidden="true">${icon}</span></button>`;
    return `<div class="plan-explorer">
      <div class="plan-tools" role="toolbar" aria-label="Plan tree controls">
        <button type="button" class="plan-control plan-mode" data-plan-action="detail" aria-label="Show grouped overview">Overview</button>
        <span class="plan-hint">Drag to pan · select an operator</span>
        ${button('out','Zoom out plan','remove')}<output class="plan-zoom" aria-live="polite">100%</output>${button('in','Zoom in plan','add')}
        ${button('fit','Fit entire plan','fit_screen')}${button('root','Show plan root','home')}
        ${button('orientation','Show vertical tree','swap_vert')}
      </div>
      <div class="plan-view" tabindex="0" aria-label="Scrollable plan tree"></div>
      <aside class="plan-inspector" aria-label="Operator details" hidden>
        <button type="button" class="plan-control plan-close" aria-label="Close operator details"><span class="ms" aria-hidden="true">close</span></button>
        <strong class="plan-detail-op"></strong><p class="plan-detail-text"></p><p class="plan-detail-count"></p><ol class="plan-members"></ol>
      </aside>
    </div>`;
  }

  function mount(container,tree){
    const host=container.querySelector('.plan-explorer'); if(!host) return ()=>{};
    const view=host.querySelector('.plan-view'), inspector=host.querySelector('.plan-inspector');
    const output=host.querySelector('.plan-zoom'), abort=new AbortController(), signal=abort.signal;
    const cardWidth=()=>Math.min(440,Math.max(280,view.clientWidth/.9-16));
    let detailed=true,vertical=false,scene=layout(tree,vertical);
    const defaultScale=()=>detailed?Math.max(.9,Math.min(1.6,view.clientWidth/1150))
      :Math.max(.5,Math.min(2,view.clientWidth/scene.width,view.clientHeight/scene.height));
    let scale=defaultScale(),selected=null,dragging=null,moved=false,manualZoom=false;
    function size(){
      const image=view.querySelector('svg');
      image.style.width=scene.width*scale+'px'; image.style.height=scene.height*scale+'px';
      image.style.marginTop=Math.max(0,(view.clientHeight-scene.height*scale)/2)+'px';
      output.textContent=Math.round(scale*100)+'%';
    }
    function render(){
      view.innerHTML=svg(scene);size();view.scrollLeft=detailed?0:Math.max(0,(scene.width*scale-view.clientWidth)/2);view.scrollTop=0;inspector.hidden=true;selected=null;
      host.querySelector('[data-plan-action="orientation"]').hidden=!detailed;
      host.querySelector('[data-plan-action="root"]').hidden=!detailed;
      host.querySelector('.plan-hint').textContent=detailed?'Drag to pan · select an operator':'Grouped operators · select to inspect';
    }
    function zoom(next,x=view.clientWidth/2,y=view.clientHeight/2){
      manualZoom=true;
      const ratio=Math.max(.1,Math.min(2.5,next))/scale;
      const sx=(view.scrollLeft+x)*ratio-x, sy=(view.scrollTop+y)*ratio-y;
      scale*=ratio;size();view.scrollLeft=sx;view.scrollTop=sy;
    }
    function fit(){manualZoom=true;scale=Math.max(.1,Math.min(1.5,view.clientWidth/scene.width,view.clientHeight/scene.height));size();view.scrollLeft=0;view.scrollTop=0;}
    function choose(id){
      selected=id;const n=scene.nodes[id];
      view.querySelectorAll('.plan-node').forEach(el=>el.setAttribute('aria-pressed',String(+el.dataset.node===id)));
      host.querySelector('.plan-detail-op').textContent=n.op;
      host.querySelector('.plan-detail-text').textContent=n.detail;
      host.querySelector('.plan-detail-count').textContent=[n.flag,n.rows==null?'':`${n.unit}: ${fmt(n.rows)}`].filter(Boolean).join(' · ');
      host.querySelector('.plan-members').innerHTML=(n.members||[]).map(op=>`<li>
        <strong>${esc(op.op)}</strong><span>${esc(op.detail)}</span>
        ${op.flag||op.rows!=null?`<small>${esc([op.flag,op.rows==null?'':`${op.unit}: ${fmt(op.rows)}`].filter(Boolean).join(' · '))}</small>`:''}
      </li>`).join('');
      inspector.hidden=false;
    }
    host.querySelector('.plan-close').addEventListener('click',()=>{
      inspector.hidden=true;view.querySelector(`[data-node="${selected}"]`)?.focus();
    },{signal});
    host.querySelectorAll('[data-plan-action]').forEach(button=>button.addEventListener('click',()=>{
      switch(button.dataset.planAction){
        case 'detail':
          detailed=!detailed;scene=detailed?layout(tree,vertical):compactLayout(tree,cardWidth());
          manualZoom=false;scale=defaultScale();render();
          button.textContent=detailed?'Overview':'All operators';
          button.setAttribute('aria-label',detailed?'Show grouped overview':'Show all operators');break;
        case 'in':zoom(scale*1.2);break;
        case 'out':zoom(scale/1.2);break;
        case 'fit':fit();break;
        case 'root':manualZoom=false;scale=defaultScale();size();view.scrollLeft=0;view.scrollTop=0;break;
        case 'orientation':vertical=!vertical;scene=layout(tree,vertical);manualZoom=false;scale=defaultScale();render();
          button.setAttribute('aria-label',vertical?'Show horizontal tree':'Show vertical tree');
          button.title=button.getAttribute('aria-label');break;
      }
    },{signal}));
    view.addEventListener('click',e=>{const el=e.target.closest('.plan-node');if(el&&!moved)choose(+el.dataset.node);},{signal});
    view.addEventListener('keydown',e=>{
      if(e.key==='Escape'){inspector.hidden=true;return;}
      const el=e.target.closest('.plan-node');
      if(el&&(e.key==='Enter'||e.key===' ')){e.preventDefault();choose(+el.dataset.node);}
    },{signal});
    view.addEventListener('wheel',e=>{
      if(!e.ctrlKey&&!e.metaKey)return;
      e.preventDefault();const r=view.getBoundingClientRect();zoom(scale*(e.deltaY>0?.9:1.1),e.clientX-r.left,e.clientY-r.top);
    },{passive:false,signal});
    view.addEventListener('pointerdown',e=>{
      if(e.button!==0||e.pointerType==='touch')return;
      moved=false;dragging={x:e.clientX,y:e.clientY,left:view.scrollLeft,top:view.scrollTop};
    },{signal});
    view.addEventListener('pointermove',e=>{
      if(!dragging)return;
      const dx=e.clientX-dragging.x,dy=e.clientY-dragging.y;
      if(Math.hypot(dx,dy)>4){moved=true;view.setPointerCapture(e.pointerId);view.classList.add('dragging');}
      if(moved){view.scrollLeft=dragging.left-dx;view.scrollTop=dragging.top-dy;}
    },{signal});
    const stop=()=>{dragging=null;view.classList.remove('dragging');};
    view.addEventListener('pointerup',stop,{signal});view.addEventListener('pointercancel',stop,{signal});
    view.addEventListener('pointerleave',()=>{if(!moved)stop();},{signal});
    render();
    const observer=new ResizeObserver(()=>{
      if(!detailed){
        const next=compactLayout(tree,cardWidth());
        if(next.width!==scene.width){
          scene=next;view.innerHTML=svg(scene);
          view.querySelector(`[data-node="${selected}"]`)?.setAttribute('aria-pressed','true');
        }
      }
      if(!manualZoom)scale=defaultScale();size();
      if(!manualZoom&&!detailed)view.scrollLeft=Math.max(0,(scene.width*scale-view.clientWidth)/2);
    });observer.observe(view);
    return ()=>{abort.abort();observer.disconnect();};
  }
  const api={model,layout,overview,compactLayout,svg,html,mount,CW,CH};
  if(typeof module!=='undefined'&&module.exports)module.exports=api;
  else root.TurboPlanTree=api;
})(typeof window!=='undefined'?window:this);
