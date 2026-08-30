const test=require('node:test');
const assert=require('node:assert/strict');
const feature=require('../feature-model.js');

test('SI removes only impossible a candidates, without removing the graph or b/c schemas',()=>{
  const off=feature.candidates(false),on=feature.candidates(true);
  assert.equal(off.filter(g=>g.read).length,14);
  assert.equal(on.filter(g=>g.read).length,4);
  assert.equal(on.filter(g=>g.eligible).length,4);
  assert.equal(on.length,off.length);
  assert.deepEqual(on.find(g=>g.label==='title').schema,['title']);
  assert.deepEqual(off[0].schema,['name','kind','genre','follows']);
  assert.deepEqual(off[2].schema,['name','kind','neighborhood','since']);
  const again=feature.candidates(true,6);
  assert.equal(again.filter(g=>g.read).length,5);
  assert.equal(again.filter(g=>g.eligible).length,4,'scan override must not alter eligibility');
  assert.deepEqual(feature.candidates(true),on,'preview overrides must not mutate catalog data');
});

test('GEM compares shared orders against the opposite per-branch orders',()=>{
  assert.deepEqual(feature.orders('follows'),Array(2).fill(['FOLLOWS','RECOMMENDS','VISITS']));
  assert.deepEqual(feature.orders('visits'),Array(2).fill(['VISITS','RECOMMENDS','FOLLOWS']));
  assert.deepEqual(feature.orders('gem'),[['VISITS','RECOMMENDS','FOLLOWS'],['FOLLOWS','RECOMMENDS','VISITS']]);
});

test('SSRF keeps exactly the same profile values and column mapping',()=>{
  assert.equal(feature.profileSections.length,40);
  const keys=feature.profileSections.flatMap(section=>section.fields);
  assert.equal(keys.length,200);assert.equal(new Set(keys).size,200);
  const categories=new Set();
  for(let i=0;i<5;i++){
    const record=feature.profile(i);categories.add(record.category);
    assert.equal(record.slots,200);assert.equal(record.entries.length,5);
    const wide=Array(record.slots).fill(null);
    record.entries.forEach(e=>{assert.ok(e.column>=0&&e.column<200);wide[e.column]=e.value;});
    assert.equal(wide.filter(v=>v===null).length,195);
    const packed=record.entries.map(e=>e.value);
    record.entries.forEach(e=>assert.equal(packed[e.offset],wide[e.column]));
  }
  assert.equal(categories.size,5);
  assert.equal(feature.profile(0,67090).section,'Events');
  assert.deepEqual(feature.profile(0,67090).entries.map(e=>e.key),
    ['event_type','event_frequency','ticketing','average_duration','audience_size']);
});

test('SI and GEM guided tours end at their enabled feature state',()=>{
  for(const mode of ['si','gem']){
    const state=feature.initial(mode),tour=feature.tours[mode];
    for(let i=0;i<tour.length;i++){
      if(i)assert.ok(tour[i].at>tour[i-1].at);
      Object.assign(state,tour[i]);
    }
    assert.equal(state.progress,1);
    if(mode==='si')assert.equal(state.prune,true);
    if(mode==='gem'){assert.equal(state.order,'gem');assert.equal(state.view,'traversal');}
  }
});
