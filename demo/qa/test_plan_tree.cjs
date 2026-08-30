const test = require('node:test');
const assert = require('node:assert/strict');
const plan = require('../plan-tree.js');
const counts = Object.freeze({result_rows:167260, venues:33452, paths:172000, peak_rows:65751448});

test('preserves the single-branch executed operator sequence and peak semantics', () => {
  const nodes = plan.layout(plan.model('base', counts)).nodes;
  assert.deepEqual(nodes.map(n => n.op), ['ProduceResults','IdSeek','AdjIdxJoin','HashAgg',
    'AdjIdxJoin','IdSeek','AdjIdxJoin','IdSeek','IdSeek','TableScan']);
  assert.equal(nodes[4].unit, 'Σ peak');
  assert.equal(nodes[4].rows, counts.peak_rows);
  assert.equal(nodes[9].rows, counts.paths);
  assert.equal(nodes[5].rows, null, 'unknown output counts must not become invented values');
  assert.match(nodes[5].detail, /kind IS NOT NULL/);
  const pruned = plan.layout(plan.model('si', counts)).nodes;
  assert.equal(pruned.at(-1).rows, 43000);
  assert.match(pruned.at(-1).detail, /4 kind-graphlets/);
});

test('GEM creates a real fork with opposite edge orders; SSRF changes profile attachment', () => {
  const root = plan.model('gem', counts);
  const nodes = plan.layout(root).nodes;
  const union = nodes.find(n => n.op === 'UnionAll');
  assert.equal(nodes.length, 17);
  assert.equal(union.children.length, 2);
  assert.deepEqual(union.children.map(n => n.branch), ['Music District','Old Town']);
  const scanOrder = n => {
    const joins=[];
    while(n){ if(n.op==='AdjIdxJoin') joins.unshift(n.detail.match(/\[(.*?)\]/)[1]); n=n.children[0]; }
    return joins;
  };
  assert.deepEqual(union.children.map(scanOrder), [
    ['VISITS','RECOMMENDS','FOLLOWS'], ['FOLLOWS','RECOMMENDS','VISITS']
  ]);
  assert.equal(root.children[0].flag, 'columnar union');
  assert.equal(plan.model('ssrf', counts).children[0].flag, 'fmt=ROW');
});

test('both orientations retain every parent link without overlapping cards', () => {
  for(const mode of ['base','si','gem','ssrf']) for(const vertical of [false,true]) {
    const scene = plan.layout(plan.model(mode, counts), vertical);
    for(const n of scene.nodes) {
      assert.ok(n.x>=0 && n.y>=0 && n.x+plan.CW<=scene.width && n.y+plan.CH<=scene.height);
      if(n.parentIdx>=0) assert.equal(n.depth, scene.nodes[n.parentIdx].depth+1);
    }
    for(let i=0;i<scene.nodes.length;i++) for(let j=i+1;j<scene.nodes.length;j++) {
      const a=scene.nodes[i], b=scene.nodes[j];
      assert.ok(!(a.x<b.x+plan.CW && b.x<a.x+plan.CW && a.y<b.y+plan.CH && b.y<a.y+plan.CH));
    }
    const rendered=plan.svg(scene);
    assert.equal((rendered.match(/class="plan-link"/g)||[]).length, scene.nodes.length-1);
    assert.equal((rendered.match(/class="plan-node"/g)||[]).length, scene.nodes.length);
  }
});

test('full operator detail stays accessible and is escaped in SVG', () => {
  const root=plan.model('base', counts);
  root.detail='<script>alert("x")</script> & full details';
  const rendered=plan.svg(plan.layout(root));
  assert.ok(!rendered.includes('<script>'));
  assert.ok(rendered.includes('&lt;script&gt;'));
  assert.ok(rendered.includes('aria-label="ProduceResults'));
});

test('overview is lossless, shallow and preserves only physical forks', () => {
  for(const mode of ['base','si','gem','ssrf']) {
    const root=plan.model(mode, counts),scene=plan.compactLayout(root);
    const actual=plan.layout(root).nodes;
    const expanded=scene.nodes.flatMap(n=>n.members||[n]);
    assert.deepEqual(expanded.map(n=>[n.op,n.detail,n.rows,n.unit]),
      actual.map(n=>[n.op,n.detail,n.rows,n.unit]));
    assert.ok(scene.height<=220 && scene.width<=850, 'overview fits a short, wide panel');
    assert.equal(scene.nodes.filter(n=>n.children.length>1).length, mode==='base'||mode==='si'?0:1);
    assert.ok(scene.nodes.every(n=>n.depth<=2));
    for(const n of scene.nodes) {
      assert.ok(n.x>=0 && n.y>=0 && n.x+n.w<=scene.width && n.y+n.h<=scene.height);
      if(n.parentIdx>=0) assert.ok(n.y>=scene.nodes[n.parentIdx].y+scene.nodes[n.parentIdx].h);
    }
    for(let i=0;i<scene.nodes.length;i++) for(let j=i+1;j<scene.nodes.length;j++) {
      const a=scene.nodes[i],b=scene.nodes[j];
      assert.ok(!(a.x<b.x+b.w && b.x<a.x+a.w && a.y<b.y+b.h && b.y<a.y+a.h));
    }
    assert.equal((plan.svg(scene).match(/class="plan-link"/g)||[]).length,scene.nodes.length-1);
    const narrow=plan.compactLayout(root,280);
    assert.equal(narrow.nodes.length,scene.nodes.length);
    assert.ok(narrow.nodes.every(n=>n.w<=280 && n.x+n.w<=narrow.width));
  }
  const gem=plan.compactLayout(plan.model('gem',counts));
  assert.deepEqual(gem.nodes.filter(n=>n.branchColor).map(n=>n.detail),[
    'VISITS → RECOMMENDS → FOLLOWS','FOLLOWS → RECOMMENDS → VISITS'
  ]);
  assert.match(plan.compactLayout(plan.model('ssrf',counts)).nodes[0].detail,/fmt=ROW/);
});
