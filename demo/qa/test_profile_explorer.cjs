const test=require('node:test');
const assert=require('node:assert/strict');
const profile=require('../profile-explorer.js');

test('intermediate comparison removes only NULL padding',()=>{
  const venue={id:67090,label:'fav_place_90'};
  const html=profile.tableHTML({venue,record:0,packed:false});
  assert.equal((html.match(/class="ivalue"/g)||[]).length,50); // 25 in each representation
  assert.equal((html.match(/class="inull" title=/g)||[]).length,0);
  assert.equal((html.match(/class="inull imore"/g)||[]).length,5);
  assert.match(html,/5 properties \+ 195 NULL slots per section/);
  assert.match(html,/Same properties, no NULL padding/);
  for(const value of ['live set','weekly','online door','120 min','320'])
    assert.equal((html.match(new RegExp('>'+value+'<','g'))||[]).length,2);
  for(const key of ['event_type','restroom_count','service_languages','parking_available','seating_style'])
    assert.match(html,new RegExp('>'+key+'<'));
  assert.match(html,/After VENUE_PROFILE lookup/);
});
