// Offline contract tests for the Etsy review workflow. No Slack or Sheet requests.
const fs = require('fs');
const path = require('path');
const assert = require('assert/strict');
const workflow = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'Etsy Design System - Slack Verdicts.json')));
const nodes = Object.fromEntries(workflow.nodes.map(n => [n.name, n]));
let state = {}, input = [], data = {}, current = {};
function run(name) {
  const $ = label => ({first:()=>({json:data[label]}), all:()=>data[label], item:{json:data[label]}});
  return new Function('$getWorkflowStaticData', '$', '$input', '$json', nodes[name].parameters.jsCode)(
    ()=>state, $, {first:()=>input[0], all:()=>input}, current);
}
data['Pick channel'] = {channel:'fixture', oldest:'10000'};
data['Read R Designs'] = {values:[['SKU','STATUS'],['R325','Design For Review']]};
data['Channel history'] = {ok:true,messages:[{ts:'1.001',bot_id:'bot',text:'R325 pilot is ready for approval'}]};
assert.equal(run('Collect threads')[0].json.ts, '1.001');
// The parent is older than the history window and has now left the first history page.
data['Channel history'].messages=[];
assert.equal(run('Collect threads')[0].json.ts,'1.001');
data['Thread replies']=[{json:{ok:true,messages:[{ts:'10001.001',thread_ts:'1.001',user:'david',text:'approve'}]}}];
let verdict=run('Parse verdicts'); assert.equal(verdict[0].json.sku,'R325'); assert.equal(verdict[0].json.verdict,'approve');
assert.equal(run('Parse verdicts').length,1,'an unwritten verdict must stay retryable');
data.Decide=verdict[0].json; current={totalUpdatedCells:2}; run('Mark applied verdict');
assert.equal(run('Parse verdicts').length,0,'a confirmed write is deduplicated');
// A fresh preview supersedes older review threads even though both are waiting on the same SKU.
data['Channel history'].messages=[{ts:'10002.001',bot_id:'bot',text:'R325 pilot is ready for approval'}];
run('Collect threads');
data['Thread replies']=[{json:{ok:true,messages:[{ts:'10003.001',thread_ts:'1.001',user:'david',text:'approve'}]}}];
assert.equal(run('Parse verdicts').length,0,'old preview approval was applied to its replacement');
data['Thread replies'][0].json.messages=[{ts:'10004.001',thread_ts:'10002.001',user:'david',text:'revise: keep the cup higher'}];
verdict=run('Parse verdicts'); assert.equal(verdict[0].json.note,'keep the cup higher'); assert.equal(verdict[0].json.verdict,'revise');
data['Channel history'].messages.push({ts:'10005.001',user:'david',text:'approve R325'});
verdict=run('Parse verdicts'); assert.equal(verdict.length,1); assert.equal(verdict[0].json.verdict,'approve','latest instruction for the SKU must win');
data['Read R Designs'].values[1][1]='Ready for Mock-up'; run('Collect threads');
assert.deepEqual(state.review_threads,{});
data['Thread replies']=[{json:{ok:false,error:'fixture_failure'}}];
assert.throws(()=>run('Parse verdicts'),/unavailable/);
assert.equal(workflow.connections['Pick channel'].main[0][0].node,'Read R Designs');
assert.equal(workflow.connections['Read R Designs'].main[0][0].node,'Channel history');
assert.equal(workflow.connections['Parse verdicts'].main[0][0].node,'Decide');
assert.equal(workflow.connections['Write verdict to sheet'].main[0][0].node,'Mark applied verdict');
// A corrected pilot posted again with the same Drive preview (R404/R408, 2026-09-15): David's reply on the earlier post, given
// after the newer one, is read and counts; a reply given before the newer post, or on a post with another preview, does not.
const same = 'Preview in Drive: <https://drive.google.com/file/d/SAMEFILE/view?usp=drivesdk>';
data['Read R Designs'] = {values:[['SKU','STATUS'],['R404','Design For Review']]};
data['Channel history'] = {ok:true,messages:[
  {ts:'20002.001',bot_id:'bot',text:':art: R404 pilot is ready for approval\n'+same},
  {ts:'20001.001',bot_id:'bot',text:':art: R404 pilot is ready for approval\n'+same},
  {ts:'20000.001',bot_id:'bot',text:':art: R404 pilot is ready for approval\nPreview in Drive: <https://drive.google.com/file/d/OTHERFILE/view>'}]};
const read = run('Collect threads').map(x => x.json);
assert.deepEqual(read.map(x => x.ts), ['20002.001','20001.001'], 'the replaced post with the same preview must be read, and only that one');
assert.equal(read[1].oldest, '20002.001', 'only replies after the newest post are read on a replaced post');
data['Thread replies']=[{json:{ok:true,messages:[{ts:'20003.001',thread_ts:'20001.001',user:'david',text:'approve'}]}}];
verdict=run('Parse verdicts'); assert.equal(verdict.length,1,'a reply on the replaced post after the newest post was dropped');
assert.equal(verdict[0].json.sku,'R404'); assert.equal(verdict[0].json.verdict,'approve');
data['Thread replies'][0].json.messages=[{ts:'20001.500',thread_ts:'20001.001',user:'david',text:'approve'}];
assert.equal(run('Parse verdicts').length,0,'a reply given before the newer post answered the version it replaced');
data['Thread replies'][0].json.messages=[{ts:'20004.001',thread_ts:'20000.001',user:'david',text:'approve'}];
assert.equal(run('Parse verdicts').length,0,'a reply on a post with a different preview was applied');

// R8xxx is a normal production SKU again; X000-X999 belongs only to the Codex test lane.
state={}; data['Pick channel']={channel:'fixture',oldest:'0'}; data['Thread replies']=[];
data['Channel history']={ok:true,messages:[{ts:'30001.001',user:'david',text:'approve R8000'}]};
verdict=run('Parse verdicts'); assert.equal(verdict[0].json.sku,'R8000');
data['Channel history'].messages=[{ts:'30002.001',user:'david',text:'approve X001'}];
assert.equal(run('Parse verdicts').length,0,'the production verdict reader accepted an X test SKU');

const testWorkflow = JSON.parse(fs.readFileSync(path.join(__dirname, '..', 'Etsy Design System - Codex Test Verdicts.json')));
const testNodes = Object.fromEntries(testWorkflow.nodes.map(n => [n.name, n]));
let testState={}, testData={}, testInput=[], testCurrent={};
function runTest(name) {
  const $ = label => ({first:()=>({json:testData[label]}), all:()=>testData[label], item:{json:testData[label]}});
  return new Function('$getWorkflowStaticData', '$', '$input', '$json', testNodes[name].parameters.jsCode)(
    ()=>testState, $, {first:()=>testInput[0], all:()=>testInput}, testCurrent);
}
testData['Pick channel']={channel:'fixture',oldest:'0'};
testData['Read Codex Test Rows']={values:[['SKU','STATUS'],['X001','Design For Review']]};
testData['Channel history']={ok:true,messages:[{ts:'40001.001',bot_id:'bot',text:'[Codex Test] :art: X001 pilot is ready for approval'}]};
assert.equal(runTest('Collect threads')[0].json.ts,'40001.001');
testData['Thread replies']=[{json:{ok:true,messages:[{ts:'40002.001',thread_ts:'40001.001',user:'david',text:'approve'}]}}];
let testVerdict=runTest('Parse verdicts');
assert.equal(testVerdict[0].json.sku,'X001'); assert.equal(testVerdict[0].json.verdict,'approve');
testData['Thread replies']=[];
testData['Channel history'].messages=[{ts:'40003.001',user:'david',text:'revise X999: move the text closer'}];
testVerdict=runTest('Parse verdicts');
assert.equal(testVerdict[0].json.sku,'X999'); assert.equal(testVerdict[0].json.note,'move the text closer');
testData['Channel history'].messages=[{ts:'40004.001',user:'david',text:'approve R8000'}];
assert.equal(runTest('Parse verdicts').length,0,'the Codex test verdict reader accepted an R production SKU');

console.log('PASS: review threads, replaced previews, retryable writes, and isolated R/X verdict namespaces.');
