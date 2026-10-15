import test from 'node:test';
import assert from 'node:assert/strict';
import { fork } from 'node:child_process';
import { once } from 'node:events';
import { generateAgentKeyPair, canonicalizeJson } from '@openagentforum/protocol';
import { fixture, opening, ballot, closing, apply, route, domain, invariant, attempts, tally, POLICY, NOW, physicalInventory } from './helpers.mjs';

const actors = await Promise.all(Array.from({ length: 3 }, () => generateAgentKeyPair()));
const [creator, alice, bob] = actors;
async function child(t, f, now = f.clock?.now) {
  const process = fork(new URL('./process.mjs', import.meta.url), [], { stdio: ['ignore', 'ignore', 'pipe', 'ipc'] });
  let serial = 0, stderr = '';
  const pending = new Map();
  process.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-2000); });
  process.on('message', message => { const p = pending.get(message.id); if (p) { pending.delete(message.id); clearTimeout(p.timer); p.resolve(message); } });
  process.on('exit', (code, signal) => { for (const p of pending.values()) { clearTimeout(p.timer); p.reject(new Error(`fixture process exited (${code}/${signal})`)); } pending.clear(); });
  const request = (action, fields = {}) => new Promise((resolve, reject) => {
    const id = ++serial;
    const timer = setTimeout(() => { pending.delete(id); reject(new Error('fixture process timed out')); }, 15000);
    pending.set(id, { resolve, reject, timer }); process.send({ id, action, ...fields });
  });
  t.after(async () => {
    if (process.exitCode === null && process.signalCode === null) {
      const exited = once(process, 'exit'); process.kill('SIGTERM'); await exited;
    }
    assert(!stderr.includes('PRIVATE'), 'child diagnostics must not include fixture details');
  });
  assert.deepEqual((await request('open', { path: f.path, config: f.config, now })).result, { opened: true });
  return { process, request, async close() { const exited = once(process, 'exit'); assert.equal((await request('close')).result.closed, true);
    assert.deepEqual(await exited, [0, null]); } };
}

test('independent processes race the last retained slot and a fresh process closes after restart', async t => {
  const f = fixture(t, actors, { policy: { history: { ...POLICY.history, records: 3 } } }), root = await opening(creator);
  await apply(f.client(), root);
  const children = await Promise.all([child(t, f), child(t, f)]);
  const votes = await Promise.all([ballot(root, alice), ballot(root, bob)]);
  const prepared = await Promise.all(children.map((c, i) => c.request('prepare', { wire: JSON.stringify(votes[i]) })));
  assert(prepared.every(r => r.result.prepared));
  const results = await Promise.all(children.map(c => c.request('commit')));
  assert.equal(results.filter(r => r.result?.outcome === 'found').length, 1);
  assert.equal(results.filter(r => ['stale', 'busy'].includes(r.error)).length, 1);
  await Promise.all(children.map(c => c.close()));
  const restarted = await child(t, f), end = await closing(root, creator);
  assert.equal((await restarted.request('prepare', { wire: JSON.stringify(end), route: route(end) })).result.prepared, true);
  assert.equal((await restarted.request('commit')).result.outcome, 'found');
  await restarted.close();
  assert.equal((await tally(f.db, root.id)).countedBallots, 1); invariant(f.db, f.config);
  assert.equal(attempts(f.db).ordinary.requests, 3);
});

for (const crash of ['before', 'after']) {
  test(`process exit immediately ${crash} COMMIT preserves charge and permits only historical reconciliation`, async t => {
    const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
    const end = await closing(root, creator), writer = await child(t, f);
    assert.equal((await writer.request('prepare', { wire: JSON.stringify(end), route: route(end) })).result.prepared, true);
    const before = domain(f.db), charged = attempts(f.db), exited = once(writer.process, 'exit');
    const pending = writer.request('commit', { crash });
    const rejected = assert.rejects(pending, /fixture process exited/);
    assert.deepEqual(await exited, [crash === 'after' ? 23 : 24, null]); await rejected;
    assert.deepEqual(attempts(f.db), charged);
    const restarted = await child(t, f), recovered = (await restarted.request('recover', { wire: JSON.stringify(end) })).result;
    if (crash === 'after') {
      assert.equal(recovered.outcome, 'found'); assert.equal(recovered.receipt.storedSeq, 2);
      const committed = domain(f.db);
      await restarted.request('prepare', { wire: JSON.stringify(end), route: route(end) });
      assert.deepEqual((await restarted.request('commit')).result.receipt, recovered.receipt);
      assert.deepEqual(domain(f.db), committed);
    } else {
      assert.deepEqual(recovered, { outcome: 'unavailable', historical: true });
      assert.deepEqual(domain(f.db), before);
      // The fixture knows it killed the process before COMMIT. The adapter does
      // not infer this from an unavailable receipt or perform another mutation.
    }
    await restarted.close(); invariant(f.db, f.config);
  });
}

test('a real database clock and a whole-process restart preserve policy and rows without reseeding', async t => {
  const f = fixture(t, actors, { realClock: true });
  const now = Date.now(), root = await opening(creator, { closes: { at: now + 60000 } }, { timestamp: now });
  const first = await child(t, f, undefined);
  assert.equal((await first.request('prepare', { wire: JSON.stringify(root) })).result.prepared, true);
  const receipt = (await first.request('commit')).result.receipt; await first.close();
  const before = domain(f.db), second = await child(t, f, undefined);
  assert.deepEqual((await second.request('recover', { wire: JSON.stringify(root) })).result.receipt, receipt);
  assert.deepEqual(domain(f.db), before); await second.close(); invariant(f.db, f.config);
  const inventory = physicalInventory(f.db, f.path);
  assert(inventory.pages > 0 && inventory.pageSize > 0 && inventory.walBytes > 0);
  t.diagnostic(`Small native fixture: ${inventory.pages} pages of ${inventory.pageSize} bytes; WAL ${inventory.walBytes} bytes. Logical allowances are not file-size quotas.`);
});

test('changed generation fences both an already prepared process and a restarted old configuration', async t => {
  const f = fixture(t, actors), root = await opening(creator), writer = await child(t, f);
  await writer.request('prepare', { wire: JSON.stringify(root) });
  f.db.prepare('UPDATE poll_admission_authority SET generation = ? WHERE id = 1').run('cd'.repeat(32));
  assert.equal((await writer.request('commit')).error, 'authority');
  assert.equal(f.db.prepare('SELECT count(*) AS n FROM messages').get().n, 0);
  await writer.close();
  await assert.rejects(child(t, f), /opened|deep-equal|Expected/);
  assert.equal(f.db.prepare('SELECT policy_json FROM poll_admission_authority').get().policy_json, canonicalizeJson(f.config.policy));
});
