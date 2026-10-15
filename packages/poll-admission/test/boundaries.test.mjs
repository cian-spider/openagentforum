import test from 'node:test';
import assert from 'node:assert/strict';
import { generateAgentKeyPair } from '@openagentforum/protocol';
import { SQLitePollAdmission } from '../dist/sqlite.js';
import { MAX_IN_FLIGHT } from '../dist/contract.js';
import { SCHEMA_OBJECTS } from '../dist/schema.js';
import { fixture, opening, ballot, closing, signed, apply, route, domain, invariant, attempts, tally, instrument, POLICY, NOW } from './helpers.mjs';

const actors = await Promise.all(Array.from({ length: 3 }, () => generateAgentKeyPair()));
const [creator, alice, bob] = actors;
function gate(t, nth = 1) {
  const original = crypto.subtle.verify.bind(crypto.subtle);
  let release, entered, count = 0;
  const waiting = new Promise(resolve => { entered = resolve; }), blocked = new Promise(resolve => { release = resolve; });
  t.mock.method(crypto.subtle, 'verify', async (...args) => {
    const n = ++count, result = await original(...args);
    if (n === nth) { entered(); await blocked; }
    return result;
  });
  return { waiting, release: () => release() };
}

test('forged/cross-instance/copied tokens and unsupported poll profiles never mutate storage', async t => {
  const f = fixture(t, actors), client = f.client(), before = domain(f.db);
  assert.throws(() => client.commit({ verified: true, cost: 0, tally: {} }), /unprepared/);
  for (const root of [await opening(creator, { closePolicy: { creator: false } }), await opening(creator, { pollId: 'legacy' }),
    await opening(creator, { ledger: { hub: 'https://other.invalid' } }), { ...await opening(creator), signature: '00'.repeat(64) }]) {
    await assert.rejects(apply(client, root), /unsupported_profile|wrong_ledger|signature/);
  }
  const token = await client.prepare(JSON.stringify(await opening(creator)));
  assert.throws(() => client.commit({ ...token }), /unprepared/);
  assert.throws(() => f.client().commit(token), /unprepared/);
  assert.deepEqual(domain(f.db), before);
});

test('completion routing binds the signed poll ID, including exact retries', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const end = await closing(root, creator);
  await assert.rejects(f.client().prepare(JSON.stringify(end)), /route/);
  await assert.rejects(f.client().prepare(JSON.stringify(end), { lane: 'completion', pollId: 'other' }), /route/);
  await apply(f.client(), end);
  await assert.rejects(f.client().prepare(JSON.stringify(end), { lane: 'completion', pollId: 'other' }), /route/);
  assert.equal(invariant(f.db, f.config).messages.length, 2);
});

test('history changes during asynchronous verification cannot combine an old tally with a newer revision', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const client = f.client(), first = await ballot(root, alice), second = await ballot(root, bob), g = gate(t, 2);
  const pending = client.prepare(JSON.stringify(first)); await g.waiting;
  try { await apply(f.client(), second); } finally { g.release(); }
  const token = await pending, before = domain(f.db);
  assert.throws(() => client.commit(token), /stale/); assert.deepEqual(domain(f.db), before); invariant(f.db, f.config);
});

test('recovery rechecks current visibility after asynchronous signature verification', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const g = gate(t), pending = f.client().recover(JSON.stringify(root)); await g.waiting;
  try { f.db.exec('UPDATE channels SET is_private = 1 WHERE name = \'general\''); } finally { g.release(); }
  assert.deepEqual(await pending, { outcome: 'unavailable', historical: true });
  const before = domain(f.db);
  assert.deepEqual(await f.client().recover(JSON.stringify(root)), { outcome: 'unavailable', historical: true });
  await assert.rejects(apply(f.client(), await signed(alice, 'intel', 'new hidden write')), /channel/);
  assert.deepEqual(domain(f.db), before); assert.equal(before.public_recent_state[0].high_seq, 1);
});

test('recovery and preparation expire across an asynchronous gap without disclosing an old result', async t => {
  for (const operation of ['recover', 'prepare']) {
    const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
    const g = gate(t), client = f.client();
    const pending = operation === 'recover' ? client.recover(JSON.stringify(root)) : client.prepare(JSON.stringify(await ballot(root, alice)));
    await g.waiting;
    try { f.clock.now += POLICY.preparationMs + 1; } finally { g.release(); }
    await assert.rejects(pending, /preparation_expired/);
    t.mock.restoreAll(); invariant(f.db, f.config);
  }
});

test('policy, generation and origin changes fence already prepared writers with no reseeding', async t => {
  for (const mutation of [
    db => db.prepare('UPDATE poll_admission_authority SET generation = ?').run('cd'.repeat(32)),
    db => db.prepare('UPDATE poll_admission_authority SET origin = ?').run('https://other.invalid'),
    db => db.prepare('UPDATE poll_admission_authority SET policy_revision = ?').run('new-policy'),
    db => db.exec("UPDATE poll_admission_authority SET policy_json = json_set(policy_json, '$.history.records', 9)"),
  ]) {
    const f = fixture(t, actors), client = f.client(), root = await opening(creator), token = await client.prepare(JSON.stringify(root));
    mutation(f.db); const before = domain(f.db);
    assert.throws(() => client.commit(token), /authority/); assert.throws(() => f.client(), /authority/);
    assert.deepEqual(domain(f.db), before);
  }
});

test('registered-time and owner-set edits advance the shared revision and fence preparation', async t => {
  for (const mutation of [
    db => db.prepare('UPDATE agents SET registered_at = ? WHERE agent_id = ?').run(NOW + 1, alice.agentId),
    db => db.exec('UPDATE agents SET wake_active = 0'),
    db => db.exec('UPDATE channels SET e2ee_required = 1'),
  ]) {
    const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
    const client = f.client(), token = await client.prepare(JSON.stringify(await ballot(root, alice)));
    mutation(f.db); const before = domain(f.db);
    assert.throws(() => client.commit(token), /stale/); assert.deepEqual(domain(f.db), before);
  }
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  f.db.prepare('UPDATE agents SET registered_at = ? WHERE agent_id = ?').run(NOW + 1, alice.agentId);
  await assert.rejects(apply(f.client(), await ballot(root, alice)), /not_in_electorate/);
});

test('immutable full keys and retained authority rows cannot be changed or deleted through the lab schema', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const before = domain(f.db);
  assert.throws(() => f.db.prepare('UPDATE agents SET public_key = ? WHERE agent_id = ?').run(bob.signingPublicKey, alice.agentId), /immutable/);
  for (const table of ['agents', 'channels', 'messages', 'poll_admission_controls', 'poll_admission_receipts', 'public_message_arrivals', 'wake_message_outbox']) {
    assert.throws(() => f.db.exec(`DELETE FROM ${table}`), /immutable/);
  }
  assert.throws(() => f.db.prepare('INSERT INTO agents VALUES (?, ?, ?, 0, 0, 1)').run('agent_' + '00'.repeat(8), '00'.repeat(32), NOW), /unsupported/);
  assert.deepEqual(domain(f.db), before); invariant(f.db, f.config);
});

test('missing authority/cursors fail closed and never create replacement rows', async t => {
  for (const table of ['poll_admission_authority', 'public_recent_state', 'wake_recent_state']) {
    const f = fixture(t, actors), client = f.client(), root = await opening(creator);
    f.db.exec(`DELETE FROM ${table}`);
    if (table === 'poll_admission_authority') await assert.rejects(client.prepare(JSON.stringify(root)), /authority/);
    else {
      const token = await client.prepare(JSON.stringify(root));
      assert.throws(() => client.commit(token), /authority/);
    }
    assert.equal(f.db.prepare(`SELECT count(*) AS n FROM ${table}`).get().n, 0);
    assert.equal(f.db.prepare('SELECT count(*) AS n FROM messages').get().n, 0);
  }
});

test('changed or missing schema objects are refused before accounting or mutation', async t => {
  for (const name of ['poll_admission_final_guard', 'public_message_arrival', 'wake_message_insert', 'agents_binding', 'idx_messages_poll_reference']) {
    const f = fixture(t, actors), client = f.client(), definition = SCHEMA_OBJECTS.find(o => o.name === name);
    f.db.exec(`DROP ${definition.type.toUpperCase()} ${name}`);
    const before = attempts(f.db);
    await assert.rejects(client.prepare(JSON.stringify(await opening(creator))), /authority/);
    assert.deepEqual(attempts(f.db), before);
    assert.throws(() => f.client(), /authority/);
  }
  const f = fixture(t, actors), db = f.connect(), client = new SQLitePollAdmission(db, f.config);
  db.exec('CREATE TEMP TABLE messages (id TEXT)');
  await assert.rejects(client.prepare(JSON.stringify(await opening(creator))), /authority/);
});

test('unsafe connection pragmas are not silently repaired', async t => {
  for (const statement of ['PRAGMA foreign_keys = OFF', 'PRAGMA synchronous = OFF', 'PRAGMA recursive_triggers = OFF',
    'PRAGMA ignore_check_constraints = ON', 'PRAGMA busy_timeout = 1001']) {
    const f = fixture(t, actors), db = f.connect(), client = new SQLitePollAdmission(db, f.config), before = attempts(f.db);
    db.exec(statement); await assert.rejects(client.prepare(JSON.stringify(await opening(creator))), /authority/);
    assert.deepEqual(attempts(f.db), before);
  }
});

test('a known busy BEGIN neither charges nor poisons, and the caller must explicitly try a new operation', async t => {
  const f = fixture(t, actors), db = f.connect(); db.exec('PRAGMA busy_timeout = 0');
  const client = new SQLitePollAdmission(db, f.config), before = attempts(f.db), root = await opening(creator);
  f.db.exec('BEGIN IMMEDIATE');
  try { await assert.rejects(client.prepare(JSON.stringify(root)), /busy/); } finally { f.db.exec('ROLLBACK'); }
  assert.deepEqual(attempts(f.db), before);
  await apply(client, root); invariant(f.db, f.config);
});

test('corrupt attempt accounting is refused rather than reset on the next window', async t => {
  const f = fixture(t, actors), client = f.client();
  f.db.exec("UPDATE poll_admission_authority SET attempts_json = json_set(attempts_json, '$.ordinary.input', 1)");
  f.clock.now += POLICY.windowMs;
  await assert.rejects(client.prepare(JSON.stringify(await opening(creator))), /authority/);
  assert.equal(attempts(f.db).ordinary.input, 1);
});

test('database clock rollback survives connection restart; normal rollover renews only attempt work', async t => {
  const f = fixture(t, actors, { policy: { attempts: { ordinary: 1, completion: 1, recovery: 1 } } });
  const root = await opening(creator, { closes: { at: NOW + 120000 } }); await apply(f.client(), root);
  const before = domain(f.db); f.clock.now--;
  await assert.rejects(f.client().recover(JSON.stringify(root)), /clock/);
  assert.equal(f.db.prepare('SELECT clock FROM poll_admission_authority').get().clock, NOW);
  f.clock.now = Math.floor(NOW / POLICY.windowMs) * POLICY.windowMs + POLICY.windowMs;
  assert.equal((await f.client().recover(JSON.stringify(root))).outcome, 'found');
  assert.deepEqual(domain(f.db), before); await apply(f.client(), await ballot(root, alice)); invariant(f.db, f.config);
});

test('expired and already consumed tokens cannot be applied again', async t => {
  const f = fixture(t, actors), client = f.client(), token = await client.prepare(JSON.stringify(await opening(creator)));
  f.clock.now += POLICY.preparationMs + 1;
  assert.throws(() => client.commit(token), /preparation_expired/); assert.throws(() => client.commit(token), /unprepared/);
  assert.equal(invariant(f.db, f.config).messages.length, 0);
});

test('root history readability reserves close bytes/nodes and input bounds precede history reads', async t => {
  for (const history of [{ ...POLICY.history, bytes: 3000 }, { ...POLICY.history, nodes: 33 }]) {
    const f = fixture(t, actors, { policy: { history } });
    await assert.rejects(apply(f.client(), await opening(creator)), /history_capacity/);
    assert.equal(invariant(f.db, f.config).messages.length, 0);
  }
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const historyReads = [], db = instrument(f.connect(), { beforeStatement(sql) { if (sql.includes('WITH picked AS MATERIALIZED')) historyReads.push(sql); } });
  const client = new SQLitePollAdmission(db, f.config);
  const padded = await signed(alice, 'vote', { pollId: root.id, pollHash: root.checksum, choice: 0, extra: 'padding' });
  await assert.rejects(apply(client, padded), /input_limit/);
  await assert.rejects(apply(client, { ...await ballot(root, alice), replyToId: 'x'.repeat(2048) }), /input_limit/);
  assert.equal(historyReads.length, 0);
  let deep = null; for (let i = 0; i < 18; i++) deep = { nested: deep };
  for (const extension of [deep, Array(1025).fill(null), { ['x'.repeat(257)]: 1 }]) {
    await assert.rejects(apply(client, await opening(creator, { extension })), /input_limit/);
  }
  assert.equal(invariant(f.db, f.config).messages.length, 1);
});

test('electorate, first-vote, root-hash and creator-close decisions use verified history', async t => {
  const f = fixture(t, actors), root = await opening(creator, { revote: 'first', electorate: { type: 'list', agentIds: [creator.agentId, alice.agentId] } });
  await apply(f.client(), root); await apply(f.client(), await ballot(root, alice));
  await assert.rejects(apply(f.client(), await ballot(root, alice, 1)), /already_voted/);
  await assert.rejects(apply(f.client(), await ballot(root, bob)), /not_in_electorate/);
  await assert.rejects(apply(f.client(), await ballot({ ...root, checksum: '00'.repeat(32) }, creator)), /poll_hash_mismatch/);
  await assert.rejects(apply(f.client(), await closing(root, alice)), /invalid_close/);
  invariant(f.db, f.config);
});

test('encrypted intel never advances public arrivals, while the separate wake predicate still applies', async t => {
  const f = fixture(t, actors), message = await signed(alice, 'intel', '00'.repeat(32), { encrypted: true, nonce: '00'.repeat(12) });
  await apply(f.client(), message);
  assert.deepEqual(await f.client().recover(JSON.stringify(message)), { outcome: 'unavailable', historical: true });
  let state = invariant(f.db, f.config);
  assert.equal(state.public_recent_state[0].high_seq, 0); assert.equal(state.wake_recent_state[0].high_seq, 1);
  const large = await signed(alice, 'intel', '00'.repeat(33000), { encrypted: true, nonce: '00'.repeat(12) });
  await apply(f.client(), large); state = invariant(f.db, f.config);
  assert.equal(state.public_recent_state[0].high_seq, 0); assert.equal(state.wake_recent_state[0].high_seq, 1);
});

test('prototype-like IDs and channel names remain literal SQL keys, and implicit channels are refused', async t => {
  const f = fixture(t, actors, { channels: ['general', 'constructor', '__proto__'] });
  for (const name of ['constructor', '__proto__']) {
    const root = await opening(creator, {}, { channel: name, id: name }); await apply(f.client(), root);
    await apply(f.client(), await closing(root, creator, { channel: name }));
  }
  await assert.rejects(apply(f.client(), await opening(creator, {}, { channel: 'unknown' })), /channel/);
  invariant(f.db, f.config);
});

test('the fixed local cap includes retained preparations and frees slots only on consumption/expiry', async t => {
  const f = fixture(t, actors), client = f.client(), tokens = [];
  for (let i = 0; i < MAX_IN_FLIGHT; i++) tokens.push(await client.prepare(JSON.stringify(await opening(creator))));
  const spent = attempts(f.db);
  await assert.rejects(client.prepare(JSON.stringify(await opening(creator))), /busy/);
  await assert.rejects(client.recover(JSON.stringify(await opening(creator))), /busy/);
  assert.deepEqual(attempts(f.db), spent);
  client.commit(tokens[0]);
  const next = await client.prepare(JSON.stringify(await opening(creator))); client.commit(next);
  for (const token of tokens.slice(1)) assert.throws(() => client.commit(token), /stale/);
  invariant(f.db, f.config);
});

test('an uncertain accounting acknowledgment cannot begin parsing/verification or return another pending success', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  let armed = false;
  const db = instrument(f.connect(), { afterExec(sql) { if (armed && sql === 'COMMIT') { armed = false; throw new Error('uncertain charge'); } } });
  const client = new SQLitePollAdmission(db, f.config), g = gate(t);
  const pending = client.recover(JSON.stringify(root)); await g.waiting;
  armed = true;
  try { await assert.rejects(client.prepare('{'), /unavailable/); } finally { g.release(); }
  await assert.rejects(pending, /unavailable/);
  assert.equal(attempts(f.db).ordinary.requests, 2); assert.equal(attempts(f.db).recovery.requests, 1);
  assert.equal(invariant(f.db, f.config).messages.length, 1);
});

test('an indexed history scan examines at most the configured candidate bound before payloads leave SQL', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  await apply(f.client(), await ballot(root, alice));
  const observed = [], db = instrument(f.connect(), { beforeStatement(sql, method, args) {
    if (sql.includes('WITH picked AS MATERIALIZED')) observed.push({ sql, method, args });
  } });
  await apply(new SQLitePollAdmission(db, f.config), await ballot(root, bob));
  assert.equal(observed.length, 1);
  const query = observed[0];
  assert.equal(query.args[3], POLICY.history.records + 1);
  const plan = f.db.prepare('EXPLAIN QUERY PLAN ' + query.sql).all(...query.args).map(r => r.detail).join('\n');
  assert.match(plan, /SEARCH messages USING INDEX idx_messages_poll_reference/);
  assert(!/SCAN messages/.test(plan)); invariant(f.db, f.config);
});
