import assert from 'node:assert/strict';
import { test } from 'node:test';
import { native, ballot, closing, signed, route, domain, invariant } from './d1-helpers.mjs';
import { DB_NOW } from '../dist/schema.js';
import { D1_SCHEMA_OBJECTS, D1_SCHEMA_JSON, SNAPSHOT_SQL } from '../dist/d1-sql.js';
import { storedEnvelope } from '@openagentforum/server/envelopes';
import { createFixture } from '../../../docs/rfc/fixtures/poll-admission-reference.mjs';

const proof = env => ({ wire: JSON.stringify(env), route: route(env) });
const requests = (state, lane) => JSON.parse(state.poll_admission_authority[0].attempts_json)[lane].requests;

test('native D1 commits a root, ballot, creator close and historical exact receipts without fixture-seeded messages', async t => {
  const f = await native(t), [owner, voter] = f.actors, root = await f.open();
  assert.deepEqual(await f.call('constructor'), { sessions: 0, reads: 0, charges: 0, batches: 0, snapshots: [] });
  const created = await f.apply(root); assert.equal(created.ok, true, JSON.stringify(created));
  assert.equal(created.value.replayed, false);
  const vote = await ballot(root, voter);
  assert.equal((await f.apply(vote)).ok, true);
  const close = await closing(root, owner); assert.equal((await f.apply(close)).ok, true);
  assert.equal((await f.tally(root.id)).closedAtSeq, 3);
  const before = domain(await f.inspect());
  assert.deepEqual((await f.apply(root)).value.receipt, created.value.receipt);
  assert.deepEqual((await f.recover(root)).value.receipt, created.value.receipt);
  assert.deepEqual(domain(await f.inspect()), before);
  const state = invariant(await f.inspect(), f.options);
  assert.equal(state.messages.length, 3); assert.equal(state.public_message_arrivals.length, 3); assert.equal(state.wake_message_outbox.length, 3);
  assert.equal(state.poll_admission_controls[0].reserve_records, 0);
});

test('native D1 last-history-slot races keep the creator close reservation in either order', async t => {
  for (const order of [[0, 1], [1, 0]]) await t.test(`commit order ${order}`, async t => {
    const f = await native(t, { history: { records: 3, bytes: 4 * 1024 * 1024, nodes: 65536 } });
    const root = await f.open(); assert.equal((await f.apply(root)).ok, true);
    const votes = await Promise.all(f.actors.slice(1).map(a => ballot(root, a)));
    const paired = await f.call('paired', { proofs: votes.map(v => ({ wire: JSON.stringify(v), route: route(v) })), order });
    assert.equal(paired.results.filter(r => r.ok).length, 1, JSON.stringify(paired));
    assert.equal(paired.results[order[1]].code, 'stale');
    const denied = await f.apply(await ballot(root, f.actors[2])); assert.equal(denied.code, 'history_capacity');
    assert.equal((await f.apply(await closing(root, f.actors[0]))).ok, true);
    assert.equal(invariant(await f.inspect(), f.options).messages.length, 3);
  });
});

test('native D1 spent work persists through lost commit acknowledgment and full runtime restart', async t => {
  const f = await native(t), root = await f.open();
  assert.equal((await f.apply(root, { fault: 'batch-after' })).code, 'unavailable');
  const state = invariant(await f.inspect(), f.options); assert.equal(state.messages.length, 1);
  const attempts = JSON.parse(state.poll_admission_authority[0].attempts_json).ordinary.requests; assert.equal(attempts, 1);
  await f.restart();
  assert.equal((await f.recover(root)).value.receipt.id, root.id);
  assert.equal((await f.apply(root)).ok, true);
  assert.equal((await f.inspect()).messages.length, 1);
  assert.equal((await f.apply(await closing(root, f.actors[0]))).ok, true);
  invariant(await f.inspect(), f.options);
});

test('native D1 concurrent primary batches recover one exact receipt without double charging storage', async t => {
  const f = await native(t), root = await f.open();
  const result = await f.call('paired-batch', { proofs: [proof(root), proof(root)] });
  assert.equal(result.count, 2); assert(result.results.every(r => r.ok), JSON.stringify(result));
  assert.equal(result.results.filter(r => r.value.replayed === false).length, 1);
  assert.deepEqual(result.results[0].value.receipt, result.results[1].value.receipt);
  const state = invariant(await f.inspect(), f.options);
  assert.equal(state.messages.length, 1); assert.equal(requests(state, 'ordinary'), 2);
});

test('native D1 concurrent separate requests cannot allocate beyond one shared ordinary work slot', async t => {
  const f = await native(t, { attempts: { ordinary: 1, completion: 3, recovery: 3 } });
  const records = await Promise.all(f.actors.map((a, i) => signed(a, 'intel', { text: String(i) })));
  const results = await Promise.all(records.map(v => f.apply(v)));
  assert.equal(results.filter(r => r.ok).length, 1, JSON.stringify(results));
  assert(results.filter(r => !r.ok).every(r => ['busy', 'attempt_capacity'].includes(r.code)));
  const state = invariant(await f.inspect(), f.options);
  assert.equal(state.messages.length, 1); assert.equal(requests(state, 'ordinary'), 1);
});

test('native D1 work CAS collision is not retried and parsing starts only after a committed charge', async t => {
  const f = await native(t), result = await f.call('charge-pair');
  assert.equal(result.count, 2);
  assert.deepEqual(result.results.map(r => r.code).sort(), ['busy', 'invalid_input']);
  const state = invariant(await f.inspect(), f.options); assert.equal(requests(state, 'ordinary'), 1); assert.equal(state.messages.length, 0);
});

test('native D1 retains close storage and completion/recovery work when ordinary allowance is exhausted', async t => {
  const f = await native(t, { service: { rows: 17, bytes: 1_000_000 }, channel: { rows: 10, bytes: 1_000_000 },
    attempts: { ordinary: 1, completion: 2, recovery: 2 } });
  const root = await f.open(); assert.equal((await f.apply(root)).ok, true);
  assert.equal((await f.apply(await signed(f.actors[1], 'intel', { text: 'ordinary' }))).code, 'attempt_capacity');
  assert.equal((await f.apply(await closing(root, f.actors[0]))).ok, true);
  assert.equal((await f.recover(root)).value.outcome, 'found');
  const state = invariant(await f.inspect(), f.options);
  assert.equal(state.messages.length, 2); assert.equal(state.poll_admission_authority[0].used_rows, 17);
  assert(state.poll_admission_controls[0].reserved_bytes > 0);
  assert.equal(state.poll_admission_controls[0].reserved_rows, 0);
});

test('native D1 storage saturation and insertion peak cannot spend future capture eviction', async t => {
  for (const cap of [17, 20]) await t.test(`service rows ${cap}`, async t => {
    const f = await native(t, { service: { rows: cap, bytes: 1_000_000 }, captureRetention: 1 });
    const root = await f.open(); assert.equal((await f.apply(root)).ok, true);
    const before = domain(await f.inspect());
    assert.equal((await f.apply(await signed(f.actors[1], 'intel', { text: 'no headroom' }))).code, 'storage_capacity');
    assert.deepEqual(domain(await f.inspect()), before);
    assert.equal((await f.apply(await closing(root, f.actors[0]))).ok, true);
    invariant(await f.inspect(), f.options);
  });
});

test('native D1 capture retirement credits its original channel and never retires poll reservations', async t => {
  const f = await native(t, { captureRetention: 1 }, ['general', 'other']), root = await f.open();
  assert.equal((await f.apply(root)).ok, true);
  const reserved = (await f.inspect()).poll_admission_controls[0];
  assert.equal((await f.apply(await signed(f.actors[1], 'intel', { text: 'other' }, { channel: 'other' }))).ok, true);
  const state = invariant(await f.inspect(), f.options);
  assert.equal(state.public_message_arrivals[0].channel, 'other');
  assert.deepEqual(state.poll_admission_controls[0], reserved);
  assert.equal((await f.apply(await closing(root, f.actors[0]))).ok, true);
  invariant(await f.inspect(), f.options);
});

test('native D1 rolls back every partial mutation while preserving already spent attempt work', async t => {
  // Root batch: stage, clock, control, message/captures, receipt, channel, actor, revision, result, final guard.
  for (const failAfter of [0, 1, 2, 3, 4, 5, 6, 7, 8]) await t.test(`failure after statement ${failAfter}`, async t => {
    const f = await native(t), root = await f.open(), before = domain(await f.inspect());
    assert.equal((await f.apply(root, { failAfter })).code, 'unavailable');
    const state = invariant(await f.inspect(), f.options);
    assert.deepEqual(domain(state), before); assert.equal(requests(state, 'ordinary'), 1);
    assert.equal((await f.recover(root)).value.outcome, 'unavailable');
  });
});

test('native D1 final SQL guard rejects silently skipped mutations, plausible bad ledgers and expired decisions', async t => {
  const faults = [
    { skipStatement: 2 }, { skipStatement: 4 }, { skipStatement: 5 }, { skipStatement: 6 }, { skipStatement: 7 },
    { beforeFinal: { sql: 'UPDATE poll_admission_authority SET used_bytes = used_bytes + 1 WHERE id = 1' } },
    { beforeFinal: { sql: `UPDATE poll_admission_guard SET expires_at = ${DB_NOW} - 1 WHERE id = 1` } },
    { beforeFinal: { sql: `UPDATE poll_admission_guard SET vote_deadline = ${DB_NOW} - 1 WHERE id = 1` } },
  ];
  for (const [index, fault] of faults.entries()) await t.test(`guard fault ${index}`, async t => {
    const f = await native(t), root = await f.open(), before = domain(await f.inspect());
    const result = await f.apply(root, fault); assert.equal(result.code, 'unavailable', JSON.stringify(result));
    assert.equal(result.trace.failedBatch, true);
    const state = invariant(await f.inspect(), f.options); assert.deepEqual(domain(state), before); assert.equal(requests(state, 'ordinary'), 1);
  });
});

test('native D1 authority, channel and actor changes cannot refresh an older prepared tally', async t => {
  const edits = [
    "UPDATE channels SET enabled = 0 WHERE name = 'general'",
    "UPDATE channels SET is_private = 1 WHERE name = 'general'",
    'UPDATE agents SET registered_at = registered_at + 1',
    'UPDATE agents SET wake_active = 0',
  ];
  for (const [index, sql] of edits.entries()) await t.test(`snapshot edit ${index}`, async t => {
    const f = await native(t), root = await f.open();
    assert.equal((await f.apply(root, { afterRead: { number: 1, sql } })).code, 'stale');
    const state = invariant(await f.inspect(), f.options); assert.equal(state.messages.length, 0); assert.equal(requests(state, 'ordinary'), 1);
  });
});

test('native D1 rechecks the entire current state in SQL after commit preflight', async t => {
  const f = await native(t), root = await f.open();
  const result = await f.apply(root, { beforeBatch: { sql: 'UPDATE agents SET registered_at = registered_at + 1' } });
  assert.equal(result.code, 'stale');
  const state = invariant(await f.inspect(), f.options); assert.equal(state.messages.length, 0); assert.equal(requests(state, 'ordinary'), 1);
});

test('native D1 protected history SQL returns metadata only at a record or byte overflow', async t => {
  const f = await native(t), root = await f.open();
  assert.equal((await f.apply(root)).ok, true); assert.equal((await f.apply(await ballot(root, f.actors[1]))).ok, true);
  // Exercise the actual protected SELECT with tighter fixture bounds. Admission
  // cannot create an overflowing history under its own unchanged policy.
  for (const limits of [{ records: 2, bytes: 4 * 1024 * 1024 }, { records: 8, bytes: 100 }]) {
    const request = { actor: f.actors[2].agentId, channel: 'general', id: 'new-vote', target: root.id,
      history: 1, capture: 0, inBatch: 0, ...limits };
    const result = await f.sql(SNAPSHOT_SQL, [D1_SCHEMA_JSON, JSON.stringify(request)]);
    assert.equal(result.success, true);
    assert.equal(result.results.filter(r => r.kind === 'bounds').length, 1);
    assert.equal(result.results.filter(r => r.kind === 'root' || r.kind === 'history').length, 0);
  }
  assert.equal((await f.inspect()).messages.length, 2);
});

test('native D1 history reference selection keeps the indexed scalar seek', async t => {
  const f = await native(t);
  const request = { actor: f.actors[0].agentId, channel: 'general', id: 'absent', target: 'absent-root',
    history: 1, capture: 0, inBatch: 0, records: 8, bytes: 4 * 1024 * 1024 };
  const plan = await f.sql('EXPLAIN QUERY PLAN ' + SNAPSHOT_SQL, [D1_SCHEMA_JSON, JSON.stringify(request)]);
  assert(plan.results.some(row => /SEARCH m USING INDEX idx_messages_poll_reference/.test(row.detail) && row.detail.includes('channel=') && row.detail.includes('<expr>=')), JSON.stringify(plan));
});

test('native D1 omitted/altered guard or unexpected application schema fails before spending work', async t => {
  for (const sql of [
    'DROP TRIGGER poll_admission_final_guard',
    'CREATE TABLE sqlitex_shadow (id INTEGER)',
    'DELETE FROM poll_admission_authority WHERE id = 1',
  ]) await t.test(sql.split(' ').slice(0, 3).join(' '), async t => {
    const f = await native(t), root = await f.open(); await f.sql(sql);
    const result = await f.apply(root); assert.equal(result.code, 'authority'); assert.equal(result.trace.charges, 0);
    assert.equal((await f.inspect()).messages.length, 0);
  });
});

test('native D1 construction and failed operations never initialize a missing schema', async t => {
  const f = await native(t, {}, ['general'], false);
  assert.equal((await f.call('constructor')).sessions, 0);
  assert.equal((await f.apply(await f.open())).code, 'unavailable');
  const tables = (await f.sql("SELECT name FROM sqlite_schema WHERE name GLOB 'poll_*' OR name = 'messages'")).results;
  assert.deepEqual(tables, []);
});

test('native D1 configuration generations are pinned and immutable key bindings remain enforced', async t => {
  const f = await native(t), root = await f.open();
  const refused = await f.apply(root, { options: { ...f.options, generation: 'cd'.repeat(32) } });
  assert.equal(refused.code, 'authority'); assert.equal(refused.trace.charges, 0);
  assert.match((await f.sql("UPDATE agents SET public_key = ? WHERE agent_id = ?", ['ef'.repeat(32), f.actors[0].agentId])).fixtureSqlError, /immutable/);
  assert.equal((await f.apply(root)).ok, true);
  await f.sql('UPDATE poll_admission_authority SET generation = ? WHERE id = 1', ['cd'.repeat(32)]);
  assert.equal((await f.recover(root)).code, 'authority');
  assert.equal((await f.inspect()).messages.length, 1);
});

test('native D1 exact receipt recovery requires the original proof and current public visibility', async t => {
  const f = await native(t), root = await f.open(); assert.equal((await f.apply(root)).ok, true);
  assert.equal((await f.recover({ ...root, signature: '00'.repeat(64) })).code, 'signature');
  assert.equal((await f.recover({ ...root, replyToId: 'changed-unsigned-metadata' })).code, 'operation_conflict');
  assert.equal((await f.recover({ ...root, storedSeq: 999, ignoredExtension: true })).value.outcome, 'found');
  const before = (await f.inspect()).poll_admission_authority[0];
  await f.sql("UPDATE channels SET is_private = 1 WHERE name = 'general'");
  assert.equal((await f.recover(root)).value.outcome, 'unavailable');
  const state = invariant(await f.inspect(), f.options);
  assert.equal(state.poll_admission_authority[0].used_bytes, before.used_bytes);
  assert.equal(state.poll_admission_authority[0].reserved_bytes, before.reserved_bytes);
});

test('native D1 encrypted intel receives its initial ACK but is excluded from public capture and recovery', async t => {
  const f = await native(t), env = await signed(f.actors[0], 'intel', 'ab'.repeat(16), { encrypted: true, nonce: 'ab'.repeat(12) });
  const result = await f.apply(env); assert.equal(result.ok, true); assert.equal(result.value.outcome, 'found');
  assert.equal((await f.recover(env)).value.outcome, 'unavailable');
  const state = invariant(await f.inspect(), f.options);
  assert.equal(state.public_message_arrivals.length, 0); assert.equal(state.wake_message_outbox.length, 1);
});

test('native D1 maximum local prepared count and tokens cannot bypass fresh work', async t => {
  const f = await native(t), root = await f.open();
  const result = await f.call('sequence', { actions: [
    ...Array.from({ length: 9 }, () => ({ kind: 'prepare', ...proof(root) })), { kind: 'commit', index: 99 },
    { kind: 'commit', index: 0 }, { kind: 'commit', index: 0 },
  ] });
  assert(result.results.slice(0, 8).every(r => r.ok)); assert.equal(result.results[8].code, 'busy');
  assert.equal(result.results[9].code, 'unprepared'); assert.equal(result.results[10].ok, true); assert.equal(result.results[11].code, 'unprepared');
  assert.equal(requests(invariant(await f.inspect(), f.options), 'ordinary'), 8);
});

test('native D1 an unknown charge or commit poisons all operations and outstanding tokens on that instance', async t => {
  for (const fault of ['charge-before', 'charge-after', 'batch-after']) await t.test(fault, async t => {
    const f = await native(t), root = await f.open();
    const result = await f.call('sequence', { fault, actions: [
      { kind: 'prepare', ...proof(root) }, { kind: 'commit', index: 0 }, { kind: 'prepare', ...proof(root) }, { kind: 'recover', wire: JSON.stringify(root) },
    ] });
    assert(result.results.slice(fault === 'batch-after' ? 1 : 0).every(r => !r.ok && r.code === 'unavailable'), JSON.stringify(result));
    const state = invariant(await f.inspect(), f.options);
    assert.equal(state.messages.length, fault === 'batch-after' ? 1 : 0);
    assert.equal(requests(state, 'ordinary'), fault === 'charge-before' ? 0 : 1);
  });
});

test('native D1 late charge, protected read and write responses cannot return fresh success', async t => {
  for (const fault of ['charge-late', 'read-late', 'batch-late-before', 'batch-late-after']) await t.test(fault, async t => {
    const f = await native(t, { preparationMs: 300 }), root = await f.open();
    const result = await f.apply(root, { fault });
    assert.equal(result.ok, false); assert(['preparation_expired', 'unavailable'].includes(result.code), JSON.stringify(result));
    const state = invariant(await f.inspect(), f.options);
    assert.equal(requests(state, 'ordinary'), 1); assert.equal(state.messages.length, fault === 'batch-late-after' ? 1 : 0);
    if (fault === 'charge-late') assert.equal(result.trace.snapshots.length, 0);
  });
});

test('native D1 clocks advance between SQL statements and durable future clock fails closed', async t => {
  const f = await native(t), clocks = await f.call('clock'); assert(clocks.last > clocks.first);
  await f.sql(`UPDATE poll_admission_authority SET clock = ${DB_NOW} + 10000, window = CAST((${DB_NOW} + 10000) / ? AS INTEGER) WHERE id = 1`, [f.options.policy.windowMs]);
  const result = await f.apply(await f.open()); assert.equal(result.code, 'clock'); assert.equal(result.trace.charges, 0);
});

test('native D1 close versus last ballot preserves both valid serializations', async t => {
  for (const order of [[0, 1], [1, 0]]) await t.test(`close/vote order ${order}`, async t => {
    const f = await native(t, { history: { records: 3, bytes: 4 * 1024 * 1024, nodes: 65536 } }), root = await f.open();
    assert.equal((await f.apply(root)).ok, true);
    const close = await closing(root, f.actors[0]), vote = await ballot(root, f.actors[1]);
    const result = await f.call('paired', { proofs: [proof(close), proof(vote)], order });
    assert.equal(result.results[order[0]].ok, true); assert.equal(result.results[order[1]].code, 'stale');
    if (order[0] === 0) assert.equal((await f.apply(vote)).code, 'poll_closed');
    else assert.equal((await f.apply(close)).ok, true);
    assert.equal((await f.tally(root.id)).closedBy, 'creator'); invariant(await f.inspect(), f.options);
  });
});

test('native D1 every superseded ballot remains retained and counted against history', async t => {
  const f = await native(t, { history: { records: 4, bytes: 4 * 1024 * 1024, nodes: 65536 } }), root = await f.open();
  assert.equal((await f.apply(root)).ok, true);
  const first = await ballot(root, f.actors[1], 0), second = await ballot(root, f.actors[1], 1);
  assert.equal((await f.apply(first)).ok, true); assert.equal((await f.apply(second)).ok, true);
  assert.equal((await f.apply(await ballot(root, f.actors[2]))).code, 'history_capacity');
  assert.equal((await f.apply(await closing(root, f.actors[0]))).ok, true);
  assert.equal((await f.tally(root.id)).countedBallots, 1);
  const state = invariant(await f.inspect(), f.options); assert.equal(state.messages.length, 4); assert.equal(state.poll_admission_receipts.length, 4);
});

test('native D1 all-voted and deadline closure never release the creator reservation', async t => {
  const f = await native(t), root = await f.open(f.actors[0], { electorate: { type: 'list', agentIds: f.actors.slice(1).map(a => a.agentId) }, closes: { allVoted: true } });
  assert.equal((await f.apply(root)).ok, true); const before = (await f.inspect()).poll_admission_controls[0];
  for (const a of f.actors.slice(1)) assert.equal((await f.apply(await ballot(root, a))).ok, true);
  assert.equal((await f.tally(root.id)).closedBy, 'allVoted');
  assert.equal((await f.apply(await closing(root, f.actors[0]))).code, 'poll_closed');
  const state = invariant(await f.inspect(), f.options), control = state.poll_admission_controls[0];
  assert.equal(control.reserved_rows, before.reserved_rows); assert.equal(control.reserved_bytes, before.reserved_bytes); assert.equal(control.reserve_records, 1);
  const expired = await f.open(f.actors[0], { closes: { at: Date.now() - 1 } });
  assert.equal((await f.apply(expired)).ok, true);
  assert.equal((await f.apply(await ballot(expired, f.actors[1]))).code, 'poll_closed');
  assert.equal((await f.apply(await closing(expired, f.actors[0]))).ok, true); invariant(await f.inspect(), f.options);
});

test('native D1 ordinary channel ordering fences a prepared ballot even across another channel', async t => {
  const f = await native(t, {}, ['general', 'other']), root = await f.open(); assert.equal((await f.apply(root)).ok, true);
  const vote = await ballot(root, f.actors[1]), message = await signed(f.actors[2], 'intel', { text: 'ordering' }, { channel: 'other' });
  const result = await f.call('paired', { proofs: [proof(vote), proof(message)], order: [1, 0] });
  assert.equal(result.results[1].ok, true); assert.equal(result.results[0].code, 'stale');
  assert.equal((await f.apply(vote)).value.receipt.storedSeq, 2); invariant(await f.inspect(), f.options);
});

test('native D1 malformed input, invalid signatures and invalid completion routes still spend their own work lane', async t => {
  const f = await native(t), root = await f.open();
  for (const [wire, code] of [['{', 'invalid_input'], ['x'.repeat(f.options.policy.wireBytes + 1), 'input_limit'],
    [JSON.stringify({ ...root, id: root.id + '\n' }), 'invalid_input'],
    [JSON.stringify({ ...root, signature: '00'.repeat(64) }), 'signature']]) {
    const result = await f.call('apply', { wire }); assert.equal(result.code, code); assert.equal(result.trace.batches, 0);
  }
  const mismatch = await f.apply(root, { route: { lane: 'completion', pollId: root.id } }); assert.equal(mismatch.code, 'route');
  const state = invariant(await f.inspect(), f.options); assert.equal(requests(state, 'ordinary'), 4); assert.equal(requests(state, 'completion'), 1);
  const v = JSON.parse(state.poll_admission_authority[0].attempts_json).ordinary;
  assert.equal(v.input, 4 * f.options.policy.wireBytes); assert.equal(v.verification, 4 * (1 + f.options.policy.history.records));
});

test('native D1 unsupported roots, missing controls and raw inserts do not become a second admission API', async t => {
  const f = await native(t), root = await f.open();
  assert.equal((await f.apply(await f.open(f.actors[0], { closePolicy: { creator: false } }))).code, 'unsupported_profile');
  assert.equal((await f.apply(await f.open(f.actors[0], { pollId: root.id }))).code, 'unsupported_profile');
  assert.equal((await f.apply(await ballot(root, f.actors[1]))).code, 'unsupported_history');
  const raw = await f.sql(`INSERT INTO messages (id, channel, sender, type, sequence, stored_seq, timestamp, payload_json, signature, checksum, encrypted)
    VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 0)`, [root.id, root.channel, root.sender, root.type, root.sequence, root.timestamp, JSON.stringify(root.payload), root.signature, root.checksum]);
  assert.match(raw.fixtureSqlError, /unprepared/); assert.equal(invariant(await f.inspect(), f.options).messages.length, 0);
});

test('native D1 work rollover renews only attempts and preserves durable reservation and records', async t => {
  const f = await native(t, { windowMs: 1000, preparationMs: 900, attempts: { ordinary: 1, completion: 3, recovery: 3 } }), root = await f.open();
  // Start well inside a fresh window so the first two operations share it.
  await new Promise(resolve => setTimeout(resolve, 1010 - Date.now() % 1000));
  assert.equal((await f.apply(root)).ok, true); const state = await f.inspect();
  assert.equal((await f.call('apply', { wire: '{' })).code, 'attempt_capacity');
  await new Promise(resolve => setTimeout(resolve, 1010 - Date.now() % 1000));
  assert.equal((await f.call('apply', { wire: '{' })).code, 'invalid_input');
  const after = invariant(await f.inspect(), f.options); assert.equal(requests(after, 'ordinary'), 1);
  assert.deepEqual(domain(after), domain(state));
});

test('native D1 retained wire bytes and hosted reader tally agree with the independent model', async t => {
  const f = await native(t), root = await f.open(f.actors[0], {}, { id: '__proto__' });
  const initial = await f.inspect();
  const model = await createFixture({ ...f.options, now: initial.poll_admission_authority[0].clock,
    agents: initial.agents.map(a => ({ agentId: a.agent_id, publicKey: a.public_key, registeredAt: a.registered_at })) });
  const sequence = [root, await ballot(root, f.actors[1]),
    await signed(f.actors[2], 'intel', { text: 'Unicode 雪🛰️ and quoted "text"', value: 1.25 }),
    await ballot(root, f.actors[1], 1), await closing(root, f.actors[0])];
  for (const env of sequence) {
    const native = await f.apply(env); assert.equal(native.ok, true);
    const now = native.value.receipt.committedAt, writer = model.connect();
    const modeled = writer.commit(await writer.prepare(JSON.stringify(env), route(env), now), now);
    assert.deepEqual(native.value.receipt, modeled.receipt);
    assert.deepEqual(await f.tally(root.id), await model.tally(root.id, now));
  }
  const state = invariant(await f.inspect(), f.options), modeled = model.inspect().state;
  for (const row of state.messages) {
    const env = storedEnvelope(row), expected = modeled.records[env.id];
    for (const key of ['id', 'channel', 'sender', 'type', 'sequence', 'storedSeq', 'timestamp', 'payload', 'checksum', 'signature']) assert.deepEqual(env[key], expected[key]);
    const columns = ['id', 'channel', 'sender', 'type', 'sequence', 'stored_seq', 'timestamp', 'payload_json', 'signature', 'checksum',
      'encrypted', 'reply_to_id', 'recipient_keys_json', 'ephemeral_public_key', 'nonce'];
    assert.equal(row.charge_bytes, 512 + columns.reduce((n, k) => n + (row[k] === null ? 0 : Buffer.byteLength(String(row[k]), 'utf8')), 0));
  }
  assert.deepEqual(state.public_message_arrivals.map(r => r.envelope_id), modeled.arrivals.map(r => r.id));
  assert.deepEqual(state.wake_message_outbox.map(r => r.envelope_id), modeled.wake.map(r => r.id));
  assert.deepEqual(await f.call('hosted-tally', { channel: 'general', id: root.id, now: Date.now() }), await f.tally(root.id));
});
