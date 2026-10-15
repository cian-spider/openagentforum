import test from 'node:test';
import assert from 'node:assert/strict';
import { generateAgentKeyPair, verifyEnvelope } from '@openagentforum/protocol';
import { storedEnvelope } from '@openagentforum/server/envelopes';
import { SQLitePollAdmission } from '../dist/sqlite.js';
import { CLOSE_RESERVATION } from '../dist/contract.js';
import { fixture, opening, ballot, closing, signed, apply, route, domain, invariant, attempts, tally, instrument, POLICY, NOW } from './helpers.mjs';

const actors = await Promise.all(Array.from({ length: 3 }, () => generateAgentKeyPair()));
const [creator, alice, bob] = actors;

test('native root admission retains signed fields and reserves a complete creator close', async t => {
  const f = fixture(t, actors), root = await opening(creator), client = f.client();
  const result = await apply(client, { ...root, storedSeq: 999, discardedExtension: true });
  assert.equal(result.receipt.storedSeq, 1);
  const state = invariant(f.db, f.config), saved = storedEnvelope(state.messages[0]), control = state.poll_admission_controls[0];
  assert.equal(saved.sequence, 77); assert.equal(saved.storedSeq, 1);
  assert.equal((await verifyEnvelope(saved, creator.signingPublicKey)).valid, true);
  assert.equal(control.reserved_rows, CLOSE_RESERVATION.rows); assert.equal(control.reserved_bytes, CLOSE_RESERVATION.bytes);
  assert.equal(state.public_message_arrivals[0].arrived_at, NOW); assert.equal(state.wake_message_outbox[0].stored_at, NOW);
  assert.equal((await tally(f.db, root.id)).closedAtSeq, null);
});

test('independent SQLite connections race the last ballot slot in both orders', async t => {
  for (const order of [[alice, bob], [bob, alice]]) {
    const f = fixture(t, actors, { policy: { history: { ...POLICY.history, records: 3 } } }), root = await opening(creator);
    await apply(f.client(), root);
    const clients = order.map(() => f.client()), wires = await Promise.all(order.map(a => ballot(root, a)));
    const tokens = await Promise.all(clients.map((client, i) => client.prepare(JSON.stringify(wires[i]))));
    clients[0].commit(tokens[0]); const won = domain(f.db);
    assert.throws(() => clients[1].commit(tokens[1]), /stale/); assert.deepEqual(domain(f.db), won);
    await assert.rejects(apply(clients[1], wires[1]), /history_capacity/);
    await apply(f.client(), await closing(root, creator));
    assert.equal((await tally(f.db, root.id)).countedBallots, 1); invariant(f.db, f.config);
  }
});

test('close racing the last ballot permits both serializations with no double allocation', async t => {
  for (const closeFirst of [true, false]) {
    const f = fixture(t, actors, { policy: { history: { ...POLICY.history, records: 3 } } }), root = await opening(creator);
    await apply(f.client(), root);
    const vote = await ballot(root, alice), end = await closing(root, creator), a = f.client(), b = f.client();
    const [v, c] = await Promise.all([a.prepare(JSON.stringify(vote)), b.prepare(JSON.stringify(end), route(end))]);
    if (closeFirst) {
      b.commit(c); assert.throws(() => a.commit(v), /stale/);
      await assert.rejects(apply(a, vote), /poll_closed/);
    } else {
      a.commit(v); assert.throws(() => b.commit(c), /stale/); await apply(b, end);
    }
    assert.equal((await tally(f.db, root.id)).closedBy, 'creator'); invariant(f.db, f.config);
  }
});

test('native attempts charge input and maximum verification before malformed work', async t => {
  const f = fixture(t, actors, { policy: { attempts: { ordinary: 2, completion: 1, recovery: 1 } } });
  const client = f.client(), before = domain(f.db);
  await assert.rejects(client.prepare('{'), /invalid_input/);
  await assert.rejects(client.prepare('x'.repeat(POLICY.wireBytes + 1)), /input_limit/);
  assert.deepEqual(attempts(f.db).ordinary, { requests: 2, input: 2 * POLICY.wireBytes, verification: 2 * (1 + POLICY.history.records) });
  await assert.rejects(apply(f.client(), await opening(creator)), /attempt_capacity/);
  assert.deepEqual(domain(f.db), before);
});

test('finite close and recovery lanes survive ordinary exhaustion without lending allowance', async t => {
  const f = fixture(t, actors, { policy: { attempts: { ordinary: 1, completion: 2, recovery: 1 } } }), root = await opening(creator);
  await apply(f.client(), root);
  await assert.rejects(apply(f.client(), await ballot(root, alice)), /attempt_capacity/);
  const end = await closing(root, creator);
  await assert.rejects(f.client().prepare(JSON.stringify(root), { lane: 'completion', pollId: root.id }), /route/);
  await apply(f.client(), end);
  await assert.rejects(apply(f.client(), end), /attempt_capacity/);
  assert.equal((await f.client().recover(JSON.stringify(end))).outcome, 'found');
  await assert.rejects(f.client().recover(JSON.stringify(end)), /attempt_capacity/);
  invariant(f.db, f.config);
});

test('shared row capacity preserves reserved close space and capture insertion peak', async t => {
  const f = fixture(t, actors, { policy: { service: { ...POLICY.service, rows: 17 }, captureRetention: 1 } });
  const root = await opening(creator); await apply(f.client(), root); const before = domain(f.db);
  await assert.rejects(apply(f.client(), await ballot(root, alice)), /storage_capacity/);
  await assert.rejects(apply(f.client(), await signed(bob, 'intel', 'ordinary')), /storage_capacity/);
  assert.deepEqual(domain(f.db), before);
  await apply(f.client(), await closing(root, creator));
  const state = invariant(f.db, f.config);
  assert.equal(state.messages.length, 2); assert.equal(state.poll_admission_receipts.length, 2);
  assert.equal(state.public_message_arrivals.length, 1); assert.equal(state.wake_message_outbox.length, 1);
  assert.equal(state.public_recent_state[0].high_seq, 2);
  assert(state.meta.reserved_bytes > 0);
});

test('byte capacity includes receipts and close leftovers; channel limits remain independent', async t => {
  const root = await opening(creator), probe = fixture(t, actors); await apply(probe.client(), root);
  const state = domain(probe.db), cap = state.meta.used_bytes + state.meta.reserved_bytes;
  for (const policy of [{ service: { ...POLICY.service, bytes: cap } }, { channel: { ...POLICY.channel, rows: 10 } }]) {
    const f = fixture(t, actors, { policy }); await apply(f.client(), root);
    await assert.rejects(apply(f.client(), await ballot(root, alice)), /storage_capacity/);
    await apply(f.client(), await closing(root, creator)); invariant(f.db, f.config);
    if (policy.service) assert.equal(domain(f.db).meta.used_bytes + domain(f.db).meta.reserved_bytes, cap);
  }
});

test('service storage is shared across keys and channels', async t => {
  const f = fixture(t, actors, { channels: ['general', 'other'], policy: { service: { ...POLICY.service, rows: 18 } } });
  const root = await opening(creator); await apply(f.client(), root);
  await assert.rejects(apply(f.client(), await opening(bob, {}, { channel: 'other' })), /storage_capacity/);
  await assert.rejects(apply(f.client(), await signed(alice, 'intel', 'another key', { channel: 'other' })), /storage_capacity/);
  await apply(f.client(), await closing(root, creator)); invariant(f.db, f.config);
});

test('capture retirement credits the original channel and never deletes messages or receipts', async t => {
  const f = fixture(t, actors, { channels: ['general', 'other'], policy: { captureRetention: 1 } });
  await apply(f.client(), await signed(alice, 'intel', 'first'));
  await apply(f.client(), await signed(bob, 'intel', 'second', { channel: 'other' }));
  const state = invariant(f.db, f.config);
  assert.equal(state.channels.find(c => c.name === 'general').used_rows, 3);
  assert.equal(state.channels.find(c => c.name === 'other').used_rows, 5);
  assert.equal(state.messages.length, 2); assert.equal(state.poll_admission_receipts.length, 2);
});

test('revotes retain every row and receipt while the protocol tally counts the selected vote', async t => {
  const f = fixture(t, actors, { policy: { history: { ...POLICY.history, records: 4 } } }), root = await opening(creator);
  await apply(f.client(), root); await apply(f.client(), await ballot(root, alice, 0)); await apply(f.client(), await ballot(root, alice, 1));
  await assert.rejects(apply(f.client(), await ballot(root, alice, 0)), /history_capacity/);
  await apply(f.client(), await closing(root, creator));
  assert.equal((await tally(f.db, root.id)).countedBallots, 1);
  assert.equal(invariant(f.db, f.config).messages.length, 4);
});

test('exact retries after close return the original receipt and conflict on retained unsigned metadata', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const vote = await ballot(root, alice), accepted = await apply(f.client(), vote); await apply(f.client(), await closing(root, creator));
  const before = domain(f.db);
  assert.deepEqual((await apply(f.client(), { ...vote, storedSeq: 999, ignored: 'extension', encrypted: false })).receipt, accepted.receipt);
  for (const extra of [{ replyToId: 'different' }, { nonce: '11'.repeat(12) }, { ephemeralPublicKey: '22'.repeat(32) },
    { recipientKeys: { [bob.agentId]: 'ciphertext' } }, { encrypted: true }]) {
    await assert.rejects(apply(f.client(), { ...vote, ...extra }), /operation_conflict/);
  }
  const conflict = await signed(alice, 'intel', 'different signed operation', { id: vote.id });
  await assert.rejects(f.client().recover(JSON.stringify(conflict)), /operation_conflict/);
  assert.deepEqual(domain(f.db), before); invariant(f.db, f.config);
});

test('same exact operation prepared twice commits once and replays without allocating a sequence', async t => {
  const f = fixture(t, actors), root = await opening(creator), a = f.client(), b = f.client();
  const [one, two] = await Promise.all([a.prepare(JSON.stringify(root)), b.prepare(JSON.stringify(root))]);
  const first = a.commit(one), second = b.commit(two);
  assert.deepEqual(first.receipt, second.receipt); assert.equal(second.replayed, undefined);
  assert.equal(invariant(f.db, f.config).messages.length, 1);
});

test('deadline equality accepts votes, expiry refuses prepared votes and still permits creator closure', async t => {
  const f = fixture(t, actors), root = await opening(creator, { closes: { at: NOW + 10 } }); await apply(f.client(), root);
  f.clock.now = NOW + 10; await apply(f.client(), await ballot(root, alice));
  const b = f.client(), vote = await ballot(root, bob), token = await b.prepare(JSON.stringify(vote));
  f.clock.now++; assert.throws(() => b.commit(token), /poll_closed/);
  await assert.rejects(apply(f.client(), vote), /poll_closed/);
  await apply(f.client(), await closing(root, creator)); assert.equal((await tally(f.db, root.id, f.clock.now)).closedBy, 'creator');
  invariant(f.db, f.config);
});

test('all-voted closure retains unused reservations and fresh creator close is refused', async t => {
  const f = fixture(t, actors), root = await opening(creator, { electorate: { type: 'list', agentIds: [alice.agentId, bob.agentId] }, closes: { allVoted: true } });
  await apply(f.client(), root); await apply(f.client(), await ballot(root, alice)); await apply(f.client(), await ballot(root, bob, 1));
  assert.equal((await tally(f.db, root.id)).closedBy, 'allVoted');
  await assert.rejects(apply(f.client(), await closing(root, creator)), /poll_closed/);
  const state = invariant(f.db, f.config); assert.equal(state.meta.reserved_bytes, CLOSE_RESERVATION.bytes);
});

test('ordinary channel ordering fences an asynchronously prepared ballot', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const a = f.client(), b = f.client(), vote = await ballot(root, alice), message = await signed(bob, 'intel', 'ordinary');
  const [v, m] = await Promise.all([a.prepare(JSON.stringify(vote)), b.prepare(JSON.stringify(message))]);
  b.commit(m); assert.throws(() => a.commit(v), /stale/);
  assert.equal((await apply(a, vote)).receipt.storedSeq, 3); invariant(f.db, f.config);
});

for (const stage of ['control', 'message', 'receipt', 'counters', 'final_guard']) {
  test(`native rollback after ${stage} retains only the already committed attempt charge`, async t => {
    const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
    const statements = { control: /^(INSERT INTO|UPDATE) poll_admission_controls/, message: /^INSERT INTO messages/,
      receipt: /^INSERT INTO poll_admission_receipts/, counters: /^UPDATE agents SET message_count/, final_guard: /^DELETE FROM poll_admission_guard/ };
    for (const env of [await opening(creator), await ballot(root, alice), await closing(root, creator)]) {
      let armed = false;
      const db = instrument(f.connect(), { afterStatement(sql, method) {
        if (armed && method === 'run' && statements[stage].test(sql)) { armed = false; throw new Error('fixture driver failure'); }
      } });
      const client = new SQLitePollAdmission(db, f.config), token = await client.prepare(JSON.stringify(env), route(env));
      const before = domain(f.db), charged = attempts(f.db); armed = true;
      assert.throws(() => client.commit(token), /unavailable/);
      assert.deepEqual(domain(f.db), before); assert.deepEqual(attempts(f.db), charged);
      await assert.rejects(client.recover(JSON.stringify(env)), /unavailable/); invariant(f.db, f.config);
    }
  });
}

test('lost commit acknowledgment poisons the writer and a fresh connection recovers exactly once', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  const end = await closing(root, creator); let armed = false;
  const db = instrument(f.connect(), { afterExec(sql) { if (armed && sql === 'COMMIT') { armed = false; throw new Error('lost acknowledgment'); } } });
  const client = new SQLitePollAdmission(db, f.config), token = await client.prepare(JSON.stringify(end), route(end)); armed = true;
  assert.throws(() => client.commit(token), /unavailable/); const committed = domain(f.db);
  await assert.rejects(client.prepare(JSON.stringify(end), route(end)), /unavailable/);
  const recovered = await f.client().recover(JSON.stringify(end)); assert.equal(recovered.outcome, 'found');
  assert.deepEqual((await apply(f.client(), end)).receipt, recovered.receipt);
  assert.deepEqual(domain(f.db), committed); invariant(f.db, f.config);
});

test('the last SQL guard rolls back a deadline crossed after application checks and captures', async t => {
  const f = fixture(t, actors), root = await opening(creator, { closes: { at: NOW + 10 } }); await apply(f.client(), root);
  const vote = await ballot(root, alice); let armed = false;
  const db = instrument(f.connect(), { beforeStatement(sql, method) {
    if (armed && method === 'run' && sql.startsWith('DELETE FROM poll_admission_guard')) { armed = false; f.clock.now = NOW + 11; }
  } });
  const client = new SQLitePollAdmission(db, f.config), token = await client.prepare(JSON.stringify(vote)), before = domain(f.db);
  armed = true; assert.throws(() => client.commit(token), /unavailable/);
  assert.deepEqual(domain(f.db), before); await apply(f.client(), await closing(root, creator)); invariant(f.db, f.config);
});

test('the final SQL guard rejects plausible but incorrect ledger/counter writes in the transaction', async t => {
  for (const mutation of [
    db => db.exec('UPDATE poll_admission_authority SET used_rows = used_rows + 1'),
    db => db.exec('UPDATE channels SET high_seq = high_seq + 1, message_count = message_count + 1'),
    db => db.prepare('UPDATE agents SET message_count = message_count + 1 WHERE agent_id = ?').run(creator.agentId),
  ]) {
    const f = fixture(t, actors), raw = f.connect(); let armed = false;
    const db = instrument(raw, { beforeStatement(sql, method) {
      if (armed && method === 'run' && sql === 'DELETE FROM poll_admission_guard WHERE id = 1') { armed = false; mutation(raw); }
    } });
    const client = new SQLitePollAdmission(db, f.config), token = await client.prepare(JSON.stringify(await opening(creator)));
    const before = domain(f.db); armed = true;
    assert.throws(() => client.commit(token), /unavailable/); assert.deepEqual(domain(f.db), before); invariant(f.db, f.config);
  }
});

test('the final SQL guard also refuses a root whose preparation expires during synchronous storage work', async t => {
  const f = fixture(t, actors); let armed = false;
  const db = instrument(f.connect(), { beforeStatement(sql, method) {
    if (armed && method === 'run' && sql === 'DELETE FROM poll_admission_guard WHERE id = 1') { armed = false; f.clock.now += POLICY.preparationMs + 1; }
  } });
  const client = new SQLitePollAdmission(db, f.config), token = await client.prepare(JSON.stringify(await opening(creator)));
  const before = domain(f.db); armed = true;
  assert.throws(() => client.commit(token), /unavailable/); assert.deepEqual(domain(f.db), before); invariant(f.db, f.config);
});
