import test from 'node:test';
import assert from 'node:assert/strict';
import { generateAgentKeyPair, signEnvelope, verifyEnvelope, tallyPoll } from '../packages/protocol/dist/index.js';
import { createFixture, FIXTURE_POLICY, CLOSE_RESERVATION } from '../docs/rfc/fixtures/poll-admission-reference.mjs';

// Ephemeral local test identities only. Interleavings and database times are explicit.
const [creator, alice, bob] = await Promise.all(Array.from({ length: 3 }, () => generateAgentKeyPair()));
const NOW = 1800000000000, ORIGIN = 'https://fixture.invalid';
let serial = 0;
const wire = env => JSON.stringify(env);
const policy = overrides => ({ ...structuredClone(FIXTURE_POLICY), ...overrides });
const fresh = (overrides = {}) => createFixture({ now: NOW,
  agents: [creator, alice, bob].map(key => ({ agentId: key.agentId, publicKey: key.signingPublicKey, registeredAt: NOW - 100 })), ...overrides });
const signed = (key, type, payload, extra = {}) => signEnvelope({ id: `fixture-${++serial}`,
  channel: 'general', sender: key.agentId, type, sequence: 77, timestamp: NOW, payload, ...extra }, key.signingPrivateKey);
const open = (payload = {}, extra = {}) => signed(creator, 'poll', { kind: 'open', title: 'Fixture poll', options: ['A', 'B'],
  ledger: { hub: ORIGIN }, electorate: { type: 'open' }, closes: { at: NOW + 1000 },
  closePolicy: { creator: true }, rule: { method: 'plurality' }, revote: 'latest', ...payload }, extra);
const vote = (root, key = alice, choice = 0, extra = {}) => signed(key, 'vote', { pollId: root.id, pollHash: root.checksum, choice }, extra);
const close = (root, key = creator, extra = {}) => signed(key, 'poll', { kind: 'close', pollId: root.id, pollHash: root.checksum }, extra);
const route = env => env.type === 'poll' && env.payload.kind === 'close' ? { lane: 'completion', pollId: env.payload.pollId } : {};
async function apply(fixture, env, now = NOW, options) {
  const client = fixture.connect();
  return client.commit(await client.prepare(wire(env), route(env), now), now, options);
}
const domain = fixture => fixture.inspect().state;
function invariant(fixture) {
  const { state, usage, reserved } = fixture.inspect();
  for (const k of ['rows', 'bytes']) {
    assert(usage.service[k] + reserved.service[k] <= state.policy.service[k]);
    for (const name of Object.keys(state.channels)) assert(usage.channels[name][k] + reserved.channels[name][k] <= state.policy.channel[k]);
  }
  assert.equal(Object.keys(state.records).length, Object.keys(state.receipts).length);
  for (const name of Object.keys(state.channels)) {
    const records = Object.values(state.records).filter(r => r.channel === name);
    assert.equal(state.channels[name].messages, records.length);
    assert.equal(new Set(records.map(r => r.storedSeq)).size, records.length);
  }
}

test('root reserves the whole close transaction and keeps the signed wire unchanged', async () => {
  const fixture = await fresh(), root = await open();
  const result = await apply(fixture, { ...root, storedSeq: 999, ignoredExtension: 'discarded' });
  const state = domain(fixture), saved = state.records[root.id];
  assert.equal(result.receipt.storedSeq, 1);
  assert.equal(saved.sequence, 77); assert.equal(saved.storedSeq, 1);
  assert.equal(saved.ignoredExtension, undefined);
  assert.equal((await verifyEnvelope(saved, creator.signingPublicKey)).valid, true);
  assert.deepEqual(state.polls[root.id].completion, CLOSE_RESERVATION);
  assert.equal(state.arrivals[0].observedAt, NOW); assert.equal(state.wake[0].observedAt, NOW);
  assert.equal(state.agents[creator.agentId].messages, 1); invariant(fixture);
});

test('unsupported profiles, wrong origins, fabricated signatures and tokens cannot mutate', async () => {
  const fixture = await fresh(), initial = domain(fixture), client = fixture.connect();
  for (const root of [await open({ closePolicy: { creator: false } }), await open({ ledger: { hub: 'https://other.invalid' } }),
    await open({ pollId: 'legacy-reference' }), { ...await open(), signature: '00'.repeat(64) }]) {
    await assert.rejects(apply(fixture, root), /unsupported_profile|poll_refused|signature/);
  }
  assert.throws(() => client.commit({ authorized: true, cost: 0, tally: {} }, NOW), /unprepared/);
  const token = await client.prepare(wire(await open()), {}, NOW);
  assert.throws(() => client.commit({ ...token }, NOW), /unprepared/);
  assert.throws(() => fixture.connect().commit(token, NOW), /unprepared/);
  assert.deepEqual(domain(fixture), initial);
});

test('invalid input consumes bounded attempt/input/verification work without a storage refund', async () => {
  const fixture = await fresh({ policy: policy({ attempts: { ordinary: 2, completion: 1, recovery: 1 } }) });
  const client = fixture.connect(), initial = domain(fixture);
  await assert.rejects(client.prepare('{', {}, NOW), /invalid_input/);
  await assert.rejects(client.prepare('x'.repeat(FIXTURE_POLICY.wireBytes + 1), {}, NOW), /input_limit/);
  const spent = fixture.inspect().attempts.ordinary;
  assert.deepEqual(spent, { requests: 2, input: 2 * FIXTURE_POLICY.wireBytes, verification: 2 * (1 + FIXTURE_POLICY.history.records) });
  await assert.rejects(client.prepare(wire(await open()), {}, NOW), /attempt_capacity/);
  assert.deepEqual(domain(fixture), initial);
});

test('last unreserved slot races admit one voter; the loser needs a separately charged fresh preparation', async () => {
  for (const order of [[alice, bob], [bob, alice]]) {
    const fixture = await fresh({ policy: policy({ history: { ...FIXTURE_POLICY.history, records: 3 } }) }), root = await open();
    await apply(fixture, root);
    const clients = order.map(() => fixture.connect());
    const envs = await Promise.all(order.map(key => vote(root, key)));
    const tokens = await Promise.all(clients.map((client, i) => client.prepare(wire(envs[i]), {}, NOW)));
    clients[0].commit(tokens[0], NOW);
    const won = domain(fixture);
    assert.throws(() => clients[1].commit(tokens[1], NOW), /stale/);
    assert.deepEqual(domain(fixture), won);
    await assert.rejects(apply(fixture, envs[1]), /history_capacity/);
    await apply(fixture, await close(root));
    assert.equal((await fixture.tally(root.id, NOW)).countedBallots, 1); invariant(fixture);
  }
});

test('close and last ballot races preserve both serializations and completion space', async () => {
  for (const closeFirst of [true, false]) {
    const fixture = await fresh({ policy: policy({ history: { ...FIXTURE_POLICY.history, records: 3 } }) }), root = await open();
    await apply(fixture, root);
    const ballot = await vote(root), ending = await close(root), a = fixture.connect(), b = fixture.connect();
    const [v, c] = await Promise.all([a.prepare(wire(ballot), {}, NOW), b.prepare(wire(ending), route(ending), NOW)]);
    if (closeFirst) {
      b.commit(c, NOW); assert.throws(() => a.commit(v, NOW), /stale/);
      await assert.rejects(apply(fixture, ballot), /poll_closed/);
    } else {
      a.commit(v, NOW); assert.throws(() => b.commit(c, NOW), /stale/);
      await apply(fixture, ending);
    }
    assert.equal((await fixture.tally(root.id, NOW)).closedBy, 'creator'); invariant(fixture);
  }
});

test('superseded revotes keep their records, receipts and charges', async () => {
  const fixture = await fresh({ policy: policy({ history: { ...FIXTURE_POLICY.history, records: 4 } }) }), root = await open();
  await apply(fixture, root);
  const first = await vote(root), second = await vote(root, alice, 1);
  await apply(fixture, first); const before = fixture.inspect().usage.service;
  await apply(fixture, second); const after = fixture.inspect().usage.service;
  assert(after.rows > before.rows && after.bytes > before.bytes);
  const tally = await fixture.tally(root.id, NOW);
  assert.equal(tally.ballots[0].state, 'superseded'); assert.equal(tally.ballots[1].state, 'counted');
  await assert.rejects(apply(fixture, await vote(root, bob)), /history_capacity/);
  await apply(fixture, await close(root)); invariant(fixture);
});

test('ordinary attempts can exhaust while a valid close still uses its finite lane', async () => {
  const fixture = await fresh({ policy: policy({ attempts: { ordinary: 1, completion: 1, recovery: 1 } }) }), root = await open();
  await apply(fixture, root);
  await assert.rejects(apply(fixture, await vote(root)), /attempt_capacity/);
  const ending = await close(root); await apply(fixture, ending);
  assert.equal((await fixture.connect().recover(wire(ending), NOW)).outcome, 'found'); invariant(fixture);
});

test('invalid completion attempts consume finite capacity without borrowing ordinary or recovery work', async () => {
  const fixture = await fresh({ policy: policy({ attempts: { ordinary: 8, completion: 1, recovery: 1 } }) }), root = await open();
  await apply(fixture, root);
  const attacker = await close(root, bob), valid = await close(root), client = fixture.connect();
  await assert.rejects(client.prepare(wire(attacker), route(attacker), NOW), /invalid_close/);
  await assert.rejects(client.prepare(wire(valid), route(valid), NOW), /attempt_capacity/);
  await assert.rejects(client.prepare(wire(valid), {}, NOW), /route/);
  await apply(fixture, await vote(root));
  assert.equal((await client.recover(wire(root), NOW)).outcome, 'found');
  await assert.rejects(client.recover(wire(root), NOW), /attempt_capacity/);
  assert.deepEqual(domain(fixture).polls[root.id].completion, CLOSE_RESERVATION);
});

test('completion routes bind the exact poll and cannot admit or replay ordinary work', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const ending = await close(root), client = fixture.connect();
  await assert.rejects(client.prepare(wire(ending), { lane: 'completion', pollId: 'another-poll' }, NOW), /route/);
  await assert.rejects(client.prepare(wire(root), { lane: 'completion', pollId: root.id }, NOW), /route/);
  assert.equal(domain(fixture).channels.general.messages, 1);
});

test('storage reservations cover capture peak and allow closing at full ordinary row capacity', async () => {
  // Base: 3 authority/cursor + 3 registry + 1 channel rows. Root: 5 rows.
  // Reservation: 4 rows. Nothing remains for another message or poll.
  const fixture = await fresh({ policy: policy({ service: { ...FIXTURE_POLICY.service, rows: 16 } }) }), root = await open();
  await apply(fixture, root);
  const before = domain(fixture);
  await assert.rejects(apply(fixture, await vote(root)), /storage_capacity/);
  await assert.rejects(apply(fixture, await signed(alice, 'intel', 'ordinary')), /storage_capacity/);
  assert.deepEqual(domain(fixture), before);
  await apply(fixture, await close(root));
  assert.equal(fixture.inspect().usage.service.rows, 16);
  assert(fixture.inspect().reserved.service.bytes > 0); invariant(fixture);
});

test('service capacity is shared by keys, channels and independently connected writers', async () => {
  const fixture = await fresh({ channels: ['general', 'other'], policy: policy({ service: { ...FIXTURE_POLICY.service, rows: 17 } }) });
  await apply(fixture, await open());
  await assert.rejects(apply(fixture, await open({}, { channel: 'other' })), /storage_capacity/);
  await assert.rejects(apply(fixture, await signed(bob, 'intel', 'other key', { channel: 'other' })), /storage_capacity/);
  invariant(fixture);
});

test('channel capacity refuses writes even with service capacity remaining', async () => {
  const fixture = await fresh({ policy: policy({ channel: { ...FIXTURE_POLICY.channel, rows: 10 } }) }), root = await open();
  await apply(fixture, root);
  await assert.rejects(apply(fixture, await vote(root)), /storage_capacity/);
  await apply(fixture, await close(root)); invariant(fixture);
});

test('byte capacity includes receipts and completion leftovers, not only envelope JSON', async () => {
  const root = await open(), probe = await fresh(); await apply(probe, root);
  const { usage, reserved } = probe.inspect(), cap = usage.service.bytes + reserved.service.bytes;
  const fixture = await fresh({ policy: policy({ service: { ...FIXTURE_POLICY.service, bytes: cap } }) });
  await apply(fixture, root);
  await assert.rejects(apply(fixture, await vote(root)), /storage_capacity/);
  await apply(fixture, await close(root));
  assert.equal(fixture.inspect().usage.service.bytes + fixture.inspect().reserved.service.bytes, cap);
  invariant(fixture);
});

test('trigger insertion needs transient headroom even when old references will be evicted', async () => {
  const fixture = await fresh({ policy: policy({ captureRetention: 1, service: { ...FIXTURE_POLICY.service, rows: 16 } }) }), root = await open();
  await apply(fixture, root);
  await assert.rejects(apply(fixture, await vote(root)), /storage_capacity/);
  await apply(fixture, await close(root));
  const state = domain(fixture);
  assert.equal(state.arrivals.length, 1); assert.equal(state.wake.length, 1);
  assert.equal(Object.keys(state.records).length, 2); assert.equal(Object.keys(state.receipts).length, 2);
  assert.equal(state.arrivalSeq, 2); invariant(fixture);
});

for (const stage of ['message', 'receipt', 'control', 'counters', 'captures', 'final_guard']) {
  test(`rollback after ${stage} leaves messages, receipts, controls, counters and captures together`, async () => {
    const fixture = await fresh(), root = await open(); await apply(fixture, root);
    for (const env of [await open(), await vote(root), await close(root)]) {
      const client = fixture.connect(), token = await client.prepare(wire(env), route(env), NOW), before = fixture.inspect();
      assert.throws(() => client.commit(token, NOW, { failAt: stage }), /rollback/);
      assert.deepEqual(fixture.inspect(), before);
      assert.throws(() => client.commit(token, NOW), /unprepared/);
    }
    invariant(fixture);
  });
}

test('lost acknowledgment commits once, poisons that writer and recovers exact historical evidence', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const ending = await close(root), client = fixture.connect(), token = await client.prepare(wire(ending), route(ending), NOW);
  assert.throws(() => client.commit(token, NOW, { loseAcknowledgment: true }), /uncertain/);
  const committed = domain(fixture);
  await assert.rejects(client.prepare(wire(ending), route(ending), NOW), /uncertain/);
  await assert.rejects(client.recover(wire(ending), NOW), /uncertain/);
  const restarted = fixture.connect(), recovered = await restarted.recover(wire(ending), NOW);
  assert.equal(recovered.outcome, 'found'); assert.equal(recovered.historical, true);
  assert.deepEqual(recovered.receipt, committed.receipts[ending.id]);
  assert.deepEqual(await apply(fixture, ending), recovered);
  assert.deepEqual(domain(fixture), committed);
  assert.equal((await fixture.tally(root.id, NOW)).closedBy, 'creator'); invariant(fixture);
});

test('exact retry survives closure and ignores assigned cursors/extensions but conflicts on retained metadata', async () => {
  const fixture = await fresh(), root = await open(), ballot = await vote(root);
  await apply(fixture, root); const accepted = await apply(fixture, ballot); await apply(fixture, await close(root));
  const before = domain(fixture);
  assert.deepEqual((await apply(fixture, { ...ballot, storedSeq: 900, extra: 'discarded', encrypted: false })).receipt, accepted.receipt);
  for (const metadata of [{ replyToId: 'different' }, { nonce: '11'.repeat(12) },
    { ephemeralPublicKey: '22'.repeat(32) }, { recipientKeys: { [bob.agentId]: 'ciphertext' } }, { encrypted: true }]) {
    await assert.rejects(apply(fixture, { ...ballot, ...metadata }), /operation_conflict/);
  }
  assert.deepEqual(domain(fixture), before);
});

test('same ID with a different valid signed operation cannot recover a receipt', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const conflict = await signed(creator, 'intel', 'different', { id: root.id });
  await assert.rejects(apply(fixture, conflict), /operation_conflict/);
  await assert.rejects(fixture.connect().recover(wire(conflict), NOW), /operation_conflict/);
});

test('deadline equality permits a ballot; commit after expiry refuses it, but permits creator closure', async () => {
  const fixture = await fresh(), root = await open({ closes: { at: NOW + 10 } }); await apply(fixture, root);
  await apply(fixture, await vote(root), NOW + 10);
  const client = fixture.connect(), late = await vote(root, bob), token = await client.prepare(wire(late), {}, NOW + 10);
  assert.throws(() => client.commit(token, NOW + 11), /poll_closed/);
  await assert.rejects(apply(fixture, late, NOW + 11), /poll_closed/);
  const before = domain(fixture);
  assert.equal((await fixture.tally(root.id, NOW + 12)).closedBy, 'deadline');
  assert.deepEqual(domain(fixture), before);
  await apply(fixture, await close(root), NOW + 12);
  assert.equal((await fixture.tally(root.id, NOW + 12)).closedBy, 'creator'); invariant(fixture);
});

test('all-voted closure retains unused completion capacity and matches the protocol tally', async () => {
  const fixture = await fresh(), root = await open({ electorate: { type: 'list', agentIds: [alice.agentId, bob.agentId] }, closes: { allVoted: true } });
  await apply(fixture, root); await apply(fixture, await vote(root)); await apply(fixture, await vote(root, bob, 1));
  const state = domain(fixture), result = await fixture.tally(root.id, NOW);
  assert.equal(result.closedBy, 'allVoted');
  assert.deepEqual(state.polls[root.id].completion, CLOSE_RESERVATION);
  await assert.rejects(apply(fixture, await close(root)), /poll_closed/);
  const expected = await tallyPoll(state.records[root.id], Object.values(state.records), async id => state.agents[id]?.publicKey,
    { registeredAt: async id => state.agents[id]?.registeredAt, now: NOW });
  assert.deepEqual(result, expected); assert.deepEqual(domain(fixture), state); invariant(fixture);
});

test('electorate, first-vote and root-hash decisions come from verified history', async () => {
  const fixture = await fresh(), root = await open({ revote: 'first', electorate: { type: 'list', agentIds: [creator.agentId, alice.agentId] } });
  await apply(fixture, root); await apply(fixture, await vote(root));
  await assert.rejects(apply(fixture, await vote(root, alice, 1)), /already_voted/);
  await assert.rejects(apply(fixture, await vote(root, bob)), /not_in_electorate/);
  await assert.rejects(apply(fixture, await vote({ ...root, checksum: '00'.repeat(32) }, creator)), /poll_hash_mismatch/);
});

test('newly registered keys cannot join an earlier open electorate', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  fixture.alterForTest(state => { state.agents[alice.agentId].registeredAt = NOW + 1; });
  await assert.rejects(apply(fixture, await vote(root)), /not_in_electorate/);
});

test('ordinary messages share atomic channel ordering with polls', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const a = fixture.connect(), b = fixture.connect(), ballot = await vote(root), message = await signed(bob, 'intel', 'unrelated');
  const [v, m] = await Promise.all([a.prepare(wire(ballot), {}, NOW), b.prepare(wire(message), {}, NOW)]);
  b.commit(m, NOW); assert.throws(() => a.commit(v, NOW), /stale/);
  const result = await apply(fixture, ballot);
  assert.equal(result.receipt.storedSeq, 3); assert.equal(domain(fixture).polls[root.id].revision, 2); invariant(fixture);
});

test('a history change during asynchronous verification cannot attach a newer revision to an old tally', async t => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const a = fixture.connect(), first = await vote(root), second = await vote(root, bob);
  const verify = crypto.subtle.verify.bind(crypto.subtle);
  let release, entered, count = 0;
  const blocked = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  t.mock.method(crypto.subtle, 'verify', async (...args) => {
    const n = ++count, result = await verify(...args);
    if (n === 2) { entered(); await gate; } // Root verification inside the captured tally.
    return result;
  });
  const pending = a.prepare(wire(first), {}, NOW);
  await blocked;
  try { await apply(fixture, second); } finally { release(); }
  const token = await pending, before = domain(fixture);
  assert.throws(() => a.commit(token, NOW), /stale/);
  assert.deepEqual(domain(fixture), before);
});

test('policy, generation, origin, key and channel changes fence prepared operations', async () => {
  for (const change of [s => { s.generation = 'restored-generation'; }, s => { s.policyRevision = 'new-policy'; },
    s => { s.origin = 'https://another.invalid'; }, s => { s.policy.history.records++; },
    s => { s.agents[alice.agentId].publicKey = bob.signingPublicKey; }, s => { s.channels.general.requiresEncryption = true; }]) {
    const fixture = await fresh(), root = await open(); await apply(fixture, root);
    const client = fixture.connect(), token = await client.prepare(wire(await vote(root)), {}, NOW);
    fixture.alterForTest(change); const before = domain(fixture);
    assert.throws(() => client.commit(token, NOW), /authority|stale/);
    assert.deepEqual(domain(fixture), before);
  }
});

test('missing authority and clock rollback fail closed without seeding or replacing state', async () => {
  const fixture = await fresh(), client = fixture.connect(), root = await open(); await apply(fixture, root);
  await assert.rejects(client.prepare(wire(await vote(root)), {}, NOW - 1), /clock/);
  fixture.removeAuthorityForTest();
  await assert.rejects(client.prepare(wire(root), {}, NOW), /authority/);
  await assert.rejects(client.recover(wire(root), NOW), /authority/);
  assert.equal(domain(fixture), null);
});

test('corrupt authority and attempting to adopt a changed generation both fail closed', async () => {
  const fixture = await fresh(), root = await open();
  fixture.alterForTest(state => { state.revision = -2; }); // alter increments to -1.
  await assert.rejects(apply(fixture, root), /authority/);
  const restored = await fresh();
  restored.alterForTest(state => { state.generation = 'restored'; });
  const { origin, generation, policyRevision, policy } = domain(restored);
  await assert.rejects(restored.connect({ origin, generation, policyRevision, policy }).prepare(wire(root), {}, NOW), /authority/);
});

test('replay decisions still require the current full-key binding', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const client = fixture.connect(), retry = await client.prepare(wire(root), {}, NOW);
  fixture.alterForTest(state => { state.agents[creator.agentId].publicKey = bob.signingPublicKey; });
  assert.throws(() => client.commit(retry, NOW), /authority/);
  await assert.rejects(apply(fixture, await signed(alice, 'intel', 'unrelated')), /authority/);
});

test('receipt recovery checks visibility and elapsed database time after asynchronous verification', async t => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const verify = crypto.subtle.verify.bind(crypto.subtle);
  let release, entered, count = 0;
  const blocked = new Promise(resolve => { entered = resolve; });
  const gate = new Promise(resolve => { release = resolve; });
  t.mock.method(crypto.subtle, 'verify', async (...args) => {
    const n = ++count, result = await verify(...args);
    if (n === 1) { entered(); await gate; }
    return result;
  });
  const pending = fixture.connect().recover(wire(root), NOW); await blocked;
  try {
    fixture.alterForTest(state => { state.channels.general.hidden = true; });
    await fixture.tally(root.id, NOW + FIXTURE_POLICY.preparationMs + 1);
  } finally { release(); }
  await assert.rejects(pending, /preparation_expired/);
  const now = NOW + FIXTURE_POLICY.preparationMs + 1;
  assert.deepEqual(await fixture.connect().recover(wire(root), now), { outcome: 'unavailable', historical: true });
});

test('attempt rollover renews only work allowances, never receipts, records or reserved storage', async () => {
  const fixture = await fresh({ policy: policy({ attempts: { ordinary: 1, completion: 1, recovery: 1 } }) }), root = await open({ closes: { at: NOW + 120000 } });
  await apply(fixture, root); const before = domain(fixture);
  await assert.rejects(apply(fixture, await vote(root)), /attempt_capacity/);
  const next = Math.floor(NOW / FIXTURE_POLICY.windowMs) * FIXTURE_POLICY.windowMs + FIXTURE_POLICY.windowMs;
  assert.equal((await fixture.connect().recover(wire(root), next)).outcome, 'found');
  assert.deepEqual(domain(fixture), before);
  await apply(fixture, await vote(root), next); invariant(fixture);
});

test('preparations expire and cannot be reused after a refused commit', async () => {
  const fixture = await fresh(), client = fixture.connect(), root = await open(), token = await client.prepare(wire(root), {}, NOW);
  assert.throws(() => client.commit(token, NOW + FIXTURE_POLICY.preparationMs + 1), /preparation_expired/);
  assert.throws(() => client.commit(token, NOW + FIXTURE_POLICY.preparationMs + 1), /unprepared/);
  assert.equal(Object.keys(domain(fixture).records).length, 0);
});

test('root readability includes reserved close bytes and nodes, depth, entries and key length', async () => {
  for (const history of [{ ...FIXTURE_POLICY.history, bytes: 3000 }, { ...FIXTURE_POLICY.history, nodes: 33 }]) {
    const fixture = await fresh({ policy: policy({ history }) }), before = domain(fixture);
    await assert.rejects(apply(fixture, await open()), /history_capacity/); assert.deepEqual(domain(fixture), before);
  }
  let deep = null; for (let i = 0; i < 18; i++) deep = { nested: deep };
  for (const extra of [deep, Array(1025).fill(null), { ['x'.repeat(257)]: 1 }]) {
    const fixture = await fresh();
    await assert.rejects(apply(fixture, await open({ extension: extra })), /input_limit/);
    assert.equal(Object.keys(domain(fixture).records).length, 0);
  }
});

test('new action padding is refused before history work or storage mutation', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  const before = domain(fixture);
  const padded = await signed(alice, 'vote', { pollId: root.id, pollHash: root.checksum, choice: 0, extra: 'padding' });
  await assert.rejects(apply(fixture, padded), /input_limit/);
  await assert.rejects(apply(fixture, { ...await vote(root), replyToId: 'x'.repeat(2048) }), /input_limit/);
  assert.deepEqual(domain(fixture), before);
});

test('legacy rows without admitted controls cannot be adopted or used to seed a reservation', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  fixture.alterForTest(state => { delete state.polls[root.id]; }); const before = domain(fixture);
  await assert.rejects(apply(fixture, await vote(root)), /unsupported_history/);
  await assert.rejects(apply(fixture, await close(root)), /unsupported_history/);
  await assert.rejects(apply(fixture, await signed(alice, 'intel', 'cannot spend the lost reservation')), /authority/);
  assert.deepEqual(domain(fixture), before);
});

test('current visibility gates historical recovery and never releases authority or capture positions', async () => {
  const fixture = await fresh(), root = await open(); await apply(fixture, root);
  fixture.alterForTest(state => { state.channels.general.hidden = true; }); const before = domain(fixture);
  const result = await fixture.connect().recover(wire(root), NOW);
  assert.deepEqual(result, { outcome: 'unavailable', historical: true });
  assert.deepEqual(domain(fixture), before); assert.equal(before.arrivalSeq, 1);
});

test('encrypted/nonpublic records never advance the public arrival sequence; wake stays independent', async () => {
  const fixture = await fresh();
  const message = await signed(alice, 'intel', '00'.repeat(32), { encrypted: true, nonce: '00'.repeat(12) });
  await apply(fixture, message);
  const state = domain(fixture);
  assert.equal(state.arrivalSeq, 0); assert.equal(state.arrivals.length, 0);
  assert.equal(state.wake.length, 1); assert.equal(state.wakeSeq, 1);
  assert.equal((await fixture.connect().recover(wire(message), NOW)).outcome, 'unavailable');
  invariant(fixture);
});

test('prototype-like identifiers remain ordinary owned keys and unknown channels stay unsupported', async () => {
  const fixture = await fresh({ channels: ['general', 'constructor', '__proto__'] });
  for (const name of ['constructor', '__proto__']) {
    const root = await open({}, { channel: name, id: name }); await apply(fixture, root);
    assert.equal(domain(fixture).records[name].id, name);
    await apply(fixture, await close(root, creator, { channel: name }));
  }
  await assert.rejects(apply(fixture, await open({}, { channel: 'unknown' })), /channel/); invariant(fixture);
});
