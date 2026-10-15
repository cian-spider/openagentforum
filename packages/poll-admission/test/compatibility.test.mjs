import test from 'node:test';
import assert from 'node:assert/strict';
import { generateAgentKeyPair } from '@openagentforum/protocol';
import { createSqlPollStore, computeTally } from '@openagentforum/server/polls';
import { storedEnvelope } from '@openagentforum/server/envelopes';
import { createFixture } from '../../../docs/rfc/fixtures/poll-admission-reference.mjs';
import { SQLitePollAdmission } from '../dist/sqlite.js';
import { fixture, opening, ballot, closing, signed, apply, route, invariant, tally, domain, attempts, instrument, NOW } from './helpers.mjs';

const actors = await Promise.all(Array.from({ length: 3 }, () => generateAgentKeyPair()));
const [creator, alice, bob] = actors;

test('native records, receipts and tally match the independent offline model through close and exact recovery', async t => {
  const f = fixture(t, actors);
  const model = await createFixture({ ...f.config, now: NOW,
    agents: actors.map(a => ({ agentId: a.agentId, publicKey: a.signingPublicKey, registeredAt: NOW - 100 })) });
  const root = await opening(creator), first = await ballot(root, alice, 0), second = await ballot(root, alice, 1);
  const sequence = [root, first, await signed(bob, 'intel', { text: 'Unicode 🛰️ and quoted "text"', value: 1.25 }), second, await closing(root, creator)];
  for (const env of sequence) {
    const m = model.connect(), modeled = m.commit(await m.prepare(JSON.stringify(env), route(env), NOW), NOW);
    const native = await apply(f.client(), env);
    assert.deepEqual(native.receipt, modeled.receipt);
    assert.deepEqual(await tally(f.db, root.id), await model.tally(root.id, NOW));
    invariant(f.db, f.config);
  }
  const state = model.inspect().state;
  const records = f.db.prepare('SELECT * FROM messages ORDER BY stored_seq').all().map(storedEnvelope);
  for (const native of records) {
    const saved = state.records[native.id];
    for (const field of ['id', 'channel', 'sender', 'type', 'sequence', 'timestamp', 'payload', 'signature', 'checksum', 'storedSeq']) {
      assert.deepEqual(native[field], saved[field]);
    }
  }
  for (const env of [root, first, second, sequence.at(-1)]) {
    assert.deepEqual(await f.client().recover(JSON.stringify(env)), await model.connect().recover(JSON.stringify(env), NOW));
  }
  assert.deepEqual(f.db.prepare('SELECT envelope_id FROM public_message_arrivals ORDER BY arrival_seq').all().map(r => r.envelope_id), state.arrivals.map(r => r.id));
  assert.deepEqual(f.db.prepare('SELECT envelope_id FROM wake_message_outbox ORDER BY seq').all().map(r => r.envelope_id), state.wake.map(r => r.id));
});

test('the existing hosted SQL reader verifies the native records with unchanged tally semantics', async t => {
  const f = fixture(t, actors), root = await opening(creator); await apply(f.client(), root);
  await apply(f.client(), await ballot(root, alice)); await apply(f.client(), await closing(root, creator));
  const store = createSqlPollStore(async (sql, args) => f.db.prepare(sql).all(...args));
  const saved = await store.getPoll('general', root.id), hosted = await computeTally(store, saved, { now: NOW });
  assert.deepEqual(hosted.tally, await tally(f.db, root.id)); invariant(f.db, f.config);
});

test('SQL byte accounting includes every retained wire column and UTF-8 metadata', async t => {
  const f = fixture(t, actors);
  const env = await signed(alice, 'intel', { text: '雪❄️', escaped: '\n"\\', value: 1.25 },
    { replyToId: 'reference', recipientKeys: { [bob.agentId]: 'wrapped key material' } });
  await apply(f.client(), env);
  const row = f.db.prepare('SELECT * FROM messages').get();
  const retained = [row.id, row.channel, row.sender, row.type, row.sequence, row.stored_seq, row.timestamp,
    row.payload_json, row.signature, row.checksum, row.encrypted, row.reply_to_id, row.recipient_keys_json, row.ephemeral_public_key, row.nonce];
  assert.equal(row.charge_bytes, 512 + retained.reduce((n, value) => n + (value === null ? 0 : Buffer.byteLength(String(value), 'utf8')), 0));
  assert.equal(domain(f.db).public_message_arrivals.length, 0); invariant(f.db, f.config);
});

test('raw SQL cannot insert unadmitted legacy roots or manufacture an opaque preparation', async t => {
  const f = fixture(t, actors), root = await opening(creator), before = domain(f.db);
  assert.throws(() => f.db.prepare(`INSERT INTO messages
    (id, channel, sender, type, sequence, stored_seq, timestamp, payload_json, signature, checksum, encrypted)
    VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?, 0)`).run(root.id, root.channel, root.sender, root.type, root.sequence,
      root.timestamp, JSON.stringify(root.payload), root.signature, root.checksum), /unprepared/);
  await assert.rejects(apply(f.client(), await ballot(root, alice)), /unsupported_history/);
  assert.deepEqual(domain(f.db), before);
});

test('construction does not write schema, seed authority or change connection settings', t => {
  const f = fixture(t, actors), writes = [], db = instrument(f.connect(), {
    beforeExec(sql) { if (!['BEGIN', 'COMMIT', 'ROLLBACK'].includes(sql)) writes.push(sql); },
    beforeStatement(sql) { if (/^(INSERT|UPDATE|DELETE|CREATE|DROP|ALTER)|PRAGMA.*=/i.test(sql)) writes.push(sql); },
  });
  const before = domain(f.db), spent = attempts(f.db);
  new SQLitePollAdmission(db, f.config);
  assert.deepEqual(writes, []); assert.deepEqual(domain(f.db), before); assert.deepEqual(attempts(f.db), spent);
});

test('a late accounting acknowledgment never starts parsing or cryptography', async t => {
  const f = fixture(t, actors, { policy: { preparationMs: 100 } });
  let armed = false, offset = 0, verifications = 0;
  const original = performance.now.bind(performance), verify = crypto.subtle.verify.bind(crypto.subtle);
  t.mock.method(performance, 'now', () => original() + offset);
  t.mock.method(crypto.subtle, 'verify', (...args) => { verifications++; return verify(...args); });
  const db = instrument(f.connect(), { afterExec(sql) { if (armed && sql === 'COMMIT') { armed = false; offset += 101; } } });
  const client = new SQLitePollAdmission(db, f.config), before = domain(f.db);
  armed = true;
  // Invalid JSON would return invalid_input if parsing ran after the late charge.
  await assert.rejects(client.prepare('{'), /preparation_expired/);
  assert.equal(verifications, 0); assert.equal(attempts(f.db).ordinary.requests, 1);
  assert.deepEqual(domain(f.db), before);
});
