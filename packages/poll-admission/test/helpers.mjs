import assert from 'node:assert/strict';
import { DatabaseSync } from 'node:sqlite';
import { mkdtempSync, rmSync, statSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { canonicalizeJson, signEnvelope, tallyPoll } from '@openagentforum/protocol';
import { storedEnvelope } from '@openagentforum/server/envelopes';
import { SQLitePollAdmission } from '../dist/sqlite.js';
import { POLL_ADMISSION_SCHEMA } from '../dist/schema.js';
import { FIXED_ROW_BYTES, GUARD_ROW_BYTES, CLOSE_RESERVATION } from '../dist/contract.js';

export const NOW = 1800000000000, ORIGIN = 'https://fixture.invalid';
export const POLICY = Object.freeze({
  service: { rows: 4096, bytes: 32 * 1024 * 1024 }, channel: { rows: 2048, bytes: 16 * 1024 * 1024 },
  history: { records: 8, bytes: 4 * 1024 * 1024, nodes: 65536 },
  wireBytes: 262144, windowMs: 60000, preparationMs: 5000,
  attempts: { ordinary: 32, completion: 8, recovery: 8 }, captureRetention: 4, wakeActive: true,
});
export const options = (overrides = {}) => ({ origin: ORIGIN, generation: 'ab'.repeat(32), policyRevision: 'sqlite-fixture-1',
  policy: { ...structuredClone(POLICY), ...overrides } });
export const route = env => env.type === 'poll' && env.payload?.kind === 'close' ? { lane: 'completion', pollId: env.payload.pollId } : {};
let serial = 0;
export const signed = (actor, type, payload, extra = {}) => signEnvelope({ id: `native-${++serial}`, channel: 'general',
  sender: actor.agentId, type, sequence: 77, timestamp: NOW, payload, ...extra }, actor.signingPrivateKey);
export const opening = (actor, payload = {}, extra = {}) => signed(actor, 'poll', { kind: 'open', title: 'Native fixture poll',
  options: ['A', 'B'], ledger: { hub: ORIGIN }, electorate: { type: 'open' }, closes: { at: NOW + 1000 },
  closePolicy: { creator: true }, rule: { method: 'plurality' }, revote: 'latest', ...payload }, extra);
export const ballot = (root, actor, choice = 0, extra = {}) => signed(actor, 'vote', { pollId: root.id, pollHash: root.checksum, choice }, extra);
export const closing = (root, actor, extra = {}) => signed(actor, 'poll', { kind: 'close', pollId: root.id, pollHash: root.checksum }, extra);
export async function apply(client, env) { return client.commit(await client.prepare(JSON.stringify(env), route(env))); }

export function openConnection(path, clock) {
  const db = new DatabaseSync(path);
  db.exec('PRAGMA journal_mode = WAL; PRAGMA synchronous = FULL; PRAGMA foreign_keys = ON; PRAGMA recursive_triggers = ON; PRAGMA busy_timeout = 1000;');
  if (clock) db.function('julianday', arg => {
    assert.equal(arg, 'now');
    return clock.now / 86400000 + 2440587.5;
  });
  return db;
}
/** Test-owned empty-database provisioning. The runtime adapter has no initializer. */
export function seed(db, config, actors, channels = ['general'], now = NOW) {
  db.exec('BEGIN IMMEDIATE');
  try {
    db.exec(POLL_ADMISSION_SCHEMA);
    for (const actor of actors) db.prepare('INSERT INTO agents VALUES (?, ?, ?, 0, 0, 1)')
      .run(actor.agentId, actor.signingPublicKey, now - 100);
    for (const name of channels) {
      assert.match(name, /^[a-z0-9_-]{1,128}$/);
      assert(!name.startsWith('dm-') && !name.startsWith('vault-'));
      db.prepare(`INSERT INTO channels VALUES (?, 0, 0, '[]', 1, 0, 0, 0, 1, ?, 0, 0)`).run(name, FIXED_ROW_BYTES);
    }
    db.exec('INSERT INTO public_recent_state VALUES (1, 0); INSERT INTO wake_recent_state VALUES (1, 0);');
    const baseRows = 4 + actors.length + channels.length;
    const baseBytes = (3 + actors.length + channels.length) * FIXED_ROW_BYTES + GUARD_ROW_BYTES;
    db.prepare(`INSERT INTO poll_admission_authority VALUES (1, 1, ?, ?, ?, ?, 0, ?, ?, ?, ?, ?, 0, 0)`)
      .run(config.origin, config.generation, config.policyRevision, canonicalizeJson(config.policy), now,
        Math.floor(now / config.policy.windowMs), canonicalizeJson(Object.fromEntries(['ordinary', 'completion', 'recovery']
          .map(lane => [lane, { requests: 0, input: 0, verification: 0 }]))), baseRows, baseBytes);
    db.exec('COMMIT');
  } catch (e) { db.exec('ROLLBACK'); throw e; }
}
export function fixture(t, actors, { policy = {}, channels = ['general'], realClock = false } = {}) {
  const directory = mkdtempSync(join(tmpdir(), 'oaf-poll-sqlite-')), path = join(directory, 'fixture.sqlite');
  const clock = realClock ? undefined : { now: NOW }, config = options(policy), connections = [];
  const connect = () => { const db = openConnection(path, clock); connections.push(db); return db; };
  const db = connect();
  seed(db, config, actors, channels, clock?.now ?? Date.now());
  t.after(() => { for (const connection of connections) { try { connection.close(); } catch {} } rmSync(directory, { recursive: true, force: true }); });
  return { db, path, clock, config, connect, client: (connection = connect()) => new SQLitePollAdmission(connection, config) };
}
const tables = ['agents', 'channels', 'messages', 'poll_admission_controls', 'poll_admission_receipts',
  'public_recent_state', 'wake_recent_state', 'public_message_arrivals', 'wake_message_outbox', 'poll_admission_guard'];
export function domain(db) {
  const meta = { ...db.prepare('SELECT * FROM poll_admission_authority').get() };
  delete meta.attempts_json; delete meta.clock; delete meta.window;
  return { meta, ...Object.fromEntries(tables.map(table => [table, db.prepare(`SELECT * FROM ${table} ORDER BY 1`).all().map(r => ({ ...r }))])) };
}
export const attempts = db => JSON.parse(db.prepare('SELECT attempts_json FROM poll_admission_authority WHERE id = 1').get().attempts_json);
export function invariant(db, config) {
  const state = domain(db), meta = state.meta, p = config.policy;
  assert.equal(state.poll_admission_guard.length, 0);
  assert.equal(db.prepare('PRAGMA foreign_key_check').all().length, 0);
  const dynamic = [state.messages, state.poll_admission_receipts, state.public_message_arrivals, state.wake_message_outbox];
  const baseRows = 4 + state.agents.length + state.channels.length;
  const baseBytes = (3 + state.agents.length + state.channels.length) * FIXED_ROW_BYTES + GUARD_ROW_BYTES;
  assert.equal(meta.used_rows, baseRows + state.poll_admission_controls.length + dynamic.reduce((n, rows) => n + rows.length, 0));
  assert.equal(meta.used_bytes, baseBytes + state.poll_admission_controls.length * FIXED_ROW_BYTES
    + dynamic.flat().reduce((n, row) => n + row.charge_bytes, 0));
  for (const field of ['rows', 'bytes']) {
    assert.equal(meta[`reserved_${field}`], state.poll_admission_controls.reduce((n, row) => n + row[`reserved_${field}`], 0));
    assert(meta[`used_${field}`] + meta[`reserved_${field}`] <= p.service[field]);
  }
  for (const channel of state.channels) {
    const messages = state.messages.filter(m => m.channel === channel.name), ids = new Set(messages.map(m => m.id));
    const rows = [...messages, ...state.poll_admission_receipts.filter(r => r.channel === channel.name),
      ...state.public_message_arrivals.filter(r => r.channel === channel.name), ...state.wake_message_outbox.filter(r => ids.has(r.envelope_id))];
    const controls = state.poll_admission_controls.filter(r => r.channel === channel.name);
    assert.equal(channel.used_rows, 1 + rows.length + controls.length);
    assert.equal(channel.used_bytes, FIXED_ROW_BYTES + rows.reduce((n, r) => n + r.charge_bytes, 0) + controls.length * FIXED_ROW_BYTES);
    assert.equal(channel.message_count, messages.length); assert.equal(channel.high_seq, messages.length);
    assert.equal(new Set(messages.map(m => m.stored_seq)).size, messages.length);
    for (const field of ['rows', 'bytes']) {
      assert.equal(channel[`reserved_${field}`], controls.reduce((n, row) => n + row[`reserved_${field}`], 0));
      assert(channel[`used_${field}`] + channel[`reserved_${field}`] <= p.channel[field]);
    }
  }
  for (const agent of state.agents) assert.equal(agent.message_count, state.messages.filter(m => m.sender === agent.agent_id).length);
  assert.equal(state.messages.length, state.poll_admission_receipts.length);
  for (const control of state.poll_admission_controls) {
    assert.equal(control.history_records, state.messages.filter(m => m.control_id === control.root_id).length);
    assert.equal(control.history_bytes, state.messages.filter(m => m.control_id === control.root_id).reduce((n, m) => n + m.charge_bytes, 0));
    assert(control.reserved_rows <= CLOSE_RESERVATION.rows && control.reserved_bytes <= CLOSE_RESERVATION.bytes);
  }
  assert(state.public_message_arrivals.length <= p.captureRetention && state.wake_message_outbox.length <= p.captureRetention);
  return state;
}
export async function tally(db, rootId, now = NOW) {
  const rows = db.prepare('SELECT * FROM messages ORDER BY stored_seq, id').all();
  const records = rows.map(storedEnvelope), root = records.find(r => r.id === rootId);
  const agents = new Map(db.prepare('SELECT * FROM agents').all().map(a => [a.agent_id, a]));
  return tallyPoll(root, records, async id => agents.get(id)?.public_key,
    { registeredAt: async id => agents.get(id)?.registered_at, now });
}
export function instrument(db, hooks = {}) {
  return new Proxy(db, {
    get(target, property) {
      if (property === 'exec') return sql => { hooks.beforeExec?.(sql); const result = target.exec(sql); hooks.afterExec?.(sql); return result; };
      if (property === 'prepare') return sql => {
        const statement = target.prepare(sql);
        return new Proxy(statement, { get(s, method) {
          const value = Reflect.get(s, method, s);
          if (typeof value !== 'function') return value;
          return (...args) => { hooks.beforeStatement?.(sql, method, args); const result = value.apply(s, args);
            hooks.afterStatement?.(sql, method, args, result); return result; };
        } });
      };
      const value = Reflect.get(target, property, target);
      return typeof value === 'function' ? value.bind(target) : value;
    },
  });
}
export function physicalInventory(db, path) {
  return { pages: db.prepare('PRAGMA page_count').get().page_count, pageSize: db.prepare('PRAGMA page_size').get().page_size,
    databaseBytes: statSync(path).size, walBytes: statSync(path + '-wal').size };
}
