import assert from 'node:assert/strict';
import { mkdtemp, readFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { build } from 'esbuild';
import { Miniflare } from 'miniflare';
import workerd from 'workerd';
import { generateAgentKeyPair, tallyPoll } from '@openagentforum/protocol';
import { storedEnvelope } from '@openagentforum/server/envelopes';
import { options, route, opening, ballot, closing, signed } from './helpers.mjs';
import { FIXED_ROW_BYTES, GUARD_ROW_BYTES } from '../dist/contract.js';

export { route, ballot, closing, signed };
let bundle;
export async function native(t, policy = {}, channels = ['general'], initialize = true) {
  const previousRuntime = process.env.MINIFLARE_WORKERD_PATH;
  process.env.MINIFLARE_WORKERD_PATH = workerd.default;
  const scratch = await mkdtemp(join(tmpdir(), 'oaf-poll-d1-'));
  bundle ??= await build({ entryPoints: [fileURLToPath(new URL('./fixtures/d1-worker.mjs', import.meta.url))],
    bundle: true, write: false, format: 'esm', platform: 'neutral', metafile: true });
  assert(Object.values(bundle.metafile.outputs).every(o => o.imports.length === 0));
  assert(Object.keys(bundle.metafile.inputs).every(path => !/sqlite|noise|libsodium/.test(path)));
  const pages = JSON.parse(await readFile(new URL('../../../apps/web/wrangler.jsonc', import.meta.url), 'utf8'));
  let outbound = 0;
  const config = { host: '127.0.0.1', port: 0, inspectorHost: '127.0.0.1', cf: false,
    telemetry: { enabled: false }, logRequests: false, resourceTmpPath: join(scratch, 'runtime'), resourcePersistencePath: join(scratch, 'state'),
    workers: [{ config: { name: 'poll-admission-test', compatibilityDate: pages.compatibility_date, compatibilityFlags: [],
      workersDev: false, previewUrls: false, domains: [], triggers: [],
      env: { DB: { type: 'd1', id: 'local-poll-admission-test', dev: { remote: false } } },
      manifest: { mainModule: 'index.mjs', modules: { 'index.mjs': { type: 'esm', contents: bundle.outputFiles[0].text } } },
    }, dev: { unsafeRegisterWorker: false, outboundService: { type: 'fetcher', handler() { outbound++; throw new Error('No outbound requests'); } } } }],
  };
  let mf, worker;
  const start = async () => { mf = new Miniflare(config); await mf.ready; worker = await mf.getWorker('poll-admission-test'); };
  t.after(async () => { await mf?.dispose(); await rm(scratch, { recursive: true, force: true });
    if (previousRuntime === undefined) delete process.env.MINIFLARE_WORKERD_PATH; else process.env.MINIFLARE_WORKERD_PATH = previousRuntime;
    assert.equal(outbound, 0); });
  await start();
  const opts = options({ windowMs: 86400000, ...policy }); opts.policyRevision = 'd1-fixture-1';
  const actors = await Promise.all(Array.from({ length: 3 }, () => generateAgentKeyPair()));
  const call = async (path, body = {}) => {
    const text = JSON.stringify({ options: opts, ...body }); assert(Buffer.byteLength(text) <= 2 * 1024 * 1024);
    const response = await worker.fetch(`https://fixture.invalid/test-only/${path}`, { method: 'POST', body: text, signal: AbortSignal.timeout(15000) });
    assert.equal(response.status, 200); assert.equal(outbound, 0); return response.json();
  };
  const now = Date.now();
  if (initialize) await call('init', { actors: actors.map(a => ({ id: a.agentId, key: a.signingPublicKey })), channels, now });
  const apply = (env, extra = {}) => call('apply', { wire: JSON.stringify(env), route: route(env), ...extra });
  const open = (actor = actors[0], data = {}, extra = {}) => opening(actor, { closes: { at: Date.now() + 60000 }, ...data }, { timestamp: Date.now(), ...extra });
  const inspect = () => call('inspect');
  const tally = async id => {
    const state = await inspect(), records = state.messages.map(storedEnvelope), root = records.find(r => r.id === id);
    const agents = new Map(state.agents.map(a => [a.agent_id, a]));
    return tallyPoll(root, records, async id => agents.get(id)?.public_key, { registeredAt: async id => agents.get(id)?.registered_at, now: Date.now() });
  };
  return { options: opts, actors, call, apply, open, inspect, tally,
    recover: (env, extra = {}) => call('recover', { wire: JSON.stringify(env), ...extra }),
    sql: (sql, args = []) => call('sql', { sql, args }), restart: async () => { await mf.dispose(); await start(); } };
}

export function domain(state) {
  const copy = structuredClone(state);
  for (const m of copy.poll_admission_authority) { delete m.clock; delete m.window; delete m.attempts_json; }
  return copy;
}
export function invariant(state, config) {
  assert.deepEqual(state.poll_admission_guard, []); assert.deepEqual(state.foreign_keys, []);
  const meta = state.poll_admission_authority[0], dynamic = [state.messages, state.poll_admission_receipts, state.public_message_arrivals, state.wake_message_outbox];
  assert.equal(meta.used_rows, 4 + state.agents.length + state.channels.length + state.poll_admission_controls.length + dynamic.flat().length);
  assert.equal(meta.used_bytes, (3 + state.agents.length + state.channels.length + state.poll_admission_controls.length) * FIXED_ROW_BYTES
    + GUARD_ROW_BYTES + dynamic.flat().reduce((n, r) => n + r.charge_bytes, 0));
  for (const field of ['rows', 'bytes']) {
    assert.equal(meta[`reserved_${field}`], state.poll_admission_controls.reduce((n, r) => n + r[`reserved_${field}`], 0));
    assert(meta[`used_${field}`] + meta[`reserved_${field}`] <= config.policy.service[field]);
  }
  for (const c of state.channels) {
    const messages = state.messages.filter(m => m.channel === c.name), ids = new Set(messages.map(m => m.id));
    const rows = [...messages, ...state.poll_admission_receipts.filter(r => r.channel === c.name),
      ...state.public_message_arrivals.filter(r => r.channel === c.name), ...state.wake_message_outbox.filter(r => ids.has(r.envelope_id))];
    const controls = state.poll_admission_controls.filter(r => r.channel === c.name);
    assert.equal(c.used_rows, 1 + rows.length + controls.length);
    assert.equal(c.used_bytes, FIXED_ROW_BYTES + controls.length * FIXED_ROW_BYTES + rows.reduce((n, r) => n + r.charge_bytes, 0));
    assert.equal(c.message_count, messages.length); assert.equal(c.high_seq, messages.length);
    assert.equal(new Set(messages.map(m => m.stored_seq)).size, messages.length);
    for (const field of ['rows', 'bytes']) {
      assert.equal(c[`reserved_${field}`], controls.reduce((n, r) => n + r[`reserved_${field}`], 0));
      assert(c[`used_${field}`] + c[`reserved_${field}`] <= config.policy.channel[field]);
    }
  }
  for (const actor of state.agents) assert.equal(actor.message_count, state.messages.filter(m => m.sender === actor.agent_id).length);
  assert.equal(state.messages.length, state.poll_admission_receipts.length);
  assert(state.public_message_arrivals.length <= config.policy.captureRetention && state.wake_message_outbox.length <= config.policy.captureRetention);
  return state;
}
