// Offline executable specification for #238. No HTTP, SQL, public export or I/O.
// The fixture owns the authoritative snapshot and clock. This is not a server.
import { canonicalizeJson, verifyEnvelope, deriveAgentId, tallyPoll, checkPollIngest,
  checkVoteIngest } from '../../../packages/protocol/dist/index.js';
import { fitsPollActionInput, POLL_ACTION_LIMITS } from '../../../packages/server/dist/poll-input.js';
import { encryptionError, sameStoredEnvelope } from '../../../packages/server/dist/envelopes.js';
import { POLL_WORK_LIMITS } from '../../../packages/server/dist/poll-store.js';

const bytes = text => Buffer.byteLength(text, 'utf8');
const copy = value => structuredClone(value);
const integer = value => Number.isSafeInteger(value) && value >= 0;
const requireThat = (ok, code) => { if (!ok) throw new Error(code); };
const freeze = value => {
  if (value && typeof value === 'object') { Object.values(value).forEach(freeze); Object.freeze(value); }
  return value;
};
const equal = (a, b) => canonicalizeJson(a) === canonicalizeJson(b);
const own = (value, key) => Object.hasOwn(value, key) ? value[key] : undefined;
const zero = () => ({ rows: 0, bytes: 0 });
const add = (a, b) => ({ rows: a.rows + b.rows, bytes: a.bytes + b.bytes });
const subtract = (a, b) => ({ rows: a.rows - b.rows, bytes: a.bytes - b.bytes });
const within = (a, b) => Object.keys(a).every(k => integer(a[k]) && a[k] <= b[k]);
const FIXED_ROW_BYTES = 4096; // Preallocated logical authority/counter/control row.
const RECEIPT_BYTES = 4096;
const CAPTURE_BYTES = 1024;
const CLOSE_BYTES = POLL_ACTION_LIMITS.envelopeBytes + 512 + 16; // Include assigned stored_seq.
export const CLOSE_RESERVATION = freeze({ rows: 4, bytes: CLOSE_BYTES + RECEIPT_BYTES + 2 * CAPTURE_BYTES });
const CLOSE_HISTORY = freeze({ records: 1, bytes: CLOSE_BYTES, nodes: POLL_ACTION_LIMITS.nodes });

// Deliberately small synthetic limits. These are not proposed production quotas.
export const FIXTURE_POLICY = freeze({
  service: { rows: 4096, bytes: 32 * 1024 * 1024 },
  channel: { rows: 2048, bytes: 16 * 1024 * 1024 },
  history: { records: 8, bytes: POLL_WORK_LIMITS.bytes, nodes: POLL_WORK_LIMITS.nodes },
  wireBytes: 262144, windowMs: 60000, preparationMs: 5000,
  attempts: { ordinary: 32, completion: 8, recovery: 8 },
  captureRetention: 4, wakeActive: true,
});

function tree(value) {
  const pending = [[value, 0]];
  let nodes = 0;
  while (pending.length) {
    const [v, depth] = pending.pop();
    requireThat(++nodes <= POLL_WORK_LIMITS.recordNodes && depth <= POLL_WORK_LIMITS.depth, 'input_limit');
    if (typeof v === 'number') requireThat(Number.isFinite(v), 'invalid_input');
    if (v !== null && typeof v === 'object') {
      const keys = Object.keys(v);
      requireThat(keys.length <= POLL_WORK_LIMITS.entries, 'input_limit');
      for (const key of keys) {
        requireThat(key.length <= POLL_WORK_LIMITS.propertyLength, 'input_limit');
        pending.push([v[key], depth + 1]);
      }
    }
  }
  return nodes;
}
function parse(wire, policy) {
  requireThat(typeof wire === 'string' && bytes(wire) <= policy.wireBytes, 'input_limit');
  // No recursive canonicalization until size and parsed tree bounds pass.
  let env;
  try { env = JSON.parse(wire); } catch { throw new Error('invalid_input'); }
  tree(env);
  requireThat(env && !Array.isArray(env) && typeof env === 'object', 'invalid_input');
  requireThat(typeof env.id === 'string' && /^[a-zA-Z0-9_:-]{1,128}$/.test(env.id)
    && typeof env.channel === 'string' && /^[a-z0-9_-]{1,128}$/.test(env.channel)
    && /^agent_[0-9a-f]{16}$/.test(env.sender) && integer(env.sequence) && integer(env.timestamp)
    && /^[a-f0-9]{64}$/i.test(env.checksum) && /^[a-f0-9]{128}$/i.test(env.signature), 'invalid_input');
  requireThat(['poll', 'vote', 'intel'].includes(env.type) && env.payload !== undefined, 'unsupported_profile');
  return env;
}
function retained(env, storedSeq) {
  const out = {};
  for (const key of ['id', 'channel', 'sender', 'type', 'sequence', 'timestamp', 'payload', 'signature', 'checksum']) out[key] = copy(env[key]);
  out.encrypted = env.encrypted === true;
  for (const key of ['replyToId', 'nonce', 'ephemeralPublicKey', 'recipientKeys']) out[key] = env[key] ?? null;
  return { ...out, storedSeq };
}
function recordCost(env) {
  // Same scalar/blob column representation as the hosted SQL history reader.
  const columns = [env.id, env.channel, env.sender, env.type, env.sequence, env.storedSeq, env.timestamp,
    JSON.stringify(env.payload), env.signature, env.checksum, Number(env.encrypted), env.replyToId,
    env.recipientKeys === null ? null : JSON.stringify(env.recipientKeys), env.ephemeralPublicKey, env.nonce];
  return 512 + columns.reduce((n, v) => n + (v === null ? 0 : bytes(String(v))), 0);
}
const rowCost = row => 512 + bytes(JSON.stringify(row));
function publicRecord(state, env) {
  const ch = own(state.channels, env.channel);
  return ch && ch.public && !ch.requiresEncryption && !ch.hidden && !env.encrypted
    && !env.nonce && !env.ephemeralPublicKey && env.recipientKeys === null;
}
function exact(saved, incoming) {
  const signed = env => Object.fromEntries(['id', 'channel', 'sender', 'type', 'sequence', 'timestamp', 'checksum', 'payload']
    .map(k => [k, env[k]]));
  return equal(signed(saved), signed(incoming)) && sameStoredEnvelope(saved, incoming);
}
function usage(state) {
  // Fixed rows cover authority, both capture cursors, every pinned registry entry,
  // and channel ordering/counts. Poll controls have preallocated revision fields.
  const perChannel = Object.fromEntries(Object.keys(state.channels).map(k => [k, { rows: 1, bytes: FIXED_ROW_BYTES }]));
  let service = { rows: 3 + Object.keys(state.agents).length, bytes: (3 + Object.keys(state.agents).length) * FIXED_ROW_BYTES };
  for (const env of Object.values(state.records)) {
    perChannel[env.channel] = add(perChannel[env.channel], { rows: 2, bytes: recordCost(env) + rowCost(state.receipts[env.id]) });
  }
  for (const control of Object.values(state.polls)) {
    perChannel[control.channel] = add(perChannel[control.channel], { rows: 1, bytes: FIXED_ROW_BYTES });
  }
  for (const capture of [...state.arrivals, ...state.wake]) {
    perChannel[capture.channel] = add(perChannel[capture.channel], { rows: 1, bytes: rowCost(capture) });
  }
  for (const cost of Object.values(perChannel)) service = add(service, cost);
  return { service, channels: perChannel };
}
function reservations(state) {
  const channels = Object.fromEntries(Object.keys(state.channels).map(k => [k, zero()]));
  let service = zero();
  for (const poll of Object.values(state.polls)) {
    channels[poll.channel] = add(channels[poll.channel], poll.completion);
    service = add(service, poll.completion);
  }
  return { service, channels };
}
function assertCapacity(state) {
  for (const control of Object.values(state.polls)) {
    requireThat(integer(control.revision) && control.revision > 0 && control.revision <= state.revision
      && within(control.completion, CLOSE_RESERVATION) && within(control.historyReserve, CLOSE_HISTORY), 'authority');
  }
  for (const channel of Object.values(state.channels)) {
    requireThat([channel.highSeq, channel.messages, channel.revision].every(integer)
      && channel.highSeq === channel.messages && channel.revision <= state.revision, 'authority');
  }
  for (const agent of Object.values(state.agents)) requireThat(integer(agent.messages) && integer(agent.lastSeen), 'authority');
  for (const env of Object.values(state.records)) requireThat(own(state.receipts, env.id), 'authority');
  const used = usage(state), reserved = reservations(state);
  requireThat(within(add(used.service, reserved.service), state.policy.service), 'storage_capacity');
  for (const channel of Object.keys(state.channels)) {
    requireThat(within(add(used.channels[channel], reserved.channels[channel]), state.policy.channel), 'storage_capacity');
  }
}
function history(state, pollId) {
  const root = own(state.records, pollId), control = own(state.polls, pollId);
  requireThat(root && control && root.id === pollId && control.rootId === pollId && root.channel === control.channel
    && root.checksum === control.rootHash && root.sender === control.creator
    && state.agents[root.sender]?.publicKey === control.creatorKey, 'unsupported_history');
  requireThat(Object.values(control.historyReserve).every(integer), 'authority');
  requireThat(integer(control.revision) && control.revision > 0 && control.revision <= state.revision
    && within(control.completion, CLOSE_RESERVATION) && within(control.historyReserve, CLOSE_HISTORY), 'authority');
  const records = Object.values(state.records).filter(e => e.id === pollId
    || (e.channel === root.channel && ['poll', 'vote'].includes(e.type) && e.payload?.pollId === pollId));
  let cost = { records: records.length, bytes: 0, nodes: 0 };
  for (const env of records) {
    const n = recordCost(env);
    requireThat(n <= POLL_WORK_LIMITS.recordBytes, 'history_capacity');
    cost.bytes += n;
    cost.nodes += tree(env.payload) + (env.recipientKeys === null ? 0 : tree(env.recipientKeys));
  }
  cost = Object.fromEntries(Object.keys(cost).map(k => [k, cost[k] + control.historyReserve[k]]));
  requireThat(within(cost, state.policy.history), 'history_capacity');
  return { root, records, control };
}
async function tally(state, pollId, now) {
  const { root, records } = history(state, pollId);
  return tallyPoll(root, records, async id => state.agents[id]?.publicKey,
    { registeredAt: async id => state.agents[id]?.registeredAt, now });
}
function validPolicy(policy) {
  for (const capacity of [policy.service, policy.channel]) requireThat(integer(capacity.rows) && integer(capacity.bytes), 'policy');
  requireThat(['records', 'bytes', 'nodes'].every(k => integer(policy.history[k]))
    && within(policy.history, POLL_WORK_LIMITS) && policy.history.records >= 2, 'policy');
  for (const k of ['wireBytes', 'windowMs', 'preparationMs', 'captureRetention']) requireThat(integer(policy[k]) && policy[k] > 0, 'policy');
  requireThat(policy.wireBytes <= 262144 && policy.captureRetention <= 10000
    && policy.preparationMs <= policy.windowMs && typeof policy.wakeActive === 'boolean', 'policy');
  for (const lane of ['ordinary', 'completion', 'recovery']) requireThat(integer(policy.attempts[lane])
    && policy.attempts[lane] <= Math.floor(Number.MAX_SAFE_INTEGER / Math.max(policy.wireBytes, policy.history.records + 1)), 'policy');
}

/** All configuration and alterForTest inputs are trusted fixture/operator data. */
export async function createFixture({ origin = 'https://fixture.invalid', generation = 'fixture-1',
  policyRevision = 'fixture-policy-1', policy = FIXTURE_POLICY, agents, channels = ['general'], now = 1000 }) {
  policy = copy(policy); validPolicy(policy);
  requireThat(/^https:\/\/[a-z0-9.-]+$/.test(origin) && typeof generation === 'string' && generation.length > 0
    && typeof policyRevision === 'string' && policyRevision.length > 0 && integer(now), 'authority');
  const registry = {};
  for (const agent of agents) {
    requireThat(await deriveAgentId(agent.publicKey) === agent.agentId && integer(agent.registeredAt), 'registry');
    requireThat(!registry[agent.agentId], 'registry');
    registry[agent.agentId] = { publicKey: agent.publicKey, registeredAt: agent.registeredAt, messages: 0, lastSeen: 0 };
  }
  const channelRows = {};
  for (const name of channels) {
    requireThat(/^[a-z0-9_-]{1,128}$/.test(name) && !name.startsWith('dm-') && !name.startsWith('vault-'), 'channel');
    requireThat(!Object.hasOwn(channelRows, name), 'channel');
    Object.defineProperty(channelRows, name, { enumerable: true, configurable: true, writable: true,
      value: { public: true, hidden: false, requiresEncryption: false, enabled: true, revision: 0, highSeq: 0, messages: 0 } });
  }
  let state = freeze({ origin, generation, policyRevision, policy, revision: 0,
    agents: registry, channels: channelRows, polls: {}, records: {}, receipts: {},
    arrivals: [], wake: [], arrivalSeq: 0, wakeSeq: 0 });
  assertCapacity(state);
  const pins = freeze({ origin, generation, policyRevision, policy });
  let greatestClock = now, window = Math.floor(now / policy.windowMs);
  let attempts = Object.fromEntries(['ordinary', 'completion', 'recovery'].map(lane => [lane, { requests: 0, input: 0, verification: 0 }]));
  const observe = now => {
    requireThat(integer(now) && now >= greatestClock, 'clock');
    greatestClock = now;
  };
  const authority = expected => requireThat(state && integer(state.revision) && equal(expected, pins)
    && ['origin', 'generation', 'policyRevision', 'policy'].every(k => equal(state[k], pins[k])), 'authority');
  const charge = (lane, now, expected) => {
    authority(expected); observe(now);
    const nextWindow = Math.floor(now / policy.windowMs);
    if (nextWindow !== window) {
      window = nextWindow;
      attempts = Object.fromEntries(Object.keys(attempts).map(k => [k, { requests: 0, input: 0, verification: 0 }]));
    }
    requireThat(Object.hasOwn(attempts, lane), 'route');
    requireThat(attempts[lane].requests < policy.attempts[lane], 'attempt_capacity');
    // Maximum work is charged before parsing or crypto; failed work is not refunded.
    attempts[lane].requests++;
    attempts[lane].input += policy.wireBytes;
    attempts[lane].verification += lane === 'recovery' ? 1 : 1 + policy.history.records;
  };
  const recoverFrom = (env, key) => {
    const saved = own(state.records, env.id);
    if (!saved) return { outcome: 'unavailable', historical: true };
    requireThat(exact(saved, env) && state.receipts[env.id]?.actorKey === key, 'operation_conflict');
    if (!publicRecord(state, saved)) return { outcome: 'unavailable', historical: true };
    return { outcome: 'found', receipt: copy(state.receipts[env.id]), historical: true };
  };
  return {
    inspect: () => ({ state: copy(state), usage: state ? usage(state) : null,
      reserved: state ? reservations(state) : null, greatestClock, window, attempts: copy(attempts) }),
    // Simulates external policy/key/history changes or corruption, never a route.
    alterForTest(change) {
      const next = copy(state); change(next); next.revision++; state = freeze(next);
    },
    removeAuthorityForTest() { state = null; },
    async tally(pollId, now) { authority(pins); observe(now); return tally(state, pollId, now); },
    connect(expected = pins) {
      expected = freeze(copy(expected));
      const prepared = new WeakMap();
      let poisoned = false;
      const active = () => requireThat(!poisoned, 'uncertain');
      return {
        async prepare(wire, { lane = 'ordinary', pollId = null } = {}, now) {
          active(); requireThat(lane === 'ordinary' || lane === 'completion', 'route');
          charge(lane, now, expected);
          const snapshot = state; // One immutable primary snapshot, before any await.
          const env = parse(wire, policy), actor = snapshot.agents[env.sender];
          requireThat(actor && (await verifyEnvelope(env, actor.publicKey)).valid, 'signature');
          authority(expected); active();
          const close = env.type === 'poll' && env.payload?.kind === 'close';
          requireThat(close ? lane === 'completion' && pollId === env.payload.pollId : lane === 'ordinary' && pollId === null, 'route');
          const saved = own(snapshot.records, env.id);
          if (saved) {
            requireThat(exact(saved, env), 'operation_conflict');
            const token = Object.freeze({});
            prepared.set(token, { env, snapshot, actorKey: actor.publicKey, now, replay: true });
            return token;
          }
          const ch = own(snapshot.channels, env.channel);
          requireThat(ch && ch.enabled, 'channel');
          requireThat(!encryptionError(env, ch.requiresEncryption || !ch.public), 'encryption');
          requireThat(fitsPollActionInput(env), 'input_limit');
          let target = null, before = null;
          if (env.type === 'poll' && env.payload?.kind === 'open') {
            requireThat(env.payload.closePolicy?.creator === true && !Object.hasOwn(env.payload, 'pollId'), 'unsupported_profile');
            requireThat(!checkPollIngest(env, null, null, { hub: origin }).refusal, 'poll_refused');
          } else if (env.type === 'poll' || env.type === 'vote') {
            target = env.payload?.pollId;
            requireThat(typeof target === 'string', 'poll_refused');
            const { root } = history(snapshot, target);
            before = await tally(snapshot, target, now);
            const refusal = env.type === 'vote'
              ? checkVoteIngest(env, root, before, { hub: origin, now, voterRegisteredAt: actor.registeredAt })
              : checkPollIngest(env, root, before, { hub: origin }).refusal;
            requireThat(!refusal, refusal ?? 'poll_refused');
          }
          const token = Object.freeze({});
          prepared.set(token, { env, snapshot, actorKey: actor.publicKey, now, target, before, close });
          return token;
        },
        commit(token, now, { failAt = null, loseAcknowledgment = false } = {}) {
          active(); authority(expected); observe(now);
          const decision = prepared.get(token);
          requireThat(decision, 'unprepared'); prepared.delete(token);
          requireThat(now - decision.now <= policy.preparationMs, 'preparation_expired');
          const { env, snapshot, actorKey, target, before, close } = decision;
          requireThat(state.agents[env.sender]?.publicKey === actorKey, 'stale');
          if (decision.replay || own(state.records, env.id)) return recoverFrom(env, actorKey);
          assertCapacity(state);
          // Coarse service CAS also fences every channel writer and registry edit.
          // A later SQL adapter must enforce these predicates in its transaction.
          requireThat(state.revision === snapshot.revision && equal(state.agents[env.sender], snapshot.agents[env.sender])
            && equal(state.channels[env.channel], snapshot.channels[env.channel]), 'stale');
          if (target) {
            requireThat(equal(state.polls[target], snapshot.polls[target]), 'stale');
            const root = state.records[target];
            const refusal = env.type === 'vote'
              ? checkVoteIngest(env, root, before, { hub: origin, now, voterRegisteredAt: state.agents[env.sender].registeredAt })
              : checkPollIngest(env, root, before, { hub: origin }).refusal;
            requireThat(!refusal, refusal ?? 'poll_refused');
          }
          const next = copy(state), ch = next.channels[env.channel];
          requireThat([ch.highSeq + 1, ch.messages + 1, ch.revision + 1, next.revision + 1,
            next.agents[env.sender].messages + 1].every(integer), 'storage_capacity');
          const stored = retained(env, ch.highSeq + 1);
          requireThat(recordCost(stored) <= POLL_WORK_LIMITS.recordBytes, 'history_capacity');
          const fail = stage => requireThat(failAt !== stage, 'rollback');
          next.records = { ...next.records, [env.id]: stored }; fail('message');
          const root = env.type === 'poll' && env.payload.kind === 'open';
          const revision = root ? 1 : target ? next.polls[target].revision + 1 : null;
          const receipt = { origin, generation, policyRevision, id: env.id, channel: env.channel,
            actorKey, signature: env.signature, checksum: env.checksum, storedSeq: stored.storedSeq,
            pollRevision: revision, committedAt: now, historical: true };
          requireThat(rowCost(receipt) <= RECEIPT_BYTES, 'storage_capacity');
          next.receipts = { ...next.receipts, [env.id]: receipt }; fail('receipt');
          if (root) next.polls = { ...next.polls, [env.id]: { rootId: env.id, rootHash: env.checksum, channel: env.channel,
            creator: env.sender, creatorKey: actorKey, revision, completion: copy(CLOSE_RESERVATION),
            historyReserve: copy(CLOSE_HISTORY), closeRecorded: false } };
          if (target) next.polls[target].revision = revision;
          fail('control');
          ch.highSeq++; ch.messages++; ch.revision++;
          next.agents[env.sender].messages++; next.agents[env.sender].lastSeen = now;
          next.revision++; fail('counters');
          const captures = [];
          if (publicRecord(next, stored)) {
            const capture = { seq: ++next.arrivalSeq, id: env.id, channel: env.channel, storedSeq: stored.storedSeq, observedAt: now };
            next.arrivals.push(capture); captures.push(capture);
          }
          if (policy.wakeActive && bytes(JSON.stringify(stored.payload)) <= 65536) {
            const capture = { seq: ++next.wakeSeq, id: env.id, channel: env.channel, observedAt: now,
              ownerAfter: '', ownerUntil: Object.keys(next.agents).sort().at(-1) ?? '' };
            next.wake.push(capture); captures.push(capture);
          }
          requireThat(captures.every(c => rowCost(c) <= CAPTURE_BYTES && integer(c.seq)), 'storage_capacity');
          fail('captures');
          if (close) {
            const control = next.polls[target];
            requireThat(!control.closeRecorded, 'poll_closed');
            const used = { rows: 2 + captures.length,
              bytes: recordCost(stored) + rowCost(receipt) + captures.reduce((n, c) => n + rowCost(c), 0) };
            requireThat(within(used, control.completion), 'storage_capacity');
            control.completion = subtract(control.completion, used); // No refund of unused reservation.
            control.historyReserve = { records: 0, bytes: CLOSE_BYTES - recordCost(stored),
              nodes: CLOSE_HISTORY.nodes - tree(stored.payload) - (stored.recipientKeys === null ? 0 : tree(stored.recipientKeys)) };
            control.closeRecorded = true;
          }
          // Check INSERT-before-eviction peak, then retire only capture references.
          assertCapacity(next);
          if (root || target) history(next, target ?? env.id);
          fail('final_guard');
          next.arrivals = next.arrivals.slice(-policy.captureRetention);
          next.wake = next.wake.slice(-policy.captureRetention);
          assertCapacity(next);
          state = freeze(next);
          if (loseAcknowledgment) { poisoned = true; throw new Error('uncertain'); }
          return { outcome: 'found', receipt: copy(receipt), historical: true, replayed: false };
        },
        async recover(wire, now) {
          active(); charge('recovery', now, expected);
          const snapshot = state, env = parse(wire, policy), actor = snapshot.agents[env.sender];
          requireThat(actor && (await verifyEnvelope(env, actor.publicKey)).valid, 'signature');
          authority(expected); active();
          requireThat(greatestClock - now <= policy.preparationMs, 'preparation_expired');
          requireThat(state.agents[env.sender]?.publicKey === actor.publicKey, 'stale');
          return recoverFrom(env, actor.publicKey);
        },
      };
    },
  };
}
