/** Bounded D1 snapshot decoding and pure proposals. No binding, clock or I/O. */
import { canonicalizeJson, type MessageEnvelope, type StoredEnvelope, type PollTally } from '@openagentforum/protocol';
import type { EnvelopeRow } from '@openagentforum/server/envelopes';
import { POLL_WORK_LIMITS } from '@openagentforum/server/polls';
import { need, safe, equal, bytes, within, plus, minus, tree, exact, decodeRow, recordRow, recordCost, columnCost,
  messageIdentifier, channelIdentifier, agentIdentifier, hex, CLOSE_RESERVATION, CLOSE_HISTORY,
  FIXED_ROW_BYTES, RECEIPT_BYTES, CAPTURE_BYTES, type Capacity, type Receipt, type ReceiptResult,
  type configuration } from './contract.js';
import { arrivalColumns, wakeColumns } from './schema.js';

export type Config = ReturnType<typeof configuration>;
export type Raw = Record<string, unknown>;
export type Wire = MessageEnvelope<unknown>;
export const payload = (env: Wire): Record<string, unknown> =>
  env.payload !== null && typeof env.payload === 'object' && !Array.isArray(env.payload)
    ? env.payload as Record<string, unknown> : {};
export const ledgerFields = ['used_rows', 'used_bytes', 'reserved_rows', 'reserved_bytes'] as const;
export const authorityFields = ['id', 'schema_version', 'origin', 'generation', 'policy_revision', 'policy_json',
  'revision', 'clock', 'window', 'attempts_json', ...ledgerFields] as const;
export const agentFields = ['agent_id', 'public_key', 'registered_at', 'message_count', 'last_seen', 'wake_active'] as const;
export const channelFields = ['name', 'is_private', 'e2ee_required', 'allowed_agents_json', 'enabled',
  'revision', 'high_seq', 'message_count', ...ledgerFields] as const;
export const controlFields = ['root_id', 'root_hash', 'channel', 'creator', 'creator_key', 'revision', 'close_recorded',
  'reserved_rows', 'reserved_bytes', 'reserve_records', 'reserve_bytes', 'reserve_nodes',
  'history_records', 'history_bytes', 'history_nodes'] as const;
export type Ledger = { used_rows: number; used_bytes: number; reserved_rows: number; reserved_bytes: number };
export type Authority = Ledger & { revision: number; clock: number; window: number; attempts_json: string };
export type Agent = { agent_id: string; public_key: string; registered_at: number; message_count: number; last_seen: number; wake_active: number };
export type Channel = Ledger & { name: string; is_private: number; e2ee_required: number; allowed_agents_json: string;
  enabled: number; revision: number; high_seq: number; message_count: number };
export type Control = { root_id: string; root_hash: string; channel: string; creator: string; creator_key: string;
  revision: number; close_recorded: number; reserved_rows: number; reserved_bytes: number;
  reserve_records: number; reserve_bytes: number; reserve_nodes: number;
  history_records: number; history_bytes: number; history_nodes: number };
export type Saved = { row: EnvelopeRow; env: StoredEnvelope; receiptJson: string; actorKey: string };
export type History = { root: StoredEnvelope; records: StoredEnvelope[]; control: Control; agents: Map<string, Agent> };
export type Capture = { arrivalSeq: number; wakeSeq: number; owner: string | null;
  retired: { journal: 'arrival' | 'wake'; channel: string; charge_bytes: number }[]; channels: Map<string, Channel> };
export type Snapshot = { meta: Authority; now: number; actor: Agent | null; channel: Channel | null;
  saved: Saved | null; control: Control | null; history: History | null; capture: Capture | null };
export type Lifetime = { started: number; preparedAt: number; expiresAt: number };
export type Decision = Lifetime & { env: Wire; snapshot: Snapshot; target: string | null; close: boolean; before: PollTally | null };
export const used = (l: Ledger): Capacity => ({ rows: l.used_rows, bytes: l.used_bytes });
export const reserved = (l: Pick<Ledger, 'reserved_rows' | 'reserved_bytes'>): Capacity => ({ rows: l.reserved_rows, bytes: l.reserved_bytes });
const zero = (): Capacity => ({ rows: 0, bytes: 0 });
export const blankAttempts = () => ({ ordinary: { requests: 0, input: 0, verification: 0 },
  completion: { requests: 0, input: 0, verification: 0 }, recovery: { requests: 0, input: 0, verification: 0 } });
export const publicChannel = (c: Channel | null) => c !== null && c.is_private === 0 && c.e2ee_required === 0
  && c.allowed_agents_json === '[]' && !c.name.startsWith('dm-') && !c.name.startsWith('vault-');
export const publicMessage = (env: Wire, channel: Channel | null) => publicChannel(channel)
  && env.encrypted !== true && env.nonce == null && env.ephemeralPublicKey == null && env.recipientKeys == null;
export const pick = (value: object, fields: readonly string[]) => Object.fromEntries(fields.map(k => [k, (value as Raw)[k]]));

export function authority(row: Raw, c: Config): Authority {
  need(row.id === 1 && row.schema_version === 1 && row.origin === c.origin && row.generation === c.generation
    && row.policy_revision === c.policyRevision && row.policy_json === c.policyJson
    && ['revision', 'clock', 'window', ...ledgerFields].every(k => safe(row[k])), 'authority');
  const result = row as Authority;
  need(within(plus(used(result), reserved(result)), c.policy.service)
    && typeof row.attempts_json === 'string' && bytes(row.attempts_json) <= 1024, 'authority');
  const attempts = JSON.parse(row.attempts_json);
  need(attempts && equal(Object.keys(attempts).sort(), ['completion', 'ordinary', 'recovery']), 'authority');
  for (const lane of ['ordinary', 'completion', 'recovery'] as const) {
    const v = attempts[lane];
    need(v && equal(Object.keys(v).sort(), ['input', 'requests', 'verification']) && safe(v.requests)
      && v.requests <= c.policy.attempts[lane] && v.input === v.requests * c.policy.wireBytes
      && v.verification === v.requests * (lane === 'recovery' ? 1 : 1 + c.policy.history.records), 'authority');
  }
  need(row.attempts_json === canonicalizeJson(attempts)
    && result.window === Math.floor(result.clock / c.policy.windowMs), 'authority');
  return pick(result, authorityFields) as Authority;
}
export function agent(row: Raw): Agent {
  need(agentIdentifier(row.agent_id) && hex(row.public_key, 64) && row.public_key === row.public_key.toLowerCase()
    && ['registered_at', 'message_count', 'last_seen'].every(k => safe(row[k]))
    && (row.wake_active === 0 || row.wake_active === 1), 'authority');
  return pick(row, agentFields) as Agent;
}
export function channel(row: Raw, c: Config): Channel {
  need(channelIdentifier(row.name) && ['revision', 'high_seq', 'message_count', ...ledgerFields].every(k => safe(row[k]))
    && row.high_seq === row.message_count && [row.is_private, row.e2ee_required, row.enabled].every(v => v === 0 || v === 1)
    && typeof row.allowed_agents_json === 'string' && bytes(row.allowed_agents_json) <= 1024, 'authority');
  need(within(plus(used(row as Channel), reserved(row as Channel)), c.policy.channel), 'authority');
  return pick(row, channelFields) as Channel;
}
export function control(row: Raw, revision: number): Control {
  need(messageIdentifier(row.root_id) && hex(row.root_hash, 64) && channelIdentifier(row.channel)
    && agentIdentifier(row.creator) && hex(row.creator_key, 64)
    && ['revision', 'reserved_rows', 'reserved_bytes', 'reserve_records', 'reserve_bytes', 'reserve_nodes',
      'history_records', 'history_bytes', 'history_nodes'].every(k => safe(row[k]))
    && (row.close_recorded === 0 || row.close_recorded === 1), 'authority');
  const r = row as Control;
  need(r.revision > 0 && r.revision <= revision && within(reserved(r), CLOSE_RESERVATION)
    && within({ records: r.reserve_records, bytes: r.reserve_bytes, nodes: r.reserve_nodes }, CLOSE_HISTORY)
    && r.reserve_records === (r.close_recorded === 1 ? 0 : 1), 'authority');
  return pick(r, controlFields) as Control;
}
export function stored(row: Raw): EnvelopeRow {
  need(messageIdentifier(row.id) && channelIdentifier(row.channel) && agentIdentifier(row.sender)
    && ['poll', 'vote', 'intel'].includes(String(row.type))
    && ['sequence', 'stored_seq', 'timestamp'].every(k => safe(row[k])) && Number(row.stored_seq) > 0
    && hex(row.signature, 128) && hex(row.checksum, 64) && (row.encrypted === 0 || row.encrypted === 1)
    && typeof row.payload_json === 'string'
    && ['reply_to_id', 'recipient_keys_json', 'ephemeral_public_key', 'nonce'].every(k => row[k] === null || typeof row[k] === 'string'), 'authority');
  // The projection includes extra snapshot columns; construct the wire shape
  // explicitly after validating them rather than casting a binding response.
  const result: EnvelopeRow = { id: row.id, channel: row.channel, sender: row.sender,
    type: row.type as EnvelopeRow['type'], sequence: row.sequence as number, stored_seq: row.stored_seq as number,
    timestamp: row.timestamp as number, payload_json: row.payload_json, signature: row.signature, checksum: row.checksum,
    encrypted: row.encrypted, reply_to_id: row.reply_to_id as string | null, recipient_keys_json: row.recipient_keys_json as string | null,
    ephemeral_public_key: row.ephemeral_public_key as string | null, nonce: row.nonce as string | null };
  need(row.charge_bytes === recordCost(result), 'authority');
  return result;
}
export function saved(row: Raw): Saved {
  need(typeof row.receipt_json === 'string' && bytes(row.receipt_json) + 512 <= RECEIPT_BYTES
    && row.receipt_channel === row.channel && hex(row.actor_key, 64), 'authority');
  const record = stored(row);
  return { row: record, env: decodeRow(record), receiptJson: row.receipt_json, actorKey: row.actor_key };
}
export function receipt(saved: Saved, env: Wire, actorKey: string, ch: Channel | null, now: number, c: Config): ReceiptResult {
  need(exact(saved.env, env) && saved.actorKey === actorKey, 'operation_conflict');
  const r: Receipt = JSON.parse(saved.receiptJson);
  need(r && r.origin === c.origin && r.generation === c.generation && r.policyRevision === c.policyRevision
    && r.id === env.id && r.channel === env.channel && r.actorKey === actorKey && r.signature === env.signature
    && r.checksum === env.checksum && r.storedSeq === saved.env.storedSeq && safe(r.committedAt)
    && r.committedAt <= now && r.historical === true && (r.pollRevision === null || safe(r.pollRevision) && r.pollRevision > 0), 'authority');
  return publicMessage(saved.env, ch) ? { outcome: 'found', receipt: r, historical: true } : { outcome: 'unavailable', historical: true };
}

/** Derive every retained cost and expected final row from the current snapshot. */
export function proposal(d: Decision, s: Snapshot, c: Config) {
  const { env, target, close } = d, { meta, now, actor, channel: ch, capture } = s, p = c.policy;
  need(actor && ch && capture, 'authority');
  const old = target === null ? null : s.control;
  need([ch.high_seq + 1, ch.revision + 1, actor.message_count + 1, meta.revision + 1].every(safe), 'storage_capacity');
  const row = recordRow(env, ch.high_seq + 1), messageBytes = recordCost(row);
  need(messageBytes <= POLL_WORK_LIMITS.recordBytes, 'history_capacity');
  const nodes = tree(env.payload) + (env.recipientKeys == null ? 0 : tree(env.recipientKeys));
  const root = env.type === 'poll' && payload(env).kind === 'open';
  const pollRevision = root ? 1 : old ? old.revision + 1 : null;
  const r: Receipt = { origin: c.origin, generation: c.generation, policyRevision: c.policyRevision,
    id: env.id, channel: env.channel, actorKey: actor.public_key, signature: env.signature, checksum: env.checksum,
    storedSeq: row.stored_seq!, pollRevision, committedAt: now, historical: true };
  const receiptJson = JSON.stringify(r), receiptBytes = 512 + bytes(receiptJson);
  need(receiptBytes <= RECEIPT_BYTES, 'storage_capacity');
  let arrivalSeq = capture.arrivalSeq, wakeSeq = capture.wakeSeq, captureBytes = 0, captureRows = 0;
  const journals: string[] = [];
  if (publicMessage(env, ch)) {
    need(safe(arrivalSeq + 1), 'storage_capacity');
    const values = { arrival_seq: ++arrivalSeq, envelope_id: env.id, channel: env.channel, stored_seq: row.stored_seq, arrived_at: now };
    const cost = columnCost(arrivalColumns.map(k => values[k as keyof typeof values]));
    need(cost <= CAPTURE_BYTES, 'authority'); captureBytes += cost; captureRows++; journals.push('arrival');
  }
  if (p.wakeActive && capture.owner !== null && bytes(JSON.stringify(env.payload)) <= 65536) {
    need(safe(wakeSeq + 1) && agentIdentifier(capture.owner), 'authority');
    const values = { seq: ++wakeSeq, envelope_id: env.id, stored_at: now, owner_after: '', owner_until: capture.owner };
    const cost = columnCost(wakeColumns.map(k => values[k as keyof typeof values]));
    need(cost <= CAPTURE_BYTES, 'authority'); captureBytes += cost; captureRows++; journals.push('wake');
  }
  const action = { rows: 2 + captureRows, bytes: messageBytes + receiptBytes + captureBytes };
  let nextControl: Control | null = null, reserveDelta = zero();
  if (root) {
    nextControl = { root_id: env.id, root_hash: env.checksum, channel: env.channel, creator: env.sender, creator_key: actor.public_key,
      revision: 1, close_recorded: 0, reserved_rows: CLOSE_RESERVATION.rows, reserved_bytes: CLOSE_RESERVATION.bytes,
      reserve_records: CLOSE_HISTORY.records, reserve_bytes: CLOSE_HISTORY.bytes, reserve_nodes: CLOSE_HISTORY.nodes,
      history_records: 1, history_bytes: messageBytes, history_nodes: nodes };
    reserveDelta = { ...CLOSE_RESERVATION };
  } else if (old) {
    nextControl = { ...old, revision: pollRevision!, history_records: old.history_records + 1,
      history_bytes: old.history_bytes + messageBytes, history_nodes: old.history_nodes + nodes };
    if (close) {
      need(old.close_recorded === 0 && within(action, reserved(old)), 'storage_capacity');
      nextControl.close_recorded = 1;
      nextControl.reserved_rows -= action.rows; nextControl.reserved_bytes -= action.bytes;
      nextControl.reserve_records--; nextControl.reserve_bytes -= messageBytes; nextControl.reserve_nodes -= nodes;
      reserveDelta = minus(zero(), action);
    }
  }
  if (nextControl) need(within({ records: nextControl.history_records + nextControl.reserve_records,
    bytes: nextControl.history_bytes + nextControl.reserve_bytes, nodes: nextControl.history_nodes + nextControl.reserve_nodes }, p.history), 'history_capacity');
  const added = plus(action, root ? { rows: 1, bytes: FIXED_ROW_BYTES } : zero());
  need(within(plus(plus(used(meta), added), plus(reserved(meta), reserveDelta)), p.service)
    && within(plus(plus(used(ch), added), plus(reserved(ch), reserveDelta)), p.channel), 'storage_capacity');
  const retired = capture.retired.filter(v => journals.includes(v.journal));
  const retirement = retired.reduce((sum, v) => plus(sum, { rows: 1, bytes: v.charge_bytes }), zero());
  const nextMeta = { revision: meta.revision + 1, clock: now, used_rows: meta.used_rows + added.rows - retirement.rows,
    used_bytes: meta.used_bytes + added.bytes - retirement.bytes, reserved_rows: meta.reserved_rows + reserveDelta.rows,
    reserved_bytes: meta.reserved_bytes + reserveDelta.bytes };
  const nextChannel: Channel = { ...ch, revision: ch.revision + 1, high_seq: ch.high_seq + 1, message_count: ch.message_count + 1,
    used_rows: ch.used_rows + added.rows, used_bytes: ch.used_bytes + added.bytes,
    reserved_rows: ch.reserved_rows + reserveDelta.rows, reserved_bytes: ch.reserved_bytes + reserveDelta.bytes };
  const affected = new Map<string, Channel>([[ch.name, nextChannel]]);
  for (const old of retired) {
    let target = affected.get(old.channel);
    if (!target) { const current = capture.channels.get(old.channel); need(current, 'authority');
      target = { ...current }; affected.set(old.channel, target); }
    target.used_rows--; target.used_bytes -= old.charge_bytes;
  }
  const expected = { authority: nextMeta,
    channels: [...affected.values()].map(v => pick(v, ['name', 'revision', 'high_seq', 'message_count', ...ledgerFields])), control: nextControl,
    actor: { agent_id: actor.agent_id, public_key: actor.public_key, message_count: actor.message_count + 1, last_seen: now },
    message: { id: env.id, channel: env.channel, sender: env.sender, stored_seq: row.stored_seq,
      signature: env.signature, checksum: env.checksum, charge_bytes: messageBytes },
    receipt_json: receiptJson, arrival_seq: arrivalSeq, wake_seq: wakeSeq };
  return { row, receipt: r, receiptJson, control: nextControl, root, expected };
}
