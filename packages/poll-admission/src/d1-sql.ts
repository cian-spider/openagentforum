/** Fixed, indexed statements for a dedicated disposable D1 binding. No initializer. */
import { canonicalizeJson } from '@openagentforum/protocol';
import { POLL_WORK_LIMITS } from '@openagentforum/server/polls';
import { DB_NOW, SCHEMA_OBJECTS, finalGuardSql, pollReference } from './schema.js';
import { wireColumns } from './contract.js';
import { authorityFields, agentFields, channelFields, controlFields, blankAttempts } from './d1-state.js';

const emptyAttempts = canonicalizeJson(blankAttempts());
export const D1_FINAL_GUARD_SQL = finalGuardSql(`
    UPDATE poll_admission_authority SET clock = ${DB_NOW},
      attempts_json = CASE WHEN window = CAST(${DB_NOW} / json_extract(policy_json, '$.windowMs') AS INTEGER)
        THEN attempts_json ELSE '${emptyAttempts}' END,
      window = CAST(${DB_NOW} / json_extract(policy_json, '$.windowMs') AS INTEGER) WHERE id = 1;`);
// Same allocations and wire tables; only the final database-time update differs.
export const D1_SCHEMA_OBJECTS = Object.freeze(SCHEMA_OBJECTS.map(o => o.name === 'poll_admission_final_guard'
  ? Object.freeze({ ...o, sql: D1_FINAL_GUARD_SQL }) : o));
export const D1_SCHEMA_JSON = JSON.stringify(D1_SCHEMA_OBJECTS);

// D1 caps a function call at 32 arguments, including JSON object keys.
const jsonRow = (alias: string, fields: readonly string[]): string => fields.length > 16
  ? `json_patch(${jsonRow(alias, fields.slice(0, 16))}, ${jsonRow(alias, fields.slice(16))})`
  : `json_object(${fields.map(k => `'${k}', ${alias}.${k}`).join(', ')})`;
const match = (alias: string, fields: readonly string[], source: string, path = '') =>
  fields.map(k => `${alias}.${k} IS json_extract(${source}, '$.${path}${k}')`).join(' AND ');
// D1 owns _cf_KV and _cf_METADATA. Every application object is pinned; an extra application trigger
// or a missing guard fails closed. Only bounded metadata crosses this boundary.
export const SCHEMA_OK = `(SELECT count(*) FROM (
    SELECT 1 FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT GLOB 'sqlite_*'
      AND NOT (type = 'table' AND name IN ('_cf_KV', '_cf_METADATA'))
    LIMIT ${D1_SCHEMA_OBJECTS.length + 1})) = ${D1_SCHEMA_OBJECTS.length}
  AND NOT EXISTS (SELECT 1 FROM json_each(i.schema_json) e LEFT JOIN sqlite_schema s ON s.name = json_extract(e.value, '$.name')
    WHERE s.type IS NOT json_extract(e.value, '$.type') OR s.sql IS NOT json_extract(e.value, '$.sql'))`;
const input = `input AS (SELECT ? AS schema_json, ? AS request_json)`;
const checked = `checked AS MATERIALIZED (SELECT (${SCHEMA_OK}) AS schema_ok FROM input i)`;
const metaCte = `meta AS MATERIALIZED (SELECT a.*, ${DB_NOW} AS db_now, v.schema_ok,
    NOT EXISTS (SELECT 1 FROM poll_admission_guard WHERE id = 1) AS guard_empty
    FROM poll_admission_authority a CROSS JOIN checked v WHERE a.id = 1)`;
export const META_SQL = `WITH ${input}, ${checked}, ${metaCte} SELECT * FROM meta`;

/** One CAS statement commits an attempt before any wire parsing/verification. */
export const CHARGE_SQL = `WITH ${input}, ${checked}
  UPDATE poll_admission_authority AS a SET clock = ${DB_NOW},
    window = json_extract((SELECT request_json FROM input), '$.window'),
    attempts_json = json_extract((SELECT request_json FROM input), '$.attempts')
  WHERE a.id = 1 AND EXISTS (SELECT 1 FROM input i, checked v WHERE v.schema_ok = 1
    AND ${match('a', authorityFields, 'i.request_json', 'meta.')}
    AND ${DB_NOW} >= a.clock AND ${DB_NOW} >= json_extract(i.request_json, '$.observedAt')
    AND ${DB_NOW} <= json_extract(i.request_json, '$.expiresAt')
    AND CAST(${DB_NOW} / json_extract(a.policy_json, '$.windowMs') AS INTEGER) = json_extract(i.request_json, '$.window'))
    AND NOT EXISTS (SELECT 1 FROM poll_admission_guard WHERE id = 1)
  RETURNING clock`;

const wireFields = [...wireColumns, 'charge_bytes'] as const;
const extraFields = ['receipt_json', 'actor_key', 'receipt_channel', 'public_key', 'registered_at'] as const;
const columns = [...wireFields, ...extraFields];
function recordSelect(kind: string, from: string, extras: Record<string, string> = {}) {
  return `SELECT '${kind}' AS kind, NULL AS body, ${columns.map(k =>
    wireFields.some(w => w === k) ? `m.${k} AS ${k}` : `${extras[k] ?? 'NULL'} AS ${k}`).join(', ')} FROM ${from}`;
}
function dataSelect(kind: string, body: string, from: string) {
  return `SELECT '${kind}' AS kind, ${body} AS body, ${columns.map(k => `NULL AS ${k}`).join(', ')} FROM ${from}`;
}
const receiptExtras = { receipt_json: 'r.receipt_json', actor_key: 'r.actor_key', receipt_channel: 'r.channel' };

/** One primary SQL snapshot. No JSON aggregation of the potentially 4-MiB history. */
export const SNAPSHOT_SQL = `WITH ${input}, ${checked}, ${metaCte},
  ready AS MATERIALIZED (SELECT m.* FROM meta m, input i WHERE m.schema_ok = 1
    AND (m.guard_empty = 1 OR json_extract(i.request_json, '$.inBatch') = 1)),
  actor AS MATERIALIZED (SELECT a.* FROM agents a, input i, ready WHERE a.agent_id = json_extract(i.request_json, '$.actor')),
  channel AS MATERIALIZED (SELECT c.* FROM channels c, input i, ready WHERE c.name = json_extract(i.request_json, '$.channel')),
  saved_id AS MATERIALIZED (SELECT m.rowid AS rid FROM messages m, input i, ready WHERE m.id = json_extract(i.request_json, '$.id')),
  control AS MATERIALIZED (SELECT c.* FROM poll_admission_controls c, input i, ready WHERE c.root_id = json_extract(i.request_json, '$.target')),
  history_on AS MATERIALIZED (SELECT 1 FROM input i, ready, channel c, actor
    WHERE json_extract(i.request_json, '$.history') = 1 AND NOT EXISTS (SELECT 1 FROM saved_id)
      AND c.enabled = 1 AND c.is_private = 0 AND c.e2ee_required = 0 AND c.allowed_agents_json = '[]'
      AND c.name NOT GLOB 'dm-*' AND c.name NOT GLOB 'vault-*'),
  root_info AS MATERIALIZED (SELECT m.rowid AS rid, m.charge_bytes AS bytes FROM messages m, input i, history_on
    WHERE m.id = json_extract(i.request_json, '$.target')),
  picked AS MATERIALIZED (SELECT m.rowid AS rid, m.charge_bytes AS bytes FROM messages m INDEXED BY idx_messages_poll_reference
    WHERE m.channel = (SELECT json_extract(request_json, '$.channel') FROM input)
      AND ${pollReference} = (SELECT json_extract(request_json, '$.target') FROM input)
      AND m.type IN ('vote','poll') AND m.id != (SELECT json_extract(request_json, '$.target') FROM input)
      AND EXISTS (SELECT 1 FROM history_on) ORDER BY m.stored_seq, m.id
    LIMIT (SELECT json_extract(request_json, '$.records') + 1 FROM input)),
  bounds AS MATERIALIZED (SELECT count(*) AS total, COALESCE(sum(bytes), 0) AS total_bytes,
    COALESCE(max(bytes), 0) AS largest FROM picked),
  readable AS MATERIALIZED (SELECT 1 FROM bounds b, root_info r, control c, input i
    WHERE b.total + 1 + c.reserve_records <= json_extract(i.request_json, '$.records')
      AND b.total_bytes + r.bytes + c.reserve_bytes <= json_extract(i.request_json, '$.bytes')
      AND max(b.largest, r.bytes) <= ${POLL_WORK_LIMITS.recordBytes}),
  capture AS MATERIALIZED (SELECT a.high_seq AS arrival_seq, w.high_seq AS wake_seq,
      (SELECT MAX(agent_id) FROM agents INDEXED BY idx_poll_agents_wake WHERE wake_active = 1) AS owner
    FROM public_recent_state a, wake_recent_state w, input i, ready
    WHERE a.id = 1 AND w.id = 1 AND json_extract(i.request_json, '$.capture') = 1),
  retired_arrival AS MATERIALIZED (SELECT 'arrival' AS journal, channel, charge_bytes FROM public_message_arrivals
    WHERE arrival_seq <= (SELECT arrival_seq + 1 - json_extract(policy_json, '$.captureRetention') FROM capture, ready) LIMIT 2),
  retired_wake AS MATERIALIZED (SELECT 'wake' AS journal, m.channel, w.charge_bytes FROM wake_message_outbox w
    JOIN messages m ON m.id = w.envelope_id
    WHERE w.seq <= (SELECT wake_seq + 1 - json_extract(policy_json, '$.captureRetention') FROM capture, ready) LIMIT 2),
  retired AS MATERIALIZED (SELECT * FROM retired_arrival UNION ALL SELECT * FROM retired_wake),
  affected AS MATERIALIZED (SELECT c.* FROM channels c WHERE c.name IN (SELECT channel FROM retired)),
  details AS MATERIALIZED (
  ${dataSelect('meta', jsonRow('m', [...authorityFields, 'db_now', 'schema_ok', 'guard_empty']), 'meta m')}
  UNION ALL ${dataSelect('actor', jsonRow('a', agentFields), 'actor a')}
  UNION ALL ${dataSelect('channel', jsonRow('c', channelFields), 'channel c')}
  UNION ALL ${dataSelect('control', jsonRow('c', controlFields), 'control c')}
  UNION ALL ${dataSelect('bounds', jsonRow('b', ['total', 'total_bytes', 'largest']), 'bounds b, history_on')}),
  capture_data AS MATERIALIZED (
  ${dataSelect('capture', jsonRow('c', ['arrival_seq', 'wake_seq', 'owner']), 'capture c')}
  UNION ALL ${dataSelect('retired', jsonRow('r', ['journal', 'channel', 'charge_bytes']), 'retired r')}
  UNION ALL ${dataSelect('affected', jsonRow('c', channelFields), 'affected c')}),
  wires AS MATERIALIZED (
  ${recordSelect('saved', 'saved_id x JOIN messages m ON m.rowid = x.rid LEFT JOIN poll_admission_receipts r ON r.message_id = m.id', receiptExtras)}
  UNION ALL ${recordSelect('root', 'root_info x JOIN messages m ON m.rowid = x.rid LEFT JOIN poll_admission_receipts r ON r.message_id = m.id JOIN agents a ON a.agent_id = m.sender, readable',
    { ...receiptExtras, public_key: 'a.public_key', registered_at: 'a.registered_at' })}
  UNION ALL ${recordSelect('history', 'picked x JOIN messages m ON m.rowid = x.rid LEFT JOIN agents a ON a.agent_id = m.sender, readable',
    { public_key: 'a.public_key', registered_at: 'a.registered_at' })})
  SELECT * FROM details UNION ALL SELECT * FROM capture_data UNION ALL SELECT * FROM wires`;

/** A proposal is staged only if the complete preflight state still matches. */
export const STAGE_SQL = `WITH ${input}, ${checked}
  INSERT INTO poll_admission_guard (id, message_id, origin, generation, policy_revision, policy_json,
    prepared_at, expires_at, vote_deadline, expected_json)
  SELECT 1, json_extract(i.request_json, '$.id'), a.origin, a.generation, a.policy_revision, a.policy_json,
    json_extract(i.request_json, '$.preparedAt'), json_extract(i.request_json, '$.expiresAt'),
    json_extract(i.request_json, '$.deadline'), json_extract(i.request_json, '$.expected')
  FROM input i, checked v, poll_admission_authority a, agents actor, channels channel
  WHERE a.id = 1 AND v.schema_ok = 1 AND ${match('a', authorityFields, 'i.request_json', 'before.meta.')}
    AND ${match('actor', agentFields, 'i.request_json', 'before.actor.')}
    AND ${match('channel', channelFields, 'i.request_json', 'before.channel.')}
    AND NOT EXISTS (SELECT 1 FROM poll_admission_guard WHERE id = 1)
    AND NOT EXISTS (SELECT 1 FROM messages WHERE id = json_extract(i.request_json, '$.id'))
    AND ${DB_NOW} >= json_extract(i.request_json, '$.observedAt') AND ${DB_NOW} >= a.clock
    AND ${DB_NOW} <= json_extract(i.request_json, '$.expiresAt')
    AND (json_extract(i.request_json, '$.deadline') IS NULL OR ${DB_NOW} <= json_extract(i.request_json, '$.deadline'))
    AND (json_extract(i.request_json, '$.before.control') IS NULL OR EXISTS
      (SELECT 1 FROM poll_admission_controls c WHERE ${match('c', controlFields, 'i.request_json', 'before.control.')}))
    AND EXISTS (SELECT 1 FROM public_recent_state WHERE id = 1 AND high_seq = json_extract(i.request_json, '$.arrivalSeq'))
    AND EXISTS (SELECT 1 FROM wake_recent_state WHERE id = 1 AND high_seq = json_extract(i.request_json, '$.wakeSeq'))
    AND (SELECT MAX(agent_id) FROM agents INDEXED BY idx_poll_agents_wake WHERE wake_active = 1) IS json_extract(i.request_json, '$.owner')
    AND NOT EXISTS (SELECT 1 FROM json_each(i.request_json, '$.affected') e LEFT JOIN channels c ON c.name = json_extract(e.value, '$.name')
      WHERE NOT (${match('c', channelFields, 'e.value')}))`;

export const ADMITTED = 'EXISTS (SELECT 1 FROM poll_admission_guard WHERE id = 1 AND message_id = ?)';
export const ADVANCE_CLOCK_SQL = `UPDATE poll_admission_authority SET clock = ?, window = ?, attempts_json = ? WHERE id = 1 AND ${ADMITTED}`;
