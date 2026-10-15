/** Explicit fixture provisioning only. Importing/opening an adapter never runs DDL. */
import { columnCostSql, wireColumns, FIXED_ROW_BYTES, GUARD_ROW_BYTES, CLOSE_BYTES, CLOSE_RESERVATION } from './contract.js';
import { POLL_WORK_LIMITS } from '@openagentforum/server/polls';

export const DB_NOW = "CAST(ROUND((julianday('now') - 2440587.5) * 86400000) AS INTEGER)";
const safeInt = (name: string) => `${name} INTEGER NOT NULL CHECK (${name} BETWEEN 0 AND 9007199254740991)`;
const ledger = ['used_rows', 'used_bytes', 'reserved_rows', 'reserved_bytes'].map(safeInt).join(',\n');
const capacityCheck = (scope: string) => `CHECK (used_rows <= json_extract(policy_json, '$.${scope}.rows') - reserved_rows),
  CHECK (used_bytes <= json_extract(policy_json, '$.${scope}.bytes') - reserved_bytes)`;
export const arrivalColumns = ['arrival_seq', 'envelope_id', 'channel', 'stored_seq', 'arrived_at'];
export const wakeColumns = ['seq', 'envelope_id', 'stored_at', 'owner_after', 'owner_until'];
export const pollReference = "(CASE WHEN json_valid(payload_json) THEN json_extract(payload_json, '$.pollId') END)";

const definitions = [
  `CREATE TABLE poll_admission_authority (
    id INTEGER PRIMARY KEY CHECK (id = 1), schema_version INTEGER NOT NULL CHECK (schema_version = 1),
    origin TEXT NOT NULL, generation TEXT NOT NULL, policy_revision TEXT NOT NULL,
    policy_json TEXT NOT NULL CHECK (json_valid(policy_json) AND length(CAST(policy_json AS BLOB)) <= 2048),
    ${safeInt('revision')}, ${safeInt('clock')}, ${safeInt('window')},
    attempts_json TEXT NOT NULL CHECK (json_valid(attempts_json) AND length(CAST(attempts_json AS BLOB)) <= 1024),
    ${ledger}, ${capacityCheck('service')},
    CHECK (length(CAST(origin || generation || policy_revision || policy_json || attempts_json AS BLOB)) <= ${FIXED_ROW_BYTES - 512})
  ) STRICT`,
  `CREATE TABLE agents (
    agent_id TEXT PRIMARY KEY, public_key TEXT NOT NULL UNIQUE CHECK (length(public_key) = 64),
    ${safeInt('registered_at')}, ${safeInt('message_count')}, ${safeInt('last_seen')},
    wake_active INTEGER NOT NULL CHECK (wake_active IN (0,1))
  ) STRICT`,
  `CREATE INDEX idx_poll_agents_wake ON agents(agent_id) WHERE wake_active = 1`,
  `CREATE TABLE channels (
    name TEXT PRIMARY KEY, is_private INTEGER NOT NULL CHECK (is_private IN (0,1)),
    e2ee_required INTEGER NOT NULL CHECK (e2ee_required IN (0,1)),
    allowed_agents_json TEXT NOT NULL CHECK (json_valid(allowed_agents_json) AND length(CAST(allowed_agents_json AS BLOB)) <= 1024),
    enabled INTEGER NOT NULL CHECK (enabled IN (0,1)), ${safeInt('revision')},
    ${safeInt('high_seq')}, ${safeInt('message_count')}, ${ledger},
    CHECK (high_seq = message_count)
  ) STRICT`,
  `CREATE TABLE messages (
    id TEXT PRIMARY KEY, channel TEXT NOT NULL REFERENCES channels(name), sender TEXT NOT NULL REFERENCES agents(agent_id),
    type TEXT NOT NULL CHECK (type IN ('poll','vote','intel')),
    ${safeInt('sequence')}, stored_seq INTEGER NOT NULL CHECK (stored_seq BETWEEN 1 AND 9007199254740991), ${safeInt('timestamp')},
    payload_json TEXT NOT NULL CHECK (json_valid(payload_json)), signature TEXT NOT NULL, checksum TEXT NOT NULL,
    encrypted INTEGER NOT NULL CHECK (encrypted IN (0,1)), reply_to_id TEXT, recipient_keys_json TEXT,
    ephemeral_public_key TEXT, nonce TEXT,
    control_id TEXT GENERATED ALWAYS AS (CASE WHEN type = 'poll' AND json_extract(payload_json, '$.kind') = 'open'
      THEN id WHEN type IN ('poll','vote') THEN json_extract(payload_json, '$.pollId') END) VIRTUAL
      REFERENCES poll_admission_controls(root_id) DEFERRABLE INITIALLY DEFERRED,
    charge_bytes INTEGER GENERATED ALWAYS AS (${columnCostSql(wireColumns)}) STORED,
    CHECK (charge_bytes <= ${POLL_WORK_LIMITS.recordBytes}), CHECK (type = 'intel' OR control_id IS NOT NULL), UNIQUE (channel, stored_seq)
  ) STRICT`,
  `CREATE INDEX idx_messages_poll_reference ON messages(channel, ${pollReference}, stored_seq, id) WHERE type IN ('vote','poll')`,
  `CREATE TABLE poll_admission_receipts (
    message_id TEXT PRIMARY KEY REFERENCES messages(id), channel TEXT NOT NULL REFERENCES channels(name),
    actor_key TEXT NOT NULL, receipt_json TEXT NOT NULL CHECK (json_valid(receipt_json)),
    charge_bytes INTEGER GENERATED ALWAYS AS (512 + length(CAST(receipt_json AS BLOB))) STORED,
    CHECK (charge_bytes <= 4096)
  ) STRICT`,
  `CREATE TABLE poll_admission_controls (
    root_id TEXT PRIMARY KEY REFERENCES messages(id) DEFERRABLE INITIALLY DEFERRED,
    root_hash TEXT NOT NULL, channel TEXT NOT NULL REFERENCES channels(name),
    creator TEXT NOT NULL REFERENCES agents(agent_id), creator_key TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision BETWEEN 1 AND 9007199254740991),
    close_recorded INTEGER NOT NULL CHECK (close_recorded IN (0,1)),
    reserved_rows INTEGER NOT NULL CHECK (reserved_rows BETWEEN 0 AND ${CLOSE_RESERVATION.rows}),
    reserved_bytes INTEGER NOT NULL CHECK (reserved_bytes BETWEEN 0 AND ${CLOSE_RESERVATION.bytes}),
    reserve_records INTEGER NOT NULL CHECK (reserve_records BETWEEN 0 AND 1),
    reserve_bytes INTEGER NOT NULL CHECK (reserve_bytes BETWEEN 0 AND ${CLOSE_BYTES}),
    reserve_nodes INTEGER NOT NULL CHECK (reserve_nodes BETWEEN 0 AND 32),
    ${safeInt('history_records')}, ${safeInt('history_bytes')}, ${safeInt('history_nodes')}
  ) STRICT`,
  `CREATE TABLE public_recent_state (id INTEGER PRIMARY KEY CHECK (id = 1), ${safeInt('high_seq')}) STRICT`,
  `CREATE TABLE wake_recent_state (id INTEGER PRIMARY KEY CHECK (id = 1), ${safeInt('high_seq')}) STRICT`,
  `CREATE TABLE public_message_arrivals (
    arrival_seq INTEGER PRIMARY KEY CHECK (arrival_seq BETWEEN 1 AND 9007199254740991),
    envelope_id TEXT NOT NULL REFERENCES messages(id), channel TEXT NOT NULL REFERENCES channels(name),
    stored_seq INTEGER NOT NULL, arrived_at INTEGER NOT NULL,
    charge_bytes INTEGER GENERATED ALWAYS AS (${columnCostSql(arrivalColumns)}) STORED,
    CHECK (charge_bytes <= 1024)
  ) STRICT`,
  `CREATE TABLE wake_message_outbox (
    seq INTEGER PRIMARY KEY CHECK (seq BETWEEN 1 AND 9007199254740991),
    envelope_id TEXT NOT NULL REFERENCES messages(id), stored_at INTEGER NOT NULL,
    owner_after TEXT NOT NULL, owner_until TEXT NOT NULL,
    charge_bytes INTEGER GENERATED ALWAYS AS (${columnCostSql(wakeColumns)}) STORED,
    CHECK (charge_bytes <= 1024)
  ) STRICT`,
  `CREATE TABLE poll_admission_guard (
    id INTEGER PRIMARY KEY CHECK (id = 1), message_id TEXT NOT NULL,
    origin TEXT NOT NULL, generation TEXT NOT NULL, policy_revision TEXT NOT NULL, policy_json TEXT NOT NULL,
    prepared_at INTEGER NOT NULL, expires_at INTEGER NOT NULL, vote_deadline INTEGER,
    expected_json TEXT NOT NULL CHECK (json_valid(expected_json)),
    CHECK (length(CAST(message_id || origin || generation || policy_revision || policy_json || expected_json AS BLOB)) <= ${GUARD_ROW_BYTES - 512})
  ) STRICT`,
];

// Public capture uses the production public-policy predicate. The wake journal is
// separate and uses a fixture-owned active-owner index, not live hook authority.
definitions.push(`CREATE TRIGGER public_message_arrival AFTER INSERT ON messages
  WHEN NEW.encrypted = 0 AND NEW.nonce IS NULL AND NEW.ephemeral_public_key IS NULL AND NEW.recipient_keys_json IS NULL
    AND NEW.stored_seq BETWEEN 1 AND 9007199254740991
    AND length(NEW.id) BETWEEN 1 AND 128 AND NEW.id NOT GLOB '*[^a-zA-Z0-9_:-]*' AND instr(NEW.id, char(0)) = 0
    AND EXISTS (SELECT 1 FROM channels WHERE name = NEW.channel AND is_private = 0 AND e2ee_required = 0
      AND allowed_agents_json = '[]' AND length(name) BETWEEN 1 AND 128 AND name NOT GLOB '*[^a-z0-9_-]*'
      AND instr(name, char(0)) = 0 AND name NOT GLOB 'dm-*' AND name NOT GLOB 'vault-*')
  BEGIN
    UPDATE public_recent_state SET high_seq = high_seq + 1 WHERE id = 1;
    INSERT INTO public_message_arrivals (arrival_seq, envelope_id, channel, stored_seq, arrived_at)
      SELECT high_seq, NEW.id, NEW.channel, NEW.stored_seq, clock FROM public_recent_state, poll_admission_authority
      WHERE public_recent_state.id = 1 AND poll_admission_authority.id = 1;
    DELETE FROM public_message_arrivals WHERE arrival_seq <=
      (SELECT high_seq - json_extract(policy_json, '$.captureRetention') FROM public_recent_state, poll_admission_authority
       WHERE public_recent_state.id = 1 AND poll_admission_authority.id = 1);
  END`);
definitions.push(`CREATE TRIGGER wake_message_insert AFTER INSERT ON messages
  WHEN length(CAST(NEW.payload_json AS BLOB)) <= 65536
    AND (SELECT json_extract(policy_json, '$.wakeActive') FROM poll_admission_authority WHERE id = 1) = 1
    AND EXISTS (SELECT 1 FROM agents INDEXED BY idx_poll_agents_wake WHERE wake_active = 1)
  BEGIN
    UPDATE wake_recent_state SET high_seq = high_seq + 1 WHERE id = 1;
    INSERT INTO wake_message_outbox (seq, envelope_id, stored_at, owner_after, owner_until)
      SELECT high_seq, NEW.id, clock, '', (SELECT MAX(agent_id) FROM agents INDEXED BY idx_poll_agents_wake WHERE wake_active = 1)
      FROM wake_recent_state, poll_admission_authority WHERE wake_recent_state.id = 1 AND poll_admission_authority.id = 1;
    DELETE FROM wake_message_outbox WHERE seq <=
      (SELECT high_seq - json_extract(policy_json, '$.captureRetention') FROM wake_recent_state, poll_admission_authority
       WHERE wake_recent_state.id = 1 AND poll_admission_authority.id = 1);
  END`);

const chargedTables = [
  ['messages', 'NEW.channel', 'NEW.charge_bytes'],
  ['poll_admission_receipts', 'NEW.channel', 'NEW.charge_bytes'],
  ['poll_admission_controls', 'NEW.channel', String(FIXED_ROW_BYTES)],
  ['public_message_arrivals', 'NEW.channel', 'NEW.charge_bytes'],
  ['wake_message_outbox', '(SELECT channel FROM messages WHERE id = NEW.envelope_id)', 'NEW.charge_bytes'],
];
for (const [table, channel, cost] of chargedTables) definitions.push(`CREATE TRIGGER ${table}_charge AFTER INSERT ON ${table}
  BEGIN
    UPDATE poll_admission_authority SET used_rows = used_rows + 1, used_bytes = used_bytes + ${cost} WHERE id = 1;
    UPDATE channels SET used_rows = used_rows + 1, used_bytes = used_bytes + ${cost} WHERE name = ${channel};
  END`);
for (const [table, channel] of [
  ['public_message_arrivals', 'OLD.channel'],
  ['wake_message_outbox', '(SELECT channel FROM messages WHERE id = OLD.envelope_id)'],
]) definitions.push(`CREATE TRIGGER ${table}_retire AFTER DELETE ON ${table}
  BEGIN
    UPDATE poll_admission_authority SET used_rows = used_rows - 1, used_bytes = used_bytes - OLD.charge_bytes WHERE id = 1;
    UPDATE channels SET used_rows = used_rows - 1, used_bytes = used_bytes - OLD.charge_bytes WHERE name = ${channel};
  END`);
definitions.push(`CREATE TRIGGER poll_control_reserve AFTER INSERT ON poll_admission_controls
  BEGIN
    UPDATE poll_admission_authority SET reserved_rows = reserved_rows + NEW.reserved_rows,
      reserved_bytes = reserved_bytes + NEW.reserved_bytes WHERE id = 1;
    UPDATE channels SET reserved_rows = reserved_rows + NEW.reserved_rows,
      reserved_bytes = reserved_bytes + NEW.reserved_bytes WHERE name = NEW.channel;
  END`);
definitions.push(`CREATE TRIGGER poll_control_convert AFTER UPDATE OF reserved_rows, reserved_bytes ON poll_admission_controls
  BEGIN
    UPDATE poll_admission_authority SET reserved_rows = reserved_rows + NEW.reserved_rows - OLD.reserved_rows,
      reserved_bytes = reserved_bytes + NEW.reserved_bytes - OLD.reserved_bytes WHERE id = 1;
    UPDATE channels SET reserved_rows = reserved_rows + NEW.reserved_rows - OLD.reserved_rows,
      reserved_bytes = reserved_bytes + NEW.reserved_bytes - OLD.reserved_bytes WHERE name = NEW.channel;
  END`);

// Raw SQL is not a parallel admission API. The dedicated database must have these
// exact objects; schema/pragma changes fail closed before any work reservation.
for (const table of ['messages', 'poll_admission_receipts']) {
  for (const action of ['UPDATE', 'DELETE']) definitions.push(`CREATE TRIGGER ${table}_no_${action.toLowerCase()} BEFORE ${action} ON ${table}
    BEGIN SELECT RAISE(ABORT, 'poll_admission_immutable'); END`);
}
for (const [table, seq, cursor] of [['public_message_arrivals', 'arrival_seq', 'public_recent_state'],
  ['wake_message_outbox', 'seq', 'wake_recent_state']]) {
  definitions.push(`CREATE TRIGGER ${table}_no_update BEFORE UPDATE ON ${table}
    BEGIN SELECT RAISE(ABORT, 'poll_admission_immutable'); END`);
  definitions.push(`CREATE TRIGGER ${table}_retention_only BEFORE DELETE ON ${table}
    WHEN NOT EXISTS (SELECT 1 FROM poll_admission_guard WHERE id = 1)
      OR OLD.${seq} > (SELECT high_seq - json_extract(policy_json, '$.captureRetention')
        FROM ${cursor}, poll_admission_authority WHERE ${cursor}.id = 1 AND poll_admission_authority.id = 1)
    BEGIN SELECT RAISE(ABORT, 'poll_admission_immutable'); END`);
}
for (const table of ['agents', 'channels', 'poll_admission_controls']) definitions.push(`CREATE TRIGGER ${table}_no_delete BEFORE DELETE ON ${table}
  BEGIN SELECT RAISE(ABORT, 'poll_admission_immutable'); END`);
for (const [table, fields] of [['agents', 'agent_id, public_key'], ['channels', 'name'],
  ['poll_admission_controls', 'root_id, root_hash, channel, creator, creator_key']]) {
  definitions.push(`CREATE TRIGGER ${table}_binding BEFORE UPDATE OF ${fields} ON ${table}
    BEGIN SELECT RAISE(ABORT, 'poll_admission_immutable'); END`);
}
for (const table of ['agents', 'channels']) definitions.push(`CREATE TRIGGER ${table}_no_adoption BEFORE INSERT ON ${table}
  WHEN EXISTS (SELECT 1 FROM poll_admission_authority WHERE id = 1)
  BEGIN SELECT RAISE(ABORT, 'poll_admission_unsupported'); END`);
definitions.push(`CREATE TRIGGER agents_policy_revision AFTER UPDATE OF registered_at, wake_active ON agents
  BEGIN UPDATE poll_admission_authority SET revision = revision + 1 WHERE id = 1; END`);
definitions.push(`CREATE TRIGGER channels_policy_revision AFTER UPDATE OF is_private, e2ee_required, allowed_agents_json, enabled ON channels
  BEGIN
    UPDATE channels SET revision = revision + 1 WHERE name = NEW.name;
    UPDATE poll_admission_authority SET revision = revision + 1 WHERE id = 1;
  END`);
definitions.push(`CREATE TRIGGER channels_capacity AFTER UPDATE OF used_rows, used_bytes, reserved_rows, reserved_bytes ON channels
  WHEN NOT EXISTS (SELECT 1 FROM poll_admission_authority WHERE id = 1
    AND NEW.used_rows <= json_extract(policy_json, '$.channel.rows') - NEW.reserved_rows
    AND NEW.used_bytes <= json_extract(policy_json, '$.channel.bytes') - NEW.reserved_bytes)
  BEGIN SELECT RAISE(ABORT, 'poll_admission_capacity'); END`);
for (const [table, id] of [['messages', 'id'], ['poll_admission_receipts', 'message_id'],
  ['public_message_arrivals', 'envelope_id'], ['wake_message_outbox', 'envelope_id']]) {
  definitions.push(`CREATE TRIGGER ${table}_admitted BEFORE INSERT ON ${table}
    WHEN NOT EXISTS (SELECT 1 FROM poll_admission_guard WHERE id = 1 AND message_id = NEW.${id})
    BEGIN SELECT RAISE(ABORT, 'poll_admission_unprepared'); END`);
}
for (const action of ['INSERT', 'UPDATE']) definitions.push(`CREATE TRIGGER poll_control_${action.toLowerCase()}_admitted BEFORE ${action} ON poll_admission_controls
  WHEN NOT EXISTS (SELECT 1 FROM poll_admission_guard WHERE id = 1 AND json_extract(expected_json, '$.control.root_id') = NEW.root_id)
  BEGIN SELECT RAISE(ABORT, 'poll_admission_unprepared'); END`);

const matched = (alias: string, fields: string[], json: string) => fields.map(k => `${alias}.${k} IS json_extract(${json}, '$.${k}')`).join(' AND ');
const channelFields = ['name', 'revision', 'high_seq', 'message_count', 'used_rows', 'used_bytes', 'reserved_rows', 'reserved_bytes'];
const controlFields = ['root_id', 'root_hash', 'channel', 'creator', 'creator_key', 'revision', 'close_recorded', 'reserved_rows', 'reserved_bytes',
  'reserve_records', 'reserve_bytes', 'reserve_nodes', 'history_records', 'history_bytes', 'history_nodes'];
export const FINAL_GUARD_SQL = `CREATE TRIGGER poll_admission_final_guard BEFORE DELETE ON poll_admission_guard
  BEGIN
    SELECT CASE WHEN COALESCE((
      EXISTS (SELECT 1 FROM poll_admission_authority a WHERE a.id = 1
        AND a.origin = OLD.origin AND a.generation = OLD.generation AND a.policy_revision = OLD.policy_revision AND a.policy_json = OLD.policy_json
        AND ${matched('a', ['revision', 'used_rows', 'used_bytes', 'reserved_rows', 'reserved_bytes', 'clock'], "json_extract(OLD.expected_json, '$.authority')")}
        AND a.clock >= OLD.prepared_at AND ${DB_NOW} >= a.clock AND ${DB_NOW} <= OLD.expires_at
        AND (OLD.vote_deadline IS NULL OR ${DB_NOW} <= OLD.vote_deadline))
      AND EXISTS (SELECT 1 FROM messages m WHERE m.id = OLD.message_id
        AND ${matched('m', ['id', 'channel', 'sender', 'stored_seq', 'signature', 'checksum', 'charge_bytes'], "json_extract(OLD.expected_json, '$.message')")})
      AND EXISTS (SELECT 1 FROM poll_admission_receipts r WHERE r.message_id = OLD.message_id
        AND r.receipt_json = json_extract(OLD.expected_json, '$.receipt_json'))
      AND EXISTS (SELECT 1 FROM agents a WHERE ${matched('a', ['agent_id', 'public_key', 'message_count', 'last_seen'], "json_extract(OLD.expected_json, '$.actor')")})
      AND NOT EXISTS (SELECT 1 FROM json_each(OLD.expected_json, '$.channels') e LEFT JOIN channels c
        ON c.name = json_extract(e.value, '$.name') WHERE COALESCE((${matched('c', channelFields, 'e.value')}), 0) = 0)
      AND (json_extract(OLD.expected_json, '$.control') IS NULL OR EXISTS
        (SELECT 1 FROM poll_admission_controls c JOIN messages m ON m.id = c.root_id JOIN agents a ON a.agent_id = c.creator
         WHERE ${matched('c', controlFields, "json_extract(OLD.expected_json, '$.control')")}
           AND m.checksum = c.root_hash AND m.channel = c.channel AND m.sender = c.creator AND a.public_key = c.creator_key
           AND c.history_records + c.reserve_records <= json_extract(OLD.policy_json, '$.history.records')
           AND c.history_bytes + c.reserve_bytes <= json_extract(OLD.policy_json, '$.history.bytes')
           AND c.history_nodes + c.reserve_nodes <= json_extract(OLD.policy_json, '$.history.nodes')))
      AND EXISTS (SELECT 1 FROM public_recent_state WHERE id = 1 AND high_seq = json_extract(OLD.expected_json, '$.arrival_seq'))
      AND EXISTS (SELECT 1 FROM wake_recent_state WHERE id = 1 AND high_seq = json_extract(OLD.expected_json, '$.wake_seq'))
    ), 0) = 0 THEN RAISE(ABORT, 'poll_admission_final_guard') END;
  END`;
definitions.push(FINAL_GUARD_SQL);

export const POLL_ADMISSION_SCHEMA = definitions.join(';\n') + ';';
export const SCHEMA_OBJECTS = Object.freeze(definitions.map(sql => {
  const [, type, name] = /^CREATE (TABLE|INDEX|TRIGGER) (\w+)/.exec(sql)!;
  return Object.freeze({ type: type.toLowerCase(), name, sql });
}));
