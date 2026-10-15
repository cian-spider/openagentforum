// Parent-owned local fixture only. No listener, network, key file or initialization.
import { SQLitePollAdmission } from '../dist/sqlite.js';
import { openConnection, instrument } from './helpers.mjs';

let db, client, token, crash = null;
process.on('message', async message => {
  const { id, action } = message;
  try {
    let result;
    if (action === 'open') {
      db = openConnection(message.path, message.now === undefined ? undefined : { now: message.now });
      const observed = instrument(db, {
        beforeExec(sql) { if (crash === 'before' && sql === 'COMMIT') process.exit(24); },
        afterExec(sql) { if (crash === 'after' && sql === 'COMMIT') process.exit(23); },
      });
      client = new SQLitePollAdmission(observed, message.config);
      result = { opened: true };
    } else if (action === 'prepare') {
      token = await client.prepare(message.wire, message.route);
      result = { prepared: true };
    } else if (action === 'commit') {
      crash = message.crash ?? null;
      result = client.commit(token);
    } else if (action === 'recover') result = await client.recover(message.wire);
    else if (action === 'close') {
      db.close();
      process.send({ id, result: { closed: true } }, () => process.disconnect());
      return;
    } else throw new Error('unknown fixture action');
    process.send({ id, result });
  } catch (error) { process.send({ id, error: error.code ?? 'fixture_error' }); }
});
