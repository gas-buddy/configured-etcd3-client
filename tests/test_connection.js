import tap from 'tap';
import pino from 'pino';
import EtcdClient from '../src/index';

const logger = pino();
const context = { logger, gb: { logger } };

tap.test('test_connection', async (connTests) => {
  const etcd = new EtcdClient({ logger }, {
    hosts: [process.env.ETCD_URL || 'http://localhost:2379'],
  });
  const client = await etcd.start();

  const testKey = `test-key-${Date.now()}`;
  const lockKey = `lock-key-${Date.now()}`;

  connTests.test('basic CRUD', async (t) => {
    const objValue = { a: true, b: 3, c: 'four', d: [1, 2, 3] };
    t.notOk(await client.get(context, testKey), 'Should start with no value');
    await client.put(context, testKey, 'helloworld', 1);
    t.strictEquals(await client.get(context, testKey), 'helloworld', 'Should get a key');

    await client.put(context, testKey, objValue, 1);
    t.same(await client.get(context, testKey), objValue, 'Should get a key with object value');
    await new Promise(accept => setTimeout(accept, 2500));
    const afterExpiration = await client.get(context, testKey);
    t.notOk(afterExpiration, 'TTL should work');

    await client.put(context, testKey, 'helloworld');
    await client.del(context, testKey);
    t.notOk(await client.get(context, testKey), 'Del should work');
  });

  connTests.test('recursive get', async (t) => {
    const folder = `folder-${Date.now()}`;
    await client.put(context, `${folder}/${testKey}`, 'helloworld');
    await client.put(context, `${folder}/${lockKey}`, 'i am here');
    const keys = await client.get(context, folder, { recursive: true });
    t.strictEquals(keys[testKey], 'helloworld', 'recursive key/value should match');
    t.strictEquals(keys[lockKey], 'i am here', 'recursive key/value should match');
    t.strictEquals(Object.values(keys).length, 2, 'Should only get two values');
  });

  connTests.test('locking', async (t) => {
    const lock1 = await client.acquireLock(context, lockKey);
    let lock2;
    const lockPromise = client.acquireLock(context, lockKey).then((l) => { lock2 = l; });
    await new Promise(accept => setTimeout(accept, 100));
    t.notOk(lock2, 'After acquiring lock1, lock2 should not fulfill');
    await etcd.releaseLock(lock1);
    await lockPromise;
    t.ok(lock2, 'After releasing lock1, lock2 should fulfill');
    await etcd.releaseLock(lock2);
  });

  connTests.test('watchers', async (t) => {
    const watch = await client.watcher(context, testKey);
    await new Promise((accept) => {
      watch.once('put', (res) => {
        t.strictEquals(res.value.toString(), '"value is changed"', 'New value should match');
        accept();
      });
      client.put(context, testKey, 'value is changed', 1);
    });
    await new Promise((accept) => {
      watch.once('put', (res) => {
        t.strictEquals(res.value.toString(), '"value is so changed"', 'Changed value should match');
        accept();
      });
      client.put(context, testKey, 'value is so changed', 1);
    });
    await watch.cancel();
  });
});
