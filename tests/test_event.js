import tap from 'tap';
import pino from 'pino';
import EtcdClient from '../src/index';

const logger = pino();
const context = { logger, gb: { logger } };

tap.test('test_get_event', async (t) => {
  const etcd = new EtcdClient(context, {
    hosts: [process.env.ETCD_URL || 'http://localhost:2379'],
  });
  const client = await etcd.start();
  const keyName = 'testkey';

  client.addListener('start', (info) => {
    t.equal(info.key, keyName);
    t.equal(info.method, 'get');
  });
  client.addListener('finish', (info) => {
    t.equal(info.key, keyName);
    t.equal(info.method, 'get');
  });

  await client.get(context, keyName);
});

tap.test('test_put_event', async (t) => {
  const etcd = new EtcdClient(context, {
    hosts: [process.env.ETCD_URL || 'http://localhost:2379'],
  });
  const client = await etcd.start();
  const keyName = 'testkey';

  client.addListener('start', (info) => {
    t.equal(info.key, keyName);
    t.equal(info.method, 'put');
  });

  client.addListener('finish', (info) => {
    t.equal(info.key, keyName);
    t.equal(info.method, 'put');
  });

  await client.put(context, keyName, { a: 1 }, 60);
});
