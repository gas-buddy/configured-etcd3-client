import tap from 'tap';
import pino from 'pino';
import uuidv4 from 'uuid/v4';
import EtcdClient from '../src/index';

const logger = pino();
const context = { logger, gb: { logger } };

tap.test('test_memoize', async (t) => {
  context.gb = context;
  const etcd = new EtcdClient(context, {
    hosts: [process.env.ETCD_URL || 'http://localhost:2379'],
  });
  const client = await etcd.start();

  const key = `cachekey-${uuidv4()}`;
  const oldValue = `oldval-${uuidv4()}`;
  const newValue = `newval-${uuidv4()}`;
  let value = oldValue;

  const memoFunc = () => value;

  let result = await client.memoize(context, key, memoFunc);
  t.strictEquals(memoFunc(), oldValue);
  t.strictEquals(result, oldValue);

  value = newValue;

  result = await client.memoize(context, key, memoFunc);
  t.strictEquals(memoFunc(), newValue);
  t.strictEquals(result, oldValue);

  const key2 = `concur-${uuidv4()}`;
  let secondRunValue = 'did not wait';
  const delayFunc = async () => {
    await new Promise(accept => setTimeout(accept, 1000));
    secondRunValue = 'new value';
    return 'old value';
  };

  const delayFunc2 = async () => {
    await new Promise(accept => setTimeout(accept, 1000));
    return secondRunValue;
  };

  const memo1 = client.memoize(context, key2, delayFunc, 0);
  await new Promise(accept => setTimeout(accept, 100));
  const memo2 = client.memoize(context, key2, delayFunc2, 0);
  t.strictEquals(await memo1, 'old value', 'Should get the old value the first time');
  t.strictEquals(await memo2, 'new value', 'Should get the new value the second time');
});
