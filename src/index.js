import assert from 'assert';
import { Etcd3, EtcdLockFailedError } from 'etcd3';
import { EventEmitter } from 'events';

function statusCode(error) {
  if (error) {
    return error.errorCode || 'unknown';
  }
  return 0;
}

const LOGGER = Symbol('Logger property for locks');

// This is a bit of a throwback to the etcd2 data model, and it only supports one "level" deep,
// but that's useful enough to keep for us...
function unpackJson(node, prefix = '') {
  return Object.entries(node).reduce((acc, [key, value]) => {
    const keyPart = key.substring(prefix.length).replace(/^\//, '');
    acc[keyPart] = value ? JSON.parse(value) : value;
    return acc;
  }, {});
}

async function delay(ms) {
  return new Promise(accept => setTimeout(accept, ms));
}

async function executeUpToTime(func, maxTimeMs) {
  if (maxTimeMs <= 0) {
    return func();
  }
  const usefulError = new Error('Function timed out');
  return Promise.race([func(), new Promise((accept, reject) => setTimeout(() => reject(usefulError), maxTimeMs))]);
}

export default class Etcd3Client extends EventEmitter {
  constructor(context, opts) {
    super();
    const { hosts, options } = (opts || {});
    this.baseLogger = context.logger || context.gb?.logger;
    assert(this.baseLogger?.info, 'Constructor must have a logger property');
    context.logger.info('Initializing etcd client', {
      hosts: hosts || '<default>',
    });
    const finalOptions = {
      hosts,
      dialTimeout: 1500,
      ...options,
    };
    if (typeof hosts === 'string') {
      finalOptions.hosts = hosts.split(',');
    }
    this.maxRetries = (options && 'maxRetries' in options) ? options.maxRetries : 2;

    this.etcd = new Etcd3(finalOptions);
  }

  async start() {
    return this;
  }

  stop() {
    this.etcd.close();
  }

  finishCall(callInfo, status) {
    this.emit('finish', { status, ...callInfo });
  }

  async get(context, key, options) {
    const callInfo = {
      client: this,
      context,
      key,
      method: 'get',
    };
    const logger = context.gb?.logger || this.baseLogger;
    logger.info('etcd get', { key });
    this.emit('start', callInfo);

    try {
      const value = await (options?.recursive ? this.etcd.getAll().prefix(key) : this.etcd.get(key));
      this.finishCall(callInfo, 0);
      if (!value) {
        logger.info('etcd get empty', { key });
        return value;
      }
      logger.info('etcd get ok', { key });
      return options?.recursive ? unpackJson(value, key) : JSON.parse(value);
    } catch (error) {
      const code = statusCode(error);
      logger.info('etcd get failed', { key, code });
      this.finishCall(callInfo, code);
      throw error;
    }
  }

  /**
   * ttl is in seconds
   */
  async put(context, key, value, ttl) {
    const callInfo = {
      client: this,
      context,
      key,
      value,
      ttl,
      method: 'put',
    };
    const logger = context.gb?.logger || this.baseLogger;
    logger.info('etcd put', { key, ttl });
    this.emit('start', callInfo);

    const stringValue = JSON.stringify(value);
    let lease;
    try {
      if (ttl) {
        lease = this.etcd.lease(ttl);
        await lease.put(key).value(stringValue);
      } else {
        await this.etcd.put(key).value(stringValue);
      }
      this.finishCall(callInfo, 0);
      logger.info('etcd was put', { key });
    } catch (error) {
      const code = statusCode(error);
      logger.info('etcd put failed', { key, code });
      this.finishCall(callInfo, code);
      throw error;
    } finally {
      if (lease) {
        lease.release();
      }
    }
  }

  async del(context, key) {
    const callInfo = {
      client: this,
      context,
      key,
      method: 'del',
    };
    const logger = context.gb?.logger || this.baseLogger;
    logger.info('etcd del', { key });
    this.emit('start', callInfo);

    try {
      await this.etcd.delete().key(key);
      this.finishCall(callInfo, 0);
      logger.info('etcd del ok', { key });
    } catch (error) {
      const code = statusCode(error);
      logger.info('etcd del failed', { key, code });
      this.finishCall(callInfo, code);
      throw error;
    }
  }

  async acquireLock(context, key, timeout = 10, maxWait = 30) {
    const callInfo = {
      client: this,
      context,
      key,
      method: 'acquireLock',
    };
    const logger = context.gb?.logger || this.baseLogger;
    logger.info('etcd acquire', { key });
    this.emit('start', callInfo);

    const lock = this.etcd.lock(key).ttl(timeout);
    lock[LOGGER] = { logger, key };

    const startTime = Date.now();
    let attempt = 0;
    while (Date.now() - startTime < maxWait * 1000) {
      attempt += 1;
      try {
        // eslint-disable-next-line no-await-in-loop
        await lock.acquire();
        const waitTime = Date.now() - startTime;
        logger.info('Acquired lock', { key, waitTime });
        this.finishCall(callInfo, 'acq');
        return lock;
      } catch (error) {
        logger.warn('Lock contention', { key, attempt });
        if (!(error instanceof EtcdLockFailedError)) {
          this.finishCall(callInfo, 'err');
          throw error;
        }
        // eslint-disable-next-line no-await-in-loop
        await delay(250 * attempt);
      }
    }
    this.finishCall(callInfo, 'timeout');
    const waitTime = Date.now() - startTime;
    const error = new Error('Timed out waiting for lock');
    error.waitTime = waitTime;
    throw error;
  }

  // eslint-disable-next-line class-methods-use-this
  async releaseLock(lock) {
    try {
      const { key, logger } = lock[LOGGER] || {};
      await lock.release();
      if (logger) {
        logger.info('Lock released', { key });
        delete lock[LOGGER];
      }
    } catch (error) {
      // Nothing to do for this error - eat it
    }
  }

  /**
   * This method is expensive. Please don't call it unless you need it.
   * For example: when making a critical area idempotent.
   * Even if you think you need it consult #guild-server-devs first.
   */
  async memoize(context, key, func, ttl = 60 * 5, timeout = 10, maxWait = 30, maxExecutionTime = 0) {
    const callInfo = {
      client: this,
      context,
      key,
      method: 'memoize',
    };

    const logger = context.gb?.logger || this.baseLogger;
    logger.info('etcd memoize', { key });
    this.emit('start', callInfo);

    let lock;
    let value;
    let fnError;
    const lockKey = `${key}-lock`;
    const valueKey = `${key}-value`;

    try {
      value = await this.get(context, valueKey);
      if (value) {
        this.finishCall(callInfo, 'val-prelock');
      } else {
        lock = await this.acquireLock(context, lockKey, timeout, maxWait);
        value = await this.get(context, valueKey);
        if (value) {
          this.finishCall(callInfo, 'val-postlock');
        } else {
          logger.info('etcd memoize exec', { key });
          try {
            await executeUpToTime(func, maxExecutionTime * 1000);
            value = await func();
          } catch (error) {
            fnError = true;
            throw error;
          }
          if (ttl !== 0) {
            await this.put(context, valueKey, value, ttl);
          }
          this.finishCall(callInfo, 'val-eval');
        }
      }
    } catch (error) {
      logger.error(fnError ? 'etcd memoize fn failed' : 'etcd memoize failed', error);
      this.finishCall(callInfo, fnError ? 'fn-error' : 'error');
      throw error;
    } finally {
      if (lock) {
        this.releaseLock(lock);
      }
    }

    return value;
  }

  async watcher(context, key) {
    const logger = context.gb?.logger || this.baseLogger;
    logger.info('Starting etcd watch', { key });

    return this.etcd.watch()
      .key(key)
      .create()
      .then(watcher => {
        watcher
          .on('disconnected', () => logger.info('etcd watcher disconnected', { key }))
          .on('connected', () => logger.info('etcd watcher connected', { key }));
        return watcher;
      });
  }
}
