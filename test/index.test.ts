import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Mutex } from '../src/Mutex.ts';
import { Semaphore } from '../src/Semaphore.ts';
import { ReadWriteLock } from '../src/ReadWriteLock.ts';
import { Barrier } from '../src/Barrier.ts';

const sleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms));

describe('Mutex', () => {
  it('starts unlocked', () => {
    const m = new Mutex();
    assert.equal(m.locked, false);
    assert.equal(m.waitingCount, 0);
  });

  it('acquire returns a release function', async () => {
    const m = new Mutex();
    const release = await m.acquire();
    assert.equal(typeof release, 'function');
    assert.equal(m.locked, true);
    release();
    assert.equal(m.locked, false);
  });

  it('serializes concurrent tasks', async () => {
    const m = new Mutex();
    const order: number[] = [];
    let active = 0;
    let maxActive = 0;

    const worker = async (id: number) => {
      const release = await m.acquire();
      try {
        active++;
        maxActive = Math.max(maxActive, active);
        order.push(id);
        await sleep(5);
      } finally {
        active--;
        release();
      }
    };

    await Promise.all([0, 1, 2, 3].map(worker));
    assert.equal(maxActive, 1);
    assert.deepEqual(order, [0, 1, 2, 3]);
  });

  it('runExclusive returns fn result', async () => {
    const m = new Mutex();
    const result = await m.runExclusive(async () => {
      await sleep(2);
      return 42;
    });
    assert.equal(result, 42);
  });

  it('runExclusive releases on error', async () => {
    const m = new Mutex();
    await assert.rejects(() =>
      m.runExclusive(async () => { throw new Error('boom'); })
    );
    assert.equal(m.locked, false);
  });

  it('release is idempotent', async () => {
    const m = new Mutex();
    const release = await m.acquire();
    release();
    release(); // double release should be safe
    assert.equal(m.locked, false);
  });

  it('FIFO ordering for waiters', async () => {
    const m = new Mutex();
    const order: number[] = [];

    // Hold lock first
    const release0 = await m.acquire();

    const workers = [1, 2, 3, 4].map(async (id) => {
      const rel = await m.acquire();
      order.push(id);
      rel();
    });

    await sleep(10);
    release0();
    await Promise.all(workers);
    assert.deepEqual(order, [1, 2, 3, 4]);
  });

  it('waitingCount tracks queued tasks', async () => {
    const m = new Mutex();
    const release = await m.acquire();
    assert.equal(m.waitingCount, 0);

    const p1 = m.acquire();
    assert.equal(m.waitingCount, 1);

    const p2 = m.acquire();
    assert.equal(m.waitingCount, 2);

    release();
    const r1 = await p1;
    assert.equal(m.locked, true);
    assert.equal(m.waitingCount, 1); // p2 still queued

    r1();
    const r2 = await p2;
    assert.equal(m.locked, true);
    r2();
    assert.equal(m.locked, false);
  });
});

describe('Semaphore', () => {
  it('throws for invalid concurrency', () => {
    assert.throws(() => new Semaphore(0), RangeError);
    assert.throws(() => new Semaphore(-1), RangeError);
    assert.throws(() => new Semaphore(1.5), RangeError);
  });

  it('allows N concurrent tasks', async () => {
    const sem = new Semaphore(3);
    let active = 0;
    let maxActive = 0;

    const worker = async () => {
      const rel = await sem.acquire();
      try {
        active++;
        maxActive = Math.max(maxActive, active);
        await sleep(10);
      } finally {
        active--;
        rel();
      }
    };

    await Promise.all(Array.from({ length: 10 }, worker));
    assert.equal(maxActive, 3);
  });

  it('tryAcquire returns null when full', async () => {
    const sem = new Semaphore(1);
    const rel = sem.tryAcquire();
    assert.ok(rel);
    assert.equal(sem.tryAcquire(), null);
    rel!();
    assert.equal(sem.available, 1);
  });

  it('runExclusive returns value', async () => {
    const sem = new Semaphore(2);
    const val = await sem.runExclusive(() => Promise.resolve('ok'));
    assert.equal(val, 'ok');
  });

  it('runExclusive releases on error', async () => {
    const sem = new Semaphore(2);
    await assert.rejects(() =>
      sem.runExclusive(async () => { throw new Error('x'); })
    );
    assert.equal(sem.available, 2);
  });

  it('release wakes queued waiter', async () => {
    const sem = new Semaphore(1);
    const r1 = await sem.acquire();
    let acquired = false;

    sem.acquire().then((r) => { acquired = true; r(); });
    await sleep(5);
    assert.equal(acquired, false);

    r1();
    await sleep(5);
    assert.equal(acquired, true);
    assert.equal(sem.available, 1);
  });
});

describe('ReadWriteLock', () => {
  it('allows concurrent readers', async () => {
    const rw = new ReadWriteLock();
    let active = 0;
    let maxActive = 0;

    const reader = async () => {
      await rw.readLock(async () => {
        active++;
        maxActive = Math.max(maxActive, active);
        await sleep(10);
        active--;
      });
    };

    await Promise.all([reader(), reader(), reader(), reader()]);
    assert.ok(maxActive >= 2, 'multiple readers should be concurrent');
  });

  it('writer is exclusive', async () => {
    const rw = new ReadWriteLock();
    let active = 0;
    let maxActive = 0;

    const writer = async () => {
      await rw.writeLock(async () => {
        active++;
        maxActive = Math.max(maxActive, active);
        await sleep(10);
        active--;
      });
    };

    await Promise.all([writer(), writer(), writer()]);
    assert.equal(maxActive, 1);
  });

  it('readLock returns value', async () => {
    const rw = new ReadWriteLock();
    const val = await rw.readLock(() => Promise.resolve(42));
    assert.equal(val, 42);
  });

  it('writeLock returns value', async () => {
    const rw = new ReadWriteLock();
    const val = await rw.writeLock(() => Promise.resolve('done'));
    assert.equal(val, 'done');
  });

  it('writer preference: readers wait for pending writer', async () => {
    const rw = new ReadWriteLock();
    const order: string[] = [];

    // Start a reader
    const r1Release = await rw.readLock();

    // Queue a writer (blocks because reader active)
    const writerP = rw.writeLock(async () => {
      order.push('writer');
      await sleep(5);
    });

    await sleep(5);

    // Queue a new reader — should wait behind writer
    const r2P = rw.readLock(async () => {
      order.push('reader2');
    });

    await sleep(5);
    order.push('reader1-release');
    r1Release();
    await sleep(15);

    await Promise.all([writerP, r2P]);

    // Writer should execute before reader2
    assert.ok(order.indexOf('writer') < order.indexOf('reader2'),
      `expected writer before reader2, got: ${order.join(', ')}`);
  });

  it('multiple writers serialize', async () => {
    const rw = new ReadWriteLock();
    let maxActive = 0;
    let active = 0;

    const writers = Array.from({ length: 5 }, () =>
      rw.writeLock(async () => {
        active++;
        maxActive = Math.max(maxActive, active);
        await sleep(5);
        active--;
      })
    );

    await Promise.all(writers);
    assert.equal(maxActive, 1);
  });

  it('release without function returns release fn', async () => {
    const rw = new ReadWriteLock();
    const rel = await rw.readLock();
    assert.equal(typeof rel, 'function');
    rel();

    const wrel = await rw.writeLock();
    assert.equal(typeof wrel, 'function');
    wrel();
  });
});

describe('Barrier', () => {
  it('throws for invalid target', () => {
    assert.throws(() => new Barrier(0), RangeError);
    assert.throws(() => new Barrier(-1), RangeError);
    assert.throws(() => new Barrier(2.5), RangeError);
  });

  it('waits for all parties', async () => {
    const b = new Barrier(3);
    let done = 0;

    const worker = async (delay: number) => {
      await sleep(delay);
      await b.wait();
      done++;
    };

    const all = Promise.all([worker(50), worker(100), worker(150)]);

    // After 80ms: worker1 done, worker2+3 still sleeping → not all arrived
    await sleep(80);
    assert.equal(done, 0); // barrier hasn't broken yet

    await all;
    assert.equal(done, 3);
  });

  it('runs completion callback', async () => {
    let cbCalled = 0;
    const b = new Barrier(2, () => { cbCalled++; });

    const w = async () => { await sleep(5); await b.wait(); };
    await Promise.all([w(), w()]);
    assert.equal(cbCalled, 1);
  });

  it('resets for reuse', async () => {
    const b = new Barrier(2);
    let round = 0;

    const doRound = async () => {
      const workers = [0, 1].map(async () => {
        await sleep(5);
        await b.wait();
      });
      await Promise.all(workers);
      round++;
    };

    await doRound();
    assert.equal(b.generation, 1);
    await doRound();
    assert.equal(b.generation, 2);
    assert.equal(round, 2);
  });

  it('waitingFor decrements as parties arrive', async () => {
    const b = new Barrier(3);
    assert.equal(b.waitingFor, 3);

    const w1 = b.wait();
    assert.equal(b.waitingFor, 2);

    const w2 = b.wait();
    assert.equal(b.waitingFor, 1);

    const w3 = b.wait();
    await Promise.all([w1, w2, w3]);
    assert.equal(b.waitingFor, 3); // reset
  });

  it('all parties unblock simultaneously', async () => {
    const b = new Barrier(3);
    let releasedAt = new Array<number | null>(3).fill(null);

    const worker = async (id: number) => {
      await sleep(id * 5);
      await b.wait();
      releasedAt[id] = Date.now();
    };

    await Promise.all([worker(0), worker(1), worker(2)]);
    const times = releasedAt.filter((t): t is number => t !== null);
    const spread = Math.max(...times) - Math.min(...times);
    assert.ok(spread < 20, `release times should be close, spread=${spread}ms`);
  });
});
