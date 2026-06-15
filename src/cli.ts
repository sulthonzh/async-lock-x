#!/usr/bin/env node
import { Mutex } from './Mutex.js';
import { Semaphore } from './Semaphore.js';
import { ReadWriteLock } from './ReadWriteLock.js';
import { Barrier } from './Barrier.js';

const cmd = process.argv[2] || 'demo';

async function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}

async function mutexDemo() {
  const mutex = new Mutex();
  let sharedValue = 0;

  const worker = async (id: number) => {
    const release = await mutex.acquire();
    try {
      const tmp = sharedValue;
      await sleep(10);
      sharedValue = tmp + 1;
      console.log(`  worker ${id}: set value to ${sharedValue}`);
    } finally {
      release();
    }
  };

  console.log('Mutex — 5 workers, only 1 active at a time');
  await Promise.all([1, 2, 3, 4, 5].map(worker));
  console.log(`  final value: ${sharedValue} (should be 5)`);
}

async function semDemo() {
  const sem = new Semaphore(2);
  let active = 0;
  let maxActive = 0;

  const worker = async (id: number) => {
    const release = await sem.acquire();
    try {
      active++;
      maxActive = Math.max(maxActive, active);
      console.log(`  worker ${id}: started (${sem.available} permits left)`);
      await sleep(20);
      active--;
    } finally {
      release();
    }
  };

  console.log('Semaphore(2) — 6 workers, max 2 concurrent');
  await Promise.all([1, 2, 3, 4, 5, 6].map(worker));
  console.log(`  max concurrent: ${maxActive} (should be 2)`);
}

async function rwDemo() {
  const rw = new ReadWriteLock();
  let data = 0;

  const reader = async (id: number) => {
    await rw.readLock(async () => {
      console.log(`  reader ${id}: read value=${data}`);
      await sleep(10);
    });
  };

  const writer = async (id: number, val: number) => {
    await rw.writeLock(async () => {
      data = val;
      console.log(`  writer ${id}: set value=${data}`);
      await sleep(10);
    });
  };

  console.log('ReadWriteLock — multiple readers, exclusive writer');
  await Promise.all([
    reader(1), reader(2), reader(3),
    writer(1, 42),
    reader(4), reader(5),
  ]);
}

async function barrierDemo() {
  const barrier = new Barrier(3, () => {
    console.log('  >> all arrived, barrier reset');
  });

  const worker = async (id: number) => {
    console.log(`  worker ${id}: working...`);
    await sleep(id * 20);
    console.log(`  worker ${id}: waiting (needs ${barrier.waitingFor} more)`);
    await barrier.wait();
    console.log(`  worker ${id}: passed barrier`);
  };

  console.log('Barrier(3) — 3 workers sync at a point');
  await Promise.all([1, 2, 3].map(worker));
}

async function info() {
  const m = new Mutex();
  const s = new Semaphore(3);
  const rw = new ReadWriteLock();
  const b = new Barrier(5);

  console.log('async-lock-x — zero-dep async synchronization primitives');
  console.log('');
  console.log('Mutex:');
  console.log(`  locked: ${m.locked}, waitingCount: ${m.waitingCount}`);
  console.log('');
  console.log('Semaphore(3):');
  console.log(`  available: ${s.available}, maxConcurrency: ${s.maxConcurrency}, waitingCount: ${s.waitingCount}`);
  console.log('');
  console.log('ReadWriteLock:');
  console.log(`  readerCount: ${rw.readerCount}, writerActive: ${rw.writerActive}, waitingCount: ${rw.waitingCount}`);
  console.log('');
  console.log('Barrier(5):');
  console.log(`  target: ${b.target}, waitingFor: ${b.waitingFor}, generation: ${b.generation}`);
}

const cmds: Record<string, () => Promise<void>> = {
  demo: async () => {
    await mutexDemo();
    console.log('');
    await semDemo();
    console.log('');
    await rwDemo();
    console.log('');
    await barrierDemo();
  },
  mutex: mutexDemo,
  semaphore: semDemo,
  rwlock: rwDemo,
  barrier: barrierDemo,
  info,
};

const fn = cmds[cmd];
if (!fn) {
  console.error(`Unknown command: ${cmd}`);
  console.error('Available: demo, mutex, semaphore, rwlock, barrier, info');
  process.exit(1);
}

fn().catch((e) => {
  console.error(e);
  process.exit(1);
});
