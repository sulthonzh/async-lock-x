# async-lock-x

Zero-dependency async synchronization primitives for Node.js and browsers.

Mutex, Semaphore, ReadWriteLock, and Barrier — everything you need to coordinate concurrent async tasks without pulling in heavy dependencies.

## Why?

Coordinating async operations in JavaScript is error-prone. You *could* use `Promise.all()` for parallelism, but what about mutual exclusion? What about limiting concurrency? What about reader/writer patterns?

`async-lock-x` gives you the four most-needed sync primitives in ~300 lines of clean TypeScript, zero dependencies.

## Install

```bash
npm install async-lock-x
```

## Quick Start

```typescript
import { Mutex, Semaphore, ReadWriteLock, Barrier } from 'async-lock-x';

// Mutex — only one at a time
const mutex = new Mutex();
await mutex.runExclusive(async () => {
  // exclusive access guaranteed
});

// Semaphore — limit concurrency
const sem = new Semaphore(5); // max 5 parallel
const results = await Promise.all(urls.map(url =>
  sem.runExclusive(() => fetch(url))
));

// ReadWriteLock — multiple readers, exclusive writer
const rw = new ReadWriteLock();
const cached = await rw.readLock(async () => cache.get(key));
await rw.writeLock(async () => cache.set(key, value));

// Barrier — wait for everyone
const barrier = new Barrier(3);
await barrier.wait(); // blocks until 3 tasks call wait()
```

## API

### Mutex

Mutual exclusion lock. One holder at a time, FIFO queue.

| Method | Description |
|--------|-------------|
| `acquire()` | Returns a release function. Queues if locked. |
| `runExclusive(fn)` | Runs `fn` while locked, auto-releases. Returns `fn`'s result. |
| `locked` | `true` if a task holds the lock. |
| `waitingCount` | Number of queued tasks. |

```typescript
const m = new Mutex();

// Manual acquire/release
const release = await m.acquire();
try { /* critical section */ } finally { release(); }

// Or use runExclusive (recommended)
const result = await m.runExclusive(async () => compute());
```

### Semaphore

Counting semaphore. Allows up to N concurrent holders.

| Method | Description |
|--------|-------------|
| `new Semaphore(max)` | Create with N permits. |
| `acquire()` | Take one permit, returns release fn. |
| `tryAcquire()` | Non-blocking. Returns release fn or `null`. |
| `runExclusive(fn)` | Run with one permit, auto-release. |
| `available` | Free permits remaining. |
| `maxConcurrency` | Configured maximum. |

```typescript
// Limit DB connections to 3
const dbSem = new Semaphore(3);

async function query(sql: string) {
  return dbSem.runExclusive(() => db.execute(sql));
}

// Non-blocking attempt
const permit = dbSem.tryAcquire();
if (permit) {
  // got a permit
  permit();
} else {
  // at capacity, try later
}
```

### ReadWriteLock

Multiple concurrent readers, exclusive writer. Writer preference prevents starvation.

| Method | Description |
|--------|-------------|
| `readLock(fn?)` | Acquire read lock (shared). Auto-release if `fn` given. |
| `writeLock(fn?)` | Acquire write lock (exclusive). Auto-release if `fn` given. |
| `readerCount` | Active readers. |
| `writerActive` | Whether a writer holds the lock. |
| `waitingCount` | Total queued tasks. |

```typescript
const rw = new ReadWriteLock();

// Multiple readers proceed concurrently
await rw.readLock(async () => {
  // safe to read shared state
});

// Writer gets exclusive access — no readers or other writers
await rw.writeLock(async () => {
  // safe to modify shared state
});

// Manual mode
const release = await rw.readLock();
try { /* read */ } finally { release(); }
```

**Writer preference:** If a writer is waiting, new readers queue behind it. This prevents writer starvation under heavy read load.

### Barrier

Synchronization point. N tasks wait until all arrive, then all proceed.

| Method | Description |
|--------|-------------|
| `new Barrier(n, onComplete?)` | Create barrier for N parties. |
| `wait()` | Wait at barrier. Returns when all parties arrive. |
| `waitingFor` | Tasks still needed. |
| `generation` | Current round (increments each reset). |

```typescript
const barrier = new Barrier(3, () => {
  console.log('All workers synced!');
});

// Worker code
async function worker() {
  await doSetup();
  await barrier.wait(); // everyone waits here
  await doWork();       // all start together
}
```

The barrier auto-resets after each round, making it reusable for pipeline patterns.

## Design Decisions

- **Zero dependencies.** Pure TypeScript, works in Node.js and modern browsers.
- **FIFO fairness.** Mutex and Semaphore serve waiters in arrival order.
- **Writer preference.** ReadWriteLock prioritizes writers to prevent starvation.
- **Auto-release on error.** `runExclusive` always releases, even if `fn` throws.
- **Idempotent release.** Calling release() twice is safe (no-op).
- **No native code.** Pure JavaScript timers and Promises.

## CLI

```bash
npx async-lock-x demo       # Run all demos
npx async-lock-x mutex      # Mutex demo
npx async-lock-x semaphore  # Semaphore demo
npx async-lock-x rwlock     # ReadWriteLock demo
npx async-lock-x barrier    # Barrier demo
npx async-lock-x info       # Show lock states
```

## Testing

27 tests covering: basic acquire/release, serialization, FIFO ordering, concurrency limits, tryAcquire, error handling, idempotent release, reader/writer concurrency, writer exclusivity, writer preference, barrier synchronization, barrier reuse, and simultaneous release.

```bash
node --test
```

## License

MIT
