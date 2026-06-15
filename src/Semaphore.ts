/**
 * Semaphore — counting lock that allows up to `maxConcurrency` simultaneous holders.
 *
 * Perfect for limiting parallelism (e.g. max 5 DB connections at once).
 *
 * @example
 * ```ts
 * const sem = new Semaphore(3); // max 3 concurrent
 * await sem.runExclusive(async () => {
 *   await doWork();
 * });
 * ```
 */
export class Semaphore {
  private _available: number;
  private readonly _max: number;
  private _waiters: Array<() => void> = [];

  constructor(maxConcurrency: number) {
    if (maxConcurrency < 1 || !Number.isInteger(maxConcurrency)) {
      throw new RangeError('Semaphore maxConcurrency must be a positive integer');
    }
    this._max = maxConcurrency;
    this._available = maxConcurrency;
  }

  /** Current available permits (0 = all in use). */
  get available(): number {
    return this._available;
  }

  /** Maximum permits configured at construction. */
  get maxConcurrency(): number {
    return this._max;
  }

  /** Tasks waiting for a permit. */
  get waitingCount(): number {
    return this._waiters.length;
  }

  /**
   * Acquire one permit. Returns a release function.
   * If all permits are in use, queues until one frees.
   */
  async acquire(): Promise<() => void> {
    if (this._available > 0) {
      this._available--;
      return this._createRelease();
    }

    return new Promise<() => void>((resolve) => {
      this._waiters.push(() => {
        this._available--;
        resolve(this._createRelease());
      });
    });
  }

  /**
   * Run `fn` with one permit, then release it automatically.
   */
  async runExclusive<T>(fn: () => Promise<T> | T): Promise<T> {
    const release = await this.acquire();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  /** Try to acquire without waiting. Returns release fn or `null` if no permits. */
  tryAcquire(): (() => void) | null {
    if (this._available === 0) return null;
    this._available--;
    return this._createRelease();
  }

  private _createRelease(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;

      this._available++;
      if (this._available > this._max) this._available = this._max;

      const next = this._waiters.shift();
      if (next && this._available > 0) {
        // The waiter will decrement _available in its queued callback
        next();
      }
    };
  }
}
