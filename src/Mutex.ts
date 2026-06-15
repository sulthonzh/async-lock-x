/**
 * Mutex — mutual exclusion lock for async code.
 *
 * Only one task can hold the lock at a time. Other callers queue
 * in FIFO order and resume when the holder releases.
 *
 * @example
 * ```ts
 * const mutex = new Mutex();
 * const result = await mutex.runExclusive(async () => {
 *   // nobody else is in here
 *   return fetch();
 * });
 * ```
 */
export class Mutex {
  private _locked = false;
  private _waiters: Array<() => void> = [];

  /** True while a task holds the lock. */
  get locked(): boolean {
    return this._locked;
  }

  /** Number of tasks waiting to acquire. */
  get waitingCount(): number {
    return this._waiters.length;
  }

  /**
   * Acquire the lock. Returns a release function.
   * Resolves immediately if unlocked; otherwise queues until previous holders release.
   */
  async acquire(): Promise<() => void> {
    if (!this._locked) {
      this._locked = true;
      return this._createRelease();
    }

    return new Promise<() => void>((resolve) => {
      this._waiters.push(() => {
        this._locked = true;
        resolve(this._createRelease());
      });
    });
  }

  /**
   * Run `fn` while holding the lock, then release automatically.
   * If `fn` throws, the lock is still released.
   * Returns whatever `fn` returns.
   */
  async runExclusive<T>(fn: () => Promise<T> | T): Promise<T> {
    const release = await this.acquire();
    try {
      return await fn();
    } finally {
      release();
    }
  }

  private _createRelease(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;

      const next = this._waiters.shift();
      if (next) {
        next();
      } else {
        this._locked = false;
      }
    };
  }
}
