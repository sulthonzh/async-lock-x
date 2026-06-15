/**
 * ReadWriteLock — multiple concurrent readers, exclusive writer.
 *
 * Readers don't block each other. A writer gets exclusive access —
 * no other reader or writer runs while a write lock is held.
 *
 * Writer preference: if a writer is waiting, new readers queue
 * behind it to prevent writer starvation.
 *
 * @example
 * ```ts
 * const rw = new ReadWriteLock();
 * const data = await rw.readLock(async () => cache.get(key));
 * await rw.writeLock(async () => cache.set(key, value));
 * ```
 */
export class ReadWriteLock {
  private _readers = 0;
  private _writerActive = false;
  private _pendingWriters = 0;
  private _readerQueue: Array<() => void> = [];
  private _writerQueue: Array<() => void> = [];

  /** Active reader count. */
  get readerCount(): number {
    return this._readers;
  }

  /** Whether a writer currently holds the lock. */
  get writerActive(): boolean {
    return this._writerActive;
  }

  /** Total tasks waiting (readers + writers). */
  get waitingCount(): number {
    return this._readerQueue.length + this._writerQueue.length;
  }

  /**
   * Acquire a read lock. Multiple readers can hold simultaneously,
   * unless a writer is active or waiting (writer preference).
   */
  async readLock<T>(fn: () => Promise<T> | T): Promise<T>;
  async readLock(): Promise<() => void>;
  async readLock(fn?: () => Promise<unknown> | unknown): Promise<(() => void) | unknown> {
    const acquire = this._acquireRead();

    if (fn) {
      const release = await acquire;
      try {
        return await fn();
      } finally {
        release();
      }
    }
    return acquire;
  }

  /**
   * Acquire a write lock. Exclusive — no readers or other writers.
   */
  async writeLock<T>(fn: () => Promise<T> | T): Promise<T>;
  async writeLock(): Promise<() => void>;
  async writeLock(fn?: () => Promise<unknown> | unknown): Promise<(() => void) | unknown> {
    const acquire = this._acquireWrite();

    if (fn) {
      const release = await acquire;
      try {
        return await fn();
      } finally {
        release();
      }
    }
    return acquire;
  }

  private _acquireRead(): Promise<() => void> {
    // Writer active or waiting → queue
    if (this._writerActive || this._pendingWriters > 0) {
      return new Promise<() => void>((resolve) => {
        this._readerQueue.push(() => {
          this._readers++;
          resolve(this._releaseRead());
        });
      });
    }

    this._readers++;
    return Promise.resolve(this._releaseRead());
  }

  private _acquireWrite(): Promise<() => void> {
    this._pendingWriters++;

    if (!this._writerActive && this._readers === 0) {
      this._pendingWriters--;
      this._writerActive = true;
      return Promise.resolve(this._releaseWrite());
    }

    return new Promise<() => void>((resolve) => {
      this._writerQueue.push(() => {
        this._pendingWriters--;
        this._writerActive = true;
        resolve(this._releaseWrite());
      });
    });
  }

  private _releaseRead(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;

      this._readers--;

      // If no more readers and a writer is waiting, let it proceed
      if (this._readers === 0) {
        const nextWriter = this._writerQueue.shift();
        if (nextWriter) {
          nextWriter();
        }
      }
    };
  }

  private _releaseWrite(): () => void {
    let released = false;
    return () => {
      if (released) return;
      released = true;

      this._writerActive = false;

      // Writer preference: next writer goes first
      const nextWriter = this._writerQueue.shift();
      if (nextWriter) {
        nextWriter();
        return;
      }

      // No writers waiting → release all queued readers
      while (this._readerQueue.length > 0) {
        const r = this._readerQueue.shift()!;
        r();
      }
    };
  }
}
