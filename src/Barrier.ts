/**
 * Barrier — synchronization point where N tasks wait until all arrive.
 *
 * Once the target count is reached, all waiting tasks unblock simultaneously.
 * Optionally runs a completion callback. Reusable for multiple rounds.
 *
 * @example
 * ```ts
 * const barrier = new Barrier(3, () => console.log('round done'));
 * // 3 workers call barrier.wait() — all proceed after the 3rd arrives
 * ```
 */
export class Barrier {
  private _count: number;
  private readonly _target: number;
  private readonly _onComplete?: () => void;
  private _waiters: Array<{ resolve: () => void }> = [];
  private _generation = 0;

  constructor(target: number, onComplete?: () => void) {
    if (target < 1 || !Number.isInteger(target)) {
      throw new RangeError('Barrier target must be a positive integer');
    }
    this._target = target;
    this._count = target;
    this._onComplete = onComplete;
  }

  /** Number of tasks still needed before this round completes. */
  get waitingFor(): number {
    return this._count;
  }

  /** Total tasks required per round. */
  get target(): number {
    return this._target;
  }

  /** Current round (increments each time the barrier resets). */
  get generation(): number {
    return this._generation;
  }

  /**
   * Wait at the barrier. Returns when `target` tasks have called wait().
   * The last arrival triggers the completion callback (if any) and resets
   * the barrier for the next round.
   */
  async wait(): Promise<void> {
    if (this._count <= 0) {
      // Already satisfied this generation (shouldn't happen with proper use)
      return;
    }

    this._count--;

    if (this._count === 0) {
      // Last one — release everyone
      this._onComplete?.();
      const waiters = this._waiters;
      this._waiters = [];
      this._generation++;
      this._count = this._target;

      for (const w of waiters) {
        w.resolve();
      }
      return;
    }

    return new Promise<void>((resolve) => {
      this._waiters.push({ resolve });
    });
  }
}
