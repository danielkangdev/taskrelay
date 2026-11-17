/**
 * Fault injection for tests and experiments. Inert unless TASKRELAY_FAULTS is set.
 *
 *   TASKRELAY_FAULTS="crash_after_write@1,handler_throws:0.2"
 *
 * Each entry is `point[:probability][@attempt]`:
 *   - probability defaults to 1
 *   - @attempt restricts the fault to that attempt number (e.g. crash only the
 *     first time, so the retry can succeed)
 */
export const FAULT_POINTS = [
  'crash_after_write', // handler committed its write, process dies before reporting success
  'crash_before_complete', // handler returned, process dies before the "succeeded" UPDATE
  'hang_without_heartbeat', // worker stops heartbeating and never finishes the job
  'handler_throws', // handler raises an error
] as const;
export type FaultPoint = (typeof FAULT_POINTS)[number];

interface FaultRule {
  point: FaultPoint;
  probability: number;
  attempt?: number;
}

/** Thrown at a crash point. The worker turns it into a real or simulated process death. */
export class SimulatedCrash extends Error {
  constructor(public readonly point: FaultPoint) {
    super(`simulated crash at ${point}`);
    this.name = 'SimulatedCrash';
  }
}

export class InjectedFault extends Error {
  constructor(point: FaultPoint) {
    super(`injected fault at ${point}`);
    this.name = 'InjectedFault';
  }
}

let cache: { spec: string; rules: FaultRule[] } | undefined;

export function parseFaults(spec: string): FaultRule[] {
  return spec
    .split(',')
    .map((s) => s.trim())
    .filter(Boolean)
    .map((entry) => {
      const m = /^([a-z_]+)(?::([0-9.]+))?(?:@(\d+))?$/.exec(entry);
      if (!m || !(FAULT_POINTS as readonly string[]).includes(m[1]!)) {
        throw new Error(`invalid TASKRELAY_FAULTS entry: ${entry}`);
      }
      return {
        point: m[1] as FaultPoint,
        probability: m[2] === undefined ? 1 : Number(m[2]),
        attempt: m[3] === undefined ? undefined : Number(m[3]),
      };
    });
}

function rules(): FaultRule[] {
  const spec = process.env.TASKRELAY_FAULTS ?? '';
  if (!spec) return [];
  if (cache?.spec !== spec) cache = { spec, rules: parseFaults(spec) };
  return cache.rules;
}

/** Does `point` fire for a job on this attempt? */
export function shouldFault(point: FaultPoint, attempt: number): boolean {
  return rules().some(
    (r) => r.point === point && (r.attempt === undefined || r.attempt === attempt) && Math.random() < r.probability,
  );
}

/** Throw the appropriate error if `point` fires. */
export function fault(point: FaultPoint, attempt: number): void {
  if (!shouldFault(point, attempt)) return;
  if (point === 'handler_throws') throw new InjectedFault(point);
  throw new SimulatedCrash(point);
}
