import threading
import time
from datetime import datetime
from statistics import mean, median
from collections import defaultdict


class LockStats:
    def __init__(self):
        self._lock = threading.Lock()
        self.wait_times = {}
        self.hold_times = {}
        self.acquire_events = []
        self.release_events = []
        self.start_t = time.perf_counter()
        self._thread_data = threading.local()

    def now_rel(self):
        return time.perf_counter() - self.start_t

    def record_acquire(self, thread_id, wait_s):

        self._thread_data.wait_s = wait_s
        self._thread_data.acquired = True

    def record_release(self, thread_id, hold_s):

        self._thread_data.hold_s = hold_s
        self._thread_data.released = True

    def _merge_thread_data(self, thread_id):
        """Merge thread-local data into global stats (called once per thread at end)"""
        with self._lock:
            if hasattr(self._thread_data, 'wait_s'):
                self.wait_times[thread_id] = self._thread_data.wait_s
                self.acquire_events.append((self.now_rel(), thread_id))
            if hasattr(self._thread_data, 'hold_s'):
                self.hold_times[thread_id] = self._thread_data.hold_s
                self.release_events.append((self.now_rel(), thread_id))

    def _percentile(self, values, p):
        if not values:
            return 0.0
        xs = sorted(values)
        k = (len(xs)-1) * (p/100.0)
        f = int(k)
        c = min(f+1, len(xs)-1)
        if f == c:
            return xs[f]
        return xs[f] + (xs[c]-xs[f]) * (k - f)

    def report(self):
        waits = list(self.wait_times.values())
        holds = list(self.hold_times.values())
        total_runtime = self.now_rel()

        def fmt(x): return f"{x:.6f}s"

        print("\n" + "="*70)
        print("LOCK CONTENTION REPORT")
        print("="*70)
        print(f"Threads observed : {len(self.wait_times)}")
        print(f"Total runtime : {fmt(total_runtime)}")
        if waits:
            print(f"Wait time (avg / med) : {fmt(mean(waits))} / {fmt(median(waits))}")
            print(f"Wait time (p95 / max) : {fmt(self._percentile(waits,95))} / {fmt(max(waits))}")
            print(f"Contention events (>0s) : {sum(1 for w in waits if w > 0.0)}")
        if holds:
            print(f"Hold time (avg / med) : {fmt(mean(holds))} / {fmt(median(holds))}")
            print(f"Hold time (p95 / max) : {fmt(self._percentile(holds,95))} / {fmt(max(holds))}")

        if self.acquire_events:
            ordered = " -> ".join(f"T{tid}" for _, tid in sorted(self.acquire_events))
            print(f"Acquisition order : {ordered}")

        print("\nPer-thread metrics:")
        print(f"{'Thread':>6} | {'Wait':>12} | {'Hold(actual)':>12} | {'Acquired@':>10} | {'Released@':>10}")
        print("-"*70)
        all_ids = sorted(set(self.wait_times) | set(self.hold_times))
        acq_at = {tid: t for t, tid in self.acquire_events}
        rel_at = {tid: t for t, tid in self.release_events}
        for tid in all_ids:
            w = fmt(self.wait_times.get(tid, 0.0))
            h = fmt(self.hold_times.get(tid, 0.0))
            a = fmt(acq_at.get(tid, 0.0))
            r = fmt(rel_at.get(tid, 0.0))
            print(f"{tid:>6} | {w:>12} | {h:>12} | {a:>10} | {r:>10}")
        print("="*70 + "\n")



lock = threading.Lock()
stats = LockStats()

def worker(thread_id, hold_time):
    """Worker function that attempts to acquire the lock"""
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Thread {thread_id}: Attempting to acquire lock")

    wait_start = time.perf_counter()
    lock.acquire()
    wait_s = time.perf_counter() - wait_start
    stats.record_acquire(thread_id, wait_s)

    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Thread {thread_id}: Lock ACQUIRED (wait {wait_s:.6f}s)")

    hold_start = time.perf_counter()
    time.sleep(hold_time)
    actual_hold = time.perf_counter() - hold_start

    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Thread {thread_id}: Releasing lock")
    lock.release()
    stats.record_release(thread_id, actual_hold)


    stats._merge_thread_data(thread_id)


if __name__ == "__main__":
    threads = []
    threads.append(threading.Thread(target=worker, args=(1, 5)))
    threads.append(threading.Thread(target=worker, args=(2, 1)))
    threads.append(threading.Thread(target=worker, args=(3, 1)))
    threads.append(threading.Thread(target=worker, args=(4, 1)))

    print("Starting threads...\n")
    for t in threads:
        t.start()

    for t in threads:
        t.join()

    print("\nAll threads completed!")
    stats.report()
