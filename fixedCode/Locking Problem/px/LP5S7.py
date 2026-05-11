import threading
import time
from datetime import datetime
from statistics import mean, median
from threading import local


_thread_stats = local()
_thread_stats.wait_times = {}
_thread_stats.hold_times = {}
_thread_stats.acquire_events = []
_thread_stats.release_events = []
_thread_stats.start_t = time.perf_counter()

def worker(thread_id, hold_time):
    """Worker function - lock-free stats collection"""
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Thread {thread_id}: Attempting to acquire lock")

    wait_start = time.perf_counter()
    lock.acquire()
    wait_s = time.perf_counter() - wait_start
    

    _thread_stats.wait_times[thread_id] = wait_s
    _thread_stats.acquire_events.append((_thread_stats.now_rel(), thread_id))

    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Thread {thread_id}: Lock ACQUIRED (wait {wait_s:.6f}s)")

    hold_start = time.perf_counter()
    time.sleep(hold_time)
    actual_hold = time.perf_counter() - hold_start

    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Thread {thread_id}: Releasing lock")
    lock.release()
    

    _thread_stats.hold_times[thread_id] = actual_hold
    _thread_stats.release_events.append((_thread_stats.now_rel(), thread_id))


class LockStats:
    def now_rel(self):
        return time.perf_counter() - _thread_stats.start_t

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
        waits = list(_thread_stats.wait_times.values())
        holds = list(_thread_stats.hold_times.values())
        total_runtime = self.now_rel()

        def fmt(x): return f"{x:.6f}s"

        print("\n" + "="*70)
        print("LOCK CONTENTION REPORT (LOCK-FREE STATS)")
        print("="*70)
        print(f"Threads observed : {len(_thread_stats.wait_times)}")
        print(f"Total runtime : {fmt(total_runtime)}")
        if waits:
            print(f"Wait time (avg / med) : {fmt(mean(waits))} / {fmt(median(waits))}")
            print(f"Wait time (p95 / max) : {fmt(self._percentile(waits,95))} / {fmt(max(waits))}")
            print(f"Contention events (>0s) : {sum(1 for w in waits if w > 0.0)}")
        if holds:
            print(f"Hold time (avg / med) : {fmt(mean(holds))} / {fmt(median(holds))}")
            print(f"Hold time (p95 / max) : {fmt(self._percentile(holds,95))} / {fmt(max(holds))}")

        if _thread_stats.acquire_events:
            ordered = " -> ".join(f"T{tid}" for _, tid in sorted(_thread_stats.acquire_events))
            print(f"Acquisition order : {ordered}")

        print("\nPer-thread metrics:")
        print(f"{'Thread':>6} | {'Wait':>12} | {'Hold(actual)':>12} | {'Acquired@':>10} | {'Released@':>10}")
        print("-"*70)
        all_ids = sorted(set(_thread_stats.wait_times) | set(_thread_stats.hold_times))
        acq_at = {tid: t for t, tid in _thread_stats.acquire_events}
        rel_at = {tid: t for t, tid in _thread_stats.release_events}
        for tid in all_ids:
            w = fmt(_thread_stats.wait_times.get(tid, 0.0))
            h = fmt(_thread_stats.hold_times.get(tid, 0.0))
            a = fmt(acq_at.get(tid, 0.0))
            r = fmt(rel_at.get(tid, 0.0))
            print(f"{tid:>6} | {w:>12} | {h:>12} | {a:>10} | {r:>10}")
        print("="*70 + "\n")


lock = threading.Lock()
stats = LockStats()

if __name__ == "__main__":
    threads = []
    threads.append(threading.Thread(target=worker, args=(1, 5)))
    threads.append(threading.Thread(target=worker, args=(2, 1)))
    threads.append(threading.Thread(target=worker, args=(3, 1)))
    threads.append(threading.Thread(target=worker, args=(4, 1)))

    print("Starting threads...\n")
    for t in threads:
        t.start()
        time.sleep(0.1)
    for t in threads:
        t.join()
    print("\nAll threads completed!")
    stats.report()
