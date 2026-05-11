import threading
import time
from datetime import datetime
from statistics import mean, median




class LockStats:
    def __init__(self, name):
        self.name = name
        self._lock = threading.Lock()
        self.start_t = time.perf_counter()


        self.wait_times = {}
        self.hold_times = {}


        self.acquire_events = []
        self.release_events = []


        self.requests = 0

    def _now_rel(self):
        return time.perf_counter() - self.start_t

    def record_request(self):
        with self._lock:
            self.requests += 1

    def record_acquire(self, thread_name, wait_s):
        with self._lock:
            self.wait_times[thread_name] = wait_s
            self.acquire_events.append((self._now_rel(), thread_name))

    def record_release(self, thread_name, hold_s):
        with self._lock:
            self.hold_times[thread_name] = hold_s
            self.release_events.append((self._now_rel(), thread_name))

    @staticmethod
    def _percentile(values, p):
        if not values:
            return 0.0
        xs = sorted(values)
        k = (len(xs) - 1) * (p / 100.0)
        f = int(k)
        c = min(f + 1, len(xs) - 1)
        if f == c:
            return xs[f]
        return xs[f] + (xs[c] - xs[f]) * (k - f)

    def report(self, title=None):
        title = title or f"LOCK CONTENTION REPORT — {self.name}"
        def fmt(x): return f"{x:.6f}s"

        waits = list(self.wait_times.values())
        holds = list(self.hold_times.values())
        total_runtime = self._now_rel()

        print("\n" + "="*90)
        print(title)
        print(f"Total runtime               : {fmt(total_runtime)}")
        print(f"Total requests              : {self.requests}")
        if waits:
            print(f"Wait time (avg / med)       : {fmt(mean(waits))} / {fmt(median(waits))}")
            print(f"Wait time (p95 / max)       : {fmt(self._percentile(waits, 95))} / {fmt(max(waits))}")
            print(f"Contention events (>0s)     : {sum(1 for w in waits if w > 0.0)}")
        if holds:
            print(f"Hold time (avg / med)       : {fmt(mean(holds))} / {fmt(median(holds))}")
            print(f"Hold time (p95 / max)       : {fmt(self._percentile(holds, 95))} / {fmt(max(holds))}")


        if self.acquire_events:
            ordered = " -> ".join(name for _, name in sorted(self.acquire_events))
            print(f"Acquisition order           : {ordered}")


        print("\nPer-thread metrics:")
        print(f"{'Thread':>18} | {'Wait':>12} | {'Hold(actual)':>12} | {'Acquired@':>10} | {'Released@':>10}")
        print("-"*90)
        acq_at = {name: t for t, name in self.acquire_events}
        rel_at = {name: t for t, name in self.release_events}
        all_names = sorted(set(self.wait_times) | set(self.hold_times))
        for name in all_names:
            w = fmt(self.wait_times.get(name, 0.0))
            h = fmt(self.hold_times.get(name, 0.0))
            a = fmt(acq_at.get(name, 0.0))
            r = fmt(rel_at.get(name, 0.0))
            print(f"{name:>18} | {w:>12} | {h:>12} | {a:>10} | {r:>10}")
        print("="*90 + "\n")





ledger_lock = threading.Lock()
stats = LockStats("LEDGER_WRITE_LOCK")

def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def post_ledger_entry(txn_id, processing_time):
    """
    Simulates posting a ledger entry that requires exclusive access to the core ledger.
    FIX: only the true ledger mutation (journal_write + commit) is done under the
    LEDGER_WRITE_LOCK; the rest of the pipeline runs without holding the lock.
    This removes the long blocking suspension of other threads.
    """
    worker = threading.current_thread().name
    trace_id = f"TXN-{txn_id:04d}"
    stats.record_request()

    print(f"[{ts()}] {worker} | {trace_id} | Starting ledger posting: enqueue request")




    pre_lock_stages = ["balance_check", "replication_sync"]
    in_lock_stages = ["journal_write", "commit"]

    total_stages = len(pre_lock_stages) + len(in_lock_stages)
    per_stage = max(processing_time / total_stages, 0.05)


    for step in pre_lock_stages:
        print(f"[{ts()}] {worker} | {trace_id} | {step} (no ledger lock) ...")
        time.sleep(per_stage)


    print(f"[{ts()}] {worker} | {trace_id} | Attempting to acquire LEDGER_WRITE_LOCK for final commit...")
    wait_start = time.perf_counter()
    ledger_lock.acquire()
    wait_s = time.perf_counter() - wait_start
    stats.record_acquire(worker, wait_s)
    print(f"[{ts()}] {worker} | {trace_id} | ✓ Lock ACQUIRED (wait {wait_s:.6f}s) -> finalizing posting...")

    hold_start = time.perf_counter()
    hold_s = 0.0
    try:
        for step in in_lock_stages:
            print(f"[{ts()}] {worker} | {trace_id} | {step} (under ledger lock) ...")
            time.sleep(per_stage)
        hold_s = time.perf_counter() - hold_start
        print(f"[{ts()}] {worker} | {trace_id} | Posting complete -> releasing lock")
    finally:
        ledger_lock.release()
        stats.record_release(worker, hold_s)

    print(f"[{ts()}] {worker} | {trace_id} | Lock released, response sent\n")



def main():
    print("=== Core Banking — Ledger Posting Service ===")
    print("Simulating contention on exclusive ledger write lock...\n")


    postings = [
        (1, 5.0),
        (2, 0.5),
        (3, 0.5),
        (4, 0.5),
        (5, 0.5),
    ]

    threads = []
    for txn_id, proc_time in postings:
        t = threading.Thread(
            target=post_ledger_entry,
            args=(txn_id, proc_time),
            name=f"PostingWorker-{txn_id}"
        )
        threads.append(t)
        t.start()
        time.sleep(0.1)

    for t in threads:
        t.join()

    print("=== All postings processed ===")
    stats.report("LOCK CONTENTION REPORT — LEDGER_WRITE_LOCK")


if __name__ == "__main__":
    main()
