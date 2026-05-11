import threading
import time
import random
from datetime import datetime
from statistics import mean, median
from collections import defaultdict




class LockStats:
    def __init__(self, name):
        self.name = name
        self._lock = threading.Lock()
        self.start_t = time.perf_counter()


        self.wait_times = defaultdict(list)
        self.hold_times = defaultdict(list)


        self.wait_samples = []
        self.hold_samples = []


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
            self.wait_times[thread_name].append(wait_s)
            self.wait_samples.append(wait_s)
            self.acquire_events.append((self._now_rel(), thread_name))

    def record_release(self, thread_name, hold_s):
        with self._lock:
            self.hold_times[thread_name].append(hold_s)
            self.hold_samples.append(hold_s)
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

        waits = list(self.wait_samples)
        holds = list(self.hold_samples)
        total_runtime = self._now_rel()

        print("=" * 90)
        print(title)
        print(f"Total requests              : {self.requests}")
        print(f"Total runtime               : {fmt(total_runtime)}")

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

        print("\nPer-thread metrics (totals):")
        print(f"{'Thread':>18} | {'Wait(total)':>12} | {'Hold(total)':>12} | {'Acqs':>4}")
        print("-" * 90)
        all_names = sorted(set(self.wait_times) | set(self.hold_times))
        for name in all_names:
            w_total = sum(self.wait_times.get(name, []))
            h_total = sum(self.hold_times.get(name, []))
            acqs = len(self.hold_times.get(name, []))
            print(f"{name:>18} | {fmt(w_total):>12} | {fmt(h_total):>12} | {acqs:>4}")
        print("=" * 90 + "\n")





ledger_lock = threading.Lock()
stats = LockStats("LEDGER_WRITE_LOCK")


ledger = []
ledger_seq = 0

def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def acquire_with_backoff(lock: threading.Lock,
                         attempt_timeout_s: float = 0.05,
                         backoff_base_s: float = 0.005,
                         max_wait_s: float | None = 10.0) -> float:
    """
    Avoids indefinite blocking on a lock:
      - tries acquire() with a small timeout
      - backs off briefly with jitter
      - optionally fails after max_wait_s
    Returns total wait time.
    """
    start = time.perf_counter()
    while True:
        got = lock.acquire(timeout=attempt_timeout_s)
        if got:
            return time.perf_counter() - start

        waited = time.perf_counter() - start
        if max_wait_s is not None and waited >= max_wait_s:
            raise TimeoutError(f"Timed out waiting for lock after {waited:.6f}s")

        time.sleep(backoff_base_s + random.random() * backoff_base_s)

def post_ledger_entry(txn_id, processing_time):
    global ledger_seq

    worker = threading.current_thread().name
    trace_id = f"TXN-{txn_id:04d}"
    stats.record_request()

    print(f"[{ts()}] {worker} | {trace_id} | Starting ledger posting: enqueue request")


    pre_stages = ["balance_check", "journal_prepare", "replication_prepare"]
    pre_budget = max(processing_time * 0.95, 0.05)
    per_pre = max(pre_budget / len(pre_stages), 0.01)

    for step in pre_stages:
        print(f"[{ts()}] {worker} | {trace_id} | {step} ... (no lock)")
        time.sleep(per_pre)

    print(f"[{ts()}] {worker} | {trace_id} | Attempting to acquire LEDGER_WRITE_LOCK for commit...")


    wait_s = acquire_with_backoff(
        ledger_lock,
        attempt_timeout_s=0.05,
        backoff_base_s=0.005,
        max_wait_s=10.0
    )
    stats.record_acquire(worker, wait_s)


    t_acq = ts()
    hold_start = time.perf_counter()
    try:

        commit_time = min(max(processing_time * 0.05, 0.01), 0.10)
        time.sleep(commit_time)


        ledger_seq += 1
        ledger.append({"seq": ledger_seq, "txn": trace_id, "by": worker, "t": t_acq})
    finally:
        hold_s = time.perf_counter() - hold_start
        ledger_lock.release()

    stats.record_release(worker, hold_s)
    t_rel = ts()


    print(f"[{t_acq}] {worker} | {trace_id} | ✓ Lock ACQUIRED (wait {wait_s:.6f}s) -> commit...")
    print(f"[{t_rel}] {worker} | {trace_id} | Commit complete -> lock released (hold {hold_s:.6f}s)")
    print(f"[{t_rel}] {worker} | {trace_id} | Response sent\n")


def main():
    print("=== Core Banking — Ledger Posting Service (FIXED) ===")
    print("Minimizing lock hold time to prevent blocking suspension...\n")

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
    stats.report("LOCK CONTENTION REPORT — LEDGER_WRITE_LOCK (FIXED)")

if __name__ == "__main__":
    main()
