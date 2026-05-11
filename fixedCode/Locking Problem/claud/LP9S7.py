import threading
import time
import queue
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
        print(f"\n{'='*90}")
        print(f"{title:^90}")
        print(f"{'='*90}\n")
        
        def fmt(x): return f"{x:.6f}s"

        waits = list(self.wait_times.values())
        holds = list(self.hold_times.values())
        total_runtime = self._now_rel()

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





class PriorityLedgerProcessor:
    """
    Decouples request acceptance from processing.
    Short jobs get priority to reduce head-of-line blocking.
    """
    def __init__(self, stats):
        self.stats = stats
        self.queue = queue.PriorityQueue()
        self.ledger_lock = threading.Lock()
        self.shutdown = threading.Event()
        self.processor_thread = threading.Thread(target=self._process_loop, daemon=True)
        self.processor_thread.start()

    def submit(self, txn_id, processing_time, arrival_time):
        """Submit a request with priority (lower processing_time = higher priority)"""
        worker = threading.current_thread().name
        trace_id = f"TXN-{txn_id:04d}"
        

        priority = (processing_time, arrival_time)
        
        self.stats.record_request()
        print(f"[{ts()}] {worker} | {trace_id} | Submitting to priority queue (priority={processing_time:.1f}s)")
        self.queue.put((priority, txn_id, processing_time, arrival_time, worker, trace_id))

    def _process_loop(self):
        """Background processor that handles requests in priority order"""
        while not self.shutdown.is_set():
            try:

                priority, txn_id, processing_time, arrival_time, worker, trace_id = \
                    self.queue.get(timeout=0.1)
                
                self._process_ledger_entry(txn_id, processing_time, arrival_time, worker, trace_id)
                self.queue.task_done()
                
            except queue.Empty:
                continue

    def _process_ledger_entry(self, txn_id, processing_time, arrival_time, worker, trace_id):
        """Process a single ledger entry with exclusive lock"""
        print(f"[{ts()}] Processor | {trace_id} | Dequeued, attempting to acquire LEDGER_WRITE_LOCK...")
        

        wait_start = time.perf_counter()
        with self.ledger_lock:
            wait_s = time.perf_counter() - wait_start
            self.stats.record_acquire(worker, wait_s)
            print(f"[{ts()}] Processor | {trace_id} | ✓ Lock ACQUIRED (wait {wait_s:.6f}s) -> posting...")


            stages = ["balance_check", "journal_write", "replication_sync", "commit"]
            per_stage = max(processing_time / len(stages), 0.05)


            hold_start = time.perf_counter()
            for step in stages:
                print(f"[{ts()}] Processor | {trace_id} | {step} ...")
                time.sleep(per_stage)
            hold_s = time.perf_counter() - hold_start

            print(f"[{ts()}] Processor | {trace_id} | Posting complete -> releasing lock")


        self.stats.record_release(worker, hold_s)
        print(f"[{ts()}] Processor | {trace_id} | Lock released, response sent\n")

    def wait_completion(self):
        """Wait for all queued jobs to complete"""
        self.queue.join()
    
    def stop(self):
        """Shutdown the processor"""
        self.shutdown.set()
        self.processor_thread.join()


def ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]


def main():
    print("=== Core Banking — Ledger Posting Service (FIXED) ===")
    print("Using priority queue to mitigate head-of-line blocking...\n")

    stats = LockStats("LEDGER_WRITE_LOCK")
    processor = PriorityLedgerProcessor(stats)


    postings = [
        (1, 5.0),
        (2, 0.5),
        (3, 0.5),
        (4, 0.5),
        (5, 0.5),
    ]

    threads = []
    start_time = time.perf_counter()
    
    for txn_id, proc_time in postings:
        arrival_time = time.perf_counter() - start_time
        t = threading.Thread(
            target=processor.submit,
            args=(txn_id, proc_time, arrival_time),
            name=f"PostingWorker-{txn_id}"
        )
        threads.append(t)
        t.start()
        time.sleep(0.1)

    for t in threads:
        t.join()


    processor.wait_completion()
    processor.stop()

    print("=== All postings processed ===")
    stats.report("LOCK CONTENTION REPORT — LEDGER_WRITE_LOCK (PRIORITY QUEUE)")


if __name__ == "__main__":
    main()