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
        self.pages_by_worker = {}
        self.failed_attempts = {}


        self.acquire_events = []
        self.release_events = []


        self.jobs = 0
        self.jobs_completed = 0
        self.jobs_failed = 0
        self.total_pages = 0

    def _now_rel(self):
        return time.perf_counter() - self.start_t

    def record_job(self, pages):
        with self._lock:
            self.jobs += 1
            self.total_pages += pages

    def record_job_completed(self):
        with self._lock:
            self.jobs_completed += 1

    def record_job_failed(self, worker):
        with self._lock:
            self.jobs_failed += 1
            self.failed_attempts[worker] = self.failed_attempts.get(worker, 0) + 1

    def record_pages_for_worker(self, worker, pages):
        with self._lock:
            self.pages_by_worker[worker] = self.pages_by_worker.get(worker, 0) + pages

    def record_acquire(self, worker, wait_s):
        with self._lock:
            self.wait_times[worker] = wait_s
            self.acquire_events.append((self._now_rel(), worker))

    def record_release(self, worker, hold_s):
        with self._lock:
            self.hold_times[worker] = hold_s
            self.release_events.append((self._now_rel(), worker))

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

        print("\n" + "="*94)
        print(title)
        print("="*94)
        print(f"Total print jobs            : {self.jobs}")
        print(f"Jobs completed              : {self.jobs_completed}")
        print(f"Jobs failed (timeout)       : {self.jobs_failed}")
        print(f"Total pages                 : {self.total_pages}")
        print(f"Threads observed            : {len(set(self.wait_times) | set(self.hold_times))}")
        print(f"Wall-clock runtime          : {fmt(total_runtime)}")

        if waits:
            print(f"Wait time (avg / med)       : {fmt(mean(waits))} / {fmt(median(waits))}")
            print(f"Wait time (p95 / max)       : {fmt(self._percentile(waits,95))} / {fmt(max(waits))}")
            print(f"Contention events (>0s)     : {sum(1 for w in waits if w > 0.0)}")
        if holds:
            print(f"Hold time (avg / med)       : {fmt(mean(holds))} / {fmt(median(holds))}")
            print(f"Hold time (p95 / max)       : {fmt(self._percentile(holds,95))} / {fmt(max(holds))}")


        if total_runtime > 0:
            print(f"Throughput (jobs/sec)       : {self.jobs_completed/total_runtime:.2f}")
            print(f"Throughput (pages/sec)      : {self.pages_by_worker.get('total', self.total_pages)/total_runtime:.2f}")


        if self.acquire_events:
            ordered = " -> ".join(name for _, name in sorted(self.acquire_events))
            print(f"Acquisition order           : {ordered}")


        print("\nPer-thread metrics:")
        print(f"{'Worker':>18} | {'Pages':>5} | {'Wait':>12} | {'Hold(actual)':>12} | {'Failed':>6} | {'Acquired@':>10} | {'Released@':>10}")
        print("-"*104)
        acq_at = {name: t for t, name in self.acquire_events}
        rel_at = {name: t for t, name in self.release_events}
        all_workers = sorted(set(self.wait_times) | set(self.hold_times) | set(self.pages_by_worker) | set(self.failed_attempts))
        for w in all_workers:
            pages = self.pages_by_worker.get(w, 0)
            wt = fmt(self.wait_times.get(w, 0.0))
            ht = fmt(self.hold_times.get(w, 0.0))
            failed = self.failed_attempts.get(w, 0)
            at = fmt(acq_at.get(w, 0.0))
            rt = fmt(rel_at.get(w, 0.0))
            print(f"{w:>18} | {pages:>5} | {wt:>12} | {ht:>12} | {failed:>6} | {at:>10} | {rt:>10}")
        print("="*104 + "\n")






spooler_mutex = threading.Lock()
stats = LockStats("PRINTER_SPOOLER_LOCK")

def now_ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def print_job(employee_name, document_name, pages):
    """
    Simulates sending a document to a shared office printer through a spooler
    that allows only one active print at a time.
    
    FIXED: Uses tryLock with timeout to avoid indefinite blocking.
    """
    worker = threading.current_thread().name
    stats.record_job(pages)

    print(f"[{now_ts()}] {employee_name}: Waiting for printer to print '{document_name}' ({pages} pages)...")

    wait_start = time.perf_counter()
    

    acquired = spooler_mutex.acquire(timeout=1.0)
    
    if acquired:
        try:
            wait_s = time.perf_counter() - wait_start
            stats.record_acquire(worker, wait_s)
            print(f"[{now_ts()}] {employee_name}: ✓ Spooler acquired, printing... (wait {wait_s:.6f}s)")


            hold_start = time.perf_counter()

            time.sleep(pages * 0.2)
            hold_s = time.perf_counter() - hold_start
            stats.record_pages_for_worker(worker, pages)
            stats.record_job_completed()

            print(f"[{now_ts()}] {employee_name}: Print complete!")
        finally:
            spooler_mutex.release()
            stats.record_release(worker, hold_s)
            print(f"[{now_ts()}] {employee_name}: Released printer\n")
    else:

        wait_s = time.perf_counter() - wait_start
        stats.record_job_failed(worker)
        print(f"[{now_ts()}] {employee_name}: ✗ Could not acquire printer (timeout after {wait_s:.6f}s), deferring job\n")

def main():
    print("=== Office Printer Spooler (Non-blocking) ===\n")


    job_specs = [
        ("Alice", "Annual_Report.pdf", 20),
        ("Bob",   "Meeting_Notes.docx", 2),
        ("Carol", "Invoice.pdf", 3),
        ("Dave",  "Timesheet.xlsx", 1),
    ]

    threads = []
    for emp, doc, pages in job_specs:
        t = threading.Thread(target=print_job, args=(emp, doc, pages), name=f"PrintWorker-{emp}")
        threads.append(t)
        t.start()
        time.sleep(0.05)

    for t in threads:
        t.join()

    print("=== All print jobs attempted ===")
    stats.report()

if __name__ == "__main__":
    main()