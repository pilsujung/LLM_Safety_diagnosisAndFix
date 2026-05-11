import threading
import time
from datetime import datetime
from statistics import mean, median
from collections import defaultdict




class LockStats:
    def __init__(self, name):
        self.name = name
        self.start_t = time.perf_counter()


        self.wait_times = defaultdict(list)
        self.hold_times = defaultdict(list)
        self.pages_by_worker = defaultdict(int)


        self.acquire_events = []
        self.release_events = []


        self.jobs = 0
        self.total_pages = 0
        self._stats_lock = threading.Lock()

    def _now_rel(self):
        return time.perf_counter() - self.start_t

    def record_job(self, pages):
        self.jobs += 1
        self.total_pages += pages

    def record_pages_for_worker(self, worker, pages):
        self.pages_by_worker[worker] += pages

    def record_acquire(self, worker, wait_s):
        self.wait_times[worker].append(wait_s)
        self.acquire_events.append((self._now_rel(), worker))

    def record_release(self, worker, hold_s):
        self.hold_times[worker].append(hold_s)
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

    def _aggregate_stats(self):
        """Aggregate lock-free data under final lock"""
        with self._stats_lock:
            all_waits = [w for ws in self.wait_times.values() for w in ws]
            all_holds = [h for hs in self.hold_times.values() for h in hs]
            return all_waits, all_holds

    def report(self, title=None):
        title = title or f"LOCK CONTENTION REPORT — {self.name}"
        def fmt(x): return f"{x:.6f}s"
        all_waits, all_holds = self._aggregate_stats()
        total_runtime = self._now_rel()

        print("\n" + "="*94)
        print(title)
        print("="*94)
        print(f"Total print jobs : {self.jobs}")
        print(f"Total pages : {self.total_pages}")
        print(f"Threads observed : {len(self.wait_times)}")
        print(f"Wall-clock runtime : {fmt(total_runtime)}")

        if all_waits:
            print(f"Wait time (avg / med) : {fmt(mean(all_waits))} / {fmt(median(all_waits))}")
            print(f"Wait time (p95 / max) : {fmt(self._percentile(all_waits,95))} / {fmt(max(all_waits))}")
            print(f"Contention events (>0s) : {sum(1 for w in all_waits if w > 0.0)}")
        if all_holds:
            print(f"Hold time (avg / med) : {fmt(mean(all_holds))} / {fmt(median(all_holds))}")
            print(f"Hold time (p95 / max) : {fmt(self._percentile(all_holds,95))} / {fmt(max(all_holds))}")


        if total_runtime > 0:
            print(f"Throughput (jobs/sec) : {self.jobs/total_runtime:.2f}")
            print(f"Throughput (pages/sec) : {self.total_pages/total_runtime:.2f}")


        if self.acquire_events:
            ordered = " -> ".join(name for _, name in sorted(self.acquire_events))
            print(f"Acquisition order : {ordered}")


        print("\nPer-thread metrics:")
        print(f"{'Worker':>18} | {'Pages':>5} | {'Wait':>12} | {'Hold(actual)':>12} | {'Acquired@':>10} | {'Released@':>10}")
        print("-"*94)
        acq_at = {name: t for t, name in self.acquire_events}
        rel_at = {name: t for t, name in self.release_events}
        all_workers = sorted(self.pages_by_worker.keys())
        for w in all_workers:
            pages = self.pages_by_worker[w]
            waits = self.wait_times[w]
            holds = self.hold_times[w]
            avg_wait = fmt(mean(waits)) if waits else "0.000000s"
            avg_hold = fmt(mean(holds)) if holds else "0.000000s"
            at = fmt(acq_at.get(w, 0.0))
            rt = fmt(rel_at.get(w, 0.0))
            print(f"{w:>18} | {pages:>5} | {avg_wait:>12} | {avg_hold:>12} | {at:>10} | {rt:>10}")
        print("="*94 + "\n")






spooler_mutex = threading.Lock()
stats = LockStats("PRINTER_SPOOLER_LOCK")

def now_ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def print_job(employee_name, document_name, pages):
    """
    FIXED: Lock-free stats collection eliminates nested lock contention.
    Only spooler_mutex is held during printing - stats are lock-free.
    """
    worker = threading.current_thread().name
    stats.record_job(pages)

    print(f"[{now_ts()}] {employee_name}: Waiting for printer to print '{document_name}' ({pages} pages)...")

    wait_start = time.perf_counter()
    with spooler_mutex:
        wait_s = time.perf_counter() - wait_start
        stats.record_acquire(worker, wait_s)
        print(f"[{now_ts()}] {employee_name}: ✓ Spooler acquired, printing... (wait {wait_s:.6f}s)")


        hold_start = time.perf_counter()

        time.sleep(pages * 0.2)
        hold_s = time.perf_counter() - hold_start
        
        print(f"[{now_ts()}] {employee_name}: Print complete!")


    stats.record_pages_for_worker(worker, pages)
    stats.record_release(worker, hold_s)
    print(f"[{now_ts()}] {employee_name}: Released printer\n")

def main():
    print("=== Office Printer Spooler (FIXED - No Blocking Suspension) ===\n")


    job_specs = [
        ("Alice", "Annual_Report.pdf", 20),
        ("Bob", "Meeting_Notes.docx", 2),
        ("Carol", "Invoice.pdf", 3),
        ("Dave", "Timesheet.xlsx", 1),
    ]

    threads = []
    for emp, doc, pages in job_specs:
        t = threading.Thread(target=print_job, args=(emp, doc, pages), name=f"PrintWorker-{emp}")
        threads.append(t)
        t.start()
        time.sleep(0.05)

    for t in threads:
        t.join()

    print("=== All documents printed ===")
    stats.report()

if __name__ == "__main__":
    main()
