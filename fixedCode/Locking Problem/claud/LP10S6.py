import threading
import time
from datetime import datetime
from statistics import mean, median
from queue import Queue




class LockStats:
    def __init__(self, name):
        self.name = name
        self._lock = threading.Lock()
        self.start_t = time.perf_counter()


        self.wait_times = {}
        self.hold_times = {}
        self.pages_by_worker = {}


        self.acquire_events = []
        self.release_events = []


        self.jobs = 0
        self.total_pages = 0

    def _now_rel(self):
        return time.perf_counter() - self.start_t

    def record_job(self, pages):
        with self._lock:
            self.jobs += 1
            self.total_pages += pages

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
            print(f"Throughput (jobs/sec)       : {self.jobs/total_runtime:.2f}")
            print(f"Throughput (pages/sec)      : {self.total_pages/total_runtime:.2f}")


        if self.acquire_events:
            ordered = " -> ".join(name for _, name in sorted(self.acquire_events))
            print(f"Acquisition order           : {ordered}")


        print("\nPer-thread metrics:")
        print(f"{'Worker':>18} | {'Pages':>5} | {'Wait':>12} | {'Hold(actual)':>12} | {'Acquired@':>10} | {'Released@':>10}")
        print("-"*94)
        acq_at = {name: t for t, name in self.acquire_events}
        rel_at = {name: t for t, name in self.release_events}
        all_workers = sorted(set(self.wait_times) | set(self.hold_times) | set(self.pages_by_worker))
        for w in all_workers:
            pages = self.pages_by_worker.get(w, 0)
            wt = fmt(self.wait_times.get(w, 0.0))
            ht = fmt(self.hold_times.get(w, 0.0))
            at = fmt(acq_at.get(w, 0.0))
            rt = fmt(rel_at.get(w, 0.0))
            print(f"{w:>18} | {pages:>5} | {wt:>12} | {ht:>12} | {at:>10} | {rt:>10}")
        print("="*94 + "\n")





class PrinterSpooler:
    """
    Queue-based printer spooler that ensures fair FIFO processing
    and prevents blocking suspension problem.
    """
    def __init__(self):
        self.job_queue = Queue()
        self.worker_thread = None
        self.running = False
        self.stats = LockStats("PRINTER_SPOOLER_QUEUE")
        
    def start(self):
        """Start the printer worker thread"""
        self.running = True
        self.worker_thread = threading.Thread(target=self._process_queue, daemon=True)
        self.worker_thread.start()
        
    def stop(self):
        """Stop the printer worker thread"""
        self.running = False
        self.job_queue.put(None)
        if self.worker_thread:
            self.worker_thread.join()
    
    def _process_queue(self):
        """Worker thread that processes print jobs sequentially"""
        while self.running:
            job = self.job_queue.get()
            if job is None:
                break
                
            employee_name, document_name, pages, worker_name, completion_event, wait_start = job
            

            wait_s = time.perf_counter() - wait_start
            self.stats.record_acquire(worker_name, wait_s)
            
            print(f"[{now_ts()}] {employee_name}: ✓ Spooler acquired, printing... (wait {wait_s:.6f}s)")
            

            hold_start = time.perf_counter()
            time.sleep(pages * 0.2)
            hold_s = time.perf_counter() - hold_start
            
            self.stats.record_pages_for_worker(worker_name, pages)
            self.stats.record_release(worker_name, hold_s)
            
            print(f"[{now_ts()}] {employee_name}: Print complete!")
            print(f"[{now_ts()}] {employee_name}: Released printer\n")
            

            completion_event.set()
            self.job_queue.task_done()
    
    def submit_job(self, employee_name, document_name, pages):
        """Submit a print job to the queue"""
        worker_name = threading.current_thread().name
        self.stats.record_job(pages)
        
        print(f"[{now_ts()}] {employee_name}: Waiting for printer to print '{document_name}' ({pages} pages)...")
        
        completion_event = threading.Event()
        wait_start = time.perf_counter()
        

        self.job_queue.put((employee_name, document_name, pages, worker_name, completion_event, wait_start))
        

        completion_event.wait()



spooler = PrinterSpooler()

def now_ts():
    return datetime.now().strftime("%H:%M:%S.%f")[:-3]

def print_job(employee_name, document_name, pages):
    """
    Simulates sending a document to a shared office printer through a spooler
    that uses a fair queue-based approach.
    """
    spooler.submit_job(employee_name, document_name, pages)

def main():
    print("=== Office Printer Spooler (Queue-Based) ===\n")


    spooler.start()


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


    spooler.stop()

    print("=== All documents printed ===")
    spooler.stats.report(title="QUEUE-BASED SPOOLER REPORT")

if __name__ == "__main__":
    main()