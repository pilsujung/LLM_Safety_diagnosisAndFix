import threading
import time
import queue
from datetime import datetime
from statistics import mean, median
from collections import defaultdict




class LockStats:
    def __init__(self, name: str):
        self.name = name
        self._lock = threading.Lock()
        self.start_t = time.perf_counter()


        self.wait_times = {}
        self.hold_times = {}


        self.acquire_events = []
        self.release_events = []


        self._all_waits = []
        self._all_holds = []

    def _now_rel(self):
        return time.perf_counter() - self.start_t

    def record_acquire(self, thread_name, wait_s):
        with self._lock:
            self.wait_times.setdefault(thread_name, []).append(wait_s)
            self._all_waits.append(wait_s)
            self.acquire_events.append((self._now_rel(), thread_name))

    def record_release(self, thread_name, hold_s):
        with self._lock:
            self.hold_times.setdefault(thread_name, []).append(hold_s)
            self._all_holds.append(hold_s)
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

    def report(self):
        def fmt(x): return f"{x:.6f}s"
        total_runtime = self._now_rel()

        waits = list(self._all_waits)
        holds = list(self._all_holds)

        print("\n" + "=" * 92)
        print(f"LOCK CONTENTION REPORT — {self.name}")
        print("=" * 92)
        print(f"Total runtime                : {fmt(total_runtime)}")
        print(f"Total lock acquisitions       : {len(waits)}")

        if waits:
            print(f"Wait time (avg / med)         : {fmt(mean(waits))} / {fmt(median(waits))}")
            print(f"Wait time (p95 / max)         : {fmt(self._percentile(waits,95))} / {fmt(max(waits))}")
            print(f"Contention events (>10ms)     : {sum(1 for w in waits if w > 0.010)}")
            print(f"Contention percentage         : {(sum(1 for w in waits if w > 0.010) / len(waits)) * 100:.1f}%")

        if holds:
            print(f"Hold time (avg / med)         : {fmt(mean(holds))} / {fmt(median(holds))}")
            print(f"Hold time (p95 / max)         : {fmt(self._percentile(holds,95))} / {fmt(max(holds))}")

        print("\nPer-thread aggregated metrics:")
        print(f"{'Thread':>26} | {'Acq#':>5} | {'Wait(avg)':>12} | {'Wait(max)':>12} | {'Hold(avg)':>12} | {'Hold(max)':>12}")
        print("-" * 92)
        with self._lock:
            all_names = sorted(set(self.wait_times) | set(self.hold_times))
            for name in all_names:
                wlist = self.wait_times.get(name, [])
                hlist = self.hold_times.get(name, [])
                acq_n = len(wlist)
                wavg = mean(wlist) if wlist else 0.0
                wmax = max(wlist) if wlist else 0.0
                havg = mean(hlist) if hlist else 0.0
                hmax = max(hlist) if hlist else 0.0
                print(f"{name:>26} | {acq_n:>5} | {fmt(wavg):>12} | {fmt(wmax):>12} | {fmt(havg):>12} | {fmt(hmax):>12}")
        print("=" * 92 + "\n")





class RWLock:
    """Reader-Writer lock: multiple concurrent readers OR one exclusive writer"""
    def __init__(self):
        self._lock = threading.Lock()
        self._readers = 0
        self._writers = 0
        self._read_ready = threading.Condition(self._lock)
        self._write_ready = threading.Condition(self._lock)
    
    def acquire_read(self):
        """Acquire read lock (shared access)"""
        with self._lock:
            while self._writers > 0:
                self._read_ready.wait()
            self._readers += 1
    
    def release_read(self):
        """Release read lock"""
        with self._lock:
            self._readers -= 1
            if self._readers == 0:
                self._write_ready.notify()
    
    def acquire_write(self):
        """Acquire write lock (exclusive access)"""
        with self._lock:
            while self._writers > 0 or self._readers > 0:
                self._write_ready.wait()
            self._writers += 1
    
    def release_write(self):
        """Release write lock"""
        with self._lock:
            self._writers -= 1
            self._write_ready.notify()
            self._read_ready.notify_all()







orders_db_rwlock = RWLock()
audit_log_mutex = threading.Lock()

orders_db_stats = LockStats("ORDERS_DB_RWLOCK")
audit_log_stats = LockStats("AUDIT_LOG_MUTEX")


orders_database = []
orders_db_version_lock = threading.Lock()


request_q = queue.Queue()


state_lock = threading.Lock()
worker_state = {}


lat_lock = threading.Lock()
req_latencies = []
max_queue_depth = 0

def log(msg: str):
    tid = threading.get_ident()
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] T{tid % 10000:04d} {threading.current_thread().name}: {msg}")

def set_state(state: str):
    with state_lock:
        worker_state[threading.current_thread().name] = state

def record_latency(lat_s: float):
    with lat_lock:
        req_latencies.append(lat_s)




def run_end_of_day_settlement_job():
    thread_name = threading.current_thread().name


    log("EOD Settlement: acquiring WRITE lock for snapshot...")
    wait_start = time.perf_counter()
    orders_db_rwlock.acquire_write()
    wait_s = time.perf_counter() - wait_start
    orders_db_stats.record_acquire(thread_name, wait_s)
    log(f"EOD Settlement: ✓ locked for WRITE (wait {wait_s:.6f}s) — taking snapshot")

    hold_start = time.perf_counter()
    

    with orders_db_version_lock:
        snapshot = list(orders_database)
    
    hold_s = time.perf_counter() - hold_start
    

    orders_db_rwlock.release_write()
    orders_db_stats.record_release(thread_name, hold_s)
    log(f"EOD Settlement: ✓ released WRITE lock (held {hold_s:.6f}s) — snapshot complete")


    phases = [
        "Scan today's orders",
        "Aggregate totals",
        "Reconcile with payment gateway",
        "Write settlement rows",
        "Update ledger balances",
        "Finalize & checkpoint",
        "External confirmation callback",
        "Post-settlement snapshot",
    ]
    
    log("EOD Settlement: processing snapshot (lock-free)...")
    for i, phase in enumerate(phases, start=1):
        log(f"EOD Settlement: phase {i}/{len(phases)} — {phase} [NO LOCK HELD]")
        time.sleep(1.0)
    
    log("EOD Settlement: all phases complete (no blocking!)")




def api_worker():
    while True:
        item = request_q.get()
        try:
            if item is None:
                return

            request_id, enq_t = item
            set_state("parsing/auth")
            time.sleep(0.06)


            set_state("waiting_db_lock")
            wait_start = time.perf_counter()
            orders_db_rwlock.acquire_read()
            wait_s = time.perf_counter() - wait_start
            orders_db_stats.record_acquire(threading.current_thread().name, wait_s)

            set_state("in_db")
            hold_start = time.perf_counter()
            

            with orders_db_version_lock:
                _ = len(orders_database)
            time.sleep(0.22)
            
            hold_s = time.perf_counter() - hold_start

            orders_db_rwlock.release_read()
            orders_db_stats.record_release(threading.current_thread().name, hold_s)

            set_state("render/response")
            time.sleep(0.04)

            done_t = time.perf_counter()
            record_latency(done_t - enq_t)

            log(f"API Request #{request_id} completed (queue->done latency {done_t - enq_t:.3f}s, db_wait {wait_s:.3f}s)")

            set_state("idle")

        finally:
            request_q.task_done()




def request_generator(total_requests: int, interval_s: float):
    global max_queue_depth
    for i in range(1, total_requests + 1):
        enq_t = time.perf_counter()
        request_q.put((i, enq_t))

        qsz = request_q.qsize()
        with lat_lock:
            if qsz > max_queue_depth:
                max_queue_depth = qsz

        log(f"ENQUEUE API Request #{i} (queue_size={qsz})")
        time.sleep(interval_s)




def flush_audit_log():
    thread_name = threading.current_thread().name

    log("AuditLog: attempting AUDIT_LOG mutex...")
    wait_start = time.perf_counter()
    audit_log_mutex.acquire()
    wait_s = time.perf_counter() - wait_start
    audit_log_stats.record_acquire(thread_name, wait_s)
    log(f"AuditLog: ✓ locked AUDIT_LOG (wait {wait_s:.6f}s) — flushing to disk")

    hold_start = time.perf_counter()
    time.sleep(2.0)
    hold_s = time.perf_counter() - hold_start

    log("AuditLog: flush complete, releasing AUDIT_LOG")
    audit_log_mutex.release()
    audit_log_stats.record_release(thread_name, hold_s)




def monitor(stop_evt: threading.Event, interval_s: float = 0.5):
    while not stop_evt.is_set():
        with state_lock:
            states = list(worker_state.values())
        with lat_lock:
            qsz = request_q.qsize()
            mqd = max_queue_depth

        total = len(states) if states else 0
        waiting = sum(1 for s in states if s == "waiting_db_lock")
        in_db = sum(1 for s in states if s == "in_db")
        idle = sum(1 for s in states if s == "idle")
        parsing = sum(1 for s in states if s == "parsing/auth")

        log(
            f"MONITOR: queue={qsz} (max={mqd}) | workers={total} "
            f"| waiting_db={waiting} in_db={in_db} parsing={parsing} idle={idle}"
        )
        time.sleep(interval_s)




if __name__ == "__main__":
    print("Order Service demo: FIXED - no more blocking suspension!\n")
    print("KEY FIXES:")
    print("1. Reader-Writer Lock: API workers use READ lock (concurrent)")
    print("2. Snapshot Pattern: Settlement takes quick snapshot, releases lock")
    print("3. Lock-free Processing: Heavy work done outside critical section\n")


    with orders_db_version_lock:
        orders_database = [{"id": i, "amount": 100} for i in range(1000)]


    WORKERS = 3
    workers = []
    for idx in range(1, WORKERS + 1):
        t = threading.Thread(target=api_worker, name=f"API-Worker-{idx}", daemon=True)
        with state_lock:
            worker_state[t.name] = "idle"
        workers.append(t)
        t.start()


    settlement = threading.Thread(target=run_end_of_day_settlement_job, name="Batch-EOD-Settlement")
    settlement.start()


    time.sleep(0.3)


    stop_evt = threading.Event()
    mon = threading.Thread(target=monitor, args=(stop_evt,), name="Monitor")
    mon.start()


    gen = threading.Thread(target=request_generator, args=(25, 0.10), name="API-Acceptor")
    gen.start()


    audit = threading.Thread(target=flush_audit_log, name="Worker-AuditLogFlush")
    audit.start()


    gen.join()
    request_q.join()


    stop_evt.set()
    mon.join()

    for _ in workers:
        request_q.put(None)
    request_q.join()

    settlement.join()
    audit.join()


    print("\nAll operations completed.\n")

    def fmt(x): return f"{x:.6f}s"
    with lat_lock:
        lats = list(req_latencies)
        mqd = max_queue_depth

    if lats:
        p95 = LockStats._percentile(lats, 95)
        print("=" * 92)
        print("END-TO-END REQUEST LATENCY REPORT")
        print("=" * 92)
        print(f"Requests completed           : {len(lats)}")
        print(f"Queue max depth              : {mqd}")
        print(f"Latency (avg / med)          : {fmt(mean(lats))} / {fmt(median(lats))}")
        print(f"Latency (p95 / max)          : {fmt(p95)} / {fmt(max(lats))}")
        print("=" * 92 + "\n")

    orders_db_stats.report()
    audit_log_stats.report()
    
    print("\n" + "=" * 92)
    print("SUMMARY OF FIXES")
    print("=" * 92)
    print("✓ Replaced mutex with Reader-Writer lock")
    print("✓ API workers now acquire READ lock (multiple concurrent readers)")
    print("✓ Settlement job uses snapshot pattern (quick write lock, then lock-free)")
    print("✓ Long operations (8 seconds) now happen WITHOUT holding lock")
    print("✓ Result: No blocking suspension, much lower latency!")
    print("=" * 92)