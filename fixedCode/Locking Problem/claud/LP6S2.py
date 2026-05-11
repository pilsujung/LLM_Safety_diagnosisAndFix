import threading
import time
import queue
from datetime import datetime
from statistics import mean, median




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


        self.failed_acquisitions = {}

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

    def record_failed_acquire(self, thread_name):
        with self._lock:
            self.failed_acquisitions[thread_name] = self.failed_acquisitions.get(thread_name, 0) + 1

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
        
        with self._lock:
            total_failed = sum(self.failed_acquisitions.values())
        print(f"Total failed acquisitions     : {total_failed}")

        if waits:
            print(f"Wait time (avg / med)         : {fmt(mean(waits))} / {fmt(median(waits))}")
            print(f"Wait time (p95 / max)         : {fmt(self._percentile(waits,95))} / {fmt(max(waits))}")
            print(f"Contention events (>10ms)     : {sum(1 for w in waits if w > 0.010)}")
            print(f"Contention percentage         : {(sum(1 for w in waits if w > 0.010) / len(waits)) * 100:.1f}%")

        if holds:
            print(f"Hold time (avg / med)         : {fmt(mean(holds))} / {fmt(median(holds))}")
            print(f"Hold time (p95 / max)         : {fmt(self._percentile(holds,95))} / {fmt(max(holds))}")

        print("\nPer-thread aggregated metrics:")
        print(f"{'Thread':>26} | {'Acq#':>5} | {'Fail#':>5} | {'Wait(avg)':>12} | {'Wait(max)':>12} | {'Hold(avg)':>12} | {'Hold(max)':>12}")
        print("-" * 104)
        with self._lock:
            all_names = sorted(set(self.wait_times) | set(self.hold_times) | set(self.failed_acquisitions))
            for name in all_names:
                wlist = self.wait_times.get(name, [])
                hlist = self.hold_times.get(name, [])
                fail_n = self.failed_acquisitions.get(name, 0)
                acq_n = len(wlist)
                wavg = mean(wlist) if wlist else 0.0
                wmax = max(wlist) if wlist else 0.0
                havg = mean(hlist) if hlist else 0.0
                hmax = max(hlist) if hlist else 0.0
                print(f"{name:>26} | {acq_n:>5} | {fail_n:>5} | {fmt(wavg):>12} | {fmt(wmax):>12} | {fmt(havg):>12} | {fmt(hmax):>12}")
        print("=" * 104 + "\n")






orders_db_mutex = threading.Lock()     
audit_log_mutex = threading.Lock()     

orders_db_stats = LockStats("ORDERS_DB_MUTEX")
audit_log_stats = LockStats("AUDIT_LOG_MUTEX")


request_q = queue.Queue()


state_lock = threading.Lock()
worker_state = {}   


lat_lock = threading.Lock()
req_latencies = []   
max_queue_depth = 0
skipped_requests = 0

def log(msg: str):
    tid = threading.get_ident()
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] T{tid % 10000:04d} {threading.current_thread().name}: {msg}")

def set_state(state: str):
    with state_lock:
        worker_state[threading.current_thread().name] = state

def record_latency(lat_s: float):
    with lat_lock:
        req_latencies.append(lat_s)

def record_skip():
    global skipped_requests
    with lat_lock:
        skipped_requests += 1




def run_end_of_day_settlement_job():
    thread_name = threading.current_thread().name

    log("EOD Settlement: attempting ORDERS_DB mutex (with 2s timeout)...")
    wait_start = time.perf_counter()
    

    acquired = orders_db_mutex.acquire(timeout=2.0)
    wait_s = time.perf_counter() - wait_start
    
    if not acquired:
        log(f"EOD Settlement: ✗ could not acquire ORDERS_DB (timeout after {wait_s:.6f}s) — rescheduling")
        orders_db_stats.record_failed_acquire(thread_name)
        return
    
    orders_db_stats.record_acquire(thread_name, wait_s)
    log(f"EOD Settlement: ✓ locked ORDERS_DB (wait {wait_s:.6f}s) — starting reconciliation")

    hold_start = time.perf_counter()


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
    for i, phase in enumerate(phases, start=1):
        log(f"EOD Settlement: phase {i}/{len(phases)} — {phase}")
        time.sleep(0.3)

    hold_s = time.perf_counter() - hold_start
    log("EOD Settlement: releasing ORDERS_DB mutex")
    orders_db_mutex.release()
    orders_db_stats.record_release(thread_name, hold_s)




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
            

            acquired = orders_db_mutex.acquire(timeout=1.0)
            wait_s = time.perf_counter() - wait_start
            
            if not acquired:
                log(f"API Request #{request_id} SKIPPED (could not acquire lock after {wait_s:.3f}s)")
                orders_db_stats.record_failed_acquire(threading.current_thread().name)
                record_skip()
                set_state("idle")
                continue
            
            orders_db_stats.record_acquire(threading.current_thread().name, wait_s)

            set_state("in_db")
            hold_start = time.perf_counter()
            time.sleep(0.22)
            hold_s = time.perf_counter() - hold_start

            orders_db_mutex.release()
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

    log("AuditLog: attempting AUDIT_LOG mutex (with 1s timeout)...")
    wait_start = time.perf_counter()
    
    acquired = audit_log_mutex.acquire(timeout=1.0)
    wait_s = time.perf_counter() - wait_start
    
    if not acquired:
        log(f"AuditLog: ✗ could not acquire AUDIT_LOG (timeout after {wait_s:.6f}s) — skipping flush")
        audit_log_stats.record_failed_acquire(thread_name)
        return
    
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
    print("Order Service demo: FIXED - non-blocking with timeouts to prevent blocking suspension\n")


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
        skipped = skipped_requests

    if lats or skipped > 0:
        print("=" * 92)
        print("END-TO-END REQUEST LATENCY REPORT")
        print("=" * 92)
        print(f"Requests completed           : {len(lats)}")
        print(f"Requests skipped (timeout)   : {skipped}")
        print(f"Queue max depth              : {mqd}")
        if lats:
            p95 = LockStats._percentile(lats, 95)
            print(f"Latency (avg / med)          : {fmt(mean(lats))} / {fmt(median(lats))}")
            print(f"Latency (p95 / max)          : {fmt(p95)} / {fmt(max(lats))}")
        print("=" * 92 + "\n")

    orders_db_stats.report()
    audit_log_stats.report()