import threading
import time
import random
from queue import Queue, Empty
from statistics import mean, median




def p95(values):
    if not values:
        return 0.0
    xs = sorted(values)
    k = int((len(xs) - 1) * 0.95)
    return xs[k]

def fmt_s(x):
    return f"{x:.6f}s"




class LockStats:
    def __init__(self, name: str, contention_threshold_s: float = 0.010):
        self.name = name
        self._meta_lock = threading.Lock()

        self.start_t = time.perf_counter()
        self.contention_threshold_s = contention_threshold_s

        self.acquire_count = 0
        self.wait_samples = []
        self.hold_samples = []
        self.contention_events = 0

    def now_rel(self):
        return time.perf_counter() - self.start_t

    def record(self, wait_s: float, hold_s: float):
        with self._meta_lock:
            self.acquire_count += 1
            self.wait_samples.append(wait_s)
            self.hold_samples.append(hold_s)
            if wait_s >= self.contention_threshold_s:
                self.contention_events += 1

    def report(self, total_runtime_s: float):
        waits = self.wait_samples
        holds = self.hold_samples
        if not waits:
            waits = [0.0]
        if not holds:
            holds = [0.0]

        contention_pct = (self.contention_events / max(1, self.acquire_count)) * 100.0

        lines = []
        lines.append("=" * 92)
        lines.append(f"LOCK CONTENTION REPORT — {self.name}")
        lines.append("=" * 92)
        lines.append(f"Total runtime                 : {fmt_s(total_runtime_s)}")
        lines.append(f"Total lock acquisitions       : {self.acquire_count}")
        lines.append(f"Wait time (avg / med)         : {fmt_s(mean(waits))} / {fmt_s(median(waits))}")
        lines.append(f"Wait time (p95 / max)         : {fmt_s(p95(waits))} / {fmt_s(max(waits))}")
        lines.append(f"Contention events (>{self.contention_threshold_s*1000:.0f}ms)     : {self.contention_events}")
        lines.append(f"Contention percentage         : {contention_pct:.1f}%")
        lines.append(f"Hold time (avg / med)         : {fmt_s(mean(holds))} / {fmt_s(median(holds))}")
        lines.append(f"Hold time (p95 / max)         : {fmt_s(p95(holds))} / {fmt_s(max(holds))}")
        lines.append("")
        return "\n".join(lines)

class InstrumentedLock:
    def __init__(self, name: str, stats: LockStats):
        self._lock = threading.Lock()
        self.name = name
        self.stats = stats

    def __enter__(self):
        t0 = time.perf_counter()
        self._lock.acquire()
        t1 = time.perf_counter()
        self._acquired_at = t1
        self._wait_s = (t1 - t0)
        return self

    def __exit__(self, exc_type, exc, tb):
        t2 = time.perf_counter()
        hold_s = (t2 - self._acquired_at)
        self._lock.release()
        self.stats.record(self._wait_s, hold_s)




class RequestStats:
    def __init__(self):
        self._lock = threading.Lock()
        self.latencies = []
        self.queue_max_depth = 0
        self.completed = 0

    def record_latency(self, latency_s: float):
        with self._lock:
            self.latencies.append(latency_s)
            self.completed += 1

    def record_queue_depth(self, depth: int):
        with self._lock:
            if depth > self.queue_max_depth:
                self.queue_max_depth = depth

    def report(self):
        xs = self.latencies or [0.0]
        lines = []
        lines.append("=" * 92)
        lines.append("END-TO-END REQUEST LATENCY REPORT")
        lines.append("=" * 92)
        lines.append(f"Requests completed            : {self.completed}")
        lines.append(f"Queue max depth               : {self.queue_max_depth}")
        lines.append(f"Latency (avg / med)           : {fmt_s(mean(xs))} / {fmt_s(median(xs))}")
        lines.append(f"Latency (p95 / max)           : {fmt_s(p95(xs))} / {fmt_s(max(xs))}")
        lines.append("")
        return "\n".join(lines)




"""
Shared resources:
- ORDERS_DB_MUTEX    : writing orders, updating status
- INVENTORY_DB_MUTEX : decrementing stock
- AUDIT_LOG_MUTEX    : writing audit log

This version is fixed:
- Slow I/O or CPU work is done OUTSIDE the locks.
- Locks only protect tiny critical sections that mutate shared state.
"""

def slow_disk_write():

    time.sleep(random.uniform(0.08, 0.40))

def slow_payment_gateway_call():

    time.sleep(random.uniform(0.05, 0.35))

def slow_json_serialize():

    time.sleep(random.uniform(0.02, 0.12))

def write_audit_log(order_id: int, lock: InstrumentedLock):
    """
    Fixed: JSON serialization and disk I/O happen without holding the audit log lock.
    The lock is only used for the quick critical section that mutates shared state.
    """

    slow_json_serialize()


    with lock:

        pass


    slow_disk_write()

def update_inventory(sku: str, qty: int, lock: InstrumentedLock):
    """
    Fixed: inventory calculations and slow disk I/O are done outside the lock.
    The lock only protects the in-memory shared inventory state.
    """

    time.sleep(random.uniform(0.01, 0.03))


    with lock:

        pass


    slow_disk_write()

def write_order_db(order_id: int, lock: InstrumentedLock):
    """
    Fixed: slow payment gateway call and disk I/O are done without holding the DB lock.
    The lock only wraps the quick DB transaction boundary (shared state mutation).
    """

    slow_payment_gateway_call()


    with lock:

        pass


    slow_disk_write()




def run_simulation(
    *,
    workers: int = 8,
    orders: int = 60,
    produce_interval_s=(0.00, 0.03),
    seed: int = 7
):
    random.seed(seed)

    req_stats = RequestStats()


    orders_lock_stats = LockStats("ORDERS_DB_MUTEX")
    inv_lock_stats = LockStats("INVENTORY_DB_MUTEX")
    audit_lock_stats = LockStats("AUDIT_LOG_MUTEX")

    ORDERS_DB_MUTEX = InstrumentedLock("ORDERS_DB_MUTEX", orders_lock_stats)
    INVENTORY_DB_MUTEX = InstrumentedLock("INVENTORY_DB_MUTEX", inv_lock_stats)
    AUDIT_LOG_MUTEX = InstrumentedLock("AUDIT_LOG_MUTEX", audit_lock_stats)

    q = Queue()
    stop_token = object()

    t_start = time.perf_counter()


    def producer():
        for oid in range(1, orders + 1):
            enqueue_t = time.perf_counter()
            sku = f"SKU-{random.randint(100, 130)}"
            qty = random.randint(1, 3)
            q.put((oid, sku, qty, enqueue_t))
            req_stats.record_queue_depth(q.qsize())
            time.sleep(random.uniform(*produce_interval_s))

        for _ in range(workers):
            q.put(stop_token)


    def worker_thread(worker_id: int):
        while True:
            item = q.get()
            if item is stop_token:
                q.task_done()
                return

            oid, sku, qty, enqueue_t = item

            update_inventory(sku, qty, INVENTORY_DB_MUTEX)
            write_order_db(oid, ORDERS_DB_MUTEX)
            write_audit_log(oid, AUDIT_LOG_MUTEX)

            done_t = time.perf_counter()
            req_stats.record_latency(done_t - enqueue_t)
            q.task_done()

    threads = []
    threads.append(threading.Thread(target=producer, name="producer", daemon=True))
    for i in range(workers):
        threads.append(threading.Thread(target=worker_thread, args=(i,), name=f"worker-{i}", daemon=True))

    for t in threads:
        t.start()

    q.join()
    total_runtime_s = time.perf_counter() - t_start

    header = []
    header.append("\n" + "#" * 92)
    header.append("SIMULATION RESULT — GOOD_MODE (Lock scope minimized)")
    header.append("#" * 92 + "\n")

    print("\n".join(header))
    print(req_stats.report())
    print(orders_lock_stats.report(total_runtime_s))
    print(inv_lock_stats.report(total_runtime_s))
    print(audit_lock_stats.report(total_runtime_s))




if __name__ == "__main__":
    run_simulation(
        workers=8,
        orders=60,
        produce_interval_s=(0.00, 0.02),
        seed=7
    )
