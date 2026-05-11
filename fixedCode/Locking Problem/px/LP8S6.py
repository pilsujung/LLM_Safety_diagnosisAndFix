import threading
import time
import random
from queue import Queue
from statistics import mean, median
from collections import deque




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
        self.start_t = time.perf_counter()
        self.contention_threshold_s = contention_threshold_s
        

        self.acquire_count = 0
        self.wait_samples = deque()
        self.hold_samples = deque()
        self.contention_events = 0
        

        self._local_wait_samples = deque()
        self._local_hold_samples = deque()
        self._local_count = 0
        self._local_contention = 0

    def now_rel(self):
        return time.perf_counter() - self.start_t

    def record(self, wait_s: float, hold_s: float):

        self._local_wait_samples.append(wait_s)
        self._local_hold_samples.append(hold_s)
        self._local_count += 1
        if wait_s >= self.contention_threshold_s:
            self._local_contention += 1
        

        if self._local_count >= 10:
            self.flush_local()

    def flush_local(self):
        if self._local_count == 0:
            return
            

        self.wait_samples.extend(self._local_wait_samples)
        self.hold_samples.extend(self._local_hold_samples)
        self.acquire_count += self._local_count
        self.contention_events += self._local_contention
        

        self._local_wait_samples.clear()
        self._local_hold_samples.clear()
        self._local_count = 0
        self._local_contention = 0

    def report(self, total_runtime_s: float):

        self.flush_local()
        
        waits = list(self.wait_samples) or [0.0]
        holds = list(self.hold_samples) or [0.0]

        contention_pct = (self.contention_events / max(1, self.acquire_count)) * 100.0

        lines = []
        lines.append("=" * 92)
        lines.append(f"LOCK CONTENTION REPORT — {self.name}")
        lines.append("=" * 92)
        lines.append(f"Total runtime : {fmt_s(total_runtime_s)}")
        lines.append(f"Total lock acquisitions : {self.acquire_count}")
        lines.append(f"Wait time (avg / med) : {fmt_s(mean(waits))} / {fmt_s(median(waits))}")
        lines.append(f"Wait time (p95 / max) : {fmt_s(p95(waits))} / {fmt_s(max(waits))}")
        lines.append(f"Contention events (>{self.contention_threshold_s*1000:.0f}ms) : {self.contention_events}")
        lines.append(f"Contention percentage : {contention_pct:.1f}%")
        lines.append(f"Hold time (avg / med) : {fmt_s(mean(holds))} / {fmt_s(median(holds))}")
        lines.append(f"Hold time (p95 / max) : {fmt_s(p95(holds))} / {fmt_s(max(holds))}")
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
        self.latencies = deque()
        self.queue_max_depth = 0
        self.completed = 0

        self._local_latencies = deque()
        self._local_completed = 0
        self._local_max_depth = 0

    def record_latency(self, latency_s: float):
        self._local_latencies.append(latency_s)
        self._local_completed += 1
        if self._local_completed >= 5:
            self.flush()

    def record_queue_depth(self, depth: int):
        if depth > self._local_max_depth:
            self._local_max_depth = depth
        if depth > self.queue_max_depth:
            self.queue_max_depth = depth
        if self._local_completed >= 5:
            self.flush()

    def flush(self):
        if self._local_completed == 0:
            return
        self.latencies.extend(self._local_latencies)
        self.completed += self._local_completed
        if self._local_max_depth > self.queue_max_depth:
            self.queue_max_depth = self._local_max_depth
        self._local_latencies.clear()
        self._local_completed = 0
        self._local_max_depth = 0

    def report(self):
        self.flush()
        xs = list(self.latencies) or [0.0]
        lines = []
        lines.append("=" * 92)
        lines.append("END-TO-END REQUEST LATENCY REPORT")
        lines.append("=" * 92)
        lines.append(f"Requests completed : {self.completed}")
        lines.append(f"Queue max depth : {self.queue_max_depth}")
        lines.append(f"Latency (avg / med) : {fmt_s(mean(xs))} / {fmt_s(median(xs))}")
        lines.append(f"Latency (p95 / max) : {fmt_s(p95(xs))} / {fmt_s(max(xs))}")
        lines.append("")
        return "\n".join(lines)




def slow_disk_write():

    time.sleep(random.uniform(0.08, 0.40))

def slow_payment_gateway_call():

    time.sleep(random.uniform(0.05, 0.35))

def slow_json_serialize():

    time.sleep(random.uniform(0.02, 0.12))

def write_audit_log(order_id: int, lock: InstrumentedLock, bad_mode: bool):
    if bad_mode:

        with lock:
            slow_json_serialize()
            slow_disk_write()
    else:

        slow_json_serialize()
        slow_disk_write()
        with lock:

            pass

def update_inventory(sku: str, qty: int, lock: InstrumentedLock, bad_mode: bool):
    if bad_mode:

        with lock:

            time.sleep(random.uniform(0.01, 0.03))
            slow_disk_write()
    else:

        time.sleep(random.uniform(0.01, 0.03))
        with lock:
            pass
        slow_disk_write()

def write_order_db(order_id: int, lock: InstrumentedLock, bad_mode: bool):
    if bad_mode:

        with lock:
            slow_payment_gateway_call()
            slow_disk_write()
    else:

        slow_payment_gateway_call()
        with lock:
            pass
        slow_disk_write()




def run_simulation(
    *,
    bad_mode: bool = True,
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

            update_inventory(sku, qty, INVENTORY_DB_MUTEX, bad_mode)
            write_order_db(oid, ORDERS_DB_MUTEX, bad_mode)
            write_audit_log(oid, AUDIT_LOG_MUTEX, bad_mode)

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

    mode_name = "BAD_MODE (Locking Suspension ON)" if bad_mode else "GOOD_MODE (Lock scope minimized)"
    header = []
    header.append("\n" + "#" * 92)
    header.append(f"SIMULATION RESULT — {mode_name}")
    header.append("#" * 92 + "\n")

    print("\n".join(header))
    print(req_stats.report())
    print(orders_lock_stats.report(total_runtime_s))
    print(inv_lock_stats.report(total_runtime_s))
    print(audit_lock_stats.report(total_runtime_s))




if __name__ == "__main__":

    run_simulation(
        bad_mode=True,
        workers=8,
        orders=60,
        produce_interval_s=(0.00, 0.02),
        seed=7
    )
    print("\n--- BAD MODE FINISHED ---\n")


    run_simulation(
        bad_mode=False,
        workers=8,
        orders=60,
        produce_interval_s=(0.00, 0.02),
        seed=7
    )
    print("\n--- GOOD MODE FINISHED ---")
