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

        self.items_loaded = 0
        self.items_delivered = 0

    def _now_rel(self):
        return time.perf_counter() - self.start_t

    def record_acquire(self, thread_name, wait_s):
        with self._lock:
            self.wait_times.setdefault(thread_name, []).append(wait_s)
            self.acquire_events.append((self._now_rel(), thread_name))

    def record_release(self, thread_name, hold_s):
        with self._lock:
            self.hold_times.setdefault(thread_name, []).append(hold_s)
            self.release_events.append((self._now_rel(), thread_name))

    def inc_loaded(self, n=1):
        with self._lock:
            self.items_loaded += n

    def inc_delivered(self, n=1):
        with self._lock:
            self.items_delivered += n

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

    def report(self, final_queue_size=None):
        def fmt(x): return f"{x:.6f}s"

        waits_all = [w for ws in self.wait_times.values() for w in ws]
        holds_all = [h for hs in self.hold_times.values() for h in hs]
        total_runtime = self._now_rel()

        print("\n" + "=" * 92)
        print(f"LOCK CONTENTION REPORT — {self.name}")
        print("=" * 92)
        print(f"Threads observed              : {len(set(self.wait_times) | set(self.hold_times))}")
        print(f"Total runtime                 : {fmt(total_runtime)}")

        if waits_all:
            print(f"Wait time (avg / med)         : {fmt(mean(waits_all))} / {fmt(median(waits_all))}")
            print(f"Wait time (p95 / max)         : {fmt(self._percentile(waits_all,95))} / {fmt(max(waits_all))}")
            print(f"Contention events (>0s)       : {sum(1 for w in waits_all if w > 0.0)}")

        if holds_all:
            print(f"Hold time (avg / med)         : {fmt(mean(holds_all))} / {fmt(median(holds_all))}")
            print(f"Hold time (p95 / max)         : {fmt(self._percentile(holds_all,95))} / {fmt(max(holds_all))}")

        if self.acquire_events:
            ordered = " -> ".join(name for _, name in sorted(self.acquire_events))
            print(f"Acquisition order             : {ordered}")

        print(f"Items loaded / delivered      : {self.items_loaded} / {self.items_delivered}")
        if final_queue_size is not None:
            print(f"Remaining queue size          : {final_queue_size}")

        print("\nPer-thread totals (across multiple dock visits):")
        print(f"{'Thread':>18} | {'Acquires':>8} | {'Wait(total)':>12} | {'Hold(total)':>12}")
        print("-" * 92)

        all_names = sorted(set(self.wait_times) | set(self.hold_times))
        for name in all_names:
            w_list = self.wait_times.get(name, [])
            h_list = self.hold_times.get(name, [])
            acquires = max(len(w_list), len(h_list))
            w_total = sum(w_list)
            h_total = sum(h_list)
            print(f"{name:>18} | {acquires:>8} | {fmt(w_total):>12} | {fmt(h_total):>12}")

        print("=" * 92 + "\n")






TOTAL_PACKAGES = 8
TRUCK_COUNT = 6

dock_lock = threading.Lock()
dock_cv = threading.Condition(dock_lock)

loading_queue = []
loader_done = False

stats = LockStats("LOADING_DOCK_LOCK_FIXED")


def timestamp():
    return datetime.now().strftime('%H:%M:%S.%f')[:-3]


def timed_acquire(lock_obj, thread_name):
    wait_start = time.perf_counter()
    lock_obj.acquire()
    wait_s = time.perf_counter() - wait_start
    stats.record_acquire(thread_name, wait_s)
    return wait_s


def timed_release(lock_obj, thread_name, hold_start):
    hold_s = time.perf_counter() - hold_start
    lock_obj.release()
    stats.record_release(thread_name, hold_s)
    return hold_s


def warehouse_loader():
    """
    Fixed loader:
      - Does slow work (picking/preparing packages) OFF the dock (no lock).
      - Acquires dock_lock briefly only to put a prepared package on the dock
        and notify waiting trucks.
    """
    global loader_done
    thread_name = threading.current_thread().name

    print(f"[{timestamp()}] Loader: Shift started (non-blocking mode)")

    for i in range(TOTAL_PACKAGES):
        pkg = f"package_{i}"


        print(f"[{timestamp()}] Loader: Picking {pkg} in warehouse...")
        time.sleep(0.8)


        print(f"[{timestamp()}] Loader: Requesting dock to place {pkg}...")
        wait_s = timed_acquire(dock_lock, thread_name)
        hold_start = time.perf_counter()
        try:
            loading_queue.append(pkg)
            stats.inc_loaded(1)
            time.sleep(0.05)
            print(f"[{timestamp()}] Loader: Placed {pkg} on dock (queue size: {len(loading_queue)}) "
                  f"(wait {wait_s:.6f}s)")
            dock_cv.notify()
        finally:
            timed_release(dock_lock, thread_name, hold_start)


    wait_s = timed_acquire(dock_lock, thread_name)
    hold_start = time.perf_counter()
    try:
        loader_done = True
        dock_cv.notify_all()
        print(f"[{timestamp()}] Loader: All packages staged. loader_done=True (wait {wait_s:.6f}s)")
    finally:
        timed_release(dock_lock, thread_name, hold_start)


def delivery_truck(truck_id: int):
    """
    Truck behavior (fixed):
      - Acquire dock_lock to enter the dock.
      - If there are no packages yet and the loader is still working,
        wait on the condition variable (which releases the lock while waiting).
      - When notified and a package is available, load exactly one package and depart.
    """
    global loader_done
    thread_name = threading.current_thread().name
    print(f"[{timestamp()}] Truck-{truck_id}: Arrived and requesting dock access...")

    wait_s = timed_acquire(dock_lock, thread_name)
    hold_start = time.perf_counter()
    try:
        print(f"[{timestamp()}] Truck-{truck_id}: ✓ Dock acquired (wait {wait_s:.6f}s)")


        while not loading_queue and not loader_done:
            print(f"[{timestamp()}] Truck-{truck_id}: No packages yet; waiting off-dock...")
            dock_cv.wait()

        if loading_queue:
            pkg = loading_queue.pop(0)
            stats.inc_delivered(1)
            print(f"[{timestamp()}] Truck-{truck_id}: Loaded {pkg} (remaining {len(loading_queue)})")
            time.sleep(0.2)
        else:
            print(f"[{timestamp()}] Truck-{truck_id}: No packages available; departing empty.")
            time.sleep(0.05)
    finally:
        timed_release(dock_lock, thread_name, hold_start)
        print(f"[{timestamp()}] Truck-{truck_id}: Released dock and departed")





if __name__ == "__main__":
    print("=== Logistics Center: Loading Dock Contention Simulation (FIXED) ===\n")

    loader = threading.Thread(target=warehouse_loader, name="WarehouseLoader")
    loader.start()


    time.sleep(0.4)

    trucks = []
    for i in range(TRUCK_COUNT):
        t = threading.Thread(target=delivery_truck, args=(i + 1,), name=f"Truck-{i+1}")
        trucks.append(t)
        t.start()
        time.sleep(0.3)

    loader.join()
    for t in trucks:
        t.join()

    print(f"\n=== All trucks departed! Remaining packages: {len(loading_queue)} ===")
    stats.report(final_queue_size=len(loading_queue))
