import threading
import time
import random

class UpgradeOps:
    def __init__(self):
        self.seat_map_lock = threading.Lock()
        self.pax_record_lock = threading.Lock()
        self.inventory_inflight = threading.Event()
        self.ticketing_inflight = threading.Event()
        self.booking = {
            "pnr": "KX3L9Q",
            "passenger": "DOE/JANE",
            "requested_seat": "1A",
            "upgrade_applied": False,
        }
        self.inventory_attempts = 0
        self.ticketing_attempts = 0
        self.progress = 0
        self.telemetry_lock = threading.Lock()
        self.stop_event = threading.Event()

        self.inventory_defers = 0
        self.ticketing_defers = 0

    def _log(self, component: str, message: str) -> None:
        print(f"[{component}] {message}")

    def _apply(self, component: str) -> None:
        with self.telemetry_lock:
            self.booking["upgrade_applied"] = True
            self.progress += 1
        self._log(component, f"Upgrade applied for PNR {self.booking['pnr']} -> seat {self.booking['requested_seat']}.")
        self.stop_event.set()

    def _backoff(self, base: float = 0.01) -> None:
        time.sleep(base * (1 + random.uniform(0, 4)))

    def inventory_worker(self, max_attempts: int = 50) -> None:
        name = "InventoryOps"
        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return
            with self.telemetry_lock:
                self.inventory_attempts += 1

            self._log(name, f"Attempt {attempt}: placing seat hold...")
            if not self.seat_map_lock.acquire(timeout=0.1):
                self._backoff()
                continue
            self.inventory_inflight.set()
            try:

                if self.ticketing_inflight.is_set():
                    with self.telemetry_lock:
                        self.inventory_defers += 1
                    self._log(name, f"Defer {self.inventory_defers}: concurrent reissue. Backing off.")
                    if self.inventory_defers >= 5:
                        time.sleep(0.5)
                        self.inventory_defers = 0
                    continue


                if self.pax_record_lock.acquire(blocking=False):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                            return
                    finally:
                        self.pax_record_lock.release()
                else:
                    self._log(name, "Pax record busy. Backing off.")
            finally:
                self.inventory_inflight.clear()
                self.seat_map_lock.release()
            self._backoff()

        self._log(name, "Max attempts reached.")

    def ticketing_worker(self, max_attempts: int = 50) -> None:
        name = "TicketingOps"
        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return
            with self.telemetry_lock:
                self.ticketing_attempts += 1

            self._log(name, f"Attempt {attempt}: preparing reissue...")
            if not self.pax_record_lock.acquire(timeout=0.1):
                self._backoff()
                continue
            self.ticketing_inflight.set()
            try:
                if self.inventory_inflight.is_set():
                    with self.telemetry_lock:
                        self.ticketing_defers += 1
                    self._log(name, f"Defer {self.ticketing_defers}: concurrent seat-hold. Backing off.")
                    continue

                if self.seat_map_lock.acquire(blocking=False):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                            return
                    finally:
                        self.seat_map_lock.release()
                else:
                    self._log(name, "Seat map busy. Backing off.")
            finally:
                self.ticketing_inflight.clear()
                self.pax_record_lock.release()
            self._backoff()

        self._log(name, "Max attempts reached.")

def run_upgrade_ops(max_attempts: int = 50) -> None:
    ops = UpgradeOps()
    t1 = threading.Thread(target=ops.inventory_worker, kwargs={"max_attempts": max_attempts})
    t2 = threading.Thread(target=ops.ticketing_worker, kwargs={"max_attempts": max_attempts})
    print("\n=== Fixed Upgrade Processing (Livelock Resolved) ===\n")
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    print("\n=== Summary ===")
    print(f"PNR: {ops.booking['pnr']}")
    print(f"Upgrade applied? {ops.booking['upgrade_applied']}")
    print(f"Progress: {ops.progress}")
    print(f"Inventory attempts: {ops.inventory_attempts}")
    print(f"Ticketing attempts: {ops.ticketing_attempts}")

if __name__ == "__main__":
    run_upgrade_ops()
