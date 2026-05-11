import threading
import random
import time

class UpgradeOps:
    """
    Airline upgrade operations model - LIVLOCK FIXED VERSION.

    Two concurrent pipelines touch the same booking:
    - Inventory pipeline (seat hold / seat map update)
    - Ticketing pipeline (pax record / reissue)

    FIXED: Added random backoff delays (50-250ms) during polite deferral
    to break perfect synchronization, following C++/Java examples.
    Removed phase alignment barriers that enforced symmetric timing.
    """

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
        self.random = random.Random()

    def _log(self, component: str, message: str) -> None:
        print(f"[{component}] {message}")

    def _apply(self, component: str) -> None:
        self.booking["upgrade_applied"] = True
        with self.telemetry_lock:
            self.progress += 1
        self._log(component, f"Upgrade applied for PNR {self.booking['pnr']} -> seat {self.booking['requested_seat']}.")
        self.stop_event.set()

    def inventory_worker(self, max_attempts: int = 300) -> None:
        """
        Inventory pipeline (FIXED): Added random backoff during deferral.
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1

            self._log(name, f"Attempt {attempt}: placing seat hold...")
            

            if not self.seat_map_lock.acquire(timeout=0.1):
                continue
                
            self.inventory_inflight.set()
            try:

                if self.ticketing_inflight.is_set():
                    wait_ms = self.random.randint(50, 250)
                    self._log(name, f"Detected concurrent reissue. Backing off {wait_ms}ms...")
                    time.sleep(wait_ms / 1000.0)
                    continue


                if self.pax_record_lock.acquire(blocking=False):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                            return
                    finally:
                        self.pax_record_lock.release()
                else:
                    self._log(name, "Passenger record busy. Releasing seat hold...")

            finally:
                self.inventory_inflight.clear()
                self.seat_map_lock.release()

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline (FIXED): Added random backoff during deferral.
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1

            self._log(name, f"Attempt {attempt}: preparing reissue transaction...")
            

            if not self.pax_record_lock.acquire(timeout=0.1):
                continue
                
            self.ticketing_inflight.set()
            try:

                if self.inventory_inflight.is_set():
                    wait_ms = self.random.randint(50, 250)
                    self._log(name, f"Detected concurrent seat-hold. Backing off {wait_ms}ms...")
                    time.sleep(wait_ms / 1000.0)
                    continue


                if self.seat_map_lock.acquire(blocking=False):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                            return
                    finally:
                        self.seat_map_lock.release()
                else:
                    self._log(name, "Seat map busy. Rolling back...")

            finally:
                self.ticketing_inflight.clear()
                self.pax_record_lock.release()

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

def run_upgrade_ops(max_attempts: int = 300) -> None:
    ops = UpgradeOps()

    t1 = threading.Thread(target=ops.inventory_worker, kwargs={"max_attempts": max_attempts})
    t2 = threading.Thread(target=ops.ticketing_worker, kwargs={"max_attempts": max_attempts})

    print("\n=== Upgrade Processing Window (LIVLOCK FIXED - Random Backoff) ===\n")
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
    run_upgrade_ops(max_attempts=50)
