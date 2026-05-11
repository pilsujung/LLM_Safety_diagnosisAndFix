import threading
import time
import random

class UpgradeOps:
    """
    Airline upgrade operations model - LIVLOCK FIXED VERSION.
    
    Fixes:
    - Removed phase_gate/retry_gate barriers (caused perfect synchronization)
    - Added lock acquire timeouts to prevent blocking
    - Added randomized progressive backoff to break retry symmetry
    - Preserved polite deferral intent with inflight checks
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
        Inventory pipeline (seat hold → pax record):
        - Timeout-based locking prevents blocking
        - Randomized backoff breaks livelock symmetry
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1

            self._log(name, f"Attempt {attempt}: placing seat hold...")
            

            if not self.seat_map_lock.acquire(timeout=0.1):
                time.sleep(random.uniform(0.01, 0.05))
                continue
                
            try:
                self.inventory_inflight.set()
                

                if self.ticketing_inflight.is_set():
                    self._log(name, "Detected concurrent reissue flow. Backing off.")
                    time.sleep(random.uniform(0.01, 0.1))
                    continue


                if self.pax_record_lock.acquire(timeout=0.05):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                            return
                    finally:
                        self.pax_record_lock.release()
                else:
                    self._log(name, "Passenger record busy. Retrying.")

            finally:
                self.inventory_inflight.clear()
                self.seat_map_lock.release()
            

            backoff = min(random.uniform(0.01 * attempt, 0.1 * attempt), 1.0)
            time.sleep(backoff)

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline (pax record → seat map):
        - Symmetric timeout + backoff strategy
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1

            self._log(name, f"Attempt {attempt}: preparing reissue transaction...")
            

            if not self.pax_record_lock.acquire(timeout=0.1):
                time.sleep(random.uniform(0.01, 0.05))
                continue
                
            try:
                self.ticketing_inflight.set()
                
                if self.inventory_inflight.is_set():
                    self._log(name, "Detected concurrent seat-hold flow. Backing off.")
                    time.sleep(random.uniform(0.01, 0.1))
                    continue


                if self.seat_map_lock.acquire(timeout=0.05):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                            return
                    finally:
                        self.seat_map_lock.release()
                else:
                    self._log(name, "Seat map busy. Retrying.")

            finally:
                self.ticketing_inflight.clear()
                self.pax_record_lock.release()
            

            backoff = min(random.uniform(0.01 * attempt, 0.1 * attempt), 1.0)
            time.sleep(backoff)

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

def run_upgrade_ops(max_attempts: int = 300) -> None:
    """Run concurrent upgrade pipelines."""
    ops = UpgradeOps()

    t1 = threading.Thread(target=ops.inventory_worker, kwargs={"max_attempts": max_attempts})
    t2 = threading.Thread(target=ops.ticketing_worker, kwargs={"max_attempts": max_attempts})

    print("\n=== FIXED Upgrade Processing Window (No Livelock) ===\n")
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
    run_upgrade_ops(max_attempts=300)
