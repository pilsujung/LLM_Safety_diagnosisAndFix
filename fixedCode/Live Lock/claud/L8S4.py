import threading
import random
import time


class UpgradeOps:
    """
    Airline upgrade operations model with livelock prevention.
    
    Fix: Random backoff breaks synchronization between competing threads.
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
        Inventory pipeline with random backoff to prevent livelock.
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1

            self._log(name, f"Attempt {attempt}: placing seat hold...")
            self.seat_map_lock.acquire()
            self.inventory_inflight.set()
            try:

                time.sleep(0.001)


                if self.ticketing_inflight.is_set():
                    self._log(name, "Detected concurrent reissue flow. Releasing seat hold and retrying.")

                    time.sleep(random.uniform(0.001, 0.01))
                    continue


                if self.pax_record_lock.acquire(blocking=False):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                        return
                    finally:
                        self.pax_record_lock.release()
                else:
                    self._log(name, "Passenger record busy. Releasing seat hold and retrying.")
                    time.sleep(random.uniform(0.001, 0.01))

            finally:
                self.inventory_inflight.clear()
                self.seat_map_lock.release()

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline with random backoff to prevent livelock.
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1

            self._log(name, f"Attempt {attempt}: preparing reissue transaction...")
            self.pax_record_lock.acquire()
            self.ticketing_inflight.set()
            try:

                time.sleep(0.001)


                if self.inventory_inflight.is_set():
                    self._log(name, "Detected concurrent seat-hold flow. Rolling back and retrying.")

                    time.sleep(random.uniform(0.001, 0.01))
                    continue


                if self.seat_map_lock.acquire(blocking=False):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                        return
                    finally:
                        self.seat_map_lock.release()
                else:
                    self._log(name, "Seat map busy. Rolling back and retrying.")
                    time.sleep(random.uniform(0.001, 0.01))

            finally:
                self.ticketing_inflight.clear()
                self.pax_record_lock.release()

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()


def run_upgrade_ops(max_attempts: int = 300) -> None:
    ops = UpgradeOps()

    t1 = threading.Thread(target=ops.inventory_worker, kwargs={"max_attempts": max_attempts})
    t2 = threading.Thread(target=ops.ticketing_worker, kwargs={"max_attempts": max_attempts})

    print("\n=== Upgrade Processing Window (Concurrent Pipelines) ===\n")
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