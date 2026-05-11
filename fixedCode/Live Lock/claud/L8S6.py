import threading
import time
import random


class UpgradeOps:
    """
    Airline upgrade operations model - FIXED VERSION.

    Changes to resolve livelock:
    1. Removed synchronization barriers that forced symmetric behavior
    2. Added randomized exponential backoff to break symmetry
    3. Implemented priority system (inventory has precedence)
    4. Added small random delays to prevent lockstep execution
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
        Inventory pipeline with priority and backoff.
        Has priority when both threads conflict.
        """
        name = "InventoryOps"
        backoff = 0.001

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1


            time.sleep(random.uniform(0, 0.001))

            self._log(name, f"Attempt {attempt}: placing seat hold...")
            self.seat_map_lock.acquire()
            self.inventory_inflight.set()
            try:

                time.sleep(0.0001)



                if self.ticketing_inflight.is_set():

                    if not self.pax_record_lock.acquire(blocking=False):
                        self._log(name, "Detected concurrent reissue flow. Releasing seat hold and retrying.")

                        time.sleep(backoff * random.uniform(0.5, 1.5))
                        backoff = min(backoff * 2, 0.1)
                        continue
                    else:

                        try:
                            if not self.booking["upgrade_applied"]:
                                self._apply(name)
                            return
                        finally:
                            self.pax_record_lock.release()
                else:

                    if self.pax_record_lock.acquire(blocking=False):
                        try:
                            if not self.booking["upgrade_applied"]:
                                self._apply(name)
                            return
                        finally:
                            self.pax_record_lock.release()
                    else:
                        self._log(name, "Passenger record busy. Releasing seat hold and retrying.")
                        time.sleep(backoff * random.uniform(0.5, 1.5))
                        backoff = min(backoff * 2, 0.1)

            finally:
                self.inventory_inflight.clear()
                self.seat_map_lock.release()


            if attempt % 10 == 0:
                backoff = max(backoff * 0.5, 0.001)

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline with lower priority and backoff.
        Defers more readily to inventory to break symmetry.
        """
        name = "TicketingOps"
        backoff = 0.002

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1


            time.sleep(random.uniform(0, 0.002))

            self._log(name, f"Attempt {attempt}: preparing reissue transaction...")
            self.pax_record_lock.acquire()
            self.ticketing_inflight.set()
            try:

                time.sleep(0.0001)


                if self.inventory_inflight.is_set():
                    self._log(name, "Detected concurrent seat-hold flow. Rolling back and retrying.")

                    time.sleep(backoff * random.uniform(1.0, 2.0))
                    backoff = min(backoff * 2, 0.2)
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
                    time.sleep(backoff * random.uniform(1.0, 2.0))
                    backoff = min(backoff * 2, 0.2)

            finally:
                self.ticketing_inflight.clear()
                self.pax_record_lock.release()


            if attempt % 10 == 0:
                backoff = max(backoff * 0.5, 0.002)

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