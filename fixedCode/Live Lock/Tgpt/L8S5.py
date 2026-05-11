import threading
import time
import random


class UpgradeOps:
    """
    Airline upgrade operations model.

    Two concurrent pipelines touch the same booking:
      - Inventory pipeline (seat hold / seat map update)
      - Ticketing pipeline (pax record / reissue)

    Original version:
      Both pipelines always used a symmetric "polite deferral" rule +
      tight phase synchronization (Barriers), which can cause livelock:
      both notice the other is inflight, both politely back off, forever.

    Fixed version:
      - Keep polite deferral for the first few collisions.
      - After a bounded number of attempts, one side (inventory) stops
        deferring earlier than the other and is allowed to block for the
        second resource -> guarantees progress.
      - Add small random backoff when deferring to break timing symmetry,
        similar to your C++/Java fixes.
    """

    def __init__(self):

        self.seat_map_lock = threading.Lock()
        self.pax_record_lock = threading.Lock()


        self.inventory_inflight = threading.Event()
        self.ticketing_inflight = threading.Event()


        self.phase_gate = threading.Barrier(2)
        self.retry_gate = threading.Barrier(2)


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





        self.inventory_polite_limit = 5
        self.ticketing_polite_limit = 10


        self._rng = random.Random()

    def _log(self, component: str, message: str) -> None:
        print(f"[{component}] {message}")

    def _apply(self, component: str) -> None:
        self.booking["upgrade_applied"] = True
        with self.telemetry_lock:
            self.progress += 1
        self._log(
            component,
            f"Upgrade applied for PNR {self.booking['pnr']} "
            f"-> seat {self.booking['requested_seat']}."
        )
        self.stop_event.set()

    def inventory_worker(self, max_attempts: int = 300) -> None:
        """
        Inventory pipeline:
          - Locks seat map first.
          - Uses a bounded polite deferral rule if ticketing is inflight.
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1


            try:
                self.phase_gate.wait(timeout=1.0)
            except threading.BrokenBarrierError:
                return

            self._log(name, f"Attempt {attempt}: placing seat hold...")
            self.seat_map_lock.acquire()
            self.inventory_inflight.set()
            try:

                try:
                    self.phase_gate.wait(timeout=1.0)
                except threading.BrokenBarrierError:
                    return


                if self.ticketing_inflight.is_set() and not self.stop_event.is_set():
                    if attempt <= self.inventory_polite_limit:
                        self._log(
                            name,
                            f"Detected concurrent reissue flow. "
                            f"Polite deferral {attempt}/{self.inventory_polite_limit}."
                        )

                        backoff = self._rng.uniform(0.05, 0.25)
                        time.sleep(backoff)
                        continue
                    else:

                        self._log(
                            name,
                            "Detected concurrent reissue flow repeatedly; "
                            "stopping polite deferral and waiting to complete upgrade."
                        )



                blocking_second = attempt > self.inventory_polite_limit

                if self.pax_record_lock.acquire(blocking=blocking_second):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                        return
                    finally:
                        self.pax_record_lock.release()
                else:
                    self._log(name, "Passenger record busy. Releasing seat hold and retrying.")

            finally:
                self.inventory_inflight.clear()
                self.seat_map_lock.release()


            try:
                self.retry_gate.wait(timeout=1.0)
            except threading.BrokenBarrierError:
                return

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline:
          - Locks passenger record first.
          - Uses a bounded polite deferral rule if inventory is inflight.
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1


            try:
                self.phase_gate.wait(timeout=1.0)
            except threading.BrokenBarrierError:
                return

            self._log(name, f"Attempt {attempt}: preparing reissue transaction...")
            self.pax_record_lock.acquire()
            self.ticketing_inflight.set()
            try:

                try:
                    self.phase_gate.wait(timeout=1.0)
                except threading.BrokenBarrierError:
                    return



                if self.inventory_inflight.is_set() and not self.stop_event.is_set():
                    if attempt <= self.ticketing_polite_limit:
                        self._log(
                            name,
                            f"Detected concurrent seat-hold flow. "
                            f"Polite deferral {attempt}/{self.ticketing_polite_limit}."
                        )
                        backoff = self._rng.uniform(0.05, 0.25)
                        time.sleep(backoff)
                        continue
                    else:
                        self._log(
                            name,
                            "Detected concurrent seat-hold flow repeatedly; "
                            "taking priority and waiting to complete upgrade."
                        )

                blocking_second = attempt > self.ticketing_polite_limit

                if self.seat_map_lock.acquire(blocking=blocking_second):
                    try:
                        if not self.booking["upgrade_applied"]:
                            self._apply(name)
                        return
                    finally:
                        self.seat_map_lock.release()
                else:
                    self._log(name, "Seat map busy. Rolling back and retrying.")

            finally:
                self.ticketing_inflight.clear()
                self.pax_record_lock.release()


            try:
                self.retry_gate.wait(timeout=1.0)
            except threading.BrokenBarrierError:
                return

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
