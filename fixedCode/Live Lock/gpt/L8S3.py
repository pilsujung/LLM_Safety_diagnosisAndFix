import threading


class UpgradeOps:
    """
    Airline upgrade operations model.

    Two concurrent pipelines touch the same booking:
      - Inventory pipeline (seat hold / seat map update)
      - Ticketing pipeline (pax record / reissue)

    Original version used a "polite deferral" rule plus synchronized phases,
    which could lead to livelock when both sides kept deferring forever.

    Fixed version:
      - Both pipelines acquire locks in a consistent global order:
          seat_map_lock -> pax_record_lock
      - No polite deferral / retry barriers are needed.
      - This removes both deadlock and livelock.
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

    def _log(self, component: str, message: str) -> None:
        print(f"[{component}] {message}")

    def _apply(self, component: str) -> None:
        self.booking["upgrade_applied"] = True
        with self.telemetry_lock:
            self.progress += 1
        self._log(
            component,
            f"Upgrade applied for PNR {self.booking['pnr']} -> seat {self.booking['requested_seat']}."
        )
        self.stop_event.set()

    def inventory_worker(self, max_attempts: int = 300) -> None:
        """
        Inventory pipeline (fixed):

        - Uses a global lock ordering:
            seat_map_lock -> pax_record_lock
        - No polite deferral; just acquires both locks and commits.
        - This ensures no deadlock and no livelock.
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1

            self._log(name, f"Attempt {attempt}: acquiring locks for inventory update...")


            self.seat_map_lock.acquire()
            try:
                self.inventory_inflight.set()
                self.pax_record_lock.acquire()
                try:

                    if not self.booking["upgrade_applied"]:
                        self._apply(name)
                    return
                finally:
                    self.pax_record_lock.release()
                    self.inventory_inflight.clear()
            finally:
                self.seat_map_lock.release()

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline (fixed):

        - Uses the SAME global lock ordering:
            seat_map_lock -> pax_record_lock
        - This matches inventory_worker and avoids circular wait.
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1

            self._log(name, f"Attempt {attempt}: acquiring locks for ticket reissue...")


            self.seat_map_lock.acquire()
            try:
                self.ticketing_inflight.set()
                self.pax_record_lock.acquire()
                try:
                    if not self.booking["upgrade_applied"]:
                        self._apply(name)
                    return
                finally:
                    self.pax_record_lock.release()
                    self.ticketing_inflight.clear()
            finally:
                self.seat_map_lock.release()

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
