import threading


class UpgradeOps:
    """
    Airline upgrade operations model.

    Fixed version: both pipelines acquire locks in a consistent order
    (seat_map_lock → pax_record_lock) and no longer use the
    "polite deferral" events/barriers that caused livelock.
    """

    def __init__(self):

        self.seat_map_lock = threading.Lock()
        self.pax_record_lock = threading.Lock()


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
            f"Upgrade applied for PNR {self.booking['pnr']} -> seat {self.booking['requested_seat']}.",
        )
        self.stop_event.set()

    def _acquire_both_locks(self, name: str, first: threading.Lock, second: threading.Lock) -> bool:
        """
        Helper that acquires the two locks in a fixed global order:
        seat_map_lock → pax_record_lock.

        'first' must always be seat_map_lock and 'second' pax_record_lock
        to preserve global ordering when used by both workers.
        """
        acquired_first = first.acquire(timeout=1.0)
        if not acquired_first:
            self._log(name, "Could not acquire first lock; retrying.")
            return False

        try:
            acquired_second = second.acquire(timeout=1.0)
            if not acquired_second:
                self._log(name, "Could not acquire second lock; retrying.")
                return False


            try:
                if not self.booking["upgrade_applied"]:
                    self._apply(name)
                else:
                    self._log(name, "Upgrade already applied by other pipeline.")
                return True
            finally:
                second.release()
        finally:
            first.release()

    def inventory_worker(self, max_attempts: int = 300) -> None:
        """
        Inventory pipeline.
        Now uses the same global lock order as ticketing to avoid deadlock
        and livelock: seat_map_lock → pax_record_lock.
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1

            self._log(name, f"Attempt {attempt}: acquiring locks for seat hold + record update...")
            succeeded = self._acquire_both_locks(name, self.seat_map_lock, self.pax_record_lock)
            if succeeded:
                return

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline.
        Also uses the same global lock order (seat_map_lock → pax_record_lock).
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1

            self._log(name, f"Attempt {attempt}: acquiring locks for reissue + seat confirmation...")
            succeeded = self._acquire_both_locks(name, self.seat_map_lock, self.pax_record_lock)
            if succeeded:
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
