import threading


class UpgradeOps:
    """
    Airline upgrade operations model (fixed to avoid livelock).

    Two concurrent pipelines touch the same booking:
      - Inventory pipeline (seat hold / seat map update)
      - Ticketing pipeline (pax record / reissue)

    The original implementation used a symmetric "polite deferral" rule
    plus phase barriers. Under unlucky scheduling, both pipelines would
    always see the other as inflight, politely back off, and retry in
    perfect lockstep – a classic livelock.

    The fix is to remove the symmetric deferral and instead enforce a
    single global lock ordering for both pipelines:
        seat_map_lock -> pax_record_lock

    With a consistent order there is no cyclic wait and no livelock;
    one of the pipelines will always be able to complete the upgrade.
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
            f"Upgrade applied for PNR {self.booking['pnr']} -> "
            f"seat {self.booking['requested_seat']}."
        )
        self.stop_event.set()

    def _coordinated_upgrade(self, component: str) -> bool:
        """
        Acquire both resources in a single global order:
            seat_map_lock -> pax_record_lock

        Returns True if this call applied the upgrade, False if the
        upgrade had already been applied by the other pipeline.
        """
        self._log(component, "Acquiring seat-map and pax-record locks (coordinated)...")


        with self.seat_map_lock:

            if component == "InventoryOps":
                self.inventory_inflight.set()
            else:
                self.ticketing_inflight.set()

            try:
                with self.pax_record_lock:
                    if self.booking["upgrade_applied"]:
                        self._log(
                            component,
                            "Upgrade already applied by the other pipeline; nothing to do."
                        )
                        return False
                    self._apply(component)
                    return True
            finally:
                if component == "InventoryOps":
                    self.inventory_inflight.clear()
                else:
                    self.ticketing_inflight.clear()

    def inventory_worker(self, max_attempts: int = 300) -> None:
        """
        Inventory pipeline (fixed):

        - No longer uses symmetric "polite deferral".
        - Performs a coordinated upgrade using the global lock order.
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                self._log(name, "Stop requested; exiting.")
                return

            with self.telemetry_lock:
                self.inventory_attempts += 1

            if self.booking["upgrade_applied"]:
                self._log(name, "Upgrade already applied; exiting.")
                return

            self._log(name, f"Attempt {attempt}: placing seat hold...")
            applied = self._coordinated_upgrade(name)
            if applied:
                return

        self._log(name, "Max attempts reached; operation not completed.")
        self.stop_event.set()

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline (fixed):

        - Uses the same global lock ordering as inventory_worker.
        - This removes the possibility of livelock or deadlock.
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            if self.stop_event.is_set():
                self._log(name, "Stop requested; exiting.")
                return

            with self.telemetry_lock:
                self.ticketing_attempts += 1

            if self.booking["upgrade_applied"]:
                self._log(name, "Upgrade already applied; exiting.")
                return

            self._log(name, f"Attempt {attempt}: preparing reissue transaction...")
            applied = self._coordinated_upgrade(name)
            if applied:
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
