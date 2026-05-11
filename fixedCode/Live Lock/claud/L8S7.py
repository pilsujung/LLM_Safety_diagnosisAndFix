import threading


class UpgradeOps:
    """
    Fixed version using consistent lock ordering.
    
    Both pipelines acquire locks in the same order:
    1. seat_map_lock first
    2. pax_record_lock second
    
    This prevents both deadlock and livelock.
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

    def _log(self, component: str, message: str) -> None:
        print(f"[{component}] {message}")

    def _apply(self, component: str) -> None:
        self.booking["upgrade_applied"] = True
        with self.telemetry_lock:
            self.progress += 1
        self._log(component, f"Upgrade applied for PNR {self.booking['pnr']} -> seat {self.booking['requested_seat']}.")

    def inventory_worker(self, max_attempts: int = 300) -> None:
        """
        Inventory pipeline: acquires locks in consistent order.
        """
        name = "InventoryOps"

        for attempt in range(1, max_attempts + 1):
            with self.telemetry_lock:
                self.inventory_attempts += 1

            self._log(name, f"Attempt {attempt}: acquiring locks...")
            

            with self.seat_map_lock:
                with self.pax_record_lock:
                    if not self.booking["upgrade_applied"]:
                        self._apply(name)
                        return
                    else:
                        self._log(name, "Upgrade already applied by other thread.")
                        return

        self._log(name, "Max attempts reached; operation not completed.")

    def ticketing_worker(self, max_attempts: int = 300) -> None:
        """
        Ticketing pipeline: acquires locks in the SAME consistent order.
        """
        name = "TicketingOps"

        for attempt in range(1, max_attempts + 1):
            with self.telemetry_lock:
                self.ticketing_attempts += 1

            self._log(name, f"Attempt {attempt}: acquiring locks...")
            

            with self.seat_map_lock:
                with self.pax_record_lock:
                    if not self.booking["upgrade_applied"]:
                        self._apply(name)
                        return
                    else:
                        self._log(name, "Upgrade already applied by other thread.")
                        return

        self._log(name, "Max attempts reached; operation not completed.")


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