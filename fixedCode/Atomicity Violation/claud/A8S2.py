import threading
import time
import random
import logging
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

                               
               
                               
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(threadName)s | %(message)s"
)

                                           
                                                       
                                           

@dataclass
class BuyerResult:
    buyer_id: int
    success: bool
    elapsed_ms: float


class TicketInventory:
    """
    Limited ticket inventory.
    - reserve_unsafe(): intentionally triggers atomicity violations (check-then-update split)
    - reserve_safe(): ensures atomicity using a Lock
    - detect_atomicity_violation(): invariant-based detection + logging
    """

    def __init__(self, initial_stock: int):
        self.initial_stock = initial_stock
        self.available = initial_stock
        self.sold_buyers: List[int] = []

                                                                 
        self._lock = threading.Lock()

                         
        self.atomicity_violations = 0
        self.violation_events: List[Dict[str, Any]] = []

                                   
                                   
                                   
    def detect_atomicity_violation(self, *, context: str, buyer_id: Optional[int] = None) -> None:
        """
        Detect atomicity violation using invariants.

        Invariant (should always hold in a correct system):
          available == initial_stock - sold_count

        If broken, log and record the event.
        """
        sold_count = len(self.sold_buyers)
        expected_available = self.initial_stock - sold_count

        violated = False
        reasons = []

        if self.available != expected_available:
            violated = True
            reasons.append(
                f"invariant broken (available={self.available}, expected={expected_available}, sold={sold_count})"
            )

        if sold_count > self.initial_stock:
            violated = True
            reasons.append(f"oversold (sold={sold_count} > initial_stock={self.initial_stock})")

        if self.available < 0:
            violated = True
            reasons.append(f"negative available (available={self.available})")

        if violated:
            self.atomicity_violations += 1
            event = {
                "time": time.strftime("%H:%M:%S"),
                "thread": threading.current_thread().name,
                "context": context,
                "buyer_id": buyer_id,
                "available": self.available,
                "sold_count": sold_count,
                "expected_available": expected_available,
                "reasons": reasons,
            }
            self.violation_events.append(event)

            logging.error(
                "ATOMICITY VIOLATION DETECTED | ctx=%s | buyer=%s | available=%d | sold=%d | expected_available=%d | reasons=%s",
                context, buyer_id, self.available, sold_count, expected_available, "; ".join(reasons)
            )

                                   
                                        
                                   
    def reserve_unsafe(self, buyer_id: int) -> bool:
        """
        FIXED VERSION: Now properly uses lock to ensure atomicity.
        The entire check-then-update operation is atomic.
        """
        with self._lock:
            if self.available <= 0:
                return False

                                                                         
            time.sleep(random.uniform(0.0005, 0.003))

                                                                     
            self.available -= 1
            self.sold_buyers.append(buyer_id)

                                                     
            self.detect_atomicity_violation(context="reserve_unsafe(after_update)", buyer_id=buyer_id)
            return True

                                   
                                  
                                   
    def reserve_safe(self, buyer_id: int) -> bool:
        with self._lock:
            if self.available <= 0:
                return False

            time.sleep(random.uniform(0.0005, 0.003))

            self.available -= 1
            self.sold_buyers.append(buyer_id)

                                                                    
            self.detect_atomicity_violation(context="reserve_safe(after_update)", buyer_id=buyer_id)
            return True

                                   
                                 
                                   
    def start_monitor(self, stop_event: threading.Event, interval_sec: float = 0.002) -> threading.Thread:
        """
        Periodically checks invariants and logs if broken.
        Now uses lock to ensure consistent snapshot.
        """
        def monitor():
            while not stop_event.is_set():
                with self._lock:                                   
                    self.detect_atomicity_violation(context="monitor(periodic_check)", buyer_id=None)
                time.sleep(interval_sec)

        t = threading.Thread(target=monitor, name="InvariantMonitor", daemon=True)
        t.start()
        return t

    def audit(self) -> dict:
        with self._lock:                                    
            sold_count = len(self.sold_buyers)
            expected_available = self.initial_stock - sold_count
            return {
                "initial_stock": self.initial_stock,
                "available": self.available,
                "sold_count": sold_count,
                "expected_available": expected_available,
                "invariant_ok": (self.available == expected_available),
                "oversold": (sold_count > self.initial_stock),
                "atomicity_violations_detected": self.atomicity_violations,
            }


def run_simulation(mode: str, initial_stock: int, buyers: int, workers: int, enable_monitor: bool = True) -> None:
    inventory = TicketInventory(initial_stock=initial_stock)
    start_event = threading.Event()

    if mode == "unsafe":
        reserve_fn = inventory.reserve_unsafe
    elif mode == "safe":
        reserve_fn = inventory.reserve_safe
    else:
        raise ValueError("mode must be 'unsafe' or 'safe'")

                                
    monitor_stop = threading.Event()
    monitor_thread = None
    if enable_monitor:
        monitor_thread = inventory.start_monitor(monitor_stop, interval_sec=0.002)

    def buyer_task(buyer_id: int) -> BuyerResult:
        start_event.wait()
        t0 = time.perf_counter()
        ok = reserve_fn(buyer_id)
        t1 = time.perf_counter()
        return BuyerResult(buyer_id=buyer_id, success=ok, elapsed_ms=(t1 - t0) * 1000.0)

    futures = []
    with ThreadPoolExecutor(max_workers=workers) as ex:
        for buyer_id in range(1, buyers + 1):
            futures.append(ex.submit(buyer_task, buyer_id))

        start_event.set()

        results = [f.result() for f in as_completed(futures)]

                  
    if enable_monitor:
        monitor_stop.set()
        if monitor_thread:
            monitor_thread.join(timeout=0.1)

    success = sum(1 for r in results if r.success)
    failure = buyers - success
    audit = inventory.audit()

    print("=" * 80)
    print(f"MODE: {mode.upper()} | initial_stock={initial_stock}, buyers={buyers}, workers={workers}")
    print("-" * 80)
    print(f"Successful reservations: {success}, Failed: {failure}")
    print(f"Audit: {audit}")

                                 
    if inventory.atomicity_violations > 0:
        print("\n[Sample violation events (up to 5)]")
        for e in inventory.violation_events[:5]:
            print(
                f"- {e['time']} | {e['thread']} | ctx={e['context']} | buyer={e['buyer_id']} | "
                f"available={e['available']} | sold={e['sold_count']} | expected={e['expected_available']} | "
                f"reasons={'; '.join(e['reasons'])}"
            )

    print("=" * 80)


if __name__ == "__main__":
    random.seed(42)

    INITIAL_STOCK = 30
    BUYERS = 120
    WORKERS = 40

                                                                
    run_simulation(mode="unsafe", initial_stock=INITIAL_STOCK, buyers=BUYERS, workers=WORKERS, enable_monitor=True)

                                             
    run_simulation(mode="safe", initial_stock=INITIAL_STOCK, buyers=BUYERS, workers=WORKERS, enable_monitor=True)