import threading
import time
import random
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Dict, Any, Optional

@dataclass
class BuyerResult:
    buyer_id: int
    success: bool
    elapsed_ms: float

class TicketInventory:
    def __init__(self, initial_stock: int):
        self.initial_stock = initial_stock
        self.available = initial_stock
        self.sold_buyers: List[int] = []
        self._lock = threading.Lock()
        self.atomicity_violations = 0
        self.violation_events: List[Dict[str, Any]] = []

    def detect_atomicity_violation(self, *, context: str, buyer_id: Optional[int] = None) -> None:
        sold_count = len(self.sold_buyers)
        expected_available = self.initial_stock - sold_count
        violated = self.available != expected_available or sold_count > self.initial_stock or self.available < 0
        
        if violated:
            self.atomicity_violations += 1
            print(f"VIOLATION[{self.atomicity_violations}] ctx={context} buyer={buyer_id} "
                  f"available={self.available} sold={sold_count} expected={expected_available}")

    def reserve_unsafe(self, buyer_id: int) -> bool:                               
        if self.available <= 0:
            return False
        seen = self.available
        time.sleep(random.uniform(0.0001, 0.0003))               
        self.available = seen - 1
        self.sold_buyers.append(buyer_id)
        self.detect_atomicity_violation(context="unsafe", buyer_id=buyer_id)
        return True

    def reserve_safe(self, buyer_id: int) -> bool:                              
        with self._lock:
            if self.available <= 0:
                return False
            time.sleep(random.uniform(0.0001, 0.0003))
            self.available -= 1
            self.sold_buyers.append(buyer_id)
            self.detect_atomicity_violation(context="safe", buyer_id=buyer_id)
            return True

    def audit(self) -> dict:
        sold_count = len(self.sold_buyers)
        return {
            "initial_stock": self.initial_stock,
            "available": self.available,
            "sold_count": sold_count,
            "oversold": sold_count > self.initial_stock,
            "violations": self.atomicity_violations
        }

def run_simulation(mode: str, initial_stock=10, buyers=25, workers=15):
    inventory = TicketInventory(initial_stock)
    start_event = threading.Event()
    
    reserve_fn = inventory.reserve_unsafe if mode == "unsafe" else inventory.reserve_safe
    
    def buyer_task(buyer_id: int) -> BuyerResult:
        start_event.wait()
        t0 = time.perf_counter()
        success = reserve_fn(buyer_id)
        return BuyerResult(buyer_id, success, (time.perf_counter() - t0) * 1000)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = [ex.submit(buyer_task, i) for i in range(1, buyers + 1)]
        start_event.set()
        results = [f.result(timeout=0.5) for f in as_completed(futures)]

    success_count = sum(1 for r in results if r.success)
    print(f"{mode.upper()}: {success_count}/{buyers} success, {inventory.audit()}")

if __name__ == "__main__":
    random.seed(42)
    print("UNSAFE (expect violations/oversell):")
    run_simulation("unsafe")
    print("\nSAFE (no violations):")
    run_simulation("safe")
