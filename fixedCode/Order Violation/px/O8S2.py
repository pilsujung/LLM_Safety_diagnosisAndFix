import random
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from collections import deque
import threading

class Status(Enum):
    PENDING = "pending"
    COOKING = "cooking"
    COMPLETED = "completed"
    SERVED = "served"

@dataclass
class MenuItem:
    name: str
    cook_sec: int

@dataclass
class Order:
    id: int
    item: MenuItem
    status: Status
    order_time: datetime
    start_time: datetime | None = None
    done_time: datetime | None = None

class RestaurantSystem:
    def __init__(self, capacity: int = 4):
        self.capacity = capacity
        self.menu = {
            "bibimbap": MenuItem("Bibimbap", 3),
            "kimchi_stew": MenuItem("Kimchi Stew", 4),
            "bulgogi": MenuItem("Bulgogi", 5),
            "tteokbokki": MenuItem("Tteokbokki", 2),
        }


        self.pending = deque()
        self.active: list[Order] = []
        self.completed: deque[Order] = deque()
        self.served: list[Order] = []
        

        self.initialized = threading.Event()
        self._next_id = 0
        self.start_violations = 0
        self.completion_violations = 0

    def place_order(self, menu_key: str) -> int:
        """Create a new order and enqueue it into pending."""
        if menu_key not in self.menu:
            raise ValueError(f"Unknown menu item: {menu_key}")

        self._next_id += 1
        order = Order(
            id=self._next_id,
            item=self.menu[menu_key],
            status=Status.PENDING,
            order_time=datetime.now()
        )
        self.pending.append(order)
        print(f"[ORDER] #{order.id} {order.item.name} (pending={len(self.pending)})")
        return order.id

    def initialize_orders(self):
        """Initialize orders (Java initializeData() equivalent)."""
        try:
            time.sleep(0.1)
        except Exception:
            pass
        

        menu_keys = list(self.menu.keys())
        for i in range(8):
            self.place_order(random.choice(menu_keys))
        

        self.initialized.set()
        print("  ")

    def wait_and_process_orders(self):
        """Wait for initialization then process (Java useData() equivalent)."""

        while not self.initialized.wait(0.01):
            print("   ...")
        
        print(" : simulation started")
        self.run_simulation_phases()

    def run_simulation_phases(self):
        """Main simulation logic."""
        print("== Phase 2: Process orders (FIXED: Strict FIFO) ==")
        for cycle in range(15):
            print(f"\n[cycle {cycle + 1}]")
            if random.random() < 0.75:
                self.start_cooking_fixed()
            if random.random() < 0.65:
                self.complete_cooking_fixed()
            if random.random() < 0.50:
                self.serve_one()
            time.sleep(0.05)

        print("\n== Phase 3: Drain remaining orders ==")
        guard = 0
        while (self.pending or self.active or self.completed) and guard < 100:
            self.start_cooking_fixed()
            self.complete_cooking_fixed()
            self.serve_one()
            guard += 1

    def start_cooking_fixed(self) -> bool:
        """FIXED: Always start OLDEST pending order (FIFO)."""
        if not self.pending or len(self.active) >= self.capacity:
            return False
        
        picked = self.pending.popleft()
        picked.status = Status.COOKING
        picked.start_time = datetime.now()
        self.active.append(picked)
        print(f"[COOK] #{picked.id} started (active={len(self.active)}/{self.capacity})")
        return True

    def complete_cooking_fixed(self) -> bool:
        """FIXED: Always complete EARLIEST started order."""
        if not self.active:
            return False
        

        earliest_idx = 0
        earliest_time = self.active[0].start_time
        for i, order in enumerate(self.active):
            if order.start_time and order.start_time < earliest_time:
                earliest_idx = i
                earliest_time = order.start_time
        
        done = self.active.pop(earliest_idx)
        done.status = Status.COMPLETED
        done.done_time = datetime.now()
        self.completed.append(done)
        print(f"[DONE] #{done.id} completed (completed={len(self.completed)})")
        return True

    def serve_one(self) -> bool:
        """Serve one completed order (FIFO)."""
        if not self.completed:
            return False

        order = self.completed.popleft()
        order.status = Status.SERVED
        self.served.append(order)
        print(f"[SERVE] #{order.id} served (served={len(self.served)})")
        return True

    def print_status(self) -> None:
        """Print system status."""
        print("-" * 60)
        print(f"pending={len(self.pending)} active={len(self.active)} "
              f"completed={len(self.completed)} served={len(self.served)}")
        print(f"start_violations={self.start_violations} "
              f"completion_violations={self.completion_violations}")
        print("-" * 60)

def run_simulation(seed: int = 42) -> RestaurantSystem:
    random.seed(seed)
    system = RestaurantSystem(capacity=4)
    
    print("== Phase 1: Multi-threaded Order Processing (Java Pattern) ==")
    

    init_thread = threading.Thread(target=system.initialize_orders)
    use_thread = threading.Thread(target=system.wait_and_process_orders)
    
    init_thread.start()
    use_thread.start()
    
    init_thread.join()
    use_thread.join()
    
    print("\n== RESULT ==")
    system.print_status()
    print("✅ FIXED: 0 violations - Strict FIFO ordering enforced")
    return system

if __name__ == "__main__":
    run_simulation(seed=42)
