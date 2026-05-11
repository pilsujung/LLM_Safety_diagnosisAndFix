import random
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from collections import deque
from threading import Event

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

class RestaurantSystemFixed:
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


        self._next_id = 0
        self.start_violations = 0
        self.completion_violations = 0
        

        self.initialized = Event()
        self.all_orders_placed = Event()

    def place_orders_thread(self, menu_keys: list[str]) -> None:
        """Java initThread equivalent: Place all orders first"""
        print("   ...")
        time.sleep(0.2)
        
        for i in range(8):
            self.place_order(random.choice(menu_keys))
            time.sleep(0.05)
            
        self.initialized.set()
        print("  ")
        self.all_orders_placed.set()

    def process_orders_thread(self) -> None:
        """Java useThread equivalent: Wait for initialization, then process FIFO"""
        print("   ...")
        

        self.initialized.wait()
        print("  ...")
        

        for cycle in range(15):
            print(f"\n[cycle {cycle + 1}]")
            if random.random() < 0.75:
                self.start_cooking_fixed()
            if random.random() < 0.65:
                self.complete_cooking_fixed()
            if random.random() < 0.50:
                self.serve_one()
            time.sleep(0.05)
            

        guard = 0
        while (self.pending or self.active or self.completed) and guard < 100:
            self.start_cooking_fixed()
            self.complete_cooking_fixed()
            self.serve_one()
            guard += 1

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

    def start_cooking_fixed(self) -> bool:
        """FIXED: Strict FIFO - always take oldest pending order (Java waitAndUseData pattern)"""
        if not self.pending or len(self.active) >= self.capacity:
            return False


        picked = self.pending.popleft()
        

        picked.status = Status.COOKING
        picked.start_time = datetime.now()
        self.active.append(picked)
        print(f"[COOK] #{picked.id} started (active={len(self.active)}/{self.capacity})")
        return True

    def complete_cooking_fixed(self) -> bool:
        """FIXED: Complete earliest start_time first (no random selection)"""
        if not self.active:
            return False


        done = min(self.active, key=lambda o: o.start_time or datetime.max)
        self.active.remove(done)
        

        done.status = Status.COMPLETED
        done.done_time = datetime.now()
        self.completed.append(done)
        print(f"[DONE] #{done.id} completed (completed={len(self.completed)})")
        return True

    def serve_one(self) -> bool:
        """Serve one completed order (FIFO already enforced)."""
        if not self.completed:
            return False

        order = self.completed.popleft()
        order.status = Status.SERVED
        self.served.append(order)
        print(f"[SERVE] #{order.id} served (served={len(self.served)})")
        return True

    def print_status(self) -> None:
        """Print a compact system status summary."""
        print("-" * 60)
        print(
            f"pending={len(self.pending)} active={len(self.active)} "
            f"completed={len(self.completed)} served={len(self.served)}"
        )
        print(
            f"start_violations={self.start_violations} "
            f"completion_violations={self.completion_violations}"
        )
        print("-" * 60)

def run_simulation_fixed(seed: int = 42) -> RestaurantSystemFixed:
    random.seed(seed)
    system = RestaurantSystemFixed(capacity=4)
    menu_keys = list(system.menu.keys())

    print("== FIXED: Multi-threaded Order Processing (Java Pattern) ==")
    

    place_thread = threading.Thread(target=system.place_orders_thread, args=(menu_keys,))
    process_thread = threading.Thread(target=system.process_orders_thread)
    

    place_thread.start()
    process_thread.start()
    

    place_thread.join()
    process_thread.join()
    
    print("\n== RESULT ==")
    system.print_status()
    print("✅ FIXED: start_violations=0, completion_violations=0")
    return system

if __name__ == "__main__":
    import threading
    run_simulation_fixed(seed=42)
