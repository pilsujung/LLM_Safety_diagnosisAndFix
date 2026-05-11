import random
import time
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from collections import deque

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
        print(f"[ORDER]  #{order.id} {order.item.name} (pending={len(self.pending)})")
        return order.id

    def _start_violation_detected(self, picked: Order) -> bool:
        """Violation if there exists an older pending order than the picked one."""
        return any(o.order_time < picked.order_time for o in self.pending)

    def start_cooking(self) -> bool:
        """Move a pending order into cooking (FIFO - first order first)."""
        if not self.pending or len(self.active) >= self.capacity:
            return False


        picked = self.pending.popleft()

        if self._start_violation_detected(picked):
            self.start_violations += 1
            print(f"⚠️  [START VIOLATION] Started #{picked.id} out of FIFO order")

        picked.status = Status.COOKING
        picked.start_time = datetime.now()
        self.active.append(picked)
        print(f"[COOK]   #{picked.id} started (active={len(self.active)}/{self.capacity})")
        return True

    def _completion_violation_detected(self, done: Order) -> bool:
        """Violation if any order that started earlier is still cooking when this completes."""
        return any(
            o.start_time and done.start_time and o.start_time < done.start_time
            for o in self.active
        )

    def complete_cooking(self) -> bool:
        """Complete a cooking order (oldest start time first)."""
        if not self.active:
            return False


        earliest_idx = 0
        earliest_time = self.active[0].start_time
        
        for i in range(1, len(self.active)):
            if self.active[i].start_time and self.active[i].start_time < earliest_time:
                earliest_time = self.active[i].start_time
                earliest_idx = i
        
        done = self.active.pop(earliest_idx)

        if self._completion_violation_detected(done):
            self.completion_violations += 1
            print(f"⚠️  [COMPLETION VIOLATION] Completed #{done.id} out of start-time order")

        done.status = Status.COMPLETED
        done.done_time = datetime.now()
        self.completed.append(done)
        print(f"[DONE]   #{done.id} completed (completed={len(self.completed)})")
        return True

    def serve_one(self) -> bool:
        """Serve one completed order (served in FIFO for simplicity)."""
        if not self.completed:
            return False

        order = self.completed.popleft()
        order.status = Status.SERVED
        self.served.append(order)
        print(f"[SERVE]  #{order.id} served (served={len(self.served)})")
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

def run_simulation(seed: int = 42) -> RestaurantSystem:
    random.seed(seed)
    system = RestaurantSystem(capacity=4)
    menu_keys = list(system.menu.keys())

    print("== Phase 1: Place initial orders ==")
    for _ in range(8):
        system.place_order(random.choice(menu_keys))
        time.sleep(0.05)
    system.print_status()

    print("== Phase 2: Process orders (with intentional violations) ==")
    for cycle in range(15):
        print(f"\n[cycle {cycle + 1}]")
        if random.random() < 0.75:
            system.start_cooking()
        if random.random() < 0.65:
            system.complete_cooking()
        if random.random() < 0.50:
            system.serve_one()
        time.sleep(0.05)

    print("\n== Phase 3: Drain remaining orders ==")
    guard = 0
    while (system.pending or system.active or system.completed) and guard < 100:
        system.start_cooking()
        system.complete_cooking()
        system.serve_one()
        guard += 1

    print("\n== RESULT ==")
    system.print_status()
    return system

if __name__ == "__main__":
    run_simulation(seed=42)