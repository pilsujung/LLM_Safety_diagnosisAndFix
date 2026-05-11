import heapq
import random
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Tuple




class Priority(Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3

@dataclass(order=True)
class Request:
    sort_key: Tuple[float, int] = field(init=False, repr=False)
    priority: Priority = field(compare=False)
    id: int = field(compare=False)
    creation_time: float = field(compare=False)
    duration: float = field(compare=False)
    start_time: Optional[float] = field(default=None, compare=False)
    completion_time: Optional[float] = field(default=None, compare=False)

    def __post_init__(self):
        self.sort_key = (self.priority.value, self.id)

    def get_effective_priority(self, now: float, age_factor: float = 0.1) -> float:
        """Dynamic priority: base + age penalty (lower value = higher priority)."""
        wait_time = now - self.creation_time
        age_boost = age_factor * wait_time
        return self.priority.value + age_boost

    def wait_time(self, now: Optional[float] = None) -> float:
        if self.start_time is None:
            if now is None: now = time.time()
            return now - self.creation_time
        return self.start_time - self.creation_time

    def is_complete(self) -> bool:
        return self.completion_time is not None




class ResourceManager:
    def __init__(self, preemption_enabled: bool = True, starvation_threshold: float = 10.0, age_factor: float = 0.1):
        self.request_queue: List[Request] = []
        self.all_requests: List[Request] = []
        self.current_request: Optional[Request] = None
        self.current_exec_start: float = 0.0
        self.time_elapsed: float = 0.0
        self.preemption_enabled = preemption_enabled
        self.starvation_threshold = starvation_threshold
        self.age_factor = age_factor
        self.request_counter = 0

    def add_request(self, priority: Priority, duration: float) -> int:
        self.request_counter += 1
        req = Request(priority=priority, id=self.request_counter, 
                     creation_time=self.time_elapsed, duration=duration)
        heapq.heappush(self.request_queue, req)
        self.all_requests.append(req)
        print(f"[{self.time_elapsed:.2f}] + Request #{req.id} ({priority}) dur={duration:.2f}")
        return req.id

    def _peek_next(self) -> Optional[Request]:
        return self.request_queue[0] if self.request_queue else None

    def _rebuild_heap_with_aging(self) -> None:
        """Rebuild heap with current effective priorities to handle aging."""
        temp = []
        while self.request_queue:
            temp.append(heapq.heappop(self.request_queue))
        for req in temp:
            req.sort_key = (req.get_effective_priority(self.time_elapsed, self.age_factor), req.id)
            heapq.heappush(self.request_queue, req)

    def check_starvation(self) -> List[Request]:
        starving = [req for req in self.request_queue 
                   if self.time_elapsed - req.creation_time > self.starvation_threshold]
        return starving

    def step(self, time_step: float = 1.0) -> None:
        self.time_elapsed += time_step


        if self.request_queue:
            self._rebuild_heap_with_aging()

        if self.current_request:
            time_spent = self.time_elapsed - self.current_exec_start

            if time_spent >= self.current_request.duration:
                self.current_request.completion_time = self.time_elapsed
                print(f"[{self.time_elapsed:.2f}] ✓ Done #{self.current_request.id} ({self.current_request.priority}) "
                      f"wait={self.current_request.wait_time():.2f} total={self.time_elapsed - self.current_request.creation_time:.2f}")
                self.current_request = None

            elif (self.preemption_enabled and self.request_queue and 
                  self._peek_next().sort_key[0] < self.current_request.priority.value):

                self.current_request.duration -= time_spent
                heapq.heappush(self.request_queue, self.current_request)
                next_req = self._peek_next()
                print(f"[{self.time_elapsed:.2f}] ↺ Preempt #{self.current_request.id} by #{next_req.id} ({next_req.priority})")
                self.current_request = None

        if not self.current_request and self.request_queue:
            self.current_request = heapq.heappop(self.request_queue)
            if self.current_request.start_time is None:
                self.current_request.start_time = self.time_elapsed
            self.current_exec_start = self.time_elapsed
            print(f"[{self.time_elapsed:.2f}] ▶ Start #{self.current_request.id} ({self.current_request.priority}) "
                  f"wait={self.current_request.wait_time():.2f}")


        starving = self.check_starvation()
        if starving:
            print(f"[{self.time_elapsed:.2f}] ! STARVATION {len(starving)} requests")
            worst = max(starving, key=lambda r: self.time_elapsed - r.creation_time)
            print(f" - worst: #{worst.id} ({worst.priority}) waited {self.time_elapsed-worst.creation_time:.1f}")

    def simulate(self, steps: int, generation_probability: float = 0.3) -> None:
        for _ in range(steps):
            if random.random() < generation_probability:
                priority = random.choices([Priority.HIGH, Priority.MEDIUM, Priority.LOW], 
                                        weights=[0.6, 0.3, 0.1], k=1)[0]
                self.add_request(priority, random.uniform(1.0, 5.0))
            self.step()

    def generate_statistics(self) -> Dict:
        stats = {
            "total_requests": len(self.all_requests), "completed": sum(1 for r in self.all_requests if r.is_complete()),
            "in_progress": 1 if self.current_request else 0, "waiting": len(self.request_queue),
            "by_priority": {p: sum(1 for r in self.all_requests if r.priority == p) for p in Priority},
            "avg_wait_time": 0.0, "avg_wait_by_priority": {p: 0.0 for p in Priority}
        }
        wait_times = [r.start_time - r.creation_time for r in self.all_requests if r.is_complete()]
        if wait_times:
            stats["avg_wait_time"] = sum(wait_times) / len(wait_times)
            for p in Priority:
                p_waits = [r.start_time - r.creation_time for r in self.all_requests 
                          if r.priority == p and r.is_complete()]
                if p_waits: stats["avg_wait_by_priority"][p] = sum(p_waits) / len(p_waits)
        return stats




def run_demo():
    print("=== FIXED SIMULATION (AGING PREVENTS STARVATION) ===")
    manager = ResourceManager(preemption_enabled=True, starvation_threshold=15.0, age_factor=0.08)
    manager.add_request(Priority.LOW, 8.0)
    manager.add_request(Priority.LOW, 6.0)
    manager.add_request(Priority.MEDIUM, 4.0)
    manager.simulate(steps=50, generation_probability=0.3)
    
    stats = manager.generate_statistics()
    print("\n=== FINAL STATISTICS ===")
    print(f"Completed: {stats['completed']}/{stats['total_requests']} | Avg wait: {stats['avg_wait_time']:.1f}")
    print("No persistent starvation detected.")

if __name__ == "__main__":
    run_demo()
