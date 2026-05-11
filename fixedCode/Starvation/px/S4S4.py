import heapq
import random
import time
from enum import Enum
from dataclasses import dataclass, field
from typing import List, Optional, Dict





class Priority(Enum):
    HIGH = 1
    MEDIUM = 2
    LOW = 3

    def __str__(self) -> str:
        return self.name


@dataclass
class Request:
    priority: Priority
    id: int
    creation_time: float
    duration: float
    start_time: Optional[float] = None
    completion_time: Optional[float] = None
    original_priority: Priority = None
    current_priority: Priority = None

    def __post_init__(self):
        self.original_priority = self.priority
        self.current_priority = self.priority

    def __lt__(self, other):

        return (self.current_priority.value, self.id) < (other.current_priority.value, other.id)

    def wait_time(self, now: Optional[float] = None) -> float:
        """Time waited before execution started."""
        if self.start_time is None:
            if now is None:
                now = time.time()
            return now - self.creation_time
        return self.start_time - self.creation_time

    def is_complete(self) -> bool:
        return self.completion_time is not None

    def update_priority(self, new_priority: Priority):
        """Update current priority and return True if changed."""
        if self.current_priority != new_priority:
            self.current_priority = new_priority
            return True
        return False





class ResourceManager:
    """
    Priority-based single-resource scheduler with starvation fix via aging.
    Uses proper heapq compatibility with custom __lt__ comparison.
    """

    def __init__(self, preemption_enabled: bool = True, starvation_threshold: float = 10.0):
        self.request_queue: List[Request] = []
        self.all_requests: List[Request] = []
        self.current_request: Optional[Request] = None
        self.current_exec_start: float = 0.0
        self.time_elapsed: float = 0.0
        self.preemption_enabled = preemption_enabled
        self.starvation_threshold = starvation_threshold
        self.request_counter = 0

    def add_request(self, priority: Priority, duration: float) -> int:
        """Add a new request to the queue."""
        self.request_counter += 1
        req = Request(
            priority=priority,
            id=self.request_counter,
            creation_time=self.time_elapsed,
            duration=duration
        )
        heapq.heappush(self.request_queue, req)
        self.all_requests.append(req)
        print(f"[{self.time_elapsed:.2f}] + Request #{req.id} ({priority}) dur={duration:.2f}")
        return req.id

    def _peek_next(self) -> Optional[Request]:
        return self.request_queue[0] if self.request_queue else None

    def _update_priorities(self) -> List[Request]:
        """Check for starvation and boost priority of waiting requests."""
        starving = []
        updated = False
        

        for req in self.request_queue:
            wait_time = self.time_elapsed - req.creation_time
            
            if wait_time > self.starvation_threshold * 2:

                if req.update_priority(Priority.HIGH):
                    print(f"[{self.time_elapsed:.2f}] ↑ Critical boost #{req.id} to HIGH (waited {wait_time:.1f})")
                    starving.append(req)
                    updated = True
                    
            elif wait_time > self.starvation_threshold:

                if req.current_priority == Priority.LOW:
                    if req.update_priority(Priority.MEDIUM):
                        print(f"[{self.time_elapsed:.2f}] ↑ Boost #{req.id} LOW→MEDIUM (waited {wait_time:.1f})")
                        starving.append(req)
                        updated = True
                elif req.current_priority == Priority.MEDIUM:
                    if req.update_priority(Priority.HIGH):
                        print(f"[{self.time_elapsed:.2f}] ↑ Boost #{req.id} MEDIUM→HIGH (waited {wait_time:.1f})")
                        starving.append(req)
                        updated = True
        

        if updated:
            temp_queue = self.request_queue[:]
            self.request_queue.clear()
            heapq.heapify(temp_queue)
            self.request_queue = temp_queue
            
        return starving

    def step(self, time_step: float = 1.0) -> None:
        """Advance simulation by one time step."""
        self.time_elapsed += time_step


        starving = self._update_priorities()


        if self.current_request:
            time_spent = self.time_elapsed - self.current_exec_start


            if time_spent >= self.current_request.duration:
                self.current_request.completion_time = self.time_elapsed
                print(
                    f"[{self.time_elapsed:.2f}] ✓ Done #{self.current_request.id} "
                    f"({self.current_request.original_priority}→{self.current_request.current_priority}) "
                    f"wait={self.current_request.wait_time():.2f}"
                )
                self.current_request = None


            elif self.preemption_enabled and self.request_queue:
                next_req = self._peek_next()
                if next_req and next_req.current_priority.value < self.current_request.current_priority.value:

                    self.current_request.duration -= time_spent
                    heapq.heappush(self.request_queue, self.current_request)
                    print(
                        f"[{self.time_elapsed:.2f}] ↺ Preempt #{self.current_request.id} "
                        f"({self.current_request.current_priority}) by #{next_req.id} ({next_req.current_priority})"
                    )
                    self.current_request = None


        if not self.current_request and self.request_queue:
            self.current_request = heapq.heappop(self.request_queue)
            if self.current_request.start_time is None:
                self.current_request.start_time = self.time_elapsed
            self.current_exec_start = self.time_elapsed
            print(
                f"[{self.time_elapsed:.2f}] ▶ Start #{self.current_request.id} "
                f"({self.current_request.original_priority}→{self.current_request.current_priority}) "
                f"wait={self.current_request.wait_time():.2f}"
            )

        if starving:
            print(f"[{self.time_elapsed:.2f}] ! Fixed starvation: boosted {len(starving)} requests")

    def simulate(self, steps: int, generation_probability: float = 0.3) -> None:
        """Run the simulation for a given number of steps."""
        for _ in range(steps):
            if random.random() < generation_probability:
                priority = random.choices(
                    [Priority.HIGH, Priority.MEDIUM, Priority.LOW],
                    weights=[0.6, 0.3, 0.1],
                    k=1
                )[0]
                duration = random.uniform(1.0, 5.0)
                self.add_request(priority, duration)
            self.step()

    def generate_statistics(self) -> Dict:
        """Compute overall statistics."""
        stats = {
            "total_requests": len(self.all_requests),
            "completed": sum(1 for r in self.all_requests if r.is_complete()),
            "in_progress": 1 if self.current_request else 0,
            "waiting": len(self.request_queue),
            "by_original_priority": {p: 0 for p in Priority},
            "avg_wait_time": 0.0,
            "avg_wait_by_priority": {p: 0.0 for p in Priority},
        }

        wait_times = []
        wait_by_pri = {p: [] for p in Priority}

        for r in self.all_requests:
            stats["by_original_priority"][r.original_priority] += 1
            if r.is_complete() and r.start_time:
                wt = r.start_time - r.creation_time
                wait_times.append(wt)
                wait_by_pri[r.original_priority].append(wt)

        if wait_times:
            stats["avg_wait_time"] = sum(wait_times) / len(wait_times)
            for p in Priority:
                if wait_by_pri[p]:
                    stats["avg_wait_by_priority"][p] = sum(wait_by_pri[p]) / len(wait_by_pri[p])

        return stats





def run_simple_demo():
    print("=== FIXED RESOURCE SCHEDULER (NO STARVATION) ===")
    manager = ResourceManager(preemption_enabled=True, starvation_threshold=10.0)


    manager.add_request(Priority.LOW, 8.0)
    manager.add_request(Priority.LOW, 6.0)
    manager.add_request(Priority.MEDIUM, 4.0)

    manager.simulate(steps=60, generation_probability=0.4)

    stats = manager.generate_statistics()
    print("\n=== STATISTICS ===")
    print(f"Total: {stats['total_requests']}, Completed: {stats['completed']}")
    print(f"Waiting: {stats['waiting']}, Avg wait: {stats['avg_wait_time']:.2f}")
    print("By original priority:")
    for p in Priority:
        print(f"  {p}: {stats['by_original_priority'][p]}")


def run_comparative_demo():
    seed = 42
    
    print("\n=== WITH STARVATION FIX ===")
    random.seed(seed)
    m1 = ResourceManager(preemption_enabled=True, starvation_threshold=10.0)
    m1.add_request(Priority.LOW, 5.0)
    m1.add_request(Priority.LOW, 5.0)
    m1.simulate(50, 0.4)
    s1 = m1.generate_statistics()

    print("\n=== WITHOUT FIX (original logic) ===")
    class OriginalManager(ResourceManager):
        def _update_priorities(self):
            return []
    
    random.seed(seed)
    m2 = OriginalManager(preemption_enabled=True, starvation_threshold=10.0)
    m2.add_request(Priority.LOW, 5.0)
    m2.add_request(Priority.LOW, 5.0)
    m2.simulate(50, 0.4)
    s2 = m2.generate_statistics()

    print("\n=== COMPARISON ===")
    print(f"{'Metric':<20} {'Fixed':<10} {'Original':<10}")
    print("-" * 40)
    print(f"{'Completed':<20} {s1['completed']:<10} {s2['completed']:<10}")
    print(f"{'Waiting':<20} {s1['waiting']:<10} {s2['waiting']:<10}")
    print(f"{'Avg wait':<20} {s1['avg_wait_time']:.1f:<10} {s2['avg_wait_time']:.1f:<10}")


if __name__ == "__main__":
    run_simple_demo()
    print()
    run_comparative_demo()
