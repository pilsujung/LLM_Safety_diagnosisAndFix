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


@dataclass(order=True)
class Request:

    sort_key: tuple = field(init=False, repr=False)


    priority: Priority = field(compare=False)
    id: int = field(compare=False)
    creation_time: float = field(compare=False)
    duration: float = field(compare=False)
    start_time: Optional[float] = field(default=None, compare=False)
    completion_time: Optional[float] = field(default=None, compare=False)

    def __post_init__(self):

        self.sort_key = (self.priority.value, self.id)

    def wait_time(self, now: Optional[float] = None) -> float:
        """Time waited before execution started."""
        if self.start_time is None:
            if now is None:
                now = time.time()
            return now - self.creation_time
        return self.start_time - self.creation_time

    def is_complete(self) -> bool:
        return self.completion_time is not None





class ResourceManager:
    """
    Priority-based single-resource scheduler (with optional preemption).
    Time advances in discrete steps (no real waiting).
    Visualization removed; statistics retained.
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

    def check_starvation(self) -> List[Request]:
        """Detect starving requests exceeding the waiting threshold."""
        starving = []
        for req in self.request_queue:
            if self.time_elapsed - req.creation_time > self.starvation_threshold:
                starving.append(req)
        return starving

    def step(self, time_step: float = 1.0) -> None:
        """Advance simulation by one time step."""
        self.time_elapsed += time_step


        if self.current_request:
            time_spent = self.time_elapsed - self.current_exec_start


            if time_spent >= self.current_request.duration:
                self.current_request.completion_time = self.time_elapsed
                print(
                    f"[{self.time_elapsed:.2f}] ✓ Done #{self.current_request.id} ({self.current_request.priority}) "
                    f"wait={self.current_request.wait_time():.2f} "
                    f"total={self.time_elapsed - self.current_request.creation_time:.2f}"
                )
                self.current_request = None


            elif self.preemption_enabled and self.request_queue:
                next_req = self._peek_next()
                if next_req and next_req.priority.value < self.current_request.priority.value:

                    self.current_request.duration -= time_spent
                    heapq.heappush(self.request_queue, self.current_request)
                    print(
                        f"[{self.time_elapsed:.2f}] ↺ Preempt #{self.current_request.id} "
                        f"by #{next_req.id} ({next_req.priority})"
                    )
                    self.current_request = None


        if not self.current_request and self.request_queue:

            starving = self.check_starvation()
            if starving:

                starving.sort(key=lambda r: (r.priority.value, r.creation_time))
                self.current_request = starving[0]
                self.request_queue.remove(self.current_request)
                heapq.heapify(self.request_queue)
            else:
                self.current_request = heapq.heappop(self.request_queue)

            if self.current_request.start_time is None:
                self.current_request.start_time = self.time_elapsed
            self.current_exec_start = self.time_elapsed
            print(
                f"[{self.time_elapsed:.2f}] ▶ Start #{self.current_request.id} "
                f"({self.current_request.priority}) wait={self.current_request.wait_time():.2f}"
            )


        starving = self.check_starvation()
        if starving:
            print(f"[{self.time_elapsed:.2f}] ! STARVATION {len(starving)} requests over threshold ({self.starvation_threshold})")
            count_by_pri: Dict[Priority, int] = {p: 0 for p in Priority}
            for r in starving:
                count_by_pri[r.priority] += 1
            for p, c in count_by_pri.items():
                if c:
                    print(f" - {p}: {c}")
            worst = max(starving, key=lambda r: self.time_elapsed - r.creation_time)
            print(f" - worst: #{worst.id} ({worst.priority}) waited {self.time_elapsed - worst.creation_time:.2f}")

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
            "completed": 0,
            "in_progress": 1 if self.current_request else 0,
            "waiting": len(self.request_queue),
            "by_priority": {p: 0 for p in Priority},
            "avg_wait_time": 0.0,
            "avg_wait_by_priority": {p: 0.0 for p in Priority},
            "starving_requests": 0,
            "starving_by_priority": {p: 0 for p in Priority},
        }

        wait_times: List[float] = []
        wait_by_pri: Dict[Priority, List[float]] = {p: [] for p in Priority}

        for r in self.all_requests:
            stats["by_priority"][r.priority] += 1
            if r.is_complete():
                stats["completed"] += 1
                wt = r.start_time - r.creation_time
                wait_times.append(wt)
                wait_by_pri[r.priority].append(wt)

        if wait_times:
            stats["avg_wait_time"] = sum(wait_times) / len(wait_times)
            for p in Priority:
                if wait_by_pri[p]:
                    stats["avg_wait_by_priority"][p] = sum(wait_by_pri[p]) / len(wait_by_pri[p])

        starving = self.check_starvation()
        stats["starving_requests"] = len(starving)
        for r in starving:
            stats["starving_by_priority"][r.priority] += 1

        return stats





def run_simple_demo():
    print("=== RESOURCE ALLOCATION SIMULATION (NO VISUALIZATION) ===")
    manager = ResourceManager(preemption_enabled=True, starvation_threshold=15.0)


    manager.add_request(Priority.LOW, 8.0)
    manager.add_request(Priority.LOW, 6.0)
    manager.add_request(Priority.MEDIUM, 4.0)


    manager.simulate(steps=50, generation_probability=0.3)


    stats = manager.generate_statistics()
    print("\n=== STATISTICS ===")
    print(f"Total requests : {stats['total_requests']}")
    print(f"Completed : {stats['completed']}")
    print(f"In progress : {stats['in_progress']}")
    print(f"Waiting : {stats['waiting']}")
    print("Requests by priority:")
    for p in Priority:
        print(f" - {p}: {stats['by_priority'][p]}")
    print(f"Average wait time : {stats['avg_wait_time']:.2f}")
    print("Average wait by priority:")
    for p in Priority:
        val = stats['avg_wait_by_priority'][p]
        if val > 0:
            print(f" - {p}: {val:.2f}")
    print(f"Starving requests : {stats['starving_requests']}")
    if stats['starving_requests'] > 0:
        print("Starving by priority:")
        for p in Priority:
            c = stats['starving_by_priority'][p]
            if c:
                print(f" - {p}: {c}")


if __name__ == "__main__":
    run_simple_demo()
