import threading
import time
import random
import logging
import heapq
from dataclasses import dataclass, field
from typing import List, Dict, Tuple


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass
class ResourceRequest:
    base_priority: int
    request_time: float
    thread_id: int


class SharedResource:
    def __init__(self, name: str):
        self.name = name

    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        time.sleep(duration)
        logging.info(f"Thread {thread_id} released {self.name}")

class PriorityBasedResourceManager:
    """
    Fair, aging priority scheduler:
      - Lower priority number means higher priority (1 beats 3 beats 5)
      - If a request waits > starvation_threshold seconds, its *effective* priority becomes 0 (top)
    """
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0):
        self.resource = resource
        self.starvation_threshold = starvation_threshold


        self._lock = threading.Lock()
        self._cv = threading.Condition(self._lock)
        self._heap: List[Tuple[int, float, int, ResourceRequest]] = []
        self._seq = 0
        self._resource_in_use = False


        self.wait_times: Dict[int, List[float]] = {}
        self._active = True

    def _effective_priority(self, req: ResourceRequest, now: float) -> int:
        """Aging: boost to priority 0 once wait exceeds threshold."""
        waited = now - req.request_time
        if waited >= self.starvation_threshold:
            return 0
        return req.base_priority

    def _reheap_with_aging(self) -> None:
        """Re-score all requests with aging and rebuild heap."""
        now = time.time()
        if not self._heap:
            return

        new_heap = []
        for _eff, req_time, seq, req in self._heap:
            eff = self._effective_priority(req, now)
            new_heap.append((eff, req_time, seq, req))
        heapq.heapify(new_heap)
        self._heap = new_heap

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time."""
        req_time = time.time()
        req = ResourceRequest(priority, req_time, thread_id)

        with self._cv:

            eff = self._effective_priority(req, req_time)
            heapq.heappush(self._heap, (eff, req_time, self._seq, req))
            self._seq += 1
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")




            while True:

                self._reheap_with_aging()


                at_top = (self._heap and self._heap[0][3] is req)
                if at_top and not self._resource_in_use:

                    heapq.heappop(self._heap)
                    self._resource_in_use = True
                    break


                current_wait = time.time() - req_time
                if current_wait > self.starvation_threshold:
                    logging.warning(
                        f"Thread {thread_id} is experiencing starvation! Waiting for {current_wait:.2f} seconds"
                    )


                self._cv.wait(timeout=0.2)


        wait_time = time.time() - req_time


        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")

        try:
            self.resource.use_resource(thread_id, use_duration)
        finally:

            with self._cv:
                self._resource_in_use = False
                self._cv.notify_all()

        return wait_time

    def stop(self):
        """Stop the manager (best-effort signal to any waiters)."""
        with self._cv:
            self._active = False
            self._cv.notify_all()

    def get_statistics(self) -> Dict:
        """Get statistics about wait times."""
        stats = {}
        for thread_id, times in self.wait_times.items():
            stats[thread_id] = {
                "min_wait": min(times),
                "max_wait": max(times),
                "avg_wait": sum(times) / len(times),
                "total_waits": len(times),
            }
        return stats

def worker_thread(thread_id: int, priority: int, manager: PriorityBasedResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource."""
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        manager.request_resource(thread_id, priority, use_duration)
        time.sleep(random.uniform(0.1, 1.0))

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    """Run the starvation simulation."""
    resource = SharedResource("Database Connection")
    manager = PriorityBasedResourceManager(resource, starvation_threshold=5.0)

    threads = []
    priorities = {}
    for i in range(num_threads):
        if i < 2:
            priority = 1
        elif i < 4:
            priority = 3
        else:
            priority = 5
        priorities[i] = priority
        t = threading.Thread(target=worker_thread, args=(i, priority, manager, 100), daemon=True)
        threads.append(t)

    logging.info(f"Starting simulation with {num_threads} threads")
    logging.info(f"Thread priorities: {priorities}")
    start = time.time()

    for t in threads:
        t.start()

    try:
        while time.time() - start < runtime_seconds:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")

    manager.stop()
    for t in threads:
        t.join(timeout=0.5)

    stats = manager.get_statistics()
    logging.info("\n----- Simulation Results -----")
    for tid, s in stats.items():
        level = "High" if priorities[tid] == 1 else "Medium" if priorities[tid] == 3 else "Low"
        logging.info(f"Thread {tid} (Priority: {level}):")
        logging.info(f"  Min wait: {s['min_wait']:.2f}s")
        logging.info(f"  Max wait: {s['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {s['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {s['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)



"""
I’m going to tip $300 if you solve it. The following code produces a starvation. Can you fix it to resolve the problem?

code : 
"""