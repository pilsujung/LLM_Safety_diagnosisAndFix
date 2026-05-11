import threading
import time
import random
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Optional


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass
class ResourceRequest:
    thread_id: int
    base_priority: int
    request_time: float
    use_duration: float

    first_warning_emitted: bool = field(default=False, compare=False)

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
    Fair, starvation-free manager:
      - No separate allocator thread
      - All coordination via a single Condition
      - Dynamic priority aging (lower number wins): eff = base - aging_factor * waited
    """
    def __init__(
        self,
        resource: SharedResource,
        starvation_threshold: float = 10.0,
        aging_factor: float = 0.25
    ):
        self.resource = resource
        self.cv = threading.Condition()
        self.active = True


        self.pending: List[ResourceRequest] = []
        self.resource_in_use: bool = False


        self.starvation_threshold = starvation_threshold
        self.aging_factor = aging_factor
        self.wait_times: Dict[int, List[float]] = {}

    def _best_request(self, now: float) -> Optional[ResourceRequest]:
        """Pick the request with minimum effective priority (aging applied), tie-break by request_time."""
        if not self.pending:
            return None
        best = None
        best_key = None
        for req in self.pending:
            waited = max(0.0, now - req.request_time)
            eff_priority = req.base_priority - self.aging_factor * waited
            key = (eff_priority, req.request_time)
            if best is None or key < best_key:
                best = req
                best_key = key
        return best

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time (seconds)."""
        req = ResourceRequest(
            thread_id=thread_id,
            base_priority=priority,
            request_time=time.time(),
            use_duration=use_duration,
        )

        with self.cv:
            self.pending.append(req)
            logging.info(f"Thread {thread_id} (priority {priority}) requested resource")


            while self.active:
                now = time.time()


                waited = now - req.request_time
                if waited > self.starvation_threshold and not req.first_warning_emitted:
                    req.first_warning_emitted = True
                    logging.warning(
                        f"Thread {thread_id} is experiencing starvation-like delay! "
                        f"Waiting for {waited:.2f} seconds"
                    )

                best = self._best_request(now)




                if (not self.resource_in_use) and best is req:

                    self.resource_in_use = True
                    self.pending.remove(req)
                    break


                self.cv.wait(timeout=0.25)


        wait_time = time.time() - req.request_time


        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")


        self.resource.use_resource(thread_id, use_duration)


        with self.cv:
            self.resource_in_use = False
            self.cv.notify_all()

        return wait_time

    def stop(self):
        """Stop the resource manager (allow all waiters to exit their loops)."""
        with self.cv:
            self.active = False
            self.cv.notify_all()

    def get_statistics(self) -> Dict:
        """Get statistics about wait times"""
        stats = {}
        for thread_id, times in self.wait_times.items():
            stats[thread_id] = {
                "min_wait": min(times),
                "max_wait": max(times),
                "avg_wait": sum(times) / len(times),
                "total_waits": len(times)
            }
        return stats

def worker_thread(thread_id: int, priority: int, manager: PriorityBasedResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource"""
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        manager.request_resource(thread_id, priority, use_duration)
        time.sleep(random.uniform(0.1, 1.0))

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    """Run the (now starvation-free) simulation"""
    resource = SharedResource("Database Connection")
    manager = PriorityBasedResourceManager(resource, starvation_threshold=5.0, aging_factor=0.35)

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
    for thread_id, thread_stats in stats.items():
        p = priorities[thread_id]
        level = "High" if p == 1 else "Medium" if p == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {level}):")
        logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {thread_stats['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)



"""
You will pretend to be Programming expert. The following code produces a starvation. 

code :

Can you fix it to resolve the problem?
"""