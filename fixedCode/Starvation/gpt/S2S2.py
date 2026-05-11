

import threading
import time
import random
import logging
from collections import deque
from dataclasses import dataclass, field
from typing import List, Dict


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass
class ResourceRequest:
    thread_id: int
    request_time: float
    seq: int = field(default=0)

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.in_use = False
        self._lock = threading.Lock()

    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        with self._lock:
            self.in_use = True

        logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
        time.sleep(duration)

        with self._lock:
            self.in_use = False

        logging.info(f"Thread {thread_id} released {self.name}")

class FairResourceManager:
    """
    Starvation-free manager that grants access in strict arrival order (FIFO),
    analogous to Java's ReentrantLock(true).
    """
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0):
        self.resource = resource
        self.cv = threading.Condition()
        self.queue: deque[ResourceRequest] = deque()
        self._next_seq = 0
        self.active = True
        self.starvation_threshold = starvation_threshold
        self.wait_times: Dict[int, List[float]] = {}

    def request_resource(self, thread_id: int, _priority_ignored: int, use_duration: float) -> float:
        """Request access to the resource (FIFO fairness) and return the wait time."""
        request_time = time.time()
        with self.cv:
            req = ResourceRequest(thread_id=thread_id, request_time=request_time, seq=self._next_seq)
            self._next_seq += 1
            self.queue.append(req)
            logging.info(f"Thread {thread_id} requested resource (ticket {req.seq})")


            while self.active and (self.queue[0] is not req or self.resource.in_use):
                current_wait = time.time() - request_time
                if current_wait > self.starvation_threshold:
                    logging.warning(
                        f"Thread {thread_id} is experiencing long wait ({current_wait:.2f}s); "
                        f"queue length={len(self.queue)}"
                    )
                self.cv.wait(timeout=1.0)


            self.resource.in_use = True


        wait_time = time.time() - request_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")


        try:
            self.resource.use_resource(thread_id, use_duration)
        finally:

            with self.cv:
                self.resource.in_use = False

                if self.queue and self.queue[0] is req:
                    self.queue.popleft()
                self.cv.notify_all()

        return wait_time

    def stop(self):
        """Stop the manager and wake all waiters."""
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
                "total_waits": len(times),
            }
        return stats

def worker_thread(thread_id: int, priority: int, manager: FairResourceManager, iterations: int):
    """Worker thread that repeatedly requests the resource"""
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        manager.request_resource(thread_id, priority, use_duration)
        time.sleep(random.uniform(0.1, 1.0))

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
    """Run the starvation-free simulation"""
    resource = SharedResource("Database Connection")
    manager = FairResourceManager(resource, starvation_threshold=5.0)

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

    logging.info(f"Starting simulation with {num_threads} threads (FIFO fairness)")
    logging.info(f"Thread priorities (informational only): {priorities}")

    start_time = time.time()
    for t in threads:
        t.start()

    try:
        while time.time() - start_time < runtime_seconds:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")

    manager.stop()
    for t in threads:
        t.join(timeout=0.5)

    stats = manager.get_statistics()
    logging.info("\n----- Simulation Results (Fair) -----")
    for thread_id, thread_stats in stats.items():
        level = "High" if priorities[thread_id] == 1 else "Medium" if priorities[thread_id] == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {level}):")
        logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {thread_stats['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)



"""
starvation occurrence code :
Fixed code :

Can you fix the code below to resolve the starvation according to the above example?

starvation occurrence code :
Fixed code :
"""