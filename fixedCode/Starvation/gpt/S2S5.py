import threading
import time
import random
import logging
from dataclasses import dataclass, field
from typing import List, Dict, Deque
from collections import deque


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass(order=True)
class ResourceRequest:
    request_time: float
    thread_id: int = field(compare=False)
    priority: int = field(default=0, compare=False)
    event: threading.Event = field(default_factory=threading.Event, compare=False)

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.lock = threading.Lock()
    
    def use_resource(self, thread_id: int, duration: float):
        """Simulate using the resource for a specific duration"""
        with self.lock:
            logging.info(f"Thread {thread_id} is USING {self.name} for {duration:.2f}s")
            time.sleep(duration)
            logging.info(f"Thread {thread_id} RELEASED {self.name}")

class FairResourceManager:
    """
    Starvation-free manager:
    - FIFO queue of requests (fair like ReentrantLock(true))
    - Only the head of the queue is allowed to proceed
    - On completion, wake up the next waiter
    """
    def __init__(self, resource: SharedResource, starvation_threshold: float = 10.0):
        self.resource = resource
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.queue: Deque[ResourceRequest] = deque()
        self.starvation_threshold = starvation_threshold
        self.wait_times: Dict[int, List[float]] = {}
        self.active = True

    def request_resource(self, thread_id: int, priority: int, use_duration: float) -> float:
        """Request access to the resource and return the wait time"""
        req_time = time.time()
        req = ResourceRequest(request_time=req_time, thread_id=thread_id, priority=priority)

        with self.cv:

            self.queue.append(req)
            logging.info(f"Thread {thread_id} (prio {priority}) enqueued")


            while self.active:

                if self.queue and self.queue[0] is req:
                    break


                waited = time.time() - req_time
                if waited > self.starvation_threshold:
                    logging.warning(
                        f"Thread {thread_id} waiting {waited:.2f}s (possible starvation if not FIFO)"
                    )

                self.cv.wait(timeout=0.01)


            if self.queue and self.queue[0] is req:
                self.queue.popleft()


        wait_time = time.time() - req_time
        self.wait_times.setdefault(thread_id, []).append(wait_time)
        logging.info(f"Thread {thread_id} proceeding after {wait_time:.3f}s wait")

        try:
            self.resource.use_resource(thread_id, use_duration)
        finally:

            with self.cv:
                self.cv.notify_all()

        return wait_time

    def stop(self):
        with self.cv:
            self.active = False
            self.cv.notify_all()

    def get_statistics(self) -> Dict:
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
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        manager.request_resource(thread_id, priority, use_duration)

        time.sleep(random.uniform(0.05, 0.3))

def run_simulation(num_threads: int = 5, runtime_seconds: int = 30):
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

    logging.info(f"Starting simulation with {num_threads} threads")
    logging.info(f"Thread priorities (informational): {priorities}")
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
    logging.info("\n----- Simulation Results (FIFO / starvation-free) -----")
    for thread_id, thread_stats in stats.items():
        pr = priorities[thread_id]
        label = "High" if pr == 1 else "Medium" if pr == 3 else "Low"
        logging.info(f"Thread {thread_id} (Priority: {label}):")
        logging.info(f"  Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f"  Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f"  Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f"  Resource accesses: {thread_stats['total_waits']}")

if __name__ == "__main__":
    run_simulation(num_threads=5, runtime_seconds=30)




"""
first starvation occurrence code :
first Fixed code :

second starvation occurrence code :
second Fixed code :

Can you fix the code below to resolve the starvation according to the above examples?

starvation occurrence code :
Fixed code :
"""