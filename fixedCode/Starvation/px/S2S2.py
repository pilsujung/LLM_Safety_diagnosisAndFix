import threading
import time
import random
import queue
import logging
from dataclasses import dataclass, field


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%H:%M:%S'
)

@dataclass
class ResourceRequest:
    thread_id: int
    request_time: float

class SharedResource:
    def __init__(self, name: str):
        self.name = name
        self.in_use = False
        self.lock = threading.Lock()

    def use_resource(self, thread_id: int, duration: float):
        with self.lock:
            self.in_use = True
            logging.info(f"Thread {thread_id} is using {self.name} for {duration:.2f} seconds")
            time.sleep(duration)
            self.in_use = False
            logging.info(f"Thread {thread_id} released {self.name}")

class FairResourceManager:
    def __init__(self, resource: SharedResource):
        self.resource = resource
        self.request_queue = queue.Queue()
        self.lock = threading.Lock()
        self.cv = threading.Condition(self.lock)
        self.active = True
        self.wait_times = {}

        self.allocation_thread = threading.Thread(target=self._allocate_resources)
        self.allocation_thread.daemon = True
        self.allocation_thread.start()

    def request_resource(self, thread_id: int, use_duration: float) -> float:
        request_time = time.time()
        request = ResourceRequest(thread_id, request_time)

        with self.lock:
            self.request_queue.put(request)
            logging.info(f"Thread {thread_id} requested resource")

            while self.active:
                if not self.request_queue.empty():
                    front_request = self.request_queue.queue[0]
                    if front_request.thread_id == thread_id and not self.resource.in_use:
                        break
                self.cv.wait(timeout=1.0)

            wait_time = time.time() - request_time
            if thread_id not in self.wait_times:
                self.wait_times[thread_id] = []
            self.wait_times[thread_id].append(wait_time)

            logging.info(f"Thread {thread_id} granted access after waiting {wait_time:.2f} seconds")


        self.resource.use_resource(thread_id, use_duration)

        with self.lock:
            if not self.request_queue.empty() and self.request_queue.queue[0].thread_id == thread_id:
                self.request_queue.get()
            self.cv.notify_all()

        return wait_time

    def _allocate_resources(self):
        while self.active:
            with self.lock:
                if not self.request_queue.empty() and not self.resource.in_use:
                    logging.info(f"Allocating resource to thread {self.request_queue.queue[0].thread_id}")
                    self.cv.notify_all()
            time.sleep(0.1)

    def stop(self):
        self.active = False
        with self.lock:
            self.cv.notify_all()
        self.allocation_thread.join(timeout=1.0)

    def get_statistics(self):
        stats = {}
        for thread_id, times in self.wait_times.items():
            stats[thread_id] = {
                "min_wait": min(times),
                "max_wait": max(times),
                "avg_wait": sum(times) / len(times),
                "total_waits": len(times)
            }
        return stats

def worker_thread(thread_id: int, manager: FairResourceManager, iterations: int):
    for _ in range(iterations):
        use_duration = random.uniform(0.1, 0.5)
        wait_time = manager.request_resource(thread_id, use_duration)
        think_time = random.uniform(0.1, 1.0)
        time.sleep(think_time)

def run_simulation(num_threads=5, runtime_seconds=30):
    resource = SharedResource("Database Connection")
    manager = FairResourceManager(resource)

    threads = []
    for i in range(num_threads):
        thread = threading.Thread(target=worker_thread, args=(i, manager, 100))
        thread.daemon = True
        threads.append(thread)

    logging.info(f"Starting simulation with {num_threads} threads")
    start_time = time.time()

    for thread in threads:
        thread.start()

    try:
        while time.time() - start_time < runtime_seconds:
            time.sleep(1)
    except KeyboardInterrupt:
        logging.info("Simulation interrupted")

    manager.stop()

    for thread in threads:
        thread.join(timeout=0.5)

    stats = manager.get_statistics()
    logging.info("\n----- Simulation Results -----")
    for thread_id, thread_stats in stats.items():
        logging.info(f"Thread {thread_id}:")
        logging.info(f" Min wait: {thread_stats['min_wait']:.2f}s")
        logging.info(f" Max wait: {thread_stats['max_wait']:.2f}s")
        logging.info(f" Avg wait: {thread_stats['avg_wait']:.2f}s")
        logging.info(f" Resource accesses: {thread_stats['total_waits']}")

if __name__ == "__main__":
    run_simulation()
