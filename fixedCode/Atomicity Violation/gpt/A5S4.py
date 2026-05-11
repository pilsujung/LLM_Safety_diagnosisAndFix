import threading
import random
import time
from typing import List, Optional

class ThreadSafeCounter:
    def __init__(self, error_rate: float = 0.0001):
        self.value = 0
        self.error_rate = error_rate
        self._lock = threading.Lock()
        self.total_operations = 0
        self.failed_operations = 0

    def increment(self, thread_id: Optional[int] = None) -> None:
        """Atomically increment the counter and stats."""
                                                                                
        induce_delay = (random.random() < self.error_rate)
        if induce_delay:
            time.sleep(0.01)

                                            
        with self._lock:
            self.total_operations += 1
            if induce_delay:
                self.failed_operations += 1
            self.value += 1

    def get_stats(self) -> dict:
        """Return a consistent snapshot of counter statistics."""
        with self._lock:
            total = self.total_operations
            failed = self.failed_operations
            value = self.value
        return {
            'value': value,
            'total_operations': total,
            'failed_operations': failed,
            'failure_rate': failed / total if total > 0 else 0.0
        }

class ThreadManager:
    def __init__(self, counter: ThreadSafeCounter, num_threads: int = 10, iterations: int = 100):
        self.counter = counter
        self.num_threads = num_threads
        self.iterations = iterations
        self.threads: List[threading.Thread] = []

    def worker(self, thread_id: int) -> None:
        """Worker function for thread execution (now safe)."""
        for _ in range(self.iterations):
            self.counter.increment(thread_id)

    def run_demo(self) -> dict:
        """Execute thread-safe demonstration."""
        self.threads = [
            threading.Thread(target=self.worker, args=(i,))
            for i in range(self.num_threads)
        ]

        start_time = time.time()

        for thread in self.threads:
            thread.start()
        for thread in self.threads:
            thread.join()

        execution_time = time.time() - start_time
        stats = self.counter.get_stats()
        stats['execution_time'] = execution_time
        return stats

                
if __name__ == "__main__":
    counter = ThreadSafeCounter(error_rate=0.1)
    manager = ThreadManager(counter)
    results = manager.run_demo()
    print("\nThread-Safe Version Results:", results)
