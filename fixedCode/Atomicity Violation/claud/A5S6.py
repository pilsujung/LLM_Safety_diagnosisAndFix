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
        """Increment counter with proper thread safety."""
                                                                     
        with self._lock:
            self.total_operations += 1
            current_value = self.value
            
                                                                                 
                                                                      
            if random.random() < self.error_rate:
                                                                  
                time.sleep(0.001)                                          
                self.failed_operations += 1
                
                                                       
            self.value = current_value + 1
        
    def get_stats(self) -> dict:
        """Return counter statistics with consistent snapshot."""
                                                                              
        with self._lock:
            return {
                'value': self.value,
                'total_operations': self.total_operations,
                'failed_operations': self.failed_operations,
                'failure_rate': self.failed_operations / self.total_operations if self.total_operations > 0 else 0
            }

class ThreadManager:
    def __init__(self, counter: ThreadSafeCounter, num_threads: int = 10, iterations: int = 100):
        self.counter = counter
        self.num_threads = num_threads
        self.iterations = iterations
        self.threads: List[threading.Thread] = []
        
    def worker(self, thread_id: int) -> None:
        """Worker function for thread execution (now thread-safe)."""
        for _ in range(self.iterations):
            self.counter.increment(thread_id)
            
    def run_demo(self) -> dict:
        """Execute thread demonstration (now thread-safe)."""
        self.threads = [
            threading.Thread(
                target=self.worker,
                args=(i,)
            ) for i in range(self.num_threads)
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
    print("=== Thread-Safe Version ===")
    counter_safe = ThreadSafeCounter(error_rate=0.1)
    manager_safe = ThreadManager(counter_safe, num_threads=10, iterations=100)
    results_safe = manager_safe.run_demo()
    print("Safe Version Results:", results_safe)
    print(f"Expected value: {10 * 100}, Actual value: {results_safe['value']}")
    
                                         
    expected_total = manager_safe.num_threads * manager_safe.iterations
    assert results_safe['value'] == expected_total, f"Counter mismatch! Expected {expected_total}, got {results_safe['value']}"
    print("✅ Thread safety verified - counter value is correct!")