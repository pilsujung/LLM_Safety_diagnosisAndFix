import threading
import random
import time
from typing import List, Optional
from collections import defaultdict

class AtomicInteger:
    """Simple atomic integer implementation for Python."""
    def __init__(self, initial_value: int = 0):
        self._value = initial_value
        self._lock = threading.Lock()
    
    def increment_and_get(self) -> int:
        """Atomically increment and return the new value."""
        with self._lock:
            self._value += 1
            return self._value
    
    def get(self) -> int:
        """Get the current value."""
        with self._lock:
            return self._value

class ThreadSafeCounter:
    def __init__(self, error_rate: float = 0.0001):
                                                        
        self.value = AtomicInteger(0)
        self.total_operations = AtomicInteger(0)
        self.failed_operations = AtomicInteger(0)
        self.error_rate = error_rate
        
    def increment(self, thread_id: Optional[int] = None) -> None:
        """Increment counter with atomic operations."""
        self.total_operations.increment_and_get()
        
                                  
        if random.random() < self.error_rate:
            self.failed_operations.increment_and_get()
                                                               
            time.sleep(0.001)
            
                                                                                 
        self.value.increment_and_get()
        
    def get_stats(self) -> dict:
        """Return counter statistics."""
        total_ops = self.total_operations.get()
        failed_ops = self.failed_operations.get()
        
        return {
            'value': self.value.get(),
            'total_operations': total_ops,
            'failed_operations': failed_ops,
            'failure_rate': failed_ops / total_ops if total_ops > 0 else 0
        }

class ThreadManager:
    def __init__(self, counter: ThreadSafeCounter, num_threads: int = 10, iterations: int = 100):
        self.counter = counter
        self.num_threads = num_threads
        self.iterations = iterations
        self.threads: List[threading.Thread] = []
        
    def worker(self, thread_id: int) -> None:
        """Worker function for thread execution."""
        for _ in range(self.iterations):
            self.counter.increment(thread_id)
                
    def run_demo(self) -> dict:
        """Execute thread demonstration."""
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

                                                 
def demonstrate_atomic_violation():
    """Demonstrate the difference between unsafe and safe implementations."""
    
    class UnsafeCounter:
        def __init__(self):
            self.value = 0
            
        def increment(self):
                                                                                        
            current = self.value        
            current += 1                    
            self.value = current         
    
    def unsafe_worker(counter, iterations):
        for _ in range(iterations):
            counter.increment()
    
                         
    print("Testing Unsafe Counter:")
    unsafe_counter = UnsafeCounter()
    unsafe_threads = []
    
    for i in range(5):
        t = threading.Thread(target=unsafe_worker, args=(unsafe_counter, 1000))
        unsafe_threads.append(t)
        t.start()
    
    for t in unsafe_threads:
        t.join()
    
    print(f"Unsafe counter result: {unsafe_counter.value} (expected: 5000)")
    
                       
    print("\nTesting Safe Counter:")
    safe_counter = ThreadSafeCounter()
    manager = ThreadManager(safe_counter, num_threads=5, iterations=1000)
    results = manager.run_demo()
    
    print(f"Safe counter result: {results['value']} (expected: 5000)")
    print(f"Operations completed successfully: {results['value'] == 5000}")

                
if __name__ == "__main__":
                           
    demonstrate_atomic_violation()
    
    print("\n" + "="*60)
    print("Full ThreadSafeCounter Demo:")
    
    counter = ThreadSafeCounter(error_rate=0.1)
    manager = ThreadManager(counter, num_threads=10, iterations=1000)
    results = manager.run_demo()
    
    expected_count = manager.num_threads * manager.iterations
    print(f"\nExpected count: {expected_count}")
    print(f"Actual count: {results['value']}")
    print(f"Count matches expected: {results['value'] == expected_count}")
    print(f"Total operations: {results['total_operations']}")
    print(f"Failed operations: {results['failed_operations']}")
    print(f"Failure rate: {results['failure_rate']:.4f}")
    print(f"Execution time: {results['execution_time']:.4f}s")