import threading
import time
from typing import List
from concurrent.futures import ThreadPoolExecutor


class ThreadSafeCounter:
    def __init__(self):
        self.count = 0
        self._lock = threading.Lock()
    
    def increment(self):
        with self._lock:

            current = self.count
            time.sleep(0.0001)
            self.count = current + 1


class RLockCounter:
    def __init__(self):
        self.count = 0
        self._lock = threading.RLock()
    
    def increment(self):
        with self._lock:
            current = self.count
            time.sleep(0.0001)
            self.count = current + 1
    
    def increment_by(self, amount):

        for _ in range(amount):
            self.increment()


class AtomicCounter:
    def __init__(self):
        self.count = 0
        self._lock = threading.Lock()
    
    def increment(self):

        time.sleep(0.0001)
        with self._lock:
            self.count += 1


class LocalCounter:
    def __init__(self):
        self.total_count = 0
        self._local = threading.local()
        self._lock = threading.Lock()
    
    def increment(self):

        if not hasattr(self._local, 'count'):
            self._local.count = 0
        
        current = self._local.count
        time.sleep(0.0001)
        self._local.count = current + 1
    
    def finalize(self):


        pass

def worker(counter, iterations: int):
    """Worker function that increments the counter multiple times"""
    for _ in range(iterations):
        counter.increment()

def run_experiment(counter_class, num_threads: int, iterations_per_thread: int) -> int:
    """
    Run the experiment with specified counter class.
    Returns the final counter value.
    """
    counter = counter_class()
    threads: List[threading.Thread] = []
    

    for _ in range(num_threads):
        thread = threading.Thread(
            target=worker,
            args=(counter, iterations_per_thread)
        )
        threads.append(thread)
        thread.start()
    

    for thread in threads:
        thread.join()
    
    return counter.count

def run_experiment_with_threadpool(counter_class, num_threads: int, iterations_per_thread: int) -> int:
    """
    Alternative implementation using ThreadPoolExecutor (cleaner)
    """
    counter = counter_class()
    
    with ThreadPoolExecutor(max_workers=num_threads) as executor:

        futures = [
            executor.submit(worker, counter, iterations_per_thread)
            for _ in range(num_threads)
        ]
        

        for future in futures:
            future.result()
    
    return counter.count

def test_solution(solution_name: str, counter_class, num_threads: int, iterations_per_thread: int):
    """Test a specific solution"""
    expected_count = num_threads * iterations_per_thread
    
    print(f"\n=== Testing {solution_name} ===")
    print(f"Expected count: {expected_count}")
    

    for trial in range(3):
        final_count = run_experiment(counter_class, num_threads, iterations_per_thread)
        missing_counts = expected_count - final_count
        success = "✅ SUCCESS" if missing_counts == 0 else "❌ FAILED"
        
        print(f"Trial {trial + 1}: {final_count} {success}")
        if missing_counts > 0:
            print(f"  Missing: {missing_counts} ({(missing_counts/expected_count)*100:.2f}%)")

def main():
    num_threads = 4
    iterations_per_thread = 500
    
    print(f"Running experiments with:")
    print(f"- Number of threads: {num_threads}")
    print(f"- Iterations per thread: {iterations_per_thread}")
    print(f"- Total expected: {num_threads * iterations_per_thread}")
    

    solutions = [
        ("Threading Lock", ThreadSafeCounter),
        ("Threading RLock", RLockCounter),
        ("Atomic Operations", AtomicCounter),
    ]
    
    for name, counter_class in solutions:
        test_solution(name, counter_class, num_threads, iterations_per_thread)
    

    print("\n=== Original Broken Version (for comparison) ===")
    
    class BrokenCounter:
        def __init__(self):
            self.count = 0
        
        def increment(self):
            current = self.count
            time.sleep(0.0001)
            self.count = current + 1
    
    test_solution("Broken Counter", BrokenCounter, num_threads, iterations_per_thread)

if __name__ == "__main__":
    main()