import threading
import random
import time
import logging
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import List

                                              
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(threadName)s - %(message)s'
)

@dataclass
class ThreadStatistics:
    """Data class to store thread execution statistics"""
    thread_id: int
    operations_completed: int
    execution_time: float
    atomic_violations_detected: int

class ThreadSafeCounter:
    """
    Thread-safe counter class that eliminates Atomic Violations using proper synchronization.
    All shared state modifications are protected by a lock.
    """
    def __init__(self, initial_value: int = 0):
        self._lock = threading.Lock()
        self._current_value = initial_value
        self._total_increments_attempted = 0
        self._atomic_violation_occurrences = 0
        self._operation_history = []

    def increment(self) -> bool:
        """
        Thread-safe increment operation that prevents Atomic Violations.
        Returns True if an Atomic Violation was simulated (for testing), False otherwise.
        """
        with self._lock:
            self._total_increments_attempted += 1
            
                                        
            temporary_value = self._current_value
            
                                                                     
            atomic_violation_occurred = False
            if random.random() < 0.0001:                
                atomic_violation_occurred = True
                self._atomic_violation_occurrences += 1
            
                                                                      
            if atomic_violation_occurred:
                logging.warning(f"Atomic violation detected! Current value: {temporary_value}")
            
                              
            self._current_value = temporary_value + 1
            
                                      
            self._operation_history.append({
                'thread_name': threading.current_thread().name,
                'old_value': temporary_value,
                'new_value': self._current_value,
                'atomic_violation': atomic_violation_occurred,
                'timestamp': time.time()
            })
            
            return atomic_violation_occurred

    def get_statistics(self) -> dict:
        """Return comprehensive statistics about counter operations"""
        with self._lock:
            return {
                'final_value': self._current_value,
                'total_attempts': self._total_increments_attempted,
                'atomic_violations': self._atomic_violation_occurrences,
                'success_rate': ((self._total_increments_attempted - self._atomic_violation_occurrences) /
                               max(self._total_increments_attempted, 1)) * 100,
                'operation_count': len(self._operation_history)
            }

def worker_thread_function(shared_counter, operations_per_thread: int, thread_identifier: int, 
                          statistics_list: List[ThreadStatistics]) -> None:
    """
    Worker function that performs multiple increment operations on the shared counter.
    """
    start_time = time.time()
    completed_operations = 0
    detected_atomic_violations = 0

    logging.info(f"Worker thread {thread_identifier} starting with {operations_per_thread} operations")

    for _ in range(operations_per_thread):
        try:
            atomic_violation_detected = shared_counter.increment()
            if atomic_violation_detected:
                detected_atomic_violations += 1
            completed_operations += 1
            
                                                           
            if random.random() < 0.001:
                time.sleep(0.001)
        except Exception as e:
            logging.error(f"Error in thread {thread_identifier}: {e}")
            break

    execution_duration = time.time() - start_time

    thread_stats = ThreadStatistics(
        thread_id=thread_identifier,
        operations_completed=completed_operations,
        execution_time=execution_duration,
        atomic_violations_detected=detected_atomic_violations
    )
    
    statistics_list.append(thread_stats)
    
    logging.info(f"Worker thread {thread_identifier} completed. "
                f"Operations: {completed_operations}, "
                f"Time: {execution_duration:.4f}s, "
                f"Atomic Violations: {detected_atomic_violations}")

def run_thread_safe_experiment(number_of_threads: int = 5, operations_per_thread: int = 100) -> None:
    """
    Fixed version demonstrating proper thread synchronization.
    Uses ThreadSafeCounter with locking to eliminate Atomic Violations.
    """
    print("=" * 80)
    print("THREAD-SAFE COUNTER EXPERIMENT (FIXED)")
    print("=" * 80)
    print(f"Configuration:")
    print(f" - Number of threads: {number_of_threads}")
    print(f" - Operations per thread: {operations_per_thread}")
    print(f" - Expected final value: {number_of_threads * operations_per_thread}")
    print()

                                    
    shared_counter = ThreadSafeCounter(initial_value=0)
    thread_statistics = []
    start_time = time.time()

    print("Starting thread-safe experiment...")
    
                              
    threads = []
    for i in range(number_of_threads):
        t = threading.Thread(
            target=worker_thread_function,
            args=(shared_counter, operations_per_thread, i + 1, thread_statistics),
            name=f"SafeWorker-{i+1}"
        )
        threads.append(t)
        t.start()

                         
    for t in threads:
        t.join()

    total_time = time.time() - start_time
    stats = shared_counter.get_statistics()

    print("\n" + "=" * 80)
    print("RESULTS (THREAD-SAFE)")
    print("=" * 80)
    print(f"✅ Final Counter Value: {stats['final_value']}")
    print(f"   Expected Value: {number_of_threads * operations_per_thread}")
    print(f"   Value Difference: {(number_of_threads * operations_per_thread) - stats['final_value']}")
    print(f"   Total Attempts: {stats['total_attempts']}")
    print(f"   Atomic Violations Detected: {stats['atomic_violations']}")
    print(f"   Success Rate: {stats['success_rate']:.2f}%")
    print(f"   Total Time: {total_time:.4f}s")

                                                                

if __name__ == "__main__":
    run_thread_safe_experiment(number_of_threads=5, operations_per_thread=100)
