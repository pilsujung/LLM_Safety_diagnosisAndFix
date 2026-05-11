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
    atomic_violation_detected: int

class SharedCounter:
    """
    Thread-safe counter class that prevents Atomic Violations using locks.
    """
    def __init__(self, initial_value: int = 0):
        self.current_value = initial_value
        self.total_increments_attempted = 0
        self.atomic_violation_occurrences = 0
        self.operation_history = []
        self._lock = threading.Lock()                                

    def increment_unsafe(self) -> bool:
        """
        Now thread-safe increment operation using lock protection.
        """
        with self._lock:                              
            self.total_increments_attempted += 1
            temporary_value = self.current_value

                                                                
            atomic_violation_occurred = False
            if random.random() < 0.0001:
                atomic_violation_occurred = True
                self.atomic_violation_occurrences += 1
                logging.warning(f"Atomic Violation detected! Current value: {temporary_value}")

                                                                                        
            time.sleep(0.01)

                              
            self.current_value = temporary_value + 1

                              
            self.operation_history.append({
                'thread_name': threading.current_thread().name,
                'old_value': temporary_value,
                'new_value': self.current_value,
                'atomic_violation': atomic_violation_occurred,
                'timestamp': time.time()
            })

        return atomic_violation_occurred

    def get_statistics(self) -> dict:
        with self._lock:
            return {
                'final_value': self.current_value,
                'total_attempts': self.total_increments_attempted,
                'atomic_violations': self.atomic_violation_occurrences,
                'success_rate': ((self.total_increments_attempted - self.atomic_violation_occurrences) /
                                max(self.total_increments_attempted, 1)) * 100,
                'operation_count': len(self.operation_history)
            }

                                                                            

def worker_thread_function(shared_counter: SharedCounter,
                          operations_per_thread: int,
                          thread_identifier: int,
                          statistics_list: List[ThreadStatistics]) -> None:
    start_time = time.time()
    completed_operations = 0
    detected_atomic_violations = 0

    logging.info(f"Worker thread {thread_identifier} starting with {operations_per_thread} operations")

    for operation_index in range(operations_per_thread):
        try:
            atomic_violation_detected = shared_counter.increment_unsafe()
            if atomic_violation_detected:
                detected_atomic_violations += 1
            completed_operations += 1

            if random.random() < 0.001:
                time.sleep(0.001)

        except Exception as exception_info:
            logging.error(f"Error in thread {thread_identifier}: {exception_info}")
            break

    execution_duration = time.time() - start_time

    thread_stats = ThreadStatistics(
        thread_id=thread_identifier,
        operations_completed=completed_operations,
        execution_time=execution_duration,
        atomic_violation_detected=detected_atomic_violations
    )
    statistics_list.append(thread_stats)

    logging.info(f"Worker thread {thread_identifier} completed. "
                 f"Operations: {completed_operations}, "
                 f"Time: {execution_duration:.4f}s, "
                 f"Atomic Violations: {detected_atomic_violations}")

                                                                                                  
                                                                                  

                                                                                 
                                                                

if __name__ == "__main__":
    THREAD_COUNT = 25
    OPERATIONS_PER_THREAD = 1000

    try:
        run_multithreaded_counter_experiment(THREAD_COUNT, OPERATIONS_PER_THREAD)
        run_experiment_with_executor(THREAD_COUNT, OPERATIONS_PER_THREAD)
    except KeyboardInterrupt:
        print("\nExperiment interrupted by user.")
    except Exception as error:
        print(f"An error occurred: {error}")
        logging.error(f"Experiment failed: {error}")
