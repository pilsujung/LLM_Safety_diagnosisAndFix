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
    Atomic_Violation_detected: int

class SharedCounter:
    """
    A counter class that demonstrates Atomic Violation issues in multi-threaded environments.
    This fixed implementation uses proper synchronization to avoid real atomicity violations.
    """
    
    def __init__(self, initial_value: int = 0):
        self.current_value = initial_value
        self.total_increments_attempted = 0
        self.Atomic_Violation_occurrences = 0
        self.operation_history = []

                                                     
        self._lock = threading.Lock()
        
    def increment_unsafe(self) -> bool:
        """
        Thread-safe increment operation (previously unsafe).
        Returns True if an Atomic Violation was *simulated*, False otherwise.
        """

                                                                
        with self._lock:
            self.total_increments_attempted += 1
            
                                                                   
            temporary_value = self.current_value
            
                                                                 
            Atomic_Violation_occurred = False
            if random.random() < 0.0001:                                              
                Atomic_Violation_occurred = True
                self.Atomic_Violation_occurrences += 1
                
                                                               
                logging.warning(f"Atomic Violation detected! Current value: {temporary_value}")
                
                                                                                   
                time.sleep(0.01)
                
                                                        
            self.current_value = temporary_value + 1
            
                                                                  
            self.operation_history.append({
                'thread_name': threading.current_thread().name,
                'old_value': temporary_value,
                'new_value': self.current_value,
                'Atomic_Violation': Atomic_Violation_occurred,
                'timestamp': time.time()
            })
            
            return Atomic_Violation_occurred
    
    def get_statistics(self) -> dict:
        """Return comprehensive statistics about counter operations in a consistent way"""
                                                                                   
        with self._lock:
            final_value = self.current_value
            total_attempts = self.total_increments_attempted
            atomic_violations = self.Atomic_Violation_occurrences
            operation_count = len(self.operation_history)
        
        return {
            'final_value': final_value,
            'total_attempts': total_attempts,
            'Atomic_Violation': atomic_violations,
            'success_rate': ((total_attempts - atomic_violations) / 
                           max(total_attempts, 1)) * 100,
            'operation_count': operation_count
        }

def worker_thread_function(shared_counter: SharedCounter, 
                          operations_per_thread: int, 
                          thread_identifier: int,
                          statistics_list: List[ThreadStatistics]) -> None:
    """
    Worker function that each thread will execute.
    Performs multiple increment operations on the shared counter.
    """
    start_time = time.time()
    completed_operations = 0
    detected_Atomic_Violation = 0
    
    logging.info(f"Worker thread {thread_identifier} starting with {operations_per_thread} operations")
    
    for operation_index in range(operations_per_thread):
        try:
                                                                
            Atomic_Violation_detected = shared_counter.increment_unsafe()
            
            if Atomic_Violation_detected:
                detected_Atomic_Violation += 1
                
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
        Atomic_Violation_detected=detected_Atomic_Violation
    )
    
    statistics_list.append(thread_stats)
    
    logging.info(f"Worker thread {thread_identifier} completed. "
                f"Operations: {completed_operations}, "
                f"Time: {execution_duration:.4f}s, "
                f"Atomic Violations: {detected_Atomic_Violation}")

def run_multithreaded_counter_experiment(number_of_threads: int = 20, 
                                       operations_per_thread: int = 500) -> None:
    """
    Main function to run the multi-threaded counter experiment.
    Creates multiple threads that increment a shared counter concurrently.
    """
    print("=" * 80)
    print("MULTI-THREADED COUNTER SYNCHRONIZATION EXPERIMENT")
    print("=" * 80)
    print(f"Configuration:")
    print(f"  - Number of threads: {number_of_threads}")
    print(f"  - Operations per thread: {operations_per_thread}")
    print(f"  - Expected final value: {number_of_threads * operations_per_thread}")
    print(f"  - Atomic Violation simulation probability: 0.01%")
    print()
    
                                 
    shared_counter_instance = SharedCounter(initial_value=0)
    thread_statistics_collection = []
    experiment_start_time = time.time()
    
                                                    
    print("Starting experiment using traditional threading...")
    
                                   
    worker_thread_list = []
    for thread_index in range(number_of_threads):
        worker_thread = threading.Thread(
            target=worker_thread_function,
            args=(shared_counter_instance, operations_per_thread, 
                  thread_index + 1, thread_statistics_collection),
            name=f"WorkerThread-{thread_index + 1}"
        )
        worker_thread_list.append(worker_thread)
    
                              
    print(f"Launching {number_of_threads} worker threads...")
    for worker_thread in worker_thread_list:
        worker_thread.start()
    
                                      
    print("Waiting for all threads to complete...")
    for worker_thread in worker_thread_list:
        worker_thread.join()
    
    total_experiment_time = time.time() - experiment_start_time
    
                                 
    print("\n" + "=" * 80)
    print("EXPERIMENT RESULTS")
    print("=" * 80)
    
    counter_statistics = shared_counter_instance.get_statistics()
    
    expected_value = number_of_threads * operations_per_thread
    print(f"Final Counter Value: {counter_statistics['final_value']}")
    print(f"Expected Value: {expected_value}")
    print(f"Value Difference: {expected_value - counter_statistics['final_value']}")
    print(f"Total Increment Attempts: {counter_statistics['total_attempts']}")
    print(f"Atomic Violations Detected (simulated): {counter_statistics['Atomic_Violation']}")
    print(f"Success Rate: {counter_statistics['success_rate']:.2f}%")
    print(f"Total Execution Time: {total_experiment_time:.4f} seconds")
    print()
    
                                          
    print("Individual Thread Performance:")
    print("-" * 80)
    print(f"{'Thread ID':<12} {'Operations':<12} {'Time (s)':<12} {'Atomic Violations':<15}")
    print("-" * 80)
    
    total_operations_completed = 0
    total_Atomic_Violation_found = 0
    
    for thread_stat in sorted(thread_statistics_collection, key=lambda x: x.thread_id):
        print(f"{thread_stat.thread_id:<12} "
              f"{thread_stat.operations_completed:<12} "
              f"{thread_stat.execution_time:<12.4f} "
              f"{thread_stat.Atomic_Violation_detected:<15}")
        
        total_operations_completed += thread_stat.operations_completed
        total_Atomic_Violation_found += thread_stat.Atomic_Violation_detected
    
    print("-" * 80)
    print(f"{'TOTAL':<12} {total_operations_completed:<12} "
          f"{'N/A':<12} {total_Atomic_Violation_found:<15}")
    
                                  
    print("\n" + "=" * 80)
    print("ANALYSIS AND RECOMMENDATIONS")
    print("=" * 80)
    
    value_discrepancy = expected_value - counter_statistics['final_value']
    
    if value_discrepancy == 0:
        print("✅ SUCCESS: Final counter value matches expected value!")
        print("   No real synchronization issues were detected in this run.")
    else:
        print(f"❌ ISSUE DETECTED: Counter value is off by {value_discrepancy}")
        print("   This indicates atomicity violations occurred during execution.")
    
    print("\nTo fix synchronization issues, this version:")
    print("1. Uses threading.Lock() to protect the entire critical section")
    print("2. Ensures read-modify-write on the shared counter is atomic")
    print("3. Keeps the simulated 'Atomic Violation' flag for experimentation\n")

                                                     
def run_experiment_with_executor(number_of_threads: int = 20, 
                               operations_per_thread: int = 500) -> None:
    """
    Alternative implementation using ThreadPoolExecutor for comparison.
    """
    print("\n" + "=" * 80)
    print("ALTERNATIVE IMPLEMENTATION USING THREADPOOLEXECUTOR")
    print("=" * 80)
    
    shared_counter_instance = SharedCounter(initial_value=0)
    thread_statistics_collection = []
    
    def executor_worker_function(thread_id: int) -> ThreadStatistics:
        """Worker function for ThreadPoolExecutor"""
        start_time = time.time()
        completed_operations = 0
        detected_Atomic_Violation = 0
        
        for _ in range(operations_per_thread):
            Atomic_Violation_detected = shared_counter_instance.increment_unsafe()
            if Atomic_Violation_detected:
                detected_Atomic_Violation += 1
            completed_operations += 1
        
        execution_duration = time.time() - start_time
        
        return ThreadStatistics(
            thread_id=thread_id,
            operations_completed=completed_operations,
            execution_time=execution_duration,
            Atomic_Violation_detected=detected_Atomic_Violation
        )
    
    start_time = time.time()
    
                                                          
    with ThreadPoolExecutor(max_workers=number_of_threads, 
                          thread_name_prefix="ExecutorWorker") as thread_executor:
        
                          
        future_tasks = [
            thread_executor.submit(executor_worker_function, thread_id + 1) 
            for thread_id in range(number_of_threads)
        ]
        
                         
        for future_task in future_tasks:
            thread_statistics_collection.append(future_task.result())
    
    total_time = time.time() - start_time
    counter_stats = shared_counter_instance.get_statistics()
    
    expected_value = number_of_threads * operations_per_thread
    print(f"ThreadPoolExecutor Results:")
    print(f"Final Value: {counter_stats['final_value']}")
    print(f"Expected: {expected_value}")
    print(f"Execution Time: {total_time:.4f} seconds")
    print(f"Atomic Violations (simulated): {counter_stats['Atomic_Violation']}")

if __name__ == "__main__":
                                                                               
    THREAD_COUNT = 25
    OPERATIONS_PER_THREAD = 1000
    
    try:
                                 
        run_multithreaded_counter_experiment(
            number_of_threads=THREAD_COUNT,
            operations_per_thread=OPERATIONS_PER_THREAD
        )
        
                                                       
        run_experiment_with_executor(
            number_of_threads=THREAD_COUNT,
            operations_per_thread=OPERATIONS_PER_THREAD
        )
        
    except KeyboardInterrupt:
        print("\nExperiment interrupted by user.")
    except Exception as error:
        print(f"An error occurred during execution: {error}")
        logging.error(f"Experiment failed: {error}")
    
    print("\nExperiment completed.")
