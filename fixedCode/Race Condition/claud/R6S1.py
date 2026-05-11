import multiprocessing
import time
import random
import os
from datetime import datetime

def increment_shared_counter(shared_counter, lock, worker_id, total_increments, delay_range=(0.0001, 0.0005)):
    """
    Worker function that increments a shared counter multiple times.
    Now uses a lock to prevent race conditions.
    
    Args:
        shared_counter: multiprocessing.Value object for shared counter
        lock: multiprocessing.Lock for synchronization
        worker_id: Unique identifier for this worker process
        total_increments: Number of times to increment the counter
        delay_range: Tuple of (min_delay, max_delay) in seconds
    """
    print(f"Worker {worker_id} (PID: {os.getpid()}) starting increment operations...")
    
    successful_increments = 0
    
    for iteration in range(total_increments):

        with lock:

            current_value = shared_counter.value
            

            delay_time = random.uniform(delay_range[0], delay_range[1])
            time.sleep(delay_time)
            

            new_value = current_value + 1
            


            shared_counter.value = new_value

        
        successful_increments += 1
        

        if (iteration + 1) % 25 == 0:
            print(f"Worker {worker_id}: Completed {iteration + 1}/{total_increments} increments")
    
    print(f"Worker {worker_id} completed all {successful_increments} increment operations")

def demonstrate_race_condition_with_multiple_workers():
    """
    Main function demonstrating proper synchronization using locks.
    Each worker process will increment a shared counter with lock protection,
    ensuring the final result matches the expected value.
    """
    print("=" * 70)
    print("SYNCHRONIZED COUNTER DEMONSTRATION (RACE CONDITION FIXED)")
    print("=" * 70)
    print(f"Demonstration started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    

    number_of_workers = 3
    increments_per_worker = 150
    expected_final_value = number_of_workers * increments_per_worker
    
    print(f"Configuration:")
    print(f"  - Number of worker processes: {number_of_workers}")
    print(f"  - Increments per worker: {increments_per_worker}")
    print(f"  - Expected final counter value: {expected_final_value}")
    print()
    


    global_shared_counter = multiprocessing.Value('i', 0)
    

    counter_lock = multiprocessing.Lock()
    
    print("Creating worker processes...")
    worker_processes = []
    

    for worker_num in range(number_of_workers):
        worker_process = multiprocessing.Process(
            target=increment_shared_counter,
            args=(global_shared_counter, counter_lock, worker_num + 1, increments_per_worker),
            name=f"Worker-{worker_num + 1}"
        )
        worker_processes.append(worker_process)
        print(f"  Created {worker_process.name}")
    
    print("\nStarting all worker processes simultaneously...")
    start_time = time.time()
    

    for process in worker_processes:
        process.start()
        print(f"  Started {process.name} (PID: {process.pid})")
    
    print("\nWaiting for all worker processes to complete...")
    

    for process in worker_processes:
        process.join()
        print(f"  {process.name} finished")
    
    end_time = time.time()
    execution_time = end_time - start_time
    

    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)
    
    actual_final_value = global_shared_counter.value
    lost_increments = expected_final_value - actual_final_value
    loss_percentage = (lost_increments / expected_final_value) * 100 if expected_final_value > 0 else 0
    
    print(f"Expected final counter value: {expected_final_value}")
    print(f"Actual final counter value:   {actual_final_value}")
    print(f"Lost increments:              {lost_increments}")
    print(f"Data loss percentage:         {loss_percentage:.2f}%")
    print(f"Total execution time:         {execution_time:.3f} seconds")
    
    if actual_final_value < expected_final_value:
        print("\n⚠️  UNEXPECTED: Some increments were lost!")
        print("This should not happen with proper lock synchronization.")
    else:
        print("\n✅ SUCCESS: No race condition!")
        print("The lock ensured all increments were properly synchronized.")
    
    print("\nHow the fix works:")
    print("1. A multiprocessing.Lock() is created and shared among all processes")
    print("2. Each process acquires the lock before reading/writing shared memory")
    print("3. While one process holds the lock, others wait")
    print("4. The lock is released after the write, allowing the next process to proceed")
    print("5. This ensures atomic read-modify-write operations")

def run_multiple_demonstrations(num_runs=3):
    """
    Run the synchronized demonstration multiple times to verify
    that the lock consistently prevents race conditions.
    """
    print("MULTIPLE SYNCHRONIZED DEMONSTRATIONS")
    print("=" * 70)
    print(f"Running {num_runs} separate demonstrations to verify consistency...\n")
    
    results = []
    
    for run_number in range(1, num_runs + 1):
        print(f"\n{'*' * 30} RUN {run_number} {'*' * 30}")
        

        test_counter = multiprocessing.Value('i', 0)
        test_lock = multiprocessing.Lock()
        processes = []
        

        for i in range(2):
            p = multiprocessing.Process(
                target=increment_shared_counter,
                args=(test_counter, test_lock, i + 1, 100, (0.0001, 0.0003))
            )
            processes.append(p)
            p.start()
        

        for p in processes:
            p.join()
        
        final_value = test_counter.value
        expected_value = 200
        results.append(final_value)
        
        print(f"Run {run_number} Result: {final_value}/200 (Lost: {expected_value - final_value})")
    
    print(f"\n{'=' * 70}")
    print("SUMMARY OF ALL RUNS")
    print(f"{'=' * 70}")
    print(f"Results across {num_runs} runs: {results}")
    print(f"Average final value: {sum(results) / len(results):.1f}")
    print(f"Expected value: 200")
    print(f"Average lost increments: {200 - (sum(results) / len(results)):.1f}")
    
    if all(r == 200 for r in results):
        print("\n✅ Perfect! All runs achieved the expected value.")
    else:
        print("\n⚠️  Some runs had lost increments - this shouldn't happen with locks.")

if __name__ == "__main__":

    demonstrate_race_condition_with_multiple_workers()
    
    print("\n" + "=" * 70)
    print("Running multiple demonstrations to verify consistency...")
    print("=" * 70)
    

    run_multiple_demonstrations(5)