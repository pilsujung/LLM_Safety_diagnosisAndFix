import multiprocessing
import time
import random
import os
from datetime import datetime

def increment_shared_counter(shared_counter, worker_id, total_increments, delay_range=(0.0001, 0.0005)):
    """
    Worker function that increments a shared counter multiple times.
    This function deliberately introduces race conditions by separating
    read and write operations with a delay.
    
    Args:
        shared_counter: multiprocessing.Value object for shared counter
        worker_id: Unique identifier for this worker process
        total_increments: Number of times to increment the counter
        delay_range: Tuple of (min_delay, max_delay) in seconds
    """
    print(f"Worker {worker_id} (PID: {os.getpid()}) starting increment operations...")
    
    successful_increments = 0
    
    for iteration in range(total_increments):

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
    Main function to demonstrate race conditions using multiple worker processes.
    Each worker process will increment a shared counter, but due to the race condition,
    the final result will likely be less than expected.
    """
    print("=" * 70)
    print("RACE CONDITION DEMONSTRATION")
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
    
    print("Creating worker processes...")
    worker_processes = []

    for worker_num in range(number_of_workers):
        worker_process = multiprocessing.Process(
            target=increment_shared_counter,
            args=(global_shared_counter, worker_num + 1, increments_per_worker),
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
    loss_percentage = (lost_increments / expected_final_value) * 100
    
    print(f"Expected final counter value: {expected_final_value}")
    print(f"Actual final counter value:   {actual_final_value}")
    print(f"Lost increments due to race:  {lost_increments}")
    print(f"Data loss percentage:         {loss_percentage:.2f}%")
    print(f"Total execution time:         {execution_time:.3f} seconds")
    
    if actual_final_value < expected_final_value:
        print("\n⚠️  RACE CONDITION DETECTED!")
        print("The actual value is less than expected due to concurrent access")
        print("to shared memory without proper synchronization.")
    else:
        print("\n✅ No race condition detected this time.")
        print("Try running the program again - race conditions are unpredictable!")
    
    print("\nExplanation of the race condition:")
    print("1. Multiple processes read the same counter value simultaneously")
    print("2. Each process increments its local copy of the value")
    print("3. Each process writes back the incremented value")
    print("4. Later writes overwrite earlier writes, causing lost increments")
    print("\nSolution: Use locks, semaphores, or atomic operations for synchronization!")

def run_multiple_demonstrations(num_runs=3):
    """
    Run the race condition demonstration multiple times to show
    the unpredictable nature of race conditions.
    """
    print("MULTIPLE RACE CONDITION DEMONSTRATIONS")
    print("=" * 70)
    print(f"Running {num_runs} separate demonstrations to show variability...\n")
    
    results = []
    
    for run_number in range(1, num_runs + 1):
        print(f"\n{'*' * 30} RUN {run_number} {'*' * 30}")

        test_counter = multiprocessing.Value('i', 0)
        processes = []

        for i in range(2):
            p = multiprocessing.Process(
                target=increment_shared_counter,
                args=(test_counter, i + 1, 100, (0.0001, 0.0003))
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

if __name__ == "__main__":

    demonstrate_race_condition_with_multiple_workers()
    
    print("\n" + "=" * 70)
    print("Would you like to see multiple runs? (Uncomment the line below)")
    print("=" * 70)
