import multiprocessing
import time
import random
import os
from datetime import datetime

def increment_shared_counter(shared_counter, worker_id, total_increments, delay_range=(0.0001, 0.0005)):
    """
    Worker function that increments a shared counter multiple times.
    This FIXED version uses synchronization to avoid race conditions
    when multiple processes update the same shared counter.
    
    Args:
        shared_counter: multiprocessing.Value object for shared counter
        worker_id: Unique identifier for this worker process
        total_increments: Number of times to increment the counter
        delay_range: Tuple of (min_delay, max_delay) in seconds
    """
    print(f"Worker {worker_id} (PID: {os.getpid()}) starting increment operations...")
    
    successful_increments = 0
    
    for iteration in range(total_increments):

        delay_time = random.uniform(delay_range[0], delay_range[1])
        time.sleep(delay_time)



        with shared_counter.get_lock():
            shared_counter.value += 1

        successful_increments += 1
        

        if (iteration + 1) % 25 == 0:
            print(f"Worker {worker_id}: Completed {iteration + 1}/{total_increments} increments")
    
    print(f"Worker {worker_id} completed all {successful_increments} increment operations")


def demonstrate_race_condition_with_multiple_workers():
    """
    Main function to demonstrate (and now FIX) race conditions using multiple worker processes.
    Each worker process will increment a shared counter, and with proper synchronization,
    the final result should be equal to the expected value.
    """
    print("=" * 70)
    print("RACE CONDITION (FIXED) DEMONSTRATION")
    print("=" * 70)
    print(f"Demonstration started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    

    number_of_workers = 3
    increments_per_worker = 150
    expected_final_value = number_of_workers * increments_per_worker
    
    print("Configuration:")
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
    loss_percentage = (lost_increments / expected_final_value) * 100 if expected_final_value != 0 else 0.0
    
    print(f"Expected final counter value: {expected_final_value}")
    print(f"Actual final counter value:   {actual_final_value}")
    print(f"Lost increments:             {lost_increments}")
    print(f"Data loss percentage:        {loss_percentage:.2f}%")
    print(f"Total execution time:        {execution_time:.3f} seconds")
    
    if actual_final_value < expected_final_value:
        print("\n⚠️  Something is still wrong: the actual value is less than expected.")
        print("Check that all increments are guarded by the same lock.")
    else:
        print("\n✅ No race condition observed.")
        print("The shared counter reached the expected final value thanks to proper synchronization.")
    
    print("\nExplanation:")
    print("1. Previously, each process read the counter, slept, then wrote back the incremented value.")
    print("2. Without a lock, multiple processes could overwrite each other's updates (race condition).")
    print("3. Now, the increment (read-modify-write) is wrapped in shared_counter.get_lock(),")
    print("   ensuring only one process updates the shared value at a time.")


def run_multiple_demonstrations(num_runs=3):
    """
    Run the demonstration multiple times to verify that
    the synchronized version consistently produces the expected result.
    """
    print("MULTIPLE RACE CONDITION (FIXED) DEMONSTRATIONS")
    print("=" * 70)
    print(f"Running {num_runs} separate demonstrations...\n")
    
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
        
        print(f"Run {run_number} Result: {final_value}/{expected_value} (Lost: {expected_value - final_value})")
    
    print(f"\n{'=' * 70}")
    print("SUMMARY OF ALL RUNS")
    print(f"{'=' * 70}")
    print(f"Results across {num_runs} runs: {results}")
    avg_final = sum(results) / len(results) if results else 0
    print(f"Average final value: {avg_final:.1f}")
    print(f"Expected value:      200")
    print(f"Average lost increments: {200 - avg_final:.1f}")


if __name__ == "__main__":

    demonstrate_race_condition_with_multiple_workers()
    
    print("\n" + "=" * 70)
    print("To run multiple fixed demonstrations, uncomment the line below:")
    print("=" * 70)
    


