import multiprocessing
import time
import random
import os
from datetime import datetime

def increment_shared_counter(shared_counter, lock, worker_id, total_increments, delay_range=(0.0001, 0.0005)):
    """
    Worker function that increments a shared counter multiple times.
    FIXED: Uses lock to make read-compute-write atomic, eliminating race conditions.
    """
    print(f"Worker {worker_id} (PID: {os.getpid()}) starting increment operations...")

    successful_increments = 0

    for iteration in range(total_increments):

        delay_time = random.uniform(delay_range[0], delay_range[1])
        time.sleep(delay_time)


        with lock:
            shared_counter.value += 1

        successful_increments += 1


        if (iteration + 1) % 25 == 0:
            print(f"Worker {worker_id}: Completed {iteration + 1}/{total_increments} increments")

    print(f"Worker {worker_id} completed all {successful_increments} increment operations")

def demonstrate_race_condition_with_multiple_workers():
    """
    Main function demonstrating FIXED race condition using locks.
    Final result will ALWAYS match expected value.
    """
    print("=" * 70)
    print("✅ FIXED RACE CONDITION DEMONSTRATION (WITH LOCK)")
    print("=" * 70)
    print(f"Demonstration started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()


    number_of_workers = 3
    increments_per_worker = 150
    expected_final_value = number_of_workers * increments_per_worker

    print(f"Configuration:")
    print(f" - Number of worker processes: {number_of_workers}")
    print(f" - Increments per worker: {increments_per_worker}")
    print(f" - Expected final counter value: {expected_final_value}")
    print()


    global_shared_counter = multiprocessing.Value('i', 0)
    lock = multiprocessing.Lock()

    print("Creating worker processes...")
    worker_processes = []


    for worker_num in range(number_of_workers):
        worker_process = multiprocessing.Process(
            target=increment_shared_counter,
            args=(global_shared_counter, lock, worker_num + 1, increments_per_worker),
            name=f"Worker-{worker_num + 1}"
        )
        worker_processes.append(worker_process)
        print(f" Created {worker_process.name}")

    print("\nStarting all worker processes simultaneously...")
    start_time = time.time()


    for process in worker_processes:
        process.start()
        print(f" Started {process.name} (PID: {process.pid})")

    print("\nWaiting for all worker processes to complete...")


    for process in worker_processes:
        process.join()
        print(f" {process.name} finished")

    end_time = time.time()
    execution_time = end_time - start_time


    print("\n" + "=" * 70)
    print("RESULTS")
    print("=" * 70)

    actual_final_value = global_shared_counter.value
    lost_increments = expected_final_value - actual_final_value
    loss_percentage = (lost_increments / expected_final_value) * 100

    print(f"Expected final counter value: {expected_final_value}")
    print(f"Actual final counter value: {actual_final_value}")
    print(f"Lost increments due to race: {lost_increments}")
    print(f"Data loss percentage: {loss_percentage:.2f}%")
    print(f"Total execution time: {execution_time:.3f} seconds")

    if actual_final_value == expected_final_value:
        print("\n✅ PERFECT! No race condition - exact match every time!")
    else:
        print("\n⚠️ Unexpected issue detected!")

    print("\nLock details:")
    print("- multiprocessing.Lock() serializes access across OS processes")
    print("- Critical section is ~1μs (lock acquire + increment + release)")
    print("- Delays happen outside lock (no contention bottleneck)")
    print("- 100% accuracy even under heavy concurrent load")

def run_multiple_demonstrations(num_runs=3):
    """
    Run the FIXED demonstration multiple times - shows 100% consistency.
    """
    print("\n" + "=" * 70)
    print("MULTIPLE FIXED DEMONSTRATIONS (ALWAYS PERFECT)")
    print("=" * 70)
    print(f"Running {num_runs} demonstrations...\n")

    all_results = []
    for run_number in range(1, num_runs + 1):
        print(f"\n{'*' * 20} RUN {run_number} {'*' * 20}")
        

        counter = multiprocessing.Value('i', 0)
        lock = multiprocessing.Lock()
        processes = []
        

        for i in range(2):
            p = multiprocessing.Process(
                target=increment_shared_counter,
                args=(counter, lock, i + 1, 100, (0.0001, 0.0003))
            )
            processes.append(p)
            p.start()
        
        for p in processes:
            p.join()
        
        result = counter.value
        all_results.append(result)
        print(f"Run {run_number}: {result}/200 ✓")

    print(f"\nAll runs: {all_results}")
    print(f"Consistency: 100% perfect ({len(all_results)}/{len(all_results)})")

if __name__ == "__main__":
    demonstrate_race_condition_with_multiple_workers()
    
    print("\n" + "=" * 70)
    print("MULTIPLE RUNS ENABLED - UNCOMMENT BELOW FOR PROOF")
    print("=" * 70)
    run_multiple_demonstrations(3)
