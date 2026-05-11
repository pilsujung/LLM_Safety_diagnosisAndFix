import multiprocessing
import time
import random
import os
from datetime import datetime

def increment_shared_counter(shared_counter, worker_id, total_increments, delay_range=(0.0001, 0.0005)):
    """
    Worker function that increments a shared counter multiple times.
    FIXED: Now uses shared_counter.get_lock() to prevent race conditions.
    
    Args:
        shared_counter: multiprocessing.Value object for shared counter
        worker_id: Unique identifier for this worker process
        total_increments: Number of times to increment the counter
        delay_range: Tuple of (min_delay, max_delay) in seconds
    """
    print(f"Worker {worker_id} (PID: {os.getpid()}) starting increment operations...")

    successful_increments = 0

    for iteration in range(total_increments):

        with shared_counter.get_lock():

            shared_counter.value += 1
            successful_increments += 1


    if total_increments >= 25:
        print(f"Worker {worker_id}: Completed {total_increments}/{total_increments} increments")

    print(f"Worker {worker_id} completed all {successful_increments} increment operations")

def demonstrate_race_condition_fixed():
    """
    Main function demonstrating the FIXED race condition using Value.get_lock().
    Now the final result will ALWAYS match the expected value.
    """
    print("=" * 70)
    print("RACE CONDITION FIXED DEMONSTRATION")
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

    print("Creating worker processes...")
    worker_processes = []


    for worker_num in range(number_of_workers):
        worker_process = multiprocessing.Process(
            target=increment_shared_counter,
            args=(global_shared_counter, worker_num + 1, increments_per_worker),
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
    print("RESULTS (FIXED VERSION)")
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
        print("\n✅ RACE CONDITION FIXED!")
        print("The actual value matches expected thanks to proper synchronization.")
        print("Every run will now produce the correct result!")
    else:
        print("\n⚠️ Unexpected result - something went wrong!")

    print("\nKey Fix Applied:")
    print("• Used `with shared_counter.get_lock():` for atomic increments")
    print("• Eliminated read-delay-write pattern causing race conditions")
    print("• Built-in RLock ensures mutual exclusion across processes")[web:6]

def run_multiple_fixed_demonstrations(num_runs=5):
    """
    Run the fixed demonstration multiple times to show consistent correct results.
    """
    print("\n" + "=" * 70)
    print("MULTIPLE FIXED DEMONSTRATIONS")
    print("=" * 70)
    print(f"Running {num_runs} demonstrations - results should be CONSISTENTLY correct...\n")

    results = []
    expected_value = 200

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
        results.append(final_value)

        print(f"Run {run_number} Result: {final_value}/{expected_value} (Lost: {expected_value - final_value})")

    print(f"\n{'=' * 70}")
    print("SUMMARY OF ALL FIXED RUNS")
    print(f"{'=' * 70}")
    print(f"Results across {num_runs} runs: {results}")
    print(f"Average final value: {sum(results) / len(results):.1f}")
    print(f"Expected value: {expected_value}")
    print(f"Average lost increments: {expected_value - (sum(results) / len(results)):.1f}")
    print("✅ All runs should show 0 lost increments!")

if __name__ == "__main__":

    demonstrate_race_condition_fixed()
    

    run_multiple_fixed_demonstrations(5)
