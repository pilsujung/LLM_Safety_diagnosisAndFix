import multiprocessing
import time
import random
import os
from datetime import datetime

def increment_shared_counter(shared_counter, worker_id, total_increments, delay_range=(0.0001, 0.0005)):
    """
    Worker function that increments a shared counter multiple times.
    This fixed version uses a lock to prevent race conditions when
    accessing shared_counter.
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
    Main function to demonstrate CORRECT synchronized behavior using multiple worker processes.
    Each worker process will increment a shared counter, and thanks to the lock,
    the final result will match the expected value.
    """
    print("=" * 70)
    print("RACE CONDITION FIXED DEMONSTRATION")
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
    loss_percentage = (lost_increments / expected_final_value) * 100 if expected_final_value else 0.0

    print(f"Expected final counter value: {expected_final_value}")
    print(f"Actual final counter value:   {actual_final_value}")
    print(f"Lost increments:              {lost_increments}")
    print(f"Data loss percentage:         {loss_percentage:.2f}%")
    print(f"Total execution time:         {execution_time:.3f} seconds")

    if actual_final_value == expected_final_value:
        print("\n✅ Race condition eliminated.")
        print("All increments have been applied correctly thanks to synchronization.")
    else:

        print("\n⚠️ Unexpected mismatch detected.")
        print("Re-check synchronization logic if this persists.")


def run_multiple_demonstrations(num_runs=3):
    """
    Run the demonstration multiple times to show
    that the final result is now stable (no race condition).
    """
    print("MULTIPLE SYNCHRONIZED INCREMENT DEMONSTRATIONS")
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
    print(f"Average final value: {sum(results) / len(results):.1f}")
    print(f"Expected value: {expected_value}")
    print(f"Average lost increments: {expected_value - (sum(results) / len(results)):.1f}")


if __name__ == "__main__":

    demonstrate_race_condition_with_multiple_workers()

    print("\n" + "=" * 70)
    print("You can also run multiple demonstrations:")
    print("=" * 70)



