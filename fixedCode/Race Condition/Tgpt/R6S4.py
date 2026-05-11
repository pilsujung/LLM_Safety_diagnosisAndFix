import multiprocessing
import time
import random
import os
from datetime import datetime

def increment_shared_counter(shared_counter, worker_id, total_increments, delay_range=(0.0001, 0.0005)):
    """
    Worker function that increments a shared counter multiple times in a
    thread-safe/process-safe manner using the lock associated with
    multiprocessing.Value.
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
    Main function; now shows correct, race-free behavior because increments
    are synchronized.
    """
    print("=" * 70)
    print("RACE-FREE SHARED COUNTER DEMONSTRATION")
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



    global_shared_counter = multiprocessing.Value('i', 0, lock=True)

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
        print("\n✅ No race condition: all increments were applied correctly.")
    else:
        print("\n⚠️ Unexpected mismatch: check synchronization logic.")

if __name__ == "__main__":
    demonstrate_race_condition_with_multiple_workers()
