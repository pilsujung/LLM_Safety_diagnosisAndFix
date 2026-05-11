import multiprocessing
import os
import time
import random
import sys
from datetime import datetime


def increment_file_counter(counter_filename, total_iterations, process_identifier,
                           enable_logging=False, file_lock=None):
    """
    Function that increments a counter stored in a file.
    This version uses a multiprocessing.Lock to avoid race conditions.
    """
    if file_lock is None:

        raise ValueError("file_lock must be provided to increment_file_counter")

    successful_increments = 0
    failed_operations = 0

    for iteration_number in range(total_iterations):
        try:

            time.sleep(random.uniform(0.001, 0.005))

            with file_lock:



                current_counter_value = 0
                if os.path.exists(counter_filename):
                    with open(counter_filename, 'r') as counter_file:
                        try:
                            file_content = counter_file.read().strip()
                            if file_content:
                                current_counter_value = int(file_content)
                            else:
                                current_counter_value = 0
                        except ValueError as value_error:
                            if enable_logging:
                                print(f"[{process_identifier}] Error reading counter: {value_error}")
                            current_counter_value = 0
                            failed_operations += 1


                new_counter_value = current_counter_value + 1


                time.sleep(random.uniform(0.001, 0.003))


                with open(counter_filename, 'w') as counter_file:
                    counter_file.write(str(new_counter_value))



            successful_increments += 1


            if enable_logging and iteration_number % 10 == 0:
                timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                print(
                    f"[{timestamp}] {process_identifier}: Iteration {iteration_number + 1}, "
                    f"Read: {current_counter_value}, Wrote: {new_counter_value}"
                )

        except Exception as general_error:
            failed_operations += 1
            if enable_logging:
                print(f"[{process_identifier}] Unexpected error in iteration {iteration_number + 1}: {general_error}")

    if enable_logging:
        print(f"[{process_identifier}] Completed: {successful_increments} successful, {failed_operations} failed")

    return successful_increments, failed_operations


def initialize_counter_file(filename, initial_value=0):
    """
    Initialize or reset the counter file with a given value.
    """
    try:
        with open(filename, 'w') as file_handle:
            file_handle.write(str(initial_value))
        return True
    except Exception as error:
        print(f"Error initializing counter file: {error}")
        return False


def read_final_counter_value(filename):
    """
    Read the final counter value from the file.
    """
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as file_handle:
                content = file_handle.read().strip()
                return int(content) if content else 0
        else:
            return 0
    except (ValueError, IOError) as error:
        print(f"Error reading final counter value: {error}")
        return 0


def run_race_condition_demonstration():
    """
    Main function to demonstrate (and now fix) the race condition with file-based counter.
    """

    counter_filename = "shared_counter.txt"
    iterations_per_process = 100
    number_of_processes = 3
    enable_debug_logging = False

    print("=" * 60)
    print("RACE CONDITION DEMONSTRATION - FILE-BASED COUNTER (FIXED)")
    print("=" * 60)
    print(f"Counter file: {counter_filename}")
    print(f"Iterations per process: {iterations_per_process}")
    print(f"Number of processes: {number_of_processes}")
    print(f"Expected final count: {iterations_per_process * number_of_processes}")
    print("-" * 60)


    print("Initializing counter file...")
    if not initialize_counter_file(counter_filename, 0):
        print("Failed to initialize counter file. Exiting.")
        return


    file_lock = multiprocessing.Lock()


    process_list = []
    process_names = [f"Process-{i + 1}" for i in range(number_of_processes)]

    print(f"Creating {number_of_processes} concurrent processes...")

    for process_index in range(number_of_processes):
        process_name = process_names[process_index]
        worker_process = multiprocessing.Process(
            target=increment_file_counter,
            args=(counter_filename, iterations_per_process, process_name, enable_debug_logging, file_lock),
            name=process_name
        )
        process_list.append(worker_process)


    start_time = time.time()
    print("Starting all processes...")

    for worker_process in process_list:
        worker_process.start()
        print(f"Started {worker_process.name} (PID: {worker_process.pid})")


    print("Waiting for all processes to complete...")

    for worker_process in process_list:
        worker_process.join()
        print(f"Process {worker_process.name} finished")

    end_time = time.time()
    execution_time = end_time - start_time


    final_counter_value = read_final_counter_value(counter_filename)
    expected_counter_value = iterations_per_process * number_of_processes
    lost_updates = expected_counter_value - final_counter_value
    success_rate = (final_counter_value / expected_counter_value) * 100 if expected_counter_value else 0.0

    print("-" * 60)
    print("RESULTS:")
    print(f"Execution time: {execution_time:.3f} seconds")
    print(f"Final counter value: {final_counter_value}")
    print(f"Expected counter value: {expected_counter_value}")
    print(f"Lost updates: {lost_updates}")
    print(f"Success rate: {success_rate:.2f}%")

    if lost_updates > 0:
        print("⚠️  Still seeing lost updates (check if all writers use the same lock).")
    else:
        print("✅ No race condition detected. All increments were applied correctly.")


    try:
        if os.path.exists(counter_filename):
            os.remove(counter_filename)
            print(f"Cleaned up: {counter_filename} removed")
    except Exception as cleanup_error:
        print(f"Warning: Could not remove {counter_filename}: {cleanup_error}")

    print("=" * 60)


if __name__ == "__main__":
    try:
        run_race_condition_demonstration()
    except KeyboardInterrupt:
        print("\nProgram interrupted by user.")
        sys.exit(1)
    except Exception as main_error:
        print(f"An unexpected error occurred: {main_error}")
        sys.exit(1)
