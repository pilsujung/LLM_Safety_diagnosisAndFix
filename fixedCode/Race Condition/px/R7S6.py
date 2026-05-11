import multiprocessing
import os
import time
import random
import sys
import fcntl
from datetime import datetime

def increment_file_counter(counter_filename, total_iterations, process_identifier, enable_logging=False):
    """
    Increments counter atomically using fcntl exclusive file locking.
    Eliminates race condition across multiple processes.
    """
    successful_increments = 0
    failed_operations = 0

    for iteration_number in range(total_iterations):
        try:

            time.sleep(random.uniform(0.001, 0.005))


            with open(counter_filename, 'r+') as counter_file:
                fcntl.flock(counter_file, fcntl.LOCK_EX)
                try:

                    counter_file.seek(0)
                    file_content = counter_file.read().strip()
                    current_counter_value = int(file_content) if file_content else 0
                    

                    new_counter_value = current_counter_value + 1
                    

                    counter_file.seek(0)
                    counter_file.write(str(new_counter_value))
                    counter_file.truncate()
                    counter_file.flush()
                    os.fsync(counter_file.fileno())
                    
                    successful_increments += 1

                    if enable_logging and iteration_number % 10 == 0:
                        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        print(f"[{timestamp}] {process_identifier}: Iteration {iteration_number + 1}, "
                              f"Read: {current_counter_value}, Wrote: {new_counter_value}")
                        
                finally:

                    fcntl.flock(counter_file, fcntl.LOCK_UN)


            time.sleep(random.uniform(0.001, 0.003))

        except Exception as general_error:
            failed_operations += 1
            if enable_logging:
                print(f"[{process_identifier}] Error iteration {iteration_number + 1}: {general_error}")

    if enable_logging:
        print(f"[{process_identifier}] Completed: {successful_increments} successful, {failed_operations} failed")
    return successful_increments, failed_operations


def initialize_counter_file(filename, initial_value=0):
    try:
        with open(filename, 'w') as file_handle:
            file_handle.write(str(initial_value))
        return True
    except Exception as error:
        print(f"Error initializing counter file: {error}")
        return False

def read_final_counter_value(filename):
    try:
        if os.path.exists(filename):
            with open(filename, 'r') as file_handle:
                content = file_handle.read().strip()
                return int(content) if content else 0
        return 0
    except (ValueError, IOError):
        return 0

def run_fixed_demonstration():
    counter_filename = "shared_counter.txt"
    iterations_per_process = 100
    number_of_processes = 3
    enable_debug_logging = False

    print("=" * 60)
    print("✅ FIXED: FILE COUNTER WITH FCNTL LOCKING")
    print("=" * 60)
    print(f"Iterations/process: {iterations_per_process}")
    print(f"Processes: {number_of_processes}")
    print(f"Expected: {iterations_per_process * number_of_processes}")
    print("-" * 60)

    if not initialize_counter_file(counter_filename, 0):
        return

    process_list = [multiprocessing.Process(
        target=increment_file_counter,
        args=(counter_filename, iterations_per_process, f"Process-{i+1}", enable_debug_logging),
        name=f"Process-{i+1}"
    ) for i in range(number_of_processes)]

    start_time = time.time()
    for p in process_list: p.start()
    for p in process_list: p.join()

    end_time = time.time()
    final = read_final_counter_value(counter_filename)
    expected = iterations_per_process * number_of_processes
    lost = expected - final

    print("-" * 60)
    print("RESULTS:")
    print(f"Time: {end_time-start_time:.3f}s")
    print(f"Final: {final}")
    print(f"Expected: {expected}")
    print(f"Lost: {lost}")
    print(f"Success: {(final/expected)*100:.1f}%")

    print("✅ PERFECT: 0 lost updates!" if lost == 0 else f"⚠️ {lost} lost")
    
    try: os.remove(counter_filename)
    except: pass
    print("=" * 60)

if __name__ == "__main__":
    run_fixed_demonstration()
