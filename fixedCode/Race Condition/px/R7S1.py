import multiprocessing
import os
import time
import random
import sys
import fcntl
from datetime import datetime

def increment_file_counter(counter_filename, lock_filename, total_iterations, process_identifier, enable_logging=False):
    """
    Fixed version using fcntl file locking for atomic increments across processes.
    """
    successful_increments = 0
    failed_operations = 0

    for iteration_number in range(total_iterations):
        try:

            time.sleep(random.uniform(0.001, 0.005))


            with open(lock_filename, 'w') as lock_file:
                fcntl.flock(lock_file.fileno(), fcntl.LOCK_EX)
                
                try:

                    current_counter_value = 0
                    if os.path.exists(counter_filename):
                        with open(counter_filename, 'r') as counter_file:
                            try:
                                file_content = counter_file.read().strip()
                                if file_content:
                                    current_counter_value = int(file_content)
                            except (ValueError, IOError):
                                current_counter_value = 0
                    

                    new_counter_value = current_counter_value + 1
                    

                    with open(counter_filename, 'w') as counter_file:
                        counter_file.write(str(new_counter_value))
                        counter_file.flush()
                    
                    successful_increments += 1
                    
                    if enable_logging and iteration_number % 10 == 0:
                        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
                        print(f"[{timestamp}] {process_identifier}: Iteration {iteration_number + 1}, "
                              f"Read: {current_counter_value}, Wrote: {new_counter_value}")
                
                finally:

                    pass
            

            time.sleep(random.uniform(0.001, 0.003))
            
        except Exception as general_error:
            failed_operations += 1
            if enable_logging:
                print(f"[{process_identifier}] Error iteration {iteration_number + 1}: {general_error}")

    if enable_logging:
        print(f"[{process_identifier}] Completed: {successful_increments} successful, {failed_operations} failed")
    return successful_increments, failed_operations

def run_fixed_demonstration():
    """
    Updated main function to use locking.
    """
    counter_filename = "shared_counter.txt"
    lock_filename = "counter.lock"
    iterations_per_process = 100
    number_of_processes = 3
    enable_debug_logging = False

    print("=" * 60)
    print("✅ FIXED: FILE COUNTER WITH FCNTL LOCKING")
    print("=" * 60)
    print(f"Counter file: {counter_filename}")
    print(f"Lock file: {lock_filename}")
    print(f"Iterations/process: {iterations_per_process}")
    print(f"Processes: {number_of_processes}")
    print(f"Expected: {iterations_per_process * number_of_processes}")
    print("-" * 60)


    print("Initializing files...")
    initialize_counter_file(counter_filename, 0)
    open(lock_filename, 'a').close()


    process_list = []
    process_names = [f"Process-{i+1}" for i in range(number_of_processes)]
    for i, name in enumerate(process_names):
        p = multiprocessing.Process(
            target=increment_file_counter,
            args=(counter_filename, lock_filename, iterations_per_process, name, enable_debug_logging),
            name=name
        )
        process_list.append(p)

    start_time = time.time()
    print("Starting processes...")
    for p in process_list:
        p.start()
        print(f"Started {p.name} (PID: {p.pid})")

    print("Waiting for completion...")
    for p in process_list:
        p.join()
        print(f"Finished {p.name}")


    execution_time = time.time() - start_time
    final_value = read_final_counter_value(counter_filename)
    expected = iterations_per_process * number_of_processes
    lost = expected - final_value
    rate = (final_value / expected) * 100

    print("-" * 60)
    print("RESULTS:")
    print(f"Time: {execution_time:.3f}s")
    print(f"Final: {final_value}")
    print(f"Expected: {expected}")
    print(f"Lost: {lost}")
    print(f"Success: {rate:.2f}%")

    print("✅ PERFECT!" if lost == 0 else f"⚠️ Still {lost} lost!")
    

    for f in [counter_filename, lock_filename]:
        try:
            os.remove(f)
            print(f"Cleaned: {f}")
        except: pass


if __name__ == "__main__":
    run_fixed_demonstration()
