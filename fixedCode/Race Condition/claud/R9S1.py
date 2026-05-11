import threading
import multiprocessing
import time
import random
import os
import fcntl
from datetime import datetime


global_thread_counter = 0
global_thread_accumulator = 0
shared_thread_balance = 1000


counter_lock = threading.Lock()
accumulator_lock = threading.Lock()
balance_lock = threading.Lock()


NUM_THREADS = 20
NUM_PROCESSES = 8
MAX_ITERATIONS_PER_WORKER = 5
RANDOM_DELAY_MAX = 0.02
PROCESS_COUNTER_FILE = "shared_process_counter.txt"
PROCESS_BALANCE_FILE = "shared_process_balance.txt"

def simulate_database_operation():
    """Simulate a slow database or I/O operation"""
    time.sleep(random.uniform(0.001, RANDOM_DELAY_MAX))

def safe_thread_increment_counter():
    """
    Thread-safe function that increments a global counter.
    Uses a lock to prevent race conditions.
    """
    global global_thread_counter
    
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        with counter_lock:

            current_counter_value = global_thread_counter
            

            simulate_database_operation()
            

            global_thread_counter = current_counter_value + 1
            
            print(f"Thread {threading.current_thread().name}: "
                  f"Read {current_counter_value}, Writing {current_counter_value + 1}")


def safe_thread_bank_transaction():
    """
    Thread-safe function simulating bank account transactions.
    Uses a lock to ensure atomic transactions.
    """
    global shared_thread_balance
    
    for transaction in range(MAX_ITERATIONS_PER_WORKER):
        with balance_lock:

            current_balance = shared_thread_balance
            

            simulate_database_operation()
            

            withdrawal_amount = random.randint(1, 10)
            if current_balance >= withdrawal_amount:
                shared_thread_balance = current_balance - withdrawal_amount
                print(f"Thread {threading.current_thread().name}: "
                      f"Withdrew ${withdrawal_amount}, New balance: ${shared_thread_balance}")

def safe_thread_accumulator():
    """
    Thread-safe accumulator function.
    Uses a lock to prevent race conditions with complex operations.
    """
    global global_thread_accumulator
    
    for round_num in range(MAX_ITERATIONS_PER_WORKER):

        random_value = random.randint(1, 100)
        
        with accumulator_lock:

            current_accumulator = global_thread_accumulator
            

            simulate_database_operation()
            

            new_accumulator_value = current_accumulator + random_value
            

            global_thread_accumulator = new_accumulator_value
            
            print(f"Thread {threading.current_thread().name}: "
                  f"Added {random_value} to {current_accumulator}, Result: {new_accumulator_value}")

def safe_process_increment_counter(process_id, counter_file):
    """
    Process-safe function that increments a shared counter via file I/O.
    Uses file locking to prevent race conditions across processes.
    """
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        try:

            with open(counter_file, "r+") as file:

                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                
                try:

                    current_file_value = int(file.read().strip())
                    

                    simulate_database_operation()
                    

                    new_file_value = current_file_value + 1
                    

                    file.seek(0)
                    file.write(str(new_file_value))
                    file.truncate()
                    
                    print(f"Process {process_id}: Read {current_file_value}, "
                          f"Wrote {new_file_value} to {counter_file}")
                finally:

                    fcntl.flock(file.fileno(), fcntl.LOCK_UN)
                    
        except (FileNotFoundError, ValueError) as error:
            print(f"Process {process_id}: Error reading file - {error}")

            with open(counter_file, "w") as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                file.write("1")
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)

def safe_process_bank_operations(process_id, balance_file):
    """
    Process-safe function simulating bank operations across processes.
    Uses file locking to ensure atomic transactions.
    """
    for transaction_num in range(MAX_ITERATIONS_PER_WORKER):
        try:

            with open(balance_file, "r+") as file:

                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                
                try:

                    current_file_balance = int(file.read().strip())
                    

                    simulate_database_operation()
                    

                    transaction_amount = random.randint(1, 50)
                    is_deposit = random.choice([True, False])
                    
                    if is_deposit:
                        new_balance = current_file_balance + transaction_amount
                        operation_type = "Deposit"
                    else:
                        new_balance = max(0, current_file_balance - transaction_amount)
                        operation_type = "Withdrawal"
                    

                    file.seek(0)
                    file.write(str(new_balance))
                    file.truncate()
                    
                    print(f"Process {process_id}: {operation_type} ${transaction_amount}, "
                          f"Balance: ${current_file_balance} -> ${new_balance}")
                finally:

                    fcntl.flock(file.fileno(), fcntl.LOCK_UN)
                    
        except (FileNotFoundError, ValueError) as error:
            print(f"Process {process_id}: Error with balance file - {error}")

def run_threading_race_condition_demo():
    """
    Demonstrate thread-safe operations using locks.
    """

    global global_thread_counter, global_thread_accumulator, shared_thread_balance
    
    print("=" * 60)
    print("THREAD-SAFE DEMONSTRATION WITH LOCKS")
    print("=" * 60)
    print(f"Starting {NUM_THREADS} threads, each performing {MAX_ITERATIONS_PER_WORKER} operations")
    print(f"Expected final counter value: {NUM_THREADS * MAX_ITERATIONS_PER_WORKER}")
    print(f"Expected final accumulator: Variable (depends on random values)")
    print(f"Initial thread balance: ${shared_thread_balance}")
    print("-" * 60)
    

    global_thread_counter = 0
    global_thread_accumulator = 0
    shared_thread_balance = 1000
    

    counter_threads = []
    accumulator_threads = []
    bank_threads = []
    
    start_time = time.time()
    

    for thread_id in range(NUM_THREADS // 3):
        thread = threading.Thread(
            target=safe_thread_increment_counter,
            name=f"CounterThread-{thread_id}"
        )
        counter_threads.append(thread)
    

    for thread_id in range(NUM_THREADS // 3):
        thread = threading.Thread(
            target=safe_thread_accumulator,
            name=f"AccumulatorThread-{thread_id}"
        )
        accumulator_threads.append(thread)
    

    for thread_id in range(NUM_THREADS // 3):
        thread = threading.Thread(
            target=safe_thread_bank_transaction,
            name=f"BankThread-{thread_id}"
        )
        bank_threads.append(thread)
    

    all_threads = counter_threads + accumulator_threads + bank_threads
    for thread in all_threads:
        thread.start()
    

    for thread in all_threads:
        thread.join()
    
    end_time = time.time()
    

    print("-" * 60)
    print("THREAD-SAFE RESULTS:")
    print(f"Expected counter value: {(NUM_THREADS // 3) * MAX_ITERATIONS_PER_WORKER}")
    print(f"Actual counter value: {global_thread_counter}")
    print(f"Lost increments: {(NUM_THREADS // 3) * MAX_ITERATIONS_PER_WORKER - global_thread_counter}")
    print(f"Final accumulator value: {global_thread_accumulator}")
    print(f"Final thread balance: ${shared_thread_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")

def run_multiprocessing_race_condition_demo():
    """
    Demonstrate process-safe operations using file locks.
    """
    print("\n" + "=" * 60)
    print("PROCESS-SAFE DEMONSTRATION WITH FILE LOCKS")
    print("=" * 60)
    print(f"Starting {NUM_PROCESSES} processes, each performing {MAX_ITERATIONS_PER_WORKER} operations")
    print(f"Expected final counter value: {NUM_PROCESSES * MAX_ITERATIONS_PER_WORKER}")
    print(f"Initial process balance: $5000")
    print("-" * 60)
    

    with open(PROCESS_COUNTER_FILE, "w") as file:
        file.write("0")
    
    with open(PROCESS_BALANCE_FILE, "w") as file:
        file.write("5000")
    

    counter_processes = []
    bank_processes = []
    
    start_time = time.time()
    

    for process_id in range(NUM_PROCESSES // 2):
        process = multiprocessing.Process(
            target=safe_process_increment_counter,
            args=(f"Counter-{process_id}", PROCESS_COUNTER_FILE)
        )
        counter_processes.append(process)
    

    for process_id in range(NUM_PROCESSES // 2):
        process = multiprocessing.Process(
            target=safe_process_bank_operations,
            args=(f"Bank-{process_id}", PROCESS_BALANCE_FILE)
        )
        bank_processes.append(process)
    

    all_processes = counter_processes + bank_processes
    for process in all_processes:
        process.start()
    

    for process in all_processes:
        process.join()
    
    end_time = time.time()
    

    try:
        with open(PROCESS_COUNTER_FILE, "r") as file:
            final_process_counter = int(file.read().strip())
    except (FileNotFoundError, ValueError):
        final_process_counter = 0
    
    try:
        with open(PROCESS_BALANCE_FILE, "r") as file:
            final_process_balance = int(file.read().strip())
    except (FileNotFoundError, ValueError):
        final_process_balance = 0
    

    print("-" * 60)
    print("PROCESS-SAFE RESULTS:")
    print(f"Expected counter value: {(NUM_PROCESSES // 2) * MAX_ITERATIONS_PER_WORKER}")
    print(f"Actual counter value: {final_process_counter}")
    print(f"Lost increments: {(NUM_PROCESSES // 2) * MAX_ITERATIONS_PER_WORKER - final_process_counter}")
    print(f"Final process balance: ${final_process_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    

    try:
        os.remove(PROCESS_COUNTER_FILE)
        os.remove(PROCESS_BALANCE_FILE)
        print("Cleaned up temporary files.")
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    print(f"Thread-Safe and Process-Safe Demonstration Started at {datetime.now()}")
    print("This program demonstrates proper synchronization in threading and multiprocessing.")
    print("The final values should now match expected values due to proper locking.")
    

    run_threading_race_condition_demo()
    run_multiprocessing_race_condition_demo()
    
    print(f"\nDemonstration Completed at {datetime.now()}")
    print("Run this program multiple times - you should see consistent, correct results!")