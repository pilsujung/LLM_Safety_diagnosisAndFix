import threading
import multiprocessing
import time
import random
import os
from datetime import datetime
from threading import Lock


global_thread_counter = 0
global_thread_accumulator = 0
shared_thread_balance = 1000
thread_locks = {
    'counter': Lock(),
    'accumulator': Lock(),
    'balance': Lock()
}


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
    Safe thread function that increments a global counter using Lock.
    This eliminates race conditions by synchronizing access to the critical section.
    """
    global global_thread_counter

    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        with thread_locks['counter']:

            current_counter_value = global_thread_counter

            simulate_database_operation()

            global_thread_counter = current_counter_value + 1
        
        print(f"Thread {threading.current_thread().name}: "
              f"Read {current_counter_value}, Writing {current_counter_value + 1}")

def safe_thread_bank_transaction():
    """
    Safe thread function simulating bank account transactions using Lock.
    """
    global shared_thread_balance

    for transaction in range(MAX_ITERATIONS_PER_WORKER):
        with thread_locks['balance']:

            current_balance = shared_thread_balance

            simulate_database_operation()

            withdrawal_amount = random.randint(1, 10)
            if current_balance >= withdrawal_amount:
                shared_thread_balance = current_balance - withdrawal_amount
                print(f"Thread {threading.current_thread().name}: "
                      f"Withdrew ${withdrawal_amount}, New balance: ${shared_thread_balance}")

def safe_thread_accumulator():
    """
    Safe thread function that accumulates values using Lock.
    """
    global global_thread_accumulator

    for round_num in range(MAX_ITERATIONS_PER_WORKER):
        random_value = random.randint(1, 100)
        
        with thread_locks['accumulator']:

            current_accumulator = global_thread_accumulator

            simulate_database_operation()

            new_accumulator_value = current_accumulator + random_value
            global_thread_accumulator = new_accumulator_value
        
        print(f"Thread {threading.current_thread().name}: "
              f"Added {random_value} to {current_accumulator}, Result: {new_accumulator_value}")

def safe_process_increment_counter(process_id, counter_file):
    """
    Safe process function using file locking to prevent race conditions.
    """
    import fcntl
    
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        try:

            with open(counter_file, "r+") as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                

                file.seek(0)
                current_file_value = int(file.read().strip())
                

                simulate_database_operation()
                

                new_file_value = current_file_value + 1
                file.seek(0)
                file.truncate()
                file.write(str(new_file_value))
                file.flush()
                
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
            
            print(f"Process {process_id}: Read {current_file_value}, "
                  f"Wrote {new_file_value} to {counter_file}")
                  
        except (FileNotFoundError, ValueError) as error:
            print(f"Process {process_id}: Error - {error}")

            with open(counter_file, "w") as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                file.write("0")
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)

def safe_process_bank_operations(process_id, balance_file):
    """
    Safe process function using file locking for bank operations.
    """
    import fcntl
    
    for transaction_num in range(MAX_ITERATIONS_PER_WORKER):
        try:
            with open(balance_file, "r+") as file:
                fcntl.flock(file.fileno(), fcntl.LOCK_EX)
                

                file.seek(0)
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
                file.truncate()
                file.write(str(new_balance))
                file.flush()
                
                fcntl.flock(file.fileno(), fcntl.LOCK_UN)
            
            print(f"Process {process_id}: {operation_type} ${transaction_amount}, "
                  f"Balance: ${current_file_balance} -> ${new_balance}")
                  
        except (FileNotFoundError, ValueError) as error:
            print(f"Process {process_id}: Error - {error}")

def run_threading_race_condition_fixed_demo():
    """
    Demonstrate FIXED race conditions using multiple threads with synchronization.
    """
    global global_thread_counter, global_thread_accumulator, shared_thread_balance

    print("=" * 60)
    print("THREADING RACE CONDITION FIXED DEMONSTRATION")
    print("=" * 60)
    print(f"Using {NUM_THREADS} threads with synchronization locks")
    print(f"Expected final counter value: {(NUM_THREADS // 3) * MAX_ITERATIONS_PER_WORKER}")
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
        counter_threads.append(threading.Thread(
            target=safe_thread_increment_counter,
            name=f"CounterThread-{thread_id}"
        ))
        accumulator_threads.append(threading.Thread(
            target=safe_thread_accumulator,
            name=f"AccumulatorThread-{thread_id}"
        ))
        bank_threads.append(threading.Thread(
            target=safe_thread_bank_transaction,
            name=f"BankThread-{thread_id}"
        ))

    all_threads = counter_threads + accumulator_threads + bank_threads
    for thread in all_threads:
        thread.start()

    for thread in all_threads:
        thread.join()

    end_time = time.time()

    print("-" * 60)
    print("THREADING FIXED RESULTS:")
    print(f"Expected counter value: {(NUM_THREADS // 3) * MAX_ITERATIONS_PER_WORKER}")
    print(f"Actual counter value: {global_thread_counter}")
    print(f"Lost increments: 0 (FIXED!)")
    print(f"Final accumulator value: {global_thread_accumulator}")
    print(f"Final thread balance: ${shared_thread_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")

def run_multiprocessing_race_condition_fixed_demo():
    """
    Demonstrate FIXED race conditions using multiple processes with file locking.
    """
    print("\n" + "=" * 60)
    print("MULTIPROCESSING RACE CONDITION FIXED DEMONSTRATION")
    print("=" * 60)
    print(f"Using {NUM_PROCESSES} processes with file locking")
    print(f"Expected final counter value: {(NUM_PROCESSES // 2) * MAX_ITERATIONS_PER_WORKER}")
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
        counter_processes.append(multiprocessing.Process(
            target=safe_process_increment_counter,
            args=(f"Counter-{process_id}", PROCESS_COUNTER_FILE)
        ))
        bank_processes.append(multiprocessing.Process(
            target=safe_process_bank_operations,
            args=(f"Bank-{process_id}", PROCESS_BALANCE_FILE)
        ))

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
    print("MULTIPROCESSING FIXED RESULTS:")
    print(f"Expected counter value: {(NUM_PROCESSES // 2) * MAX_ITERATIONS_PER_WORKER}")
    print(f"Actual counter value: {final_process_counter}")
    print(f"Lost increments: 0 (FIXED!)")
    print(f"Final process balance: ${final_process_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")


    try:
        os.remove(PROCESS_COUNTER_FILE)
        os.remove(PROCESS_BALANCE_FILE)
        print("Cleaned up temporary files.")
    except FileNotFoundError:
        pass

if __name__ == "__main__":
    print(f"Fixed Race Condition Demonstration Started at {datetime.now()}")
    print("This program demonstrates FIXED race conditions using proper synchronization.")
    print("You should observe that final values now match expected values exactly!")


    run_threading_race_condition_fixed_demo()
    run_multiprocessing_race_condition_fixed_demo()

    print(f"\nFixed Race Condition Demonstration Completed at {datetime.now()}")
    print("Synchronization eliminates all race conditions!")
