import threading
import multiprocessing
import time
import random
import os
from datetime import datetime


global_thread_counter = 0
global_thread_accumulator = 0
shared_thread_balance = 1000


thread_counter_lock = threading.Lock()
thread_accumulator_lock = threading.Lock()
thread_bank_lock = threading.Lock()


NUM_THREADS = 20
NUM_PROCESSES = 8
MAX_ITERATIONS_PER_WORKER = 5
RANDOM_DELAY_MAX = 0.02
PROCESS_COUNTER_FILE = "shared_process_counter.txt"
PROCESS_BALANCE_FILE = "shared_process_balance.txt"


def simulate_database_operation():
    """Simulate a slow database or I/O operation"""
    time.sleep(random.uniform(0.001, RANDOM_DELAY_MAX))


def unsafe_thread_increment_counter():
    """
    Thread function that increments a global counter.
    Now uses a lock to prevent race conditions.
    """
    global global_thread_counter

    for iteration in range(MAX_ITERATIONS_PER_WORKER):

        simulate_database_operation()


        with thread_counter_lock:
            current_counter_value = global_thread_counter
            new_value = current_counter_value + 1
            global_thread_counter = new_value

        print(
            f"Thread {threading.current_thread().name}: "
            f"Read {current_counter_value}, Writing {new_value}"
        )


def unsafe_thread_bank_transaction():
    """
    Thread function simulating bank account transactions.
    Updated to use a lock so that balance updates are consistent.
    """
    global shared_thread_balance

    for transaction in range(MAX_ITERATIONS_PER_WORKER):

        withdrawal_amount = random.randint(1, 10)
        simulate_database_operation()


        with thread_bank_lock:
            current_balance = shared_thread_balance
            if current_balance >= withdrawal_amount:
                new_balance = current_balance - withdrawal_amount
                shared_thread_balance = new_balance
                print(
                    f"Thread {threading.current_thread().name}: "
                    f"Withdrew ${withdrawal_amount}, New balance: ${new_balance}"
                )


def unsafe_thread_accumulator():
    """
    Thread function that accumulates values.
    Now uses a lock to avoid lost updates.
    """
    global global_thread_accumulator

    for round_num in range(MAX_ITERATIONS_PER_WORKER):
        random_value = random.randint(1, 100)
        simulate_database_operation()


        with thread_accumulator_lock:
            current_accumulator = global_thread_accumulator
            new_accumulator_value = current_accumulator + random_value
            global_thread_accumulator = new_accumulator_value

        print(
            f"Thread {threading.current_thread().name}: "
            f"Added {random_value} to {current_accumulator}, Result: {new_accumulator_value}"
        )


def unsafe_process_increment_counter(process_id, counter_file, lock):
    """
    Process function that increments a shared counter via file I/O.
    Synchronized with a multiprocessing.Lock to prevent race conditions.
    """
    for iteration in range(MAX_ITERATIONS_PER_WORKER):

        simulate_database_operation()

        try:
            with lock:

                with open(counter_file, "r") as file:
                    current_file_value = int(file.read().strip())

                new_file_value = current_file_value + 1

                with open(counter_file, "w") as file:
                    file.write(str(new_file_value))

            print(
                f"Process {process_id}: Read {current_file_value}, "
                f"Wrote {new_file_value} to {counter_file}"
            )
        except (FileNotFoundError, ValueError) as error:
            print(f"Process {process_id}: Error reading file - {error}")

            with lock:
                with open(counter_file, "w") as file:
                    file.write("1")


def unsafe_process_bank_operations(process_id, balance_file, lock):
    """
    Process function simulating bank operations across processes.
    Protected by a multiprocessing.Lock so reads/writes are consistent.
    """
    for transaction_num in range(MAX_ITERATIONS_PER_WORKER):

        transaction_amount = random.randint(1, 50)
        is_deposit = random.choice([True, False])
        simulate_database_operation()

        try:
            with lock:
                with open(balance_file, "r") as file:
                    current_file_balance = int(file.read().strip())

                if is_deposit:
                    new_balance = current_file_balance + transaction_amount
                    operation_type = "Deposit"
                else:
                    new_balance = max(0, current_file_balance - transaction_amount)
                    operation_type = "Withdrawal"

                with open(balance_file, "w") as file:
                    file.write(str(new_balance))

            print(
                f"Process {process_id}: {operation_type} ${transaction_amount}, "
                f"Balance: ${current_file_balance} -> ${new_balance}"
            )
        except (FileNotFoundError, ValueError) as error:
            print(f"Process {process_id}: Error with balance file - {error}")


def run_threading_race_condition_demo():
    """
    Demonstrate (now fixed) race conditions using multiple threads.
    """
    global global_thread_counter, global_thread_accumulator, shared_thread_balance

    print("=" * 60)
    print("THREADING RACE CONDITION DEMONSTRATION (FIXED)")
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
            target=unsafe_thread_increment_counter,
            name=f"CounterThread-{thread_id}"
        )
        counter_threads.append(thread)


    for thread_id in range(NUM_THREADS // 3):
        thread = threading.Thread(
            target=unsafe_thread_accumulator,
            name=f"AccumulatorThread-{thread_id}"
        )
        accumulator_threads.append(thread)


    for thread_id in range(NUM_THREADS // 3):
        thread = threading.Thread(
            target=unsafe_thread_bank_transaction,
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
    print("THREADING RESULTS (FIXED):")
    expected_counter = (NUM_THREADS // 3) * MAX_ITERATIONS_PER_WORKER
    print(f"Expected counter value: {expected_counter}")
    print(f"Actual counter value: {global_thread_counter}")
    print(f"Lost increments: {expected_counter - global_thread_counter}")
    print(f"Final accumulator value: {global_thread_accumulator}")
    print(f"Final thread balance: ${shared_thread_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")


def run_multiprocessing_race_condition_demo():
    """
    Demonstrate race-free multiprocessing using multiple processes with file I/O.
    """
    print("\n" + "=" * 60)
    print("MULTIPROCESSING RACE CONDITION DEMONSTRATION (FIXED)")
    print("=" * 60)
    print(f"Starting {NUM_PROCESSES} processes, each performing {MAX_ITERATIONS_PER_WORKER} operations")
    expected_counter_processes = (NUM_PROCESSES // 2) * MAX_ITERATIONS_PER_WORKER
    print(f"Expected final counter value: {expected_counter_processes}")
    print(f"Initial process balance: $5000")
    print("-" * 60)


    with open(PROCESS_COUNTER_FILE, "w") as file:
        file.write("0")

    with open(PROCESS_BALANCE_FILE, "w") as file:
        file.write("5000")


    counter_lock = multiprocessing.Lock()
    balance_lock = multiprocessing.Lock()

    counter_processes = []
    bank_processes = []

    start_time = time.time()


    for process_id in range(NUM_PROCESSES // 2):
        process = multiprocessing.Process(
            target=unsafe_process_increment_counter,
            args=(f"Counter-{process_id}", PROCESS_COUNTER_FILE, counter_lock),
        )
        counter_processes.append(process)


    for process_id in range(NUM_PROCESSES // 2):
        process = multiprocessing.Process(
            target=unsafe_process_bank_operations,
            args=(f"Bank-{process_id}", PROCESS_BALANCE_FILE, balance_lock),
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
    print("MULTIPROCESSING RESULTS (FIXED):")
    print(f"Expected counter value: {expected_counter_processes}")
    print(f"Actual counter value: {final_process_counter}")
    print(f"Lost increments: {expected_counter_processes - final_process_counter}")
    print(f"Final process balance: ${final_process_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")


    try:
        os.remove(PROCESS_COUNTER_FILE)
        os.remove(PROCESS_BALANCE_FILE)
        print("Cleaned up temporary files.")
    except FileNotFoundError:
        pass


if __name__ == "__main__":
    print(f"Race Condition Demonstration Started at {datetime.now()}")
    print("This program demonstrates race conditions in both threading and multiprocessing.")
    print("With the fixes applied, you should see that the counters reach the expected values,")
    print("and shared resources are updated consistently across threads and processes.\n")


    run_threading_race_condition_demo()
    run_multiprocessing_race_condition_demo()

    print(f"\nRace Condition Demonstration Completed at {datetime.now()}")
    print("You can still tweak timing and counts to see how synchronization affects behavior.")
