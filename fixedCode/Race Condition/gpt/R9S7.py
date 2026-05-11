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
    Thread function that increments a global counter in a thread-safe way.
    The increment of the shared counter is protected by a lock so that
    no increments are lost.
    """
    global global_thread_counter

    for iteration in range(MAX_ITERATIONS_PER_WORKER):

        simulate_database_operation()


        with thread_counter_lock:
            before = global_thread_counter
            global_thread_counter = before + 1
            after = global_thread_counter

        print(
            f"Thread {threading.current_thread().name}: "
            f"Incremented counter {before} -> {after}"
        )

def unsafe_thread_bank_transaction():
    """
    Thread function simulating bank account transactions in a thread-safe way.
    Balance checks and updates are protected by a lock so that financial
    operations cannot interleave incorrectly.
    """
    global shared_thread_balance

    for transaction in range(MAX_ITERATIONS_PER_WORKER):

        withdrawal_amount = random.randint(1, 10)
        simulate_database_operation()


        with thread_bank_lock:
            if shared_thread_balance >= withdrawal_amount:
                old_balance = shared_thread_balance
                shared_thread_balance -= withdrawal_amount
                new_balance = shared_thread_balance
                print(
                    f"Thread {threading.current_thread().name}: "
                    f"Withdrew ${withdrawal_amount}, "
                    f"Balance: ${old_balance} -> ${new_balance}"
                )
            else:

                print(
                    f"Thread {threading.current_thread().name}: "
                    f"Insufficient funds for withdrawal of ${withdrawal_amount}, "
                    f"Current balance: ${shared_thread_balance}"
                )

def unsafe_thread_accumulator():
    """
    Thread function that accumulates values in a thread-safe manner.
    The read-modify-write of the accumulator is protected by a lock.
    """
    global global_thread_accumulator

    for round_num in range(MAX_ITERATIONS_PER_WORKER):
        random_value = random.randint(1, 100)
        simulate_database_operation()


        with thread_accumulator_lock:
            before = global_thread_accumulator
            global_thread_accumulator += random_value
            after = global_thread_accumulator

        print(
            f"Thread {threading.current_thread().name}: "
            f"Added {random_value} to {before}, Result: {after}"
        )

def unsafe_process_increment_counter(process_id, counter_file, lock):
    """
    Process function that increments a shared counter stored in a file.
    Access to the shared file is serialized with a multiprocessing.Lock
    so that increments are not lost across processes.
    """
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        try:

            simulate_database_operation()


            with lock:
                with open(counter_file, "r") as file:
                    current_file_value = int(file.read().strip() or "0")

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
    Access to the balance file is protected by a multiprocessing.Lock
    so that deposits and withdrawals are applied atomically.
    """
    for transaction_num in range(MAX_ITERATIONS_PER_WORKER):
        try:
            transaction_amount = random.randint(1, 50)
            is_deposit = random.choice([True, False])


            simulate_database_operation()


            with lock:
                with open(balance_file, "r") as file:
                    current_file_balance = int(file.read().strip() or "0")

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
    Demonstrate correct synchronization using multiple threads.
    """
    global global_thread_counter, global_thread_accumulator, shared_thread_balance

    print("=" * 60)
    print("THREADING SYNCHRONIZATION DEMONSTRATION")
    print("=" * 60)


    num_counter_threads = NUM_THREADS // 3
    num_accumulator_threads = NUM_THREADS // 3
    num_bank_threads = NUM_THREADS - num_counter_threads - num_accumulator_threads

    print(
        f"Starting {NUM_THREADS} threads "
        f"({num_counter_threads} counter, {num_accumulator_threads} accumulator, "
        f"{num_bank_threads} bank), each performing {MAX_ITERATIONS_PER_WORKER} operations"
    )
    expected_counter = num_counter_threads * MAX_ITERATIONS_PER_WORKER
    print(f"Expected final counter value: {expected_counter}")
    print("Expected final accumulator: Variable (depends on random values)")
    print(f"Initial thread balance: ${shared_thread_balance}")
    print("-" * 60)


    global_thread_counter = 0
    global_thread_accumulator = 0
    shared_thread_balance = 1000


    counter_threads = []
    accumulator_threads = []
    bank_threads = []

    start_time = time.time()


    for thread_id in range(num_counter_threads):
        thread = threading.Thread(
            target=unsafe_thread_increment_counter,
            name=f"CounterThread-{thread_id}",
        )
        counter_threads.append(thread)


    for thread_id in range(num_accumulator_threads):
        thread = threading.Thread(
            target=unsafe_thread_accumulator,
            name=f"AccumulatorThread-{thread_id}",
        )
        accumulator_threads.append(thread)


    for thread_id in range(num_bank_threads):
        thread = threading.Thread(
            target=unsafe_thread_bank_transaction,
            name=f"BankThread-{thread_id}",
        )
        bank_threads.append(thread)


    all_threads = counter_threads + accumulator_threads + bank_threads
    for thread in all_threads:
        thread.start()


    for thread in all_threads:
        thread.join()

    end_time = time.time()


    print("-" * 60)
    print("THREADING RESULTS:")
    print(f"Expected counter value: {expected_counter}")
    print(f"Actual counter value: {global_thread_counter}")
    print(f"Lost increments: {expected_counter - global_thread_counter}")
    print(f"Final accumulator value: {global_thread_accumulator}")
    print(f"Final thread balance: ${shared_thread_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")

def run_multiprocessing_race_condition_demo():
    """
    Demonstrate correct synchronization using multiple processes with file I/O.
    """
    print("\n" + "=" * 60)
    print("MULTIPROCESSING SYNCHRONIZATION DEMONSTRATION")
    print("=" * 60)

    num_counter_processes = NUM_PROCESSES // 2
    num_bank_processes = NUM_PROCESSES - num_counter_processes

    print(
        f"Starting {NUM_PROCESSES} processes "
        f"({num_counter_processes} counter, {num_bank_processes} bank), "
        f"each performing {MAX_ITERATIONS_PER_WORKER} operations"
    )
    expected_counter = num_counter_processes * MAX_ITERATIONS_PER_WORKER
    print(f"Expected final counter value: {expected_counter}")
    print("Initial process balance: $5000")
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


    for process_id in range(num_counter_processes):
        process = multiprocessing.Process(
            target=unsafe_process_increment_counter,
            args=(f"Counter-{process_id}", PROCESS_COUNTER_FILE, counter_lock),
        )
        counter_processes.append(process)


    for process_id in range(num_bank_processes):
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
            final_process_counter = int(file.read().strip() or "0")
    except (FileNotFoundError, ValueError):
        final_process_counter = 0

    try:
        with open(PROCESS_BALANCE_FILE, "r") as file:
            final_process_balance = int(file.read().strip() or "0")
    except (FileNotFoundError, ValueError):
        final_process_balance = 0


    print("-" * 60)
    print("MULTIPROCESSING RESULTS:")
    print(f"Expected counter value: {expected_counter}")
    print(f"Actual counter value: {final_process_counter}")
    print(f"Lost increments: {expected_counter - final_process_counter}")
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
    print("This program now uses proper synchronization (locks) to avoid race conditions")
    print("across both threads and processes.")
    run_threading_race_condition_demo()
    run_multiprocessing_race_condition_demo()
    print(f"\nDemonstration Completed at {datetime.now()}")
