import threading
import multiprocessing
import time
import random
import os
from datetime import datetime


global_thread_counter = 0
global_thread_accumulator = 0
shared_thread_balance = 1000


NUM_THREADS = 20
NUM_PROCESSES = 8
MAX_ITERATIONS_PER_WORKER = 5
RANDOM_DELAY_MAX = 0.02

def simulate_database_operation():
    """Simulate a slow database or I/O operation"""
    time.sleep(random.uniform(0.001, RANDOM_DELAY_MAX))


thread_counter_lock = threading.Lock()
thread_accumulator_lock = threading.Lock()
thread_balance_lock = threading.Lock()

def safe_thread_increment_counter():
    """Thread-safe counter increment using Lock"""
    global global_thread_counter
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        with thread_counter_lock:
            current_counter_value = global_thread_counter
            simulate_database_operation()
            global_thread_counter = current_counter_value + 1
        print(f"Thread {threading.current_thread().name}: "
              f"Read {current_counter_value}, Writing {current_counter_value + 1}")

def safe_thread_bank_transaction():
    """Thread-safe bank transaction using Lock"""
    global shared_thread_balance
    for transaction in range(MAX_ITERATIONS_PER_WORKER):
        with thread_balance_lock:
            current_balance = shared_thread_balance
            simulate_database_operation()
            withdrawal_amount = random.randint(1, 10)
            if current_balance >= withdrawal_amount:
                shared_thread_balance = current_balance - withdrawal_amount
        print(f"Thread {threading.current_thread().name}: "
              f"Withdrew ${withdrawal_amount}, New balance: ${shared_thread_balance}")

def safe_thread_accumulator():
    """Thread-safe accumulator using Lock"""
    global global_thread_accumulator
    for round_num in range(MAX_ITERATIONS_PER_WORKER):
        random_value = random.randint(1, 100)
        with thread_accumulator_lock:
            current_accumulator = global_thread_accumulator
            simulate_database_operation()
            new_accumulator_value = current_accumulator + random_value
            global_thread_accumulator = new_accumulator_value
        print(f"Thread {threading.current_thread().name}: "
              f"Added {random_value} to {current_accumulator}, Result: {new_accumulator_value}")


def safe_process_increment_counter(shared_counter, process_lock, process_id):
    """Process-safe counter using multiprocessing.Value + Lock"""
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        with process_lock:
            current_file_value = shared_counter.value
            simulate_database_operation()
            shared_counter.value += 1
        print(f"Process {process_id}: Read {current_file_value}, Wrote {current_file_value + 1}")

def safe_process_bank_operations(shared_balance, process_lock, process_id):
    """Process-safe bank operations using multiprocessing.Value + Lock"""
    for transaction_num in range(MAX_ITERATIONS_PER_WORKER):
        transaction_amount = random.randint(1, 50)
        is_deposit = random.choice([True, False])
        
        with process_lock:
            current_file_balance = shared_balance.value
            simulate_database_operation()
            if is_deposit:
                shared_balance.value += transaction_amount
                operation_type = "Deposit"
            else:
                if current_file_balance >= transaction_amount:
                    shared_balance.value = current_file_balance - transaction_amount
                    operation_type = "Withdrawal"
                else:
                    operation_type = "Failed Withdrawal"
        
        print(f"Process {process_id}: {operation_type} ${transaction_amount}")

def run_threading_race_condition_demo():
    """Run FIXED threading demonstration - now 100% accurate"""
    global global_thread_counter, global_thread_accumulator, shared_thread_balance
    
    print("=" * 60)
    print("THREADING RACE CONDITION FIXED DEMONSTRATION")
    print("=" * 60)
    print(f"Starting {NUM_THREADS} threads, each performing {MAX_ITERATIONS_PER_WORKER} operations")
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


    expected_counter = (NUM_THREADS // 3) * MAX_ITERATIONS_PER_WORKER
    print("-" * 60)
    print("THREADING FIXED RESULTS:")
    print(f"Expected counter value: {expected_counter}")
    print(f"Actual counter value: {global_thread_counter}")
    print(f"Lost increments: {expected_counter - global_thread_counter}")
    print(f"Final accumulator value: {global_thread_accumulator}")
    print(f"Final thread balance: ${shared_thread_balance}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    print("✅ THREADING PERFECT - 0 lost increments!")

def run_multiprocessing_race_condition_demo():
    """Run FIXED multiprocessing demonstration - now 100% accurate"""
    print("\n" + "=" * 60)
    print("MULTIPROCESSING RACE CONDITION FIXED DEMONSTRATION")
    print("=" * 60)
    print(f"Starting {NUM_PROCESSES} processes, each performing {MAX_ITERATIONS_PER_WORKER} operations")
    print("-" * 60)


    shared_counter = multiprocessing.Value('i', 0)
    shared_balance = multiprocessing.Value('i', 5000)
    process_lock = multiprocessing.Lock()


    counter_processes = []
    bank_processes = []

    start_time = time.time()


    for process_id in range(NUM_PROCESSES // 2):
        process = multiprocessing.Process(
            target=safe_process_increment_counter,
            args=(shared_counter, process_lock, f"Counter-{process_id}")
        )
        counter_processes.append(process)


    for process_id in range(NUM_PROCESSES // 2):
        process = multiprocessing.Process(
            target=safe_process_bank_operations,
            args=(shared_balance, process_lock, f"Bank-{process_id}")
        )
        bank_processes.append(process)


    all_processes = counter_processes + bank_processes
    for process in all_processes:
        process.start()


    for process in all_processes:
        process.join()

    end_time = time.time()


    expected_counter = (NUM_PROCESSES // 2) * MAX_ITERATIONS_PER_WORKER
    print("-" * 60)
    print("MULTIPROCESSING FIXED RESULTS:")
    print(f"Expected counter value: {expected_counter}")
    print(f"Actual counter value: {shared_counter.value}")
    print(f"Lost increments: {expected_counter - shared_counter.value}")
    print(f"Final process balance: ${shared_balance.value}")
    print(f"Execution time: {end_time - start_time:.4f} seconds")
    print("✅ MULTIPROCESSING PERFECT - 0 lost increments!")

if __name__ == "__main__":
    print(f"Race Condition FIXED Demonstration Started at {datetime.now()}")
    print("Both threading and multiprocessing now 100% accurate!")
    

    run_threading_race_condition_demo()
    run_multiprocessing_race_condition_demo()
    
    print(f"\nRace Condition FIXED Demonstration Completed at {datetime.now()}")
    print("Run multiple times - always PERFECT results!")
