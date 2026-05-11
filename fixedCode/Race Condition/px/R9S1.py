import threading
import multiprocessing
import time
import random
import os
import fcntl
from datetime import datetime


NUM_THREADS = 20
NUM_PROCESSES = 8
MAX_ITERATIONS_PER_WORKER = 5
RANDOM_DELAY_MAX = 0.02
PROCESS_COUNTER_FILE = "shared_process_counter.txt"
PROCESS_BALANCE_FILE = "shared_process_balance.txt"

def simulate_database_operation():
    """Simulate a slow database or I/O operation"""
    time.sleep(random.uniform(0.001, RANDOM_DELAY_MAX))


thread_counter_lock = threading.Lock()
thread_accumulator_lock = threading.Lock()
thread_balance_lock = threading.Lock()
global_thread_counter = 0
global_thread_accumulator = 0
shared_thread_balance = 1000

def safe_thread_increment_counter():
    global global_thread_counter
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        with thread_counter_lock:
            current_counter_value = global_thread_counter
            simulate_database_operation()
            global_thread_counter = current_counter_value + 1
        print(f"Thread {threading.current_thread().name}: "
              f"Read {current_counter_value}, Wrote {global_thread_counter}")

def safe_thread_bank_transaction():
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
    global global_thread_accumulator
    for round_num in range(MAX_ITERATIONS_PER_WORKER):
        random_value = random.randint(1, 100)
        with thread_accumulator_lock:
            current_accumulator = global_thread_accumulator
            simulate_database_operation()
            global_thread_accumulator = current_accumulator + random_value
        print(f"Thread {threading.current_thread().name}: "
              f"Added {random_value} to {current_accumulator}, Result: {global_thread_accumulator}")

def run_threading_fixed_demo():
    global global_thread_counter, global_thread_accumulator, shared_thread_balance
    print("=" * 60)
    print("THREADING FIXED (LOCKS) DEMONSTRATION")
    print("=" * 60)
    

    global_thread_counter = 0
    global_thread_accumulator = 0
    shared_thread_balance = 1000
    
    counter_threads = [threading.Thread(target=safe_thread_increment_counter, 
                                       name=f"CounterThread-{i}") 
                      for i in range(NUM_THREADS // 3)]
    accumulator_threads = [threading.Thread(target=safe_thread_accumulator, 
                                           name=f"AccumulatorThread-{i}") 
                          for i in range(NUM_THREADS // 3)]
    bank_threads = [threading.Thread(target=safe_thread_bank_transaction, 
                                    name=f"BankThread-{i}") 
                   for i in range(NUM_THREADS // 3)]
    
    all_threads = counter_threads + accumulator_threads + bank_threads
    start_time = time.time()
    
    for t in all_threads: t.start()
    for t in all_threads: t.join()
    
    print("-" * 60)
    print("THREADING FIXED RESULTS:")
    expected_counter = (NUM_THREADS // 3) * MAX_ITERATIONS_PER_WORKER
    print(f"Expected counter: {expected_counter}")
    print(f"Actual counter:   {global_thread_counter}")
    print(f"Lost increments:  {expected_counter - global_thread_counter}")
    print(f"Final accumulator: {global_thread_accumulator}")
    print(f"Final balance:    ${shared_thread_balance}")
    print(f"Execution time:   {time.time() - start_time:.4f}s")


def safe_process_increment_counter(shared_counter, lock, process_id):
    for iteration in range(MAX_ITERATIONS_PER_WORKER):
        with lock:
            current_value = shared_counter.value
            simulate_database_operation()
            shared_counter.value += 1
        print(f"Process {process_id}: Read {current_value}, Wrote {shared_counter.value}")

def safe_process_bank_operations(shared_balance, lock, process_id):
    for transaction_num in range(MAX_ITERATIONS_PER_WORKER):
        transaction_amount = random.randint(1, 50)
        is_deposit = random.choice([True, False])
        operation_type = "Deposit" if is_deposit else "Withdrawal"
        
        with lock:
            current_balance = shared_balance.value
            simulate_database_operation()
            if is_deposit:
                shared_balance.value += transaction_amount
            else:
                shared_balance.value = max(0, current_balance - transaction_amount)
            new_balance = shared_balance.value
        print(f"Process {process_id}: {operation_type} ${transaction_amount}, "
              f"Balance: ${current_balance} -> ${new_balance}")

def run_multiprocessing_fixed_demo():
    print("\n" + "=" * 60)
    print("MULTIPROCESSING FIXED (Value+Lock) DEMONSTRATION")
    print("=" * 60)
    
    counter_lock = multiprocessing.Lock()
    balance_lock = multiprocessing.Lock()
    shared_counter = multiprocessing.Value('i', 0)
    shared_balance = multiprocessing.Value('i', 5000)
    
    counter_processes = [multiprocessing.Process(
        target=safe_process_increment_counter,
        args=(shared_counter, counter_lock, f"Counter-{i}"))
        for i in range(NUM_PROCESSES // 2)]
    
    bank_processes = [multiprocessing.Process(
        target=safe_process_bank_operations,
        args=(shared_balance, balance_lock, f"Bank-{i}"))
        for i in range(NUM_PROCESSES // 2)]
    
    all_processes = counter_processes + bank_processes
    start_time = time.time()
    
    for p in all_processes: p.start()
    for p in all_processes: p.join()
    
    print("-" * 60)
    print("MULTIPROCESSING FIXED RESULTS:")
    expected_counter = (NUM_PROCESSES // 2) * MAX_ITERATIONS_PER_WORKER
    print(f"Expected counter: {expected_counter}")
    print(f"Actual counter:   {shared_counter.value}")
    print(f"Lost increments:  {expected_counter - shared_counter.value}")
    print(f"Final balance:    ${shared_balance.value}")
    print(f"Execution time:   {time.time() - start_time:.4f}s")

if __name__ == "__main__":
    print(f"Fixed Race Condition Demo Started at {datetime.now()}")
    run_threading_fixed_demo()
    run_multiprocessing_fixed_demo()
    print(f"Completed at {datetime.now()}")
