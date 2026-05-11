import threading
import multiprocessing
import time
import random
import os
from datetime import datetime


thread_counter_lock = threading.Lock()
thread_accumulator_lock = threading.Lock()
thread_balance_lock = threading.Lock()

NUM_THREADS = 20
NUM_PROCESSES = 8
MAX_ITERATIONS_PER_WORKER = 5
RANDOM_DELAY_MAX = 0.02
PROCESS_COUNTER_FILE = "shared_process_counter.txt"
PROCESS_BALANCE_FILE = "shared_process_balance.txt"

def simulate_database_operation():
    time.sleep(random.uniform(0.001, RANDOM_DELAY_MAX))


def safe_thread_increment_counter():
    global global_thread_counter
    for _ in range(MAX_ITERATIONS_PER_WORKER):
        with thread_counter_lock:
            current = global_thread_counter
            global_thread_counter += 1
        print(f"Thread {threading.current_thread().name}: Read {current}, Wrote {current+1}")

def safe_thread_accumulator():
    global global_thread_accumulator
    for _ in range(MAX_ITERATIONS_PER_WORKER):
        value = random.randint(1, 100)
        with thread_accumulator_lock:
            global_thread_accumulator += value
        print(f"Thread {threading.current_thread().name}: Added {value}")

def safe_thread_bank_transaction():
    global shared_thread_balance
    for _ in range(MAX_ITERATIONS_PER_WORKER):
        amount = random.randint(1, 10)
        with thread_balance_lock:
            if shared_thread_balance >= amount:
                shared_thread_balance -= amount
                print(f"Thread {threading.current_thread().name}: Withdrew ${amount}, Balance: ${shared_thread_balance}")


def safe_process_increment_counter(process_id, counter_file):


def safe_process_bank_operations(process_id, balance_file):
    for _ in range(MAX_ITERATIONS_PER_WORKER):
        try:
            amount = random.randint(1, 50)
            is_deposit = random.choice([True, False])
            with open(balance_file, "r+") as file:
                file.seek(0)
                current = int(file.read().strip())
                file.seek(0)
                file.truncate()
                if is_deposit:
                    new_balance = current + amount
                    op = "Deposit"
                else:
                    new_balance = max(0, current - amount)
                    op = "Withdrawal"
                file.write(str(new_balance))
                file.flush()
                os.fsync(file.fileno())
            print(f"Process {process_id}: {op} ${amount}, Balance: ${current} -> ${new_balance}")
        except Exception as e:
            print(f"Process {process_id}: Error - {e}")




if __name__ == "__main__":
    global global_thread_counter, global_thread_accumulator, shared_thread_balance
    global_thread_counter = global_thread_accumulator = 0
    shared_thread_balance = 1000
    run_threading_race_condition_demo()
    run_multiprocessing_race_condition_demo()
