import threading
import time
import random
from contextlib import contextmanager

class BankAccount:
    def __init__(self, account_id, balance):
        self.account_id = account_id
        self.balance = balance
        self.lock = threading.Lock()

    def transfer(self, amount):
        self.balance += amount

@contextmanager
def acquire_two_locks(acc_x, acc_y):
    """
    Acquire two account locks in a consistent global order
    (smallest account_id first) to prevent circular wait.
    """
    first, second = (acc_x, acc_y) if acc_x.account_id < acc_y.account_id else (acc_y, acc_x)

    print(f"Acquiring first lock: Account {first.account_id}")
    first.lock.acquire()
    try:
        print(f"Acquiring second lock: Account {second.account_id}")
        second.lock.acquire()
        try:
            yield
        finally:
            print(f"Releasing lock: Account {second.account_id}")
            second.lock.release()
    finally:
        print(f"Releasing lock: Account {first.account_id}")
        first.lock.release()

class BankSys:
    def __init__(self, deadlock_probability=0.3):
        self.account1 = BankAccount(1, 1000)
        self.account2 = BankAccount(2, 1500)
        self.deadlock_probability = deadlock_probability

    def random_delay(self, max_delay=2):
        
        if random.random() < self.deadlock_probability:
            time.sleep(random.uniform(0.1, max_delay))

    def transaction_a(self):
        try:
            print("Transaction A: start (A1 -> A2)")
            self.random_delay()
            with acquire_two_locks(self.account1, self.account2):
                transfer_amount = 100
                self.account1.transfer(-transfer_amount)
                self.account2.transfer(transfer_amount)
                print(f"Transaction A: Transferred {transfer_amount}")
        except Exception as e:
            print(f"Transaction A error: {e}")

    def transaction_b(self):
        try:
            print("Transaction B: start (A2 -> A1)")
            self.random_delay()
            
            with acquire_two_locks(self.account2, self.account1):
                transfer_amount = 50
                self.account2.transfer(-transfer_amount)
                self.account1.transfer(transfer_amount)
                print(f"Transaction B: Transferred {transfer_amount}")
        except Exception as e:
            print(f"Transaction B error: {e}")

    def run_simulation(self, timeout=5):
        thread_a = threading.Thread(target=self.transaction_a, name="T-A")
        thread_b = threading.Thread(target=self.transaction_b, name="T-B")

        thread_a.start()
        thread_b.start()

        thread_a.join(timeout)
        thread_b.join(timeout)

        if thread_a.is_alive() or thread_b.is_alive():
            print("Potential Deadlock Detected!")  
        else:
            print(f"Final balances: A1={self.account1.balance}, A2={self.account2.balance}")

if __name__ == "__main__":
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
