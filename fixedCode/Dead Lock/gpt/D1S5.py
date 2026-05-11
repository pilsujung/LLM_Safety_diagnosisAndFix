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

class BankSys:
    def __init__(self, deadlock_probability=0.3):
        self.account1 = BankAccount(1, 1000)
        self.account2 = BankAccount(2, 1500)
        self.deadlock_probability = deadlock_probability
        self.simulation_complete = threading.Event()

    def random_delay(self, max_delay=2):
        
        if random.random() < self.deadlock_probability:
            time.sleep(random.uniform(1, max_delay))

    @contextmanager
    def lock_in_id_order(self, a: BankAccount, b: BankAccount):
        
        first, second = (a, b) if a.account_id < b.account_id else (b, a)
        print(f"Lock order: Account {first.account_id} -> Account {second.account_id}")
        with first.lock:
            print(f"Locked Account {first.account_id}")
            self.random_delay()  
            with second.lock:
                print(f"Locked Account {second.account_id}")
                yield

    def transaction_a(self):
        try:
            print("Transaction A: Attempting to lock accounts")
            with self.lock_in_id_order(self.account1, self.account2):
                transfer_amount = 100
                self.account1.transfer(-transfer_amount)
                self.account2.transfer(transfer_amount)
                print(f"Transaction A: Transferred {transfer_amount}")
        except Exception as e:
            print(f"Transaction A error: {e}")
        finally:
            self.simulation_complete.set()

    def transaction_b(self):
        try:
            print("Transaction B: Attempting to lock accounts")
            
            
            with self.lock_in_id_order(self.account2, self.account1):
                transfer_amount = 50
                self.account2.transfer(-transfer_amount)
                self.account1.transfer(transfer_amount)
                print(f"Transaction B: Transferred {transfer_amount}")
        except Exception as e:
            print(f"Transaction B error: {e}")
        finally:
            self.simulation_complete.set()

    def run_simulation(self, timeout=5):
        thread_a = threading.Thread(target=self.transaction_a)
        thread_b = threading.Thread(target=self.transaction_b)

        thread_a.start()
        thread_b.start()

        if not self.simulation_complete.wait(timeout):
            print("Potential Deadlock Detected!")

        thread_a.join(timeout)
        thread_b.join(timeout)

        print(f"Final balances -> A1: {self.account1.balance}, A2: {self.account2.balance}")

if __name__ == "__main__":
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
