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
            time.sleep(random.uniform(0.1, max_delay))

    @contextmanager
    def _acquire_two(self, a: BankAccount, b: BankAccount):
        """
        Acquire two account locks in a globally consistent order
        (by account_id) to prevent deadlocks.
        """
        first, second = (a, b) if a.account_id < b.account_id else (b, a)
        first.lock.acquire()
        try:
            
            self.random_delay()
            second.lock.acquire()
            try:
                yield
            finally:
                second.lock.release()
        finally:
            first.lock.release()

    def transfer_between(self, src: BankAccount, dst: BankAccount, amount: int):
        with self._acquire_two(src, dst):
            src.transfer(-amount)
            dst.transfer(amount)

    def transaction_a(self):
        try:
            print("Transaction A: Starting")
            self.transfer_between(self.account1, self.account2, 100)
            print("Transaction A: Transferred 100")
        except Exception as e:
            print(f"Transaction A error: {e}")
        finally:
            self.simulation_complete.set()

    def transaction_b(self):
        try:
            print("Transaction B: Starting")
            self.transfer_between(self.account2, self.account1, 50)
            print("Transaction B: Transferred 50")
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

        print(f"Final balances: A1={self.account1.balance}, A2={self.account2.balance}")

if __name__ == "__main__":
    
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
