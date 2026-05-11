import threading
import time
import random

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

    def random_delay(self, max_delay=2):
        
        if random.random() < self.deadlock_probability:
            time.sleep(random.uniform(1, max_delay))

    def _two_account_transfer(self, from_acc, to_acc, amount):
        """
        Perform a transfer while acquiring locks in a globally consistent order
        (by account_id) to avoid deadlocks.
        """
        
        first, second = sorted([from_acc, to_acc], key=lambda a: a.account_id)

        print(f"Locking order -> first: Account {first.account_id}, second: Account {second.account_id}")

        
        with first.lock:
            print(f"Locked Account {first.account_id}")
            self.random_delay()
            with second.lock:
                print(f"Locked Account {second.account_id}")
                from_acc.transfer(-amount)
                to_acc.transfer(amount)
                print(f"Transferred {amount} from Account {from_acc.account_id} to Account {to_acc.account_id}")

    def transaction_a(self):
        try:
            print("Transaction A: start")
            self._two_account_transfer(self.account1, self.account2, 100)
        except Exception as e:
            print(f"Transaction A error: {e}")

    def transaction_b(self):
        try:
            print("Transaction B: start")
            self._two_account_transfer(self.account2, self.account1, 50)
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
            print("Potential Deadlock Detected! (should not occur with ordered locking)")
        else:
            print("Simulation completed without deadlock.")
            print(f"Final balances -> A1: {self.account1.balance}, A2: {self.account2.balance}")

if __name__ == "__main__":
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
