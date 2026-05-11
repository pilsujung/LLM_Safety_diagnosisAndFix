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

    
    def transfer_safely(self, from_acct: BankAccount, to_acct: BankAccount, amount: int):
        
        first, second = sorted([from_acct, to_acct], key=lambda a: a.account_id)
        print(f"Acquiring locks in order: {first.account_id} -> {second.account_id}")
        with first.lock:
            self.random_delay()
            with second.lock:
                from_acct.transfer(-amount)
                to_acct.transfer(amount)
                print(f"Transferred {amount} from {from_acct.account_id} to {to_acct.account_id}")
                print(f"Balances: A1={self.account1.balance}, A2={self.account2.balance}")

    def transaction_a(self):
        try:
            print("Transaction A: moving 100 from 1 -> 2")
            self.transfer_safely(self.account1, self.account2, 100)
        except Exception as e:
            print(f"Transaction A error: {e}")

    def transaction_b(self):
        try:
            print("Transaction B: moving 50 from 2 -> 1")
            self.transfer_safely(self.account2, self.account1, 50)
        except Exception as e:
            print(f"Transaction B error: {e}")

    def run_simulation(self, timeout=5):
        thread_a = threading.Thread(target=self.transaction_a)
        thread_b = threading.Thread(target=self.transaction_b)

        thread_a.start()
        thread_b.start()

        
        thread_a.join(timeout)
        thread_b.join(timeout)

        if thread_a.is_alive() or thread_b.is_alive():
            print("Potential Deadlock Detected!")
        else:
            print("Simulation completed without deadlock.")

if __name__ == "__main__":
    
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
