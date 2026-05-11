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
        self.simulation_complete = threading.Event()

    def random_delay(self, max_delay=2):
        
        if random.random() < self.deadlock_probability:
            time.sleep(random.uniform(1, max_delay))

    
    def _transfer_between(self, from_acct: BankAccount, to_acct: BankAccount, amount: int, label: str):
        
        first, second = sorted([from_acct, to_acct], key=lambda a: a.account_id)

        print(f"{label}: Attempting to lock Account {first.account_id}")
        with first.lock:
            self.random_delay()
            print(f"{label}: Locked Account {first.account_id}")

            print(f"{label}: Attempting to lock Account {second.account_id}")
            with second.lock:
                
                from_acct.transfer(-amount)
                to_acct.transfer(amount)
                print(f"{label}: Transferred {amount} from {from_acct.account_id} to {to_acct.account_id}")
                print(f"Balances -> A1: {self.account1.balance}, A2: {self.account2.balance}")

    def transaction_a(self):
        try:
            self._transfer_between(self.account1, self.account2, 100, "Transaction A")
        except Exception as e:
            print(f"Transaction A error: {e}")
        finally:
            self.simulation_complete.set()

    def transaction_b(self):
        try:
            self._transfer_between(self.account2, self.account1, 50, "Transaction B")
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

if __name__ == "__main__":
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
