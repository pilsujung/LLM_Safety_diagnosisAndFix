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

    def ordered_transfer(self, from_account, to_account, amount):
        """
        Transfer money between accounts using consistent lock ordering.
        Always acquire locks in order of account_id to prevent deadlock.
        """
        
        if from_account.account_id < to_account.account_id:
            first_lock = from_account
            second_lock = to_account
        else:
            first_lock = to_account
            second_lock = from_account
        
        try:
            print(f"Attempting to lock Account {first_lock.account_id}")
            with first_lock.lock:
                self.random_delay()
                print(f"Locked Account {first_lock.account_id}")
                
                print(f"Attempting to lock Account {second_lock.account_id}")
                with second_lock.lock:
                    print(f"Locked Account {second_lock.account_id}")
                    from_account.transfer(-amount)
                    to_account.transfer(amount)
                    print(f"Transferred {amount} from Account {from_account.account_id} to Account {to_account.account_id}")
        except Exception as e:
            print(f"Transfer error: {e}")

    def transaction_a(self):
        try:
            print("Transaction A: Starting transfer from Account 1 to Account 2")
            self.ordered_transfer(self.account1, self.account2, 100)
        except Exception as e:
            print(f"Transaction A error: {e}")
        finally:
            self.simulation_complete.set()

    def transaction_b(self):
        try:
            print("Transaction B: Starting transfer from Account 2 to Account 1")
            self.ordered_transfer(self.account2, self.account1, 50)
        except Exception as e:
            print(f"Transaction B error: {e}")
        finally:
            self.simulation_complete.set()

    def run_simulation(self, timeout=5):
        
        self.simulation_complete.clear()
        
        thread_a = threading.Thread(target=self.transaction_a)
        thread_b = threading.Thread(target=self.transaction_b)

        thread_a.start()
        thread_b.start()

        
        if not self.simulation_complete.wait(timeout):
            print("Potential Deadlock Detected!")

        thread_a.join(timeout)
        thread_b.join(timeout)
        
        
        print(f"Final balance - Account 1: {self.account1.balance}, Account 2: {self.account2.balance}")

if __name__ == "__main__":
    
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()