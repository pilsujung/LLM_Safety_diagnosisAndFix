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

    def acquire_locks_ordered(self, account_a, account_b):
        """Acquire locks in consistent order based on account ID to prevent deadlock"""
        if account_a.account_id < account_b.account_id:
            first_account, second_account = account_a, account_b
        else:
            first_account, second_account = account_b, account_a
        
        return first_account.lock, second_account.lock

    def transaction_a(self):
        try:
            print("Transaction A: Attempting to acquire locks")
            first_lock, second_lock = self.acquire_locks_ordered(self.account1, self.account2)
            
            with first_lock:
                self.random_delay()
                print("Transaction A: Acquired first lock")
                
                with second_lock:
                    print("Transaction A: Acquired second lock")
                    transfer_amount = 100
                    self.account1.transfer(-transfer_amount)
                    self.account2.transfer(transfer_amount)
                    print(f"Transaction A: Transferred {transfer_amount} from Account 1 to Account 2")
                    print(f"Transaction A: Account 1 balance: {self.account1.balance}, Account 2 balance: {self.account2.balance}")
        except Exception as e:
            print(f"Transaction A error: {e}")
        finally:
            self.simulation_complete.set()

    def transaction_b(self):
        try:
            print("Transaction B: Attempting to acquire locks")
            first_lock, second_lock = self.acquire_locks_ordered(self.account2, self.account1)
            
            with first_lock:
                self.random_delay()
                print("Transaction B: Acquired first lock")
                
                with second_lock:
                    print("Transaction B: Acquired second lock")
                    transfer_amount = 50
                    self.account2.transfer(-transfer_amount)
                    self.account1.transfer(transfer_amount)
                    print(f"Transaction B: Transferred {transfer_amount} from Account 2 to Account 1")
                    print(f"Transaction B: Account 1 balance: {self.account1.balance}, Account 2 balance: {self.account2.balance}")
        except Exception as e:
            print(f"Transaction B error: {e}")
        finally:
            self.simulation_complete.set()

    def run_simulation(self, timeout=5):
        print(f"Initial balances - Account 1: {self.account1.balance}, Account 2: {self.account2.balance}")
        
        thread_a = threading.Thread(target=self.transaction_a)
        thread_b = threading.Thread(target=self.transaction_b)

        thread_a.start()
        thread_b.start()

        
        if not self.simulation_complete.wait(timeout):
            print("Timeout occurred - but this should not be a deadlock anymore!")
        else:
            print("Simulation completed successfully!")

        thread_a.join(timeout)
        thread_b.join(timeout)

if __name__ == "__main__":
    
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
        print("=" * 50)