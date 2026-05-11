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
        self.completed_transactions = 0
        self.completion_lock = threading.Lock()

    def random_delay(self, max_delay=2):
        
        if random.random() < self.deadlock_probability:
            time.sleep(random.uniform(1, max_delay))

    def acquire_locks_ordered(self, account_from, account_to):
        """Acquire locks in a consistent order based on account_id to prevent deadlock"""
        if account_from.account_id < account_to.account_id:
            first_lock, second_lock = account_from.lock, account_to.lock
        else:
            first_lock, second_lock = account_to.lock, account_from.lock
        
        return first_lock, second_lock

    def transfer_between_accounts(self, from_account, to_account, amount, transaction_name):
        """Perform transfer between accounts using ordered locking"""
        try:
            print(f"{transaction_name}: Starting transfer of {amount}")
            
            
            first_lock, second_lock = self.acquire_locks_ordered(from_account, to_account)
            
            print(f"{transaction_name}: Attempting to acquire first lock")
            with first_lock:
                print(f"{transaction_name}: Acquired first lock")
                self.random_delay()
                
                print(f"{transaction_name}: Attempting to acquire second lock")
                with second_lock:
                    print(f"{transaction_name}: Acquired both locks")
                    
                    from_account.transfer(-amount)
                    to_account.transfer(amount)
                    print(f"{transaction_name}: Successfully transferred {amount}")
                    print(f"{transaction_name}: Account {from_account.account_id} balance: {from_account.balance}")
                    print(f"{transaction_name}: Account {to_account.account_id} balance: {to_account.balance}")
                    
        except Exception as e:
            print(f"{transaction_name} error: {e}")
        finally:
            
            with self.completion_lock:
                self.completed_transactions += 1
                if self.completed_transactions >= 2:
                    self.simulation_complete.set()

    def transaction_a(self):
        self.transfer_between_accounts(self.account1, self.account2, 100, "Transaction A")

    def transaction_b(self):
        self.transfer_between_accounts(self.account2, self.account1, 50, "Transaction B")

    def run_simulation(self, timeout=5):
        
        self.completed_transactions = 0
        self.simulation_complete.clear()
        
        print(f"Initial balances - Account 1: {self.account1.balance}, Account 2: {self.account2.balance}")
        
        thread_a = threading.Thread(target=self.transaction_a)
        thread_b = threading.Thread(target=self.transaction_b)

        start_time = time.time()
        thread_a.start()
        thread_b.start()

        
        if not self.simulation_complete.wait(timeout):
            print("Timeout reached - possible deadlock!")
        else:
            elapsed_time = time.time() - start_time
            print(f"Simulation completed successfully in {elapsed_time:.2f} seconds")

        thread_a.join(timeout)
        thread_b.join(timeout)
        
        print(f"Final balances - Account 1: {self.account1.balance}, Account 2: {self.account2.balance}")

if __name__ == "__main__":
    
    print("Running deadlock-free bank transfer simulations...")
    for i in range(5):
        print(f"\n{'='*50}")
        print(f"--- Simulation {i+1} ---")
        print(f"{'='*50}")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
        time.sleep(0.5)  
    
    print(f"\n{'='*50}")
    print("All simulations completed without deadlock!")