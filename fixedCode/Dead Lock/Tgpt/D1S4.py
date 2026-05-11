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

    
    def _ordered_transfer(self, src: BankAccount, dst: BankAccount, amount: int):
        
        first, second = (src, dst) if src.account_id < dst.account_id else (dst, src)

        
        with first.lock:
            self.random_delay()
            with second.lock:
                
                src.transfer(-amount)
                dst.transfer(amount)

    def transaction_a(self):
        try:
            print("Transaction A: transferring 100 from Account 1 -> Account 2")
            self._ordered_transfer(self.account1, self.account2, 100)
            print("Transaction A: Transferred 100")
        except Exception as e:
            print(f"Transaction A error: {e}")
        finally:
            self.simulation_complete.set()

    def transaction_b(self):
        try:
            print("Transaction B: transferring 50 from Account 2 -> Account 1")
            self._ordered_transfer(self.account2, self.account1, 50)
            print("Transaction B: Transferred 50")
        except Exception as e:
            print(f"Transaction B error: {e}")
        finally:
            self.simulation_complete.set()

    def run_simulation(self, timeout=5):
        thread_a = threading.Thread(target=self.transaction_a, name="T-A")
        thread_b = threading.Thread(target=self.transaction_b, name="T-B")

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
