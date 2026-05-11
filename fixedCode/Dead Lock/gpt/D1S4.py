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
            time.sleep(random.uniform(0.1, max_delay))

    def _ordered_transfer(self, src: BankAccount, dst: BankAccount, amount: int, label: str):
        """Acquire both account locks in a consistent global order (by account_id)."""
        a, b = (src, dst) if src.account_id < dst.account_id else (dst, src)

        print(f"{label}: Attempting to lock Account {a.account_id}")
        with a.lock:
            self.random_delay()
            print(f"{label}: Locked Account {a.account_id}")

            print(f"{label}: Attempting to lock Account {b.account_id}")
            with b.lock:
                print(f"{label}: Locked Account {b.account_id}")
                src.transfer(-amount)
                dst.transfer(amount)
                print(f"{label}: Transferred {amount} from {src.account_id} → {dst.account_id}. "
                      f"Balances: A1={self.account1.balance}, A2={self.account2.balance}")

    def transaction_a(self):
        try:
            self._ordered_transfer(self.account1, self.account2, 100, "Transaction A")
        except Exception as e:
            print(f"Transaction A error: {e}")

    def transaction_b(self):
        try:
            self._ordered_transfer(self.account2, self.account1, 50, "Transaction B")
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
            print("Potential Deadlock Detected (threads still alive after timeout).")
        else:
            print("Simulation completed without deadlock.")

if __name__ == "__main__":
    for i in range(5):
        print(f"\n--- Simulation {i+1} ---")
        simulation = BankSys(deadlock_probability=0.4)
        simulation.run_simulation()
