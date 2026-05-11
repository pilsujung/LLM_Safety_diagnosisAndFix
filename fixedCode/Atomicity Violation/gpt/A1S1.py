import threading
import time
import random

class BankAccount:
    def __init__(self, account_id, balance):
        self.account_id = account_id
        self.balance = balance
        self._lock = threading.Lock()

    def get_balance(self):
        with self._lock:
            return self.balance

                                              
    def withdraw(self, amount):
        with self._lock:
            current_balance = self.balance
            time.sleep(0.001)                 
            if current_balance >= amount:
                self.balance = current_balance - amount
                return True
            return False

    def deposit(self, amount):
        with self._lock:
            current_balance = self.balance
            time.sleep(0.001)                 
            self.balance = current_balance + amount
            return True


def transfer(from_account, to_account, amount):
    """
    Transfer money atomically between accounts by locking both.
    We acquire locks in a globally consistent order (by account_id)
    to prevent deadlocks when multiple threads do cross-transfers.
    """
                                         
    first, second = (
        (from_account, to_account)
        if from_account.account_id < to_account.account_id
        else (to_account, from_account)
    )

                        
    with first._lock:
        with second._lock:
                                                                  
            if from_account.balance >= amount:
                                                                      
                time.sleep(0.001)
                from_account.balance -= amount
                to_account.balance += amount
                return True
            return False


def run_transfers(from_account, to_account, num_transfers):
    for _ in range(num_transfers):
        amount = random.randint(10, 50)
        transfer(from_account, to_account, amount)


def main():
    random.seed(42)                             

    account_a = BankAccount("A", 1000)
    account_b = BankAccount("B", 1000)

    print(f"Initial balances:")
    print(f"Account A: ${account_a.get_balance()}")
    print(f"Account B: ${account_b.get_balance()}")
    print(f"Total money in system: ${account_a.get_balance() + account_b.get_balance()}")
    print("\nStarting transfers...\n")

    threads = []
    for _ in range(5):
        threads.append(threading.Thread(target=run_transfers, args=(account_a, account_b, 20)))
    for _ in range(5):
        threads.append(threading.Thread(target=run_transfers, args=(account_b, account_a, 20)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    a_bal = account_a.get_balance()
    b_bal = account_b.get_balance()
    final_total = a_bal + b_bal

    print("All transfers completed!")
    print("Final balances:")
    print(f"Account A: ${a_bal}")
    print(f"Account B: ${b_bal}")
    print(f"Total money in system: ${final_total}")

    initial_total = 2000
    if final_total != initial_total:
        print("\n⚠️ ATOMIC VIOLATION DETECTED! ⚠️")
        print(f"Expected total: ${initial_total}, Actual total: ${final_total}")
    else:
        print("\nSystem integrity maintained ✅")

    if a_bal < 0 or b_bal < 0:
        print("\n⚠️ NEGATIVE BALANCE DETECTED! ⚠️")

if __name__ == "__main__":
    main()
