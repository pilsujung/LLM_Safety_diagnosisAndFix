import threading
import time
import random

class BankAccount:
    def __init__(self, account_id, balance):
        self.account_id = account_id
        self.balance = balance
        self._lock = threading.RLock()                                       

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

def _lock_in_order(a: BankAccount, b: BankAccount):
    """
    Context manager that acquires both account locks in a stable order
    to prevent deadlocks.
    """
    first, second = (a, b) if a.account_id <= b.account_id else (b, a)
    class _DoubleLock:
        def __enter__(self_inner):
            first._lock.acquire()
            second._lock.acquire()
        def __exit__(self_inner, exc_type, exc, tb):
            second._lock.release()
            first._lock.release()
    return _DoubleLock()

def transfer(from_account: BankAccount, to_account: BankAccount, amount: int):
    """Transfer money atomically between accounts."""
                                                                                  
    with _lock_in_order(from_account, to_account):
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
    account_a = BankAccount("A", 1000)
    account_b = BankAccount("B", 1000)

    print(f"Initial balances:")
    print(f"Account A: ${account_a.balance}")
    print(f"Account B: ${account_b.balance}")
    print(f"Total money in system: ${account_a.balance + account_b.balance}")
    print("\nStarting transfers...\n")

    threads = []
    for _ in range(5):
        threads.append(threading.Thread(target=run_transfers, args=(account_a, account_b, 20)))
    for _ in range(5):
        threads.append(threading.Thread(target=run_transfers, args=(account_b, account_a, 20)))

    for t in threads: t.start()
    for t in threads: t.join()

    print("All transfers completed!")
    print("Final balances:")
    print(f"Account A: ${account_a.balance}")
    print(f"Account B: ${account_b.balance}")
    final_total = account_a.balance + account_b.balance
    print(f"Total money in system: ${final_total}")

    initial_total = 2000
    if final_total != initial_total:
        print(f"\n⚠️ ATOMIC VIOLATION DETECTED! ⚠️")
        print(f"Expected total: ${initial_total}, Actual total: ${final_total}")
    else:
        print("\nSystem integrity maintained ✅")

    if account_a.balance < 0 or account_b.balance < 0:
        print(f"\n⚠️ NEGATIVE BALANCE DETECTED! ⚠️")
        if account_a.balance < 0:
            print(f"Account A has gone negative: ${account_a.balance}")
        if account_b.balance < 0:
            print(f"Account B has gone negative: ${account_b.balance}")

if __name__ == "__main__":
    main()
