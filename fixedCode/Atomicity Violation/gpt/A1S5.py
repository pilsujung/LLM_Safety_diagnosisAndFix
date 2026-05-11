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

    def _deposit_nolock(self, amount):
                                         
        self.balance += amount

    def _withdraw_nolock(self, amount):
                                         
        if self.balance >= amount:
            self.balance -= amount
            return True
        return False

                                                                 
    def deposit(self, amount):
        with self._lock:
            time.sleep(0.001)                 
            self._deposit_nolock(amount)
            return True

    def withdraw(self, amount):
        with self._lock:
            time.sleep(0.001)                 
            return self._withdraw_nolock(amount)


def transfer(from_account, to_account, amount):
    """
    Atomically transfer money between accounts.
    Acquire both locks in a fixed, consistent order to prevent deadlock,
    then perform debit and credit as one critical section.
    """
                                                                  
    first, second = sorted([from_account, to_account], key=lambda a: a.account_id)

    with first._lock:
        with second._lock:
                                                               
            time.sleep(0.001)

            if from_account._withdraw_nolock(amount):
                                                              
                time.sleep(0.001)
                to_account._deposit_nolock(amount)
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
    total = a_bal + b_bal

    print("All transfers completed!")
    print(f"Final balances:")
    print(f"Account A: ${a_bal}")
    print(f"Account B: ${b_bal}")
    print(f"Total money in system: ${total}")

    initial_total = 2000
    if total != initial_total:
        print(f"\n⚠️ ATOMIC VIOLATION DETECTED! ⚠️")
        print(f"Expected total: ${initial_total}, Actual total: ${total}, Discrepancy: ${total - initial_total}")
    else:
        print("\nSystem integrity maintained ✅ (atomic transfers with locks)")


if __name__ == "__main__":
    main()
