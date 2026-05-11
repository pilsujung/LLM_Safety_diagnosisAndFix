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

    def _withdraw_unlocked(self, amount):
                                         
        if self.balance >= amount:
                                           
            time.sleep(0.001)
            self.balance -= amount
            return True
        return False

    def _deposit_unlocked(self, amount):
                                         
        time.sleep(0.001)
        self.balance += amount
        return True


def transfer(from_account, to_account, amount):
    """        """
                             
    first, second = (
        (from_account, to_account)
        if from_account.account_id <= to_account.account_id
        else (to_account, from_account)
    )

    with first._lock:
        with second._lock:
                                 
            if from_account._withdraw_unlocked(amount):
                to_account._deposit_unlocked(amount)
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
    print("Final balances:")
    print(f"Account A: ${a_bal}")
    print(f"Account B: ${b_bal}")
    print(f"Total money in system: ${total}")

    initial_total = 2000
    if total != initial_total:
        print(f"\n⚠️ ATOMIC VIOLATION DETECTED! ⚠️")
        print(f"Expected total: ${initial_total}, Actual total: ${total}, Δ={total - initial_total}")
    else:
        print("\nSystem integrity maintained ✅")

    if a_bal < 0 or b_bal < 0:
        print(f"\n⚠️ NEGATIVE BALANCE DETECTED! ⚠️")
        if a_bal < 0:
            print(f"Account A has gone negative: ${a_bal}")
        if b_bal < 0:
            print(f"Account B has gone negative: ${b_bal}")


if __name__ == "__main__":
    main()
