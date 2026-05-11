import threading
import time
import random


class BankAccount:
    def __init__(self, account_id, balance):
        self.account_id = account_id
        self.balance = balance
        self.lock = threading.Lock()                           

    def withdraw(self, amount):
        with self.lock:                          
            current_balance = self.balance
            time.sleep(0.001)                  

            if current_balance >= amount:
                self.balance = current_balance - amount
                return True
            return False

    def deposit(self, amount):
        with self.lock:                         
            current_balance = self.balance
            time.sleep(0.001)                  
            self.balance = current_balance + amount
            return True


def transfer(from_account, to_account, amount):
    """Transfer money between accounts safely"""
                                     
    first, second = (from_account, to_account) if from_account.account_id < to_account.account_id else (to_account, from_account)

    with first.lock:                      
        with second.lock:
            if from_account.withdraw(amount):
                to_account.deposit(amount)
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
    print(f"Total money in system: ${account_a.balance + account_b.balance}\n")

    threads = []

    for i in range(5):
        threads.append(threading.Thread(target=run_transfers, args=(account_a, account_b, 20)))
    for i in range(5):
        threads.append(threading.Thread(target=run_transfers, args=(account_b, account_a, 20)))

    for t in threads:
        t.start()
    for t in threads:
        t.join()

    print("All transfers completed!")
    print(f"Final balances:")
    print(f"Account A: ${account_a.balance}")
    print(f"Account B: ${account_b.balance}")
    print(f"Total money in system: ${account_a.balance + account_b.balance}")

    initial_total = 2000
    final_total = account_a.balance + account_b.balance

    if final_total != initial_total:
        print(f"\n⚠️ ATOMIC VIOLATION DETECTED! ⚠️")
        print(f"Expected total: ${initial_total}, Actual total: ${final_total}")
    else:
        print("\nSystem integrity maintained ✅")

    if account_a.balance < 0 or account_b.balance < 0:
        print("\n⚠️ NEGATIVE BALANCE DETECTED! ⚠️")
        if account_a.balance < 0:
            print(f"Account A: ${account_a.balance}")
        if account_b.balance < 0:
            print(f"Account B: ${account_b.balance}")


if __name__ == "__main__":
    main()
