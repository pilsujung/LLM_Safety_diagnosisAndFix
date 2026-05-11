import threading
import time
import random
import queue
from datetime import datetime

class BankAccount:
    def __init__(self, balance=1000):
        self.balance = balance
        self.lock = threading.Lock()
        self.transaction_history = queue.Queue()

    def deposit(self, amount, client_id):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")

        acquired = self.lock.acquire(timeout=1)
        if not acquired:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire the lock, moving on")
            return None

        try:
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")

            time.sleep(random.uniform(0.1, 0.5))

            old_balance = self.balance
            self.balance += amount

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'deposit',
                'amount': amount,
                'old_balance': old_balance,
                'new_balance': self.balance,
                'wait_time': wait_time,
                'timestamp': datetime.now()
            })

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${self.balance}")

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
            return wait_time
        finally:
            self.lock.release()

    def withdraw(self, amount, client_id):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")

        acquired = self.lock.acquire(timeout=1)
        if not acquired:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire the lock, moving on")
            return None

        try:
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")

            time.sleep(random.uniform(0.1, 0.5))

            old_balance = self.balance
            if self.balance >= amount:
                self.balance -= amount
                success = True
            else:
                success = False

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'withdrawal',
                'amount': amount,
                'old_balance': old_balance,
                'new_balance': self.balance if success else old_balance,
                'success': success,
                'wait_time': wait_time,
                'timestamp': datetime.now()
            })

            if success:
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${self.balance}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${self.balance}")

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
            return wait_time
        finally:
            self.lock.release()

def client_activity(account, client_id, num_transactions):
    wait_times = []
    for _ in range(num_transactions):
        action = random.choice(['deposit', 'withdraw'])
        amount = random.randint(10, 200)

        if action == 'deposit':
            wait_time = account.deposit(amount, client_id)
        else:
            wait_time = account.withdraw(amount, client_id)

        if wait_time is not None:
            wait_times.append(wait_time)

        time.sleep(random.uniform(0.1, 1.0))

    return wait_times

def analyze_transaction_history(account):
    print("\n===== TRANSACTION ANALYSIS =====")

    transactions = []
    while not account.transaction_history.empty():
        transactions.append(account.transaction_history.get())

    total_transactions = len(transactions)
    if total_transactions == 0:
        print("No transactions recorded.")
        return

    total_wait_time = sum(t['wait_time'] for t in transactions)
    avg_wait_time = total_wait_time / total_transactions
    max_wait_time = max(t['wait_time'] for t in transactions)
    min_wait_time = min(t['wait_time'] for t in transactions)

    print(f"Total transactions: {total_transactions}")
    print(f"Average wait time: {avg_wait_time:.3f} seconds")
    print(f"Maximum wait time: {max_wait_time:.3f} seconds")
    print(f"Minimum wait time: {min_wait_time:.3f} seconds")

    deposits = [t for t in transactions if t['type'] == 'deposit']
    withdrawals = [t for t in transactions if t['type'] == 'withdrawal']

    if deposits:
        avg_deposit_wait = sum(t['wait_time'] for t in deposits) / len(deposits)
        print(f"Average wait time for deposits: {avg_deposit_wait:.3f} seconds")

    if withdrawals:
        avg_withdrawal_wait = sum(t['wait_time'] for t in withdrawals) / len(withdrawals)
        print(f"Average wait time for withdrawals: {avg_withdrawal_wait:.3f} seconds")

    client_ids = set(t['client_id'] for t in transactions)
    for client_id in client_ids:
        client_txns = [t for t in transactions if t['client_id'] == client_id]
        avg_client_wait = sum(t['wait_time'] for t in client_txns) / len(client_txns)
        print(f"Client {client_id}: {len(client_txns)} transactions, avg wait {avg_client_wait:.3f} seconds")

def run_simulation(num_clients, transactions_per_client):
    print("===== STARTING BANK ACCOUNT SIMULATION =====")
    print(f"Number of clients: {num_clients}")
    print(f"Transactions per client: {transactions_per_client}")
    print("============================================\n")

    account = BankAccount()
    threads = []
    for i in range(1, num_clients + 1):
        t = threading.Thread(target=client_activity, args=(account, i, transactions_per_client))
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n===== SIMULATION COMPLETED =====")
    print(f"Final account balance: ${account.balance}")

    analyze_transaction_history(account)

if __name__ == "__main__":
    run_simulation(num_clients=10, transactions_per_client=5)
