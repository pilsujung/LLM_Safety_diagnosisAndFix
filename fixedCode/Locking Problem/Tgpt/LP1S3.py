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

    def deposit(self, amount, client_id, lock_timeout=5.0):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")


        acquired = self.lock.acquire(timeout=lock_timeout)
        lock_acquired_time = time.time()
        wait_time = lock_acquired_time - start_time

        if not acquired:

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'deposit',
                'amount': amount,
                'old_balance': None,
                'new_balance': None,
                'wait_time': wait_time,
                'success': False,
                'error': 'lock_timeout',
                'timestamp': datetime.now()
            })
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire lock for deposit (waited {wait_time:.3f}s)")
            return wait_time

        try:
            old_balance = self.balance
            new_balance = old_balance + amount
            self.balance = new_balance
        finally:
            self.lock.release()


        time.sleep(random.uniform(0.1, 0.5))


        self.transaction_history.put({
            'client_id': client_id,
            'type': 'deposit',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'wait_time': wait_time,
            'success': True,
            'timestamp': datetime.now()
        })

        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${new_balance} (waited {wait_time:.3f}s)")
        return wait_time

    def withdraw(self, amount, client_id, lock_timeout=5.0):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")

        acquired = self.lock.acquire(timeout=lock_timeout)
        lock_acquired_time = time.time()
        wait_time = lock_acquired_time - start_time

        if not acquired:
            self.transaction_history.put({
                'client_id': client_id,
                'type': 'withdrawal',
                'amount': amount,
                'old_balance': None,
                'new_balance': None,
                'wait_time': wait_time,
                'success': False,
                'error': 'lock_timeout',
                'timestamp': datetime.now()
            })
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire lock for withdrawal (waited {wait_time:.3f}s)")
            return wait_time

        try:
            old_balance = self.balance
            if old_balance >= amount:
                self.balance = old_balance - amount
                success = True
                new_balance = self.balance
            else:
                success = False
                new_balance = old_balance
        finally:
            self.lock.release()


        time.sleep(random.uniform(0.1, 0.5))

        self.transaction_history.put({
            'client_id': client_id,
            'type': 'withdrawal',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'wait_time': wait_time,
            'success': success,
            'timestamp': datetime.now()
        })

        if success:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${new_balance} (waited {wait_time:.3f}s)")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${new_balance} (waited {wait_time:.3f}s)")
        return wait_time

def client_activity(account, client_id, num_transactions):
    wait_times = []
    for _ in range(num_transactions):
        action = random.choice(['deposit', 'withdraw'])
        amount = random.randint(10, 200)
        if action == 'deposit':
            wait_time = account.deposit(amount, client_id)
        else:
            wait_time = account.withdraw(amount, client_id)
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

    deposits = [t for t in transactions if t['type'] == 'deposit' and t.get('success', True)]
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
        t = threading.Thread(target=client_activity, args=(account, i, transactions_per_client), daemon=True)
        threads.append(t)
        t.start()

    for t in threads:
        t.join()

    print("\n===== SIMULATION COMPLETED =====")
    print(f"Final account balance: ${account.balance}")
    analyze_transaction_history(account)

if __name__ == "__main__":
    run_simulation(num_clients=10, transactions_per_client=5)
