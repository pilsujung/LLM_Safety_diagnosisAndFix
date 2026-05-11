import threading
import time
import random
import queue
from datetime import datetime

_now_ts = lambda: datetime.now().strftime('%H:%M:%S.%f')[:-3]

class BankAccount:
    def __init__(self, balance=1000, lock_timeout=5.0):
        self.balance = balance
        self.lock = threading.Lock()
        self.transaction_history = queue.Queue()
        self.lock_timeout = lock_timeout

    def _acquire_with_timeout(self):
        start = time.perf_counter()
        acquired = self.lock.acquire(timeout=self.lock_timeout)
        waited = time.perf_counter() - start
        return acquired, waited

    def deposit(self, amount, client_id):
        print(f"[{_now_ts()}] Client {client_id} waiting to deposit ${amount}...")
        acquired, wait_time = self._acquire_with_timeout()
        if not acquired:
            msg = f"[{_now_ts()}] Client {client_id} deposit ${amount} timed out waiting for lock after {wait_time:.3f}s"
            print(msg)

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'deposit',
                'amount': amount,
                'old_balance': self.balance,
                'new_balance': self.balance,
                'wait_time': wait_time,
                'timeout': True,
                'timestamp': datetime.now(),
            })
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
            'timeout': False,
            'timestamp': datetime.now(),
        })
        print(f"[{_now_ts()}] Client {client_id} deposited ${amount}. New balance: ${new_balance}")
        return wait_time

    def withdraw(self, amount, client_id):
        print(f"[{_now_ts()}] Client {client_id} waiting to withdraw ${amount}...")
        acquired, wait_time = self._acquire_with_timeout()
        if not acquired:
            msg = f"[{_now_ts()}] Client {client_id} withdrawal ${amount} timed out waiting for lock after {wait_time:.3f}s"
            print(msg)
            self.transaction_history.put({
                'client_id': client_id,
                'type': 'withdrawal',
                'amount': amount,
                'old_balance': self.balance,
                'new_balance': self.balance,
                'success': False,
                'wait_time': wait_time,
                'timeout': True,
                'timestamp': datetime.now(),
            })
            return wait_time

        try:
            old_balance = self.balance
            if old_balance >= amount:
                new_balance = old_balance - amount
                success = True
                self.balance = new_balance
            else:
                new_balance = old_balance
                success = False
        finally:
            self.lock.release()


        time.sleep(random.uniform(0.1, 0.5))
        self.transaction_history.put({
            'client_id': client_id,
            'type': 'withdrawal',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'success': success,
            'wait_time': wait_time,
            'timeout': False,
            'timestamp': datetime.now(),
        })

        if success:
            print(f"[{_now_ts()}] Client {client_id} withdrew ${amount}. New balance: ${new_balance}")
        else:
            print(f"[{_now_ts()}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${old_balance}")
        return wait_time


def client_activity(account, client_id, num_transactions):
    wait_times = []
    for _ in range(num_transactions):
        action = random.choice(['deposit', 'withdraw'])
        amount = random.randint(10, 200)
        if action == 'deposit':
            wt = account.deposit(amount, client_id)
        else:
            wt = account.withdraw(amount, client_id)
        wait_times.append(wt)

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

    timeouts = sum(1 for t in transactions if t.get('timeout'))
    print(f"Total transactions: {total_transactions} (timeouts: {timeouts})")
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
    for cid in sorted(client_ids):
        client_txns = [t for t in transactions if t['client_id'] == cid]
        avg_client_wait = sum(t['wait_time'] for t in client_txns) / len(client_txns)
        print(f"Client {cid}: {len(client_txns)} transactions, avg wait {avg_client_wait:.3f} seconds")


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



"""
The following code produces a blocking suspension problem. You must fix it to resolve the problem.

code : 
"""