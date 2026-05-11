import threading
import time
import random
import queue
from datetime import datetime

ACQUIRE_TIMEOUT_SEC = 1.0
WORK_MIN_SEC = 0.05
WORK_MAX_SEC = 0.20

class BankAccount:
    def __init__(self, balance=1000):
        self.balance = balance
        self.lock = threading.Lock()
        self.transaction_history = queue.Queue()

    def _log_skip(self, txn_type, amount, client_id, start_time):
        wait_time = time.time() - start_time
        self.transaction_history.put({
            'client_id': client_id,
            'type': txn_type,
            'amount': amount,
            'old_balance': self.balance,
            'new_balance': self.balance,
            'success': False,
            'skipped': True,
            'wait_time': wait_time,
            'timestamp': datetime.now()
        })
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}] Client {client_id} could not acquire lock in {wait_time:.3f}s for {txn_type} ${amount}, moving on")
        return wait_time

    def deposit(self, amount, client_id):
        start_time = time.time()
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}] Client {client_id} waiting to deposit ${amount}...")

        acquired = self.lock.acquire(timeout=ACQUIRE_TIMEOUT_SEC)
        if not acquired:
            return self._log_skip('deposit', amount, client_id, start_time)

        try:
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] Client {client_id} acquired lock after {wait_time:.3f}s (deposit ${amount})")

            time.sleep(random.uniform(WORK_MIN_SEC, WORK_MAX_SEC))

            old_balance = self.balance
            self.balance += amount
            self.transaction_history.put({
                'client_id': client_id,
                'type': 'deposit',
                'amount': amount,
                'old_balance': old_balance,
                'new_balance': self.balance,
                'success': True,
                'skipped': False,
                'wait_time': wait_time,
                'timestamp': datetime.now()
            })

            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] Client {client_id} deposited ${amount}. New balance: ${self.balance}")
            return wait_time
        finally:
            self.lock.release()
            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] Client {client_id} released lock after deposit")

    def withdraw(self, amount, client_id):
        start_time = time.time()
        ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
        print(f"[{ts}] Client {client_id} waiting to withdraw ${amount}...")

        acquired = self.lock.acquire(timeout=ACQUIRE_TIMEOUT_SEC)
        if not acquired:
            return self._log_skip('withdrawal', amount, client_id, start_time)

        try:
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] Client {client_id} acquired lock after {wait_time:.3f}s (withdraw ${amount})")

            time.sleep(random.uniform(WORK_MIN_SEC, WORK_MAX_SEC))

            old_balance = self.balance
            if self.balance >= amount:
                self.balance -= amount
                success = True
                new_balance = self.balance
            else:
                success = False
                new_balance = old_balance

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'withdrawal',
                'amount': amount,
                'old_balance': old_balance,
                'new_balance': new_balance,
                'success': success,
                'skipped': False,
                'wait_time': wait_time,
                'timestamp': datetime.now()
            })

            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            if success:
                print(f"[{ts}] Client {client_id} withdrew ${amount}. New balance: ${self.balance}")
            else:
                print(f"[{ts}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${self.balance}")
            return wait_time
        finally:
            self.lock.release()
            ts = datetime.now().strftime('%H:%M:%S.%f')[:-3]
            print(f"[{ts}] Client {client_id} released lock after withdrawal attempt")

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

    print(f"Total transactions (incl. skipped): {total_transactions}")
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


    skipped = [t for t in transactions if t.get('skipped')]
    print(f"Skipped due to timeout: {len(skipped)}")

    client_ids = set(t['client_id'] for t in transactions)
    for client_id in client_ids:
        client_txns = [t for t in transactions if t['client_id'] == client_id]
        avg_client_wait = sum(t['wait_time'] for t in client_txns) / len(client_txns)
        skipped_by_client = sum(1 for t in client_txns if t.get('skipped'))
        print(f"Client {client_id}: {len(client_txns)} transactions, avg wait {avg_client_wait:.3f}s, skipped {skipped_by_client}")

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


"""
blocking suspension problem occurrence code :
Fixed code :

Can you fix the code below to resolve the blocking suspension problem according to the above example?

blocking suspension problem occurrence code :
Fixed code :
"""