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
        

        lock_acquired = self.lock.acquire(timeout=1.0)
        
        if lock_acquired:
            try:

                lock_acquired_time = time.time()
                wait_time = lock_acquired_time - start_time
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
                

                time.sleep(random.uniform(0.05, 0.2))
                

                old_balance = self.balance
                self.balance += amount
                

                self.transaction_history.put({
                    'client_id': client_id,
                    'type': 'deposit',
                    'amount': amount,
                    'old_balance': old_balance,
                    'new_balance': self.balance,
                    'wait_time': wait_time,
                    'timestamp': datetime.now(),
                    'success': True
                })
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${self.balance}")
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
                
            finally:
                self.lock.release()
                
            return wait_time
        else:

            wait_time = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire lock for deposit, trying alternative action")
            

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'deposit',
                'amount': amount,
                'old_balance': None,
                'new_balance': None,
                'wait_time': wait_time,
                'timestamp': datetime.now(),
                'success': False,
                'reason': 'lock_timeout'
            })
            

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} performing alternative task instead of waiting")
            time.sleep(0.1)
            
            return wait_time
        
    def withdraw(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        

        lock_acquired = self.lock.acquire(timeout=1.0)
        
        if lock_acquired:
            try:

                lock_acquired_time = time.time()
                wait_time = lock_acquired_time - start_time
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
                

                time.sleep(random.uniform(0.05, 0.2))
                

                old_balance = self.balance
                if self.balance >= amount:
                    self.balance -= amount
                    success = True
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${self.balance}")
                else:
                    success = False
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${self.balance}")
                

                self.transaction_history.put({
                    'client_id': client_id,
                    'type': 'withdrawal',
                    'amount': amount,
                    'old_balance': old_balance,
                    'new_balance': self.balance if success else old_balance,
                    'success': success,
                    'wait_time': wait_time,
                    'timestamp': datetime.now(),
                    'reason': 'insufficient_funds' if not success else None
                })
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
                
            finally:
                self.lock.release()
                
            return wait_time
        else:

            wait_time = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire lock for withdrawal, trying alternative action")
            

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'withdrawal',
                'amount': amount,
                'old_balance': None,
                'new_balance': None,
                'wait_time': wait_time,
                'timestamp': datetime.now(),
                'success': False,
                'reason': 'lock_timeout'
            })
            

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} performing alternative task instead of waiting")
            time.sleep(0.1)
            
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
        

        time.sleep(random.uniform(0.1, 0.5))
    
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
    

    successful_txns = [t for t in transactions if t.get('success', True) == True]
    failed_txns = [t for t in transactions if t.get('success', True) == False]
    
    print(f"Total transaction attempts: {total_transactions}")
    print(f"Successful transactions: {len(successful_txns)}")
    print(f"Failed transactions (timeouts/insufficient funds): {len(failed_txns)}")
    
    if successful_txns:
        total_wait_time = sum(t['wait_time'] for t in successful_txns)
        avg_wait_time = total_wait_time / len(successful_txns)
        max_wait_time = max(t['wait_time'] for t in successful_txns)
        min_wait_time = min(t['wait_time'] for t in successful_txns)
        
        print(f"Average wait time (successful): {avg_wait_time:.3f} seconds")
        print(f"Maximum wait time (successful): {max_wait_time:.3f} seconds")
        print(f"Minimum wait time (successful): {min_wait_time:.3f} seconds")
    

    timeout_failures = [t for t in failed_txns if t.get('reason') == 'lock_timeout']
    if timeout_failures:
        print(f"Lock timeout failures: {len(timeout_failures)}")
        avg_timeout_wait = sum(t['wait_time'] for t in timeout_failures) / len(timeout_failures)
        print(f"Average wait time before timeout: {avg_timeout_wait:.3f} seconds")
    

    deposits = [t for t in successful_txns if t['type'] == 'deposit']
    withdrawals = [t for t in successful_txns if t['type'] == 'withdrawal']
    
    if deposits:
        avg_deposit_wait = sum(t['wait_time'] for t in deposits) / len(deposits)
        print(f"Average wait time for successful deposits: {avg_deposit_wait:.3f} seconds")
    
    if withdrawals:
        successful_withdrawals = [t for t in withdrawals if t.get('reason') != 'insufficient_funds']
        if successful_withdrawals:
            avg_withdrawal_wait = sum(t['wait_time'] for t in successful_withdrawals) / len(successful_withdrawals)
            print(f"Average wait time for successful withdrawals: {avg_withdrawal_wait:.3f} seconds")
    

    client_ids = set(t['client_id'] for t in transactions)
    for client_id in sorted(client_ids):
        client_txns = [t for t in transactions if t['client_id'] == client_id]
        client_successful = [t for t in client_txns if t.get('success', True) == True]
        client_timeouts = [t for t in client_txns if t.get('reason') == 'lock_timeout']
        
        if client_successful:
            avg_client_wait = sum(t['wait_time'] for t in client_successful) / len(client_successful)
            print(f"Client {client_id}: {len(client_successful)} successful transactions, "
                  f"{len(client_timeouts)} timeouts, avg wait {avg_client_wait:.3f} seconds")

def run_simulation(num_clients, transactions_per_client):
    print("===== STARTING NON-BLOCKING BANK ACCOUNT SIMULATION =====")
    print(f"Number of clients: {num_clients}")
    print(f"Transactions per client: {transactions_per_client}")
    print(f"Lock timeout: 1.0 seconds")
    print("=========================================================\n")
    

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