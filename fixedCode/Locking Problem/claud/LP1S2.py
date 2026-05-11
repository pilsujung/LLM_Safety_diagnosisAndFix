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
        

        if self.lock.acquire(timeout=2.0):
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
                    'success': True,
                    'timestamp': datetime.now()
                })
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${self.balance}")
                return wait_time
                
            finally:
                self.lock.release()
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
        else:

            timeout_time = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire lock for deposit after {timeout_time:.3f} seconds, skipping transaction")
            

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'deposit',
                'amount': amount,
                'old_balance': None,
                'new_balance': None,
                'wait_time': timeout_time,
                'success': False,
                'timestamp': datetime.now()
            })
            
            return timeout_time
        
    def withdraw(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        

        if self.lock.acquire(timeout=2.0):
            try:

                lock_acquired_time = time.time()
                wait_time = lock_acquired_time - start_time
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
                

                time.sleep(random.uniform(0.05, 0.2))
                

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
                
                return wait_time
                
            finally:
                self.lock.release()
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
        else:

            timeout_time = time.time() - start_time
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} could not acquire lock for withdrawal after {timeout_time:.3f} seconds, skipping transaction")
            

            self.transaction_history.put({
                'client_id': client_id,
                'type': 'withdrawal',
                'amount': amount,
                'old_balance': None,
                'new_balance': None,
                'wait_time': timeout_time,
                'success': False,
                'timestamp': datetime.now()
            })
            
            return timeout_time

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
        

        time.sleep(random.uniform(0.05, 0.3))
    
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
    
    successful_transactions = [t for t in transactions if t['success']]
    failed_transactions = [t for t in transactions if not t['success']]
    
    total_wait_time = sum(t['wait_time'] for t in transactions)
    avg_wait_time = total_wait_time / total_transactions
    max_wait_time = max(t['wait_time'] for t in transactions)
    min_wait_time = min(t['wait_time'] for t in transactions)
    
    print(f"Total transaction attempts: {total_transactions}")
    print(f"Successful transactions: {len(successful_transactions)}")
    print(f"Failed/Timeout transactions: {len(failed_transactions)}")
    print(f"Success rate: {len(successful_transactions)/total_transactions*100:.1f}%")
    print(f"Average wait time: {avg_wait_time:.3f} seconds")
    print(f"Maximum wait time: {max_wait_time:.3f} seconds")
    print(f"Minimum wait time: {min_wait_time:.3f} seconds")
    

    if successful_transactions:
        successful_wait_time = sum(t['wait_time'] for t in successful_transactions)
        avg_successful_wait = successful_wait_time / len(successful_transactions)
        print(f"Average wait time for successful transactions: {avg_successful_wait:.3f} seconds")
    

    deposits = [t for t in transactions if t['type'] == 'deposit']
    withdrawals = [t for t in transactions if t['type'] == 'withdrawal']
    
    if deposits:
        successful_deposits = [t for t in deposits if t['success']]
        print(f"Deposits: {len(deposits)} attempts, {len(successful_deposits)} successful")
        if successful_deposits:
            avg_deposit_wait = sum(t['wait_time'] for t in successful_deposits) / len(successful_deposits)
            print(f"Average wait time for successful deposits: {avg_deposit_wait:.3f} seconds")
    
    if withdrawals:
        successful_withdrawals = [t for t in withdrawals if t['success']]
        print(f"Withdrawals: {len(withdrawals)} attempts, {len(successful_withdrawals)} successful")
        if successful_withdrawals:
            avg_withdrawal_wait = sum(t['wait_time'] for t in successful_withdrawals) / len(successful_withdrawals)
            print(f"Average wait time for successful withdrawals: {avg_withdrawal_wait:.3f} seconds")
    

    client_ids = set(t['client_id'] for t in transactions)
    for client_id in sorted(client_ids):
        client_txns = [t for t in transactions if t['client_id'] == client_id]
        successful_client_txns = [t for t in client_txns if t['success']]
        if successful_client_txns:
            avg_client_wait = sum(t['wait_time'] for t in successful_client_txns) / len(successful_client_txns)
            print(f"Client {client_id}: {len(client_txns)} attempts, {len(successful_client_txns)} successful, avg wait {avg_client_wait:.3f} seconds")

def run_simulation(num_clients, transactions_per_client):
    print("===== STARTING BANK ACCOUNT SIMULATION (NON-BLOCKING) =====")
    print(f"Number of clients: {num_clients}")
    print(f"Transactions per client: {transactions_per_client}")
    print(f"Lock timeout: 2.0 seconds")
    print("===========================================================\n")
    

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