import threading
import time
import random
import queue
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
import asyncio

class BankAccount:
    def __init__(self, balance=1000):
        self.balance = balance
        self.lock = threading.RLock()
        self.transaction_history = queue.Queue()
        self.read_write_lock = threading.Condition(self.lock)
        self.active_readers = 0
        self.pending_writers = 0
        
    def _acquire_read_lock(self):
        """Acquire read lock for balance checking operations"""
        with self.read_write_lock:
            while self.pending_writers > 0:
                self.read_write_lock.wait()
            self.active_readers += 1
    
    def _release_read_lock(self):
        """Release read lock"""
        with self.read_write_lock:
            self.active_readers -= 1
            if self.active_readers == 0:
                self.read_write_lock.notify_all()
    
    def _acquire_write_lock(self):
        """Acquire write lock for balance modification operations"""
        with self.read_write_lock:
            self.pending_writers += 1
            while self.active_readers > 0:
                self.read_write_lock.wait()
            self.pending_writers -= 1
    
    def _release_write_lock(self):
        """Release write lock"""
        with self.read_write_lock:
            self.read_write_lock.notify_all()
    
    def get_balance(self, client_id=None):
        """Non-blocking balance check"""
        start_time = time.time()
        self._acquire_read_lock()
        try:
            balance = self.balance
            wait_time = time.time() - start_time
            return balance, wait_time
        finally:
            self._release_read_lock()
    
    def deposit(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")
        

        self._acquire_write_lock()
        try:

            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
            

            old_balance = self.balance
            self.balance += amount
            new_balance = self.balance
            
        finally:
            self._release_write_lock()
        


        processing_time = random.uniform(0.1, 0.5)
        time.sleep(processing_time)
        

        self.transaction_history.put({
            'client_id': client_id,
            'type': 'deposit',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'wait_time': wait_time,
            'processing_time': processing_time,
            'timestamp': datetime.now()
        })
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${new_balance}")
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
        return wait_time
        
    def withdraw(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        

        self._acquire_write_lock()
        try:

            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
            

            old_balance = self.balance
            if self.balance >= amount:
                self.balance -= amount
                success = True
                new_balance = self.balance
            else:
                success = False
                new_balance = old_balance
                
        finally:
            self._release_write_lock()
        


        processing_time = random.uniform(0.1, 0.5)
        time.sleep(processing_time)
        

        self.transaction_history.put({
            'client_id': client_id,
            'type': 'withdrawal',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'success': success,
            'wait_time': wait_time,
            'processing_time': processing_time,
            'timestamp': datetime.now()
        })
        
        if success:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${new_balance}")
        else:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${old_balance}")
    
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
        return wait_time

def client_activity(account, client_id, num_transactions):
    wait_times = []
    
    for i in range(num_transactions):

        action = random.choice(['deposit', 'withdraw'])
        amount = random.randint(10, 200)
        

        if random.random() < 0.3:
            balance, balance_wait = account.get_balance(client_id)
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} checked balance: ${balance} (wait: {balance_wait:.3f}s)")
        

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
    
    total_wait_time = sum(t['wait_time'] for t in transactions)
    avg_wait_time = total_wait_time / total_transactions
    max_wait_time = max(t['wait_time'] for t in transactions)
    min_wait_time = min(t['wait_time'] for t in transactions)
    

    total_processing_time = sum(t.get('processing_time', 0) for t in transactions)
    avg_processing_time = total_processing_time / total_transactions
    
    print(f"Total transactions: {total_transactions}")
    print(f"Average wait time: {avg_wait_time:.3f} seconds")
    print(f"Maximum wait time: {max_wait_time:.3f} seconds")
    print(f"Minimum wait time: {min_wait_time:.3f} seconds")
    print(f"Average processing time: {avg_processing_time:.3f} seconds")
    print(f"Lock efficiency ratio: {avg_processing_time/(avg_wait_time + avg_processing_time):.2%}")
    

    deposits = [t for t in transactions if t['type'] == 'deposit']
    withdrawals = [t for t in transactions if t['type'] == 'withdrawal']
    
    if deposits:
        avg_deposit_wait = sum(t['wait_time'] for t in deposits) / len(deposits)
        print(f"Average wait time for deposits: {avg_deposit_wait:.3f} seconds")
    
    if withdrawals:
        avg_withdrawal_wait = sum(t['wait_time'] for t in withdrawals) / len(withdrawals)
        successful_withdrawals = len([t for t in withdrawals if t.get('success', True)])
        print(f"Average wait time for withdrawals: {avg_withdrawal_wait:.3f} seconds")
        print(f"Successful withdrawals: {successful_withdrawals}/{len(withdrawals)} ({successful_withdrawals/len(withdrawals):.1%})")
    

    client_ids = set(t['client_id'] for t in transactions)
    for client_id in sorted(client_ids):
        client_txns = [t for t in transactions if t['client_id'] == client_id]
        avg_client_wait = sum(t['wait_time'] for t in client_txns) / len(client_txns)
        print(f"Client {client_id}: {len(client_txns)} transactions, avg wait {avg_client_wait:.3f} seconds")

def run_simulation(num_clients, transactions_per_client):
    print("===== STARTING IMPROVED BANK ACCOUNT SIMULATION =====")
    print(f"Number of clients: {num_clients}")
    print(f"Transactions per client: {transactions_per_client}")
    print("=====================================================\n")
    

    account = BankAccount()
    

    start_time = time.time()
    

    threads = []
    for i in range(1, num_clients + 1):
        t = threading.Thread(target=client_activity, args=(account, i, transactions_per_client))
        threads.append(t)
        t.start()
    

    for t in threads:
        t.join()
    

    end_time = time.time()
    total_time = end_time - start_time
    
    print(f"\n===== SIMULATION COMPLETED =====")
    print(f"Total execution time: {total_time:.3f} seconds")
    print(f"Final account balance: ${account.balance}")
    print(f"Throughput: {(num_clients * transactions_per_client) / total_time:.2f} transactions/second")
    

    analyze_transaction_history(account)

if __name__ == "__main__":

    run_simulation(num_clients=10, transactions_per_client=5)