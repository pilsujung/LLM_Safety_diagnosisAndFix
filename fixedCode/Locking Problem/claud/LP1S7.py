import threading
import time
import random
import queue
from datetime import datetime
from collections import defaultdict

class OptimizedBankAccount:
    def __init__(self, balance=1000):
        self.balance = balance

        self.lock = threading.RLock()
        self.transaction_history = queue.Queue()

        self.history_lock = threading.Lock()
        
    def _log_transaction(self, transaction_data):
        """Helper method to log transactions with separate lock"""
        with self.history_lock:
            self.transaction_history.put(transaction_data)
    
    def deposit(self, amount, client_id):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")
        
        with self.lock:
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
            

            processing_time = random.uniform(0.05, 0.2)
            time.sleep(processing_time)
            
            old_balance = self.balance
            self.balance += amount
            new_balance = self.balance
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${self.balance}")
        

        self._log_transaction({
            'client_id': client_id,
            'type': 'deposit',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'wait_time': wait_time,
            'processing_time': processing_time,
            'timestamp': datetime.now()
        })
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
        return wait_time
        
    def withdraw(self, amount, client_id):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        
        with self.lock:
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
            

            processing_time = random.uniform(0.05, 0.2)
            time.sleep(processing_time)
            
            old_balance = self.balance
            success = self.balance >= amount
            
            if success:
                self.balance -= amount
                new_balance = self.balance
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${self.balance}")
            else:
                new_balance = old_balance
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${self.balance}")
        

        self._log_transaction({
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
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
        return wait_time

    def get_balance(self, client_id=None):
        """Non-blocking balance check (may be slightly stale but much faster)"""


        if client_id:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} checked balance: ${self.balance}")
        return self.balance


class ReadWriteLockBankAccount:
    """Alternative implementation using reader-writer locks for better concurrency"""
    def __init__(self, balance=1000):
        self.balance = balance
        self.transaction_history = queue.Queue()
        self.history_lock = threading.Lock()
        

        self._read_ready = threading.Condition(threading.RLock())
        self._readers = 0
        
    def deposit(self, amount, client_id):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")
        
        with self._read_ready:

            while self._readers > 0:
                self._read_ready.wait()
            
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired write lock after waiting {wait_time:.3f} seconds")
            
            processing_time = random.uniform(0.05, 0.2)
            time.sleep(processing_time)
            
            old_balance = self.balance
            self.balance += amount
            new_balance = self.balance
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${self.balance}")
        
        with self.history_lock:
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
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released write lock after deposit")
        return wait_time

    def withdraw(self, amount, client_id):
        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        
        with self._read_ready:

            while self._readers > 0:
                self._read_ready.wait()
            
            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired write lock after waiting {wait_time:.3f} seconds")
            
            processing_time = random.uniform(0.05, 0.2)
            time.sleep(processing_time)
            
            old_balance = self.balance
            success = self.balance >= amount
            
            if success:
                self.balance -= amount
                new_balance = self.balance
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${self.balance}")
            else:
                new_balance = old_balance
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${self.balance}")
        
        with self.history_lock:
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
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released write lock after withdrawal attempt")
        return wait_time

    def get_balance(self, client_id):
        """Fast read operation that can run concurrently with other reads"""
        with self._read_ready:
            self._readers += 1
        
        try:

            time.sleep(0.01)
            balance = self.balance
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} read balance: ${balance}")
            return balance
        finally:
            with self._read_ready:
                self._readers -= 1
                if self._readers == 0:
                    self._read_ready.notify_all()


def client_activity_optimized(account, client_id, num_transactions):
    """Enhanced client activity with occasional balance checks"""
    wait_times = []
    
    for i in range(num_transactions):

        if i % 3 == 0 and hasattr(account, 'get_balance'):
            account.get_balance(client_id)
            time.sleep(random.uniform(0.05, 0.2))
        

        action = random.choice(['deposit', 'withdraw'])
        amount = random.randint(10, 200)
        

        if action == 'deposit':
            wait_time = account.deposit(amount, client_id)
        else:
            wait_time = account.withdraw(amount, client_id)
        
        wait_times.append(wait_time)
        

        time.sleep(random.uniform(0.05, 0.3))
    
    return wait_times


def analyze_transaction_history_enhanced(account):
    print("\n===== ENHANCED TRANSACTION ANALYSIS =====")
    

    transactions = []
    while not account.transaction_history.empty():
        transactions.append(account.transaction_history.get())
    
    if not transactions:
        print("No transactions recorded.")
        return
    
    total_transactions = len(transactions)
    total_wait_time = sum(t['wait_time'] for t in transactions)
    avg_wait_time = total_wait_time / total_transactions
    max_wait_time = max(t['wait_time'] for t in transactions)
    min_wait_time = min(t['wait_time'] for t in transactions)
    

    if 'processing_time' in transactions[0]:
        total_processing_time = sum(t['processing_time'] for t in transactions)
        avg_processing_time = total_processing_time / total_transactions
        print(f"Average processing time: {avg_processing_time:.3f} seconds")
    
    print(f"Total transactions: {total_transactions}")
    print(f"Average wait time: {avg_wait_time:.3f} seconds")
    print(f"Maximum wait time: {max_wait_time:.3f} seconds")
    print(f"Minimum wait time: {min_wait_time:.3f} seconds")
    

    high_wait_count = sum(1 for t in transactions if t['wait_time'] > 1.0)
    print(f"Transactions with >1s wait: {high_wait_count} ({high_wait_count/total_transactions*100:.1f}%)")
    

    by_type = defaultdict(list)
    for t in transactions:
        by_type[t['type']].append(t)
    
    for tx_type, txs in by_type.items():
        avg_wait = sum(t['wait_time'] for t in txs) / len(txs)
        print(f"Average wait time for {tx_type}s: {avg_wait:.3f} seconds ({len(txs)} transactions)")
    

    by_client = defaultdict(list)
    for t in transactions:
        by_client[t['client_id']].append(t)
    
    print("\nPer-client analysis:")
    for client_id, txs in sorted(by_client.items()):
        avg_wait = sum(t['wait_time'] for t in txs) / len(txs)
        max_wait = max(t['wait_time'] for t in txs)
        print(f"Client {client_id}: {len(txs)} txns, avg wait {avg_wait:.3f}s, max wait {max_wait:.3f}s")


def run_comparison():
    """Run comparison between original and optimized implementations"""
    print("===== RUNNING PERFORMANCE COMPARISON =====\n")
    
    configs = [
        ("Original-style Account", OptimizedBankAccount),
        ("Read-Write Lock Account", ReadWriteLockBankAccount)
    ]
    
    for name, account_class in configs:
        print(f"\n--- Testing {name} ---")
        account = account_class()
        
        start_time = time.time()
        

        threads = []
        for i in range(1, 6):
            t = threading.Thread(target=client_activity_optimized, args=(account, i, 3))
            threads.append(t)
            t.start()
        

        for t in threads:
            t.join()
        
        end_time = time.time()
        total_time = end_time - start_time
        
        print(f"Total execution time: {total_time:.3f} seconds")
        print(f"Final balance: ${account.balance}")
        

        transactions = []
        while not account.transaction_history.empty():
            transactions.append(account.transaction_history.get())
        
        if transactions:
            avg_wait = sum(t['wait_time'] for t in transactions) / len(transactions)
            max_wait = max(t['wait_time'] for t in transactions)
            print(f"Average wait time: {avg_wait:.3f}s, Max wait: {max_wait:.3f}s")


if __name__ == "__main__":

    run_comparison()
    
    print("\n" + "="*50)
    print("DETAILED ANALYSIS WITH OPTIMIZED ACCOUNT")
    print("="*50)
    

    print("\n===== DETAILED OPTIMIZED SIMULATION =====")
    account = OptimizedBankAccount()
    
    threads = []
    for i in range(1, 8):
        t = threading.Thread(target=client_activity_optimized, args=(account, i, 4))
        threads.append(t)
        t.start()
    
    for t in threads:
        t.join()
    
    print(f"\nFinal account balance: ${account.balance}")
    analyze_transaction_history_enhanced(account)