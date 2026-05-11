import threading
import time
import random
import queue
from datetime import datetime
from collections import deque

class BankAccount:
    def __init__(self, balance=1000):
        self.balance = balance
        self.balance_lock = threading.RLock()
        self.transaction_history = queue.Queue()
        self.history_lock = threading.Lock()
        
    def _log_transaction(self, transaction_data):
        """Separate method to log transactions without blocking balance operations"""
        with self.history_lock:
            self.transaction_history.put(transaction_data)
    
    def _simulate_processing(self):
        """Simulate processing time outside of critical sections"""
        time.sleep(random.uniform(0.05, 0.1))
        
    def deposit(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")
        

        self._simulate_processing()
        

        with self.balance_lock:

            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
            

            old_balance = self.balance
            self.balance += amount
            new_balance = self.balance
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${new_balance}")
        

        transaction_data = {
            'client_id': client_id,
            'type': 'deposit',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'wait_time': wait_time,
            'timestamp': datetime.now()
        }
        self._log_transaction(transaction_data)
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
        return wait_time
        
    def withdraw(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        

        self._simulate_processing()
        

        with self.balance_lock:

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
            
            if success:
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${new_balance}")
            else:
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${old_balance}")
        

        transaction_data = {
            'client_id': client_id,
            'type': 'withdrawal',
            'amount': amount,
            'old_balance': old_balance,
            'new_balance': new_balance,
            'success': success,
            'wait_time': wait_time,
            'timestamp': datetime.now()
        }
        self._log_transaction(transaction_data)
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
        return wait_time
    
    def get_balance(self):
        """Thread-safe balance getter"""
        with self.balance_lock:
            return self.balance

class OptimizedBankAccount:
    """Alternative implementation using atomic operations and reduced locking"""
    def __init__(self, balance=1000):
        self.balance = balance
        self.balance_lock = threading.RLock()

        self.transaction_history = deque()
        self.history_lock = threading.Lock()
        self.transaction_counter = 0
        
    def _quick_log(self, **kwargs):
        """Optimized logging with minimal lock time"""
        transaction_id = None
        with self.history_lock:
            self.transaction_counter += 1
            transaction_id = self.transaction_counter
            self.transaction_history.append({
                'transaction_id': transaction_id,
                'timestamp': datetime.now(),
                **kwargs
            })
        return transaction_id
    
    def atomic_deposit(self, amount, client_id):
        """Optimized deposit with minimal locking"""
        start_time = time.time()
        

        processing_delay = random.uniform(0.01, 0.05)
        time.sleep(processing_delay)
        

        with self.balance_lock:
            lock_time = time.time()
            wait_time = lock_time - start_time
            
            old_balance = self.balance
            self.balance += amount
            new_balance = self.balance
        

        self._quick_log(
            client_id=client_id,
            type='deposit',
            amount=amount,
            old_balance=old_balance,
            new_balance=new_balance,
            wait_time=wait_time
        )
        
        return wait_time, new_balance
    
    def atomic_withdraw(self, amount, client_id):
        """Optimized withdrawal with minimal locking"""
        start_time = time.time()
        

        processing_delay = random.uniform(0.01, 0.05)
        time.sleep(processing_delay)
        

        with self.balance_lock:
            lock_time = time.time()
            wait_time = lock_time - start_time
            
            old_balance = self.balance
            if self.balance >= amount:
                self.balance -= amount
                success = True
                new_balance = self.balance
            else:
                success = False
                new_balance = old_balance
        

        self._quick_log(
            client_id=client_id,
            type='withdrawal',
            amount=amount,
            old_balance=old_balance,
            new_balance=new_balance,
            success=success,
            wait_time=wait_time
        )
        
        return wait_time, new_balance, success

def client_activity_optimized(account, client_id, num_transactions, use_optimized=False):
    """Optimized client activity with reduced contention"""
    wait_times = []
    
    for i in range(num_transactions):
        action = random.choice(['deposit', 'withdraw'])
        amount = random.randint(10, 200)
        
        try:
            if use_optimized and hasattr(account, 'atomic_deposit'):
                if action == 'deposit':
                    wait_time, new_balance = account.atomic_deposit(amount, client_id)
                    print(f"Client {client_id} deposited ${amount}. Balance: ${new_balance}")
                else:
                    wait_time, new_balance, success = account.atomic_withdraw(amount, client_id)
                    if success:
                        print(f"Client {client_id} withdrew ${amount}. Balance: ${new_balance}")
                    else:
                        print(f"Client {client_id} failed to withdraw ${amount}. Balance: ${new_balance}")
            else:

                if action == 'deposit':
                    wait_time = account.deposit(amount, client_id)
                else:
                    wait_time = account.withdraw(amount, client_id)
            
            wait_times.append(wait_time)
            
        except Exception as e:
            print(f"Error in client {client_id} transaction {i}: {e}")
        

        time.sleep(random.uniform(0.05, 0.2))
    
    return wait_times

def analyze_transaction_history_optimized(account):
    """Optimized transaction analysis"""
    print("\n===== TRANSACTION ANALYSIS =====")
    
    transactions = []
    

    if hasattr(account, 'transaction_history'):
        if isinstance(account.transaction_history, queue.Queue):
            while not account.transaction_history.empty():
                try:
                    transactions.append(account.transaction_history.get_nowait())
                except queue.Empty:
                    break
        elif isinstance(account.transaction_history, deque):
            with account.history_lock:
                transactions = list(account.transaction_history)
    
    if not transactions:
        print("No transactions recorded.")
        return
    

    total_transactions = len(transactions)
    wait_times = [t.get('wait_time', 0) for t in transactions if 'wait_time' in t]
    
    if wait_times:
        avg_wait_time = sum(wait_times) / len(wait_times)
        max_wait_time = max(wait_times)
        min_wait_time = min(wait_times)
        
        print(f"Total transactions: {total_transactions}")
        print(f"Average wait time: {avg_wait_time:.3f} seconds")
        print(f"Maximum wait time: {max_wait_time:.3f} seconds")
        print(f"Minimum wait time: {min_wait_time:.3f} seconds")
        

        if avg_wait_time < 0.1:
            print("✅ PERFORMANCE: Excellent - Very low contention!")
        elif avg_wait_time < 0.5:
            print("✅ PERFORMANCE: Good - Moderate contention")
        else:
            print("⚠️  PERFORMANCE: Poor - High contention detected")

def run_comparison_simulation(num_clients=5, transactions_per_client=3):
    """Run both original and optimized versions for comparison"""
    
    print("===== ORIGINAL IMPLEMENTATION =====")
    account1 = BankAccount()
    start_time = time.time()
    
    threads1 = []
    for i in range(1, num_clients + 1):
        t = threading.Thread(target=client_activity_optimized, 
                           args=(account1, i, transactions_per_client, False))
        threads1.append(t)
        t.start()
    
    for t in threads1:
        t.join()
    
    original_time = time.time() - start_time
    print(f"Original implementation completed in {original_time:.2f} seconds")
    print(f"Final balance: ${account1.get_balance()}")
    analyze_transaction_history_optimized(account1)
    
    print("\n" + "="*50)
    print("===== OPTIMIZED IMPLEMENTATION =====")
    
    account2 = OptimizedBankAccount()
    start_time = time.time()
    
    threads2 = []
    for i in range(1, num_clients + 1):
        t = threading.Thread(target=client_activity_optimized, 
                           args=(account2, i, transactions_per_client, True))
        threads2.append(t)
        t.start()
    
    for t in threads2:
        t.join()
    
    optimized_time = time.time() - start_time
    print(f"Optimized implementation completed in {optimized_time:.2f} seconds")
    print(f"Final balance: ${account2.balance}")
    analyze_transaction_history_optimized(account2)
    

    print(f"\n===== PERFORMANCE COMPARISON =====")
    improvement = ((original_time - optimized_time) / original_time) * 100
    print(f"Original time: {original_time:.2f}s")
    print(f"Optimized time: {optimized_time:.2f}s")
    print(f"Performance improvement: {improvement:.1f}%")

if __name__ == "__main__":
    print("Testing the fixed bank account simulation...")
    run_comparison_simulation(num_clients=8, transactions_per_client=4)