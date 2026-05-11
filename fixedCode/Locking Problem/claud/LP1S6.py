import threading
import time
import random
import queue
from datetime import datetime
from contextlib import contextmanager

class BankAccount:
    def __init__(self, balance=1000):
        self.balance = balance

        self.balance_lock = threading.RLock()
        

        self.history_lock = threading.RLock()
        self.transaction_history = queue.Queue()
        

        self.lock_timeout = 5.0
        
    @contextmanager
    def _acquire_balance_lock_with_timeout(self, timeout=None):
        """Context manager for acquiring balance lock with timeout"""
        timeout = timeout or self.lock_timeout
        acquired = self.balance_lock.acquire(timeout=timeout)
        if not acquired:
            raise TimeoutError("Failed to acquire balance lock within timeout")
        try:
            yield
        finally:
            self.balance_lock.release()
    
    def _log_transaction_async(self, transaction_data):
        """Step 4: Non-blocking transaction logging"""
        def log_worker():
            with self.history_lock:
                self.transaction_history.put(transaction_data)
        

        log_thread = threading.Thread(target=log_worker)
        log_thread.daemon = True
        log_thread.start()
        
    def deposit(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")
        
        try:

            with self._acquire_balance_lock_with_timeout():

                lock_acquired_time = time.time()
                wait_time = lock_acquired_time - start_time
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
                

                old_balance = self.balance
                self.balance += amount
                new_balance = self.balance
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${new_balance}")
            

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
            

            time.sleep(random.uniform(0.05, 0.1))
            

            self._log_transaction_async({
                'client_id': client_id,
                'type': 'deposit',
                'amount': amount,
                'old_balance': old_balance,
                'new_balance': new_balance,
                'wait_time': wait_time,
                'timestamp': datetime.now(),
                'success': True
            })
            
            return wait_time
            
        except TimeoutError:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} timeout waiting for deposit lock")
            return -1
        
    def withdraw(self, amount, client_id):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        
        try:

            with self._acquire_balance_lock_with_timeout():

                lock_acquired_time = time.time()
                wait_time = lock_acquired_time - start_time
                
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
                

                old_balance = self.balance
                success = False
                new_balance = old_balance
                
                if self.balance >= amount:
                    self.balance -= amount
                    new_balance = self.balance
                    success = True
                
                if success:
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} withdrew ${amount}. New balance: ${new_balance}")
                else:
                    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} failed to withdraw ${amount}. Insufficient funds: ${old_balance}")
            

            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
            

            time.sleep(random.uniform(0.05, 0.1))
            

            self._log_transaction_async({
                'client_id': client_id,
                'type': 'withdrawal',
                'amount': amount,
                'old_balance': old_balance,
                'new_balance': new_balance,
                'success': success,
                'wait_time': wait_time,
                'timestamp': datetime.now()
            })
            
            return wait_time
            
        except TimeoutError:
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} timeout waiting for withdrawal lock")
            return -1

    def get_balance_safely(self):
        """Step 8: Add safe balance reading method"""
        try:
            with self._acquire_balance_lock_with_timeout(timeout=1.0):
                return self.balance
        except TimeoutError:
            return None

def client_activity(account, client_id, num_transactions):
    wait_times = []
    timeouts = 0
    
    for transaction_num in range(num_transactions):

        max_retries = 3
        retry_count = 0
        
        while retry_count < max_retries:

            action = random.choice(['deposit', 'withdraw'])
            amount = random.randint(10, 200)
            

            if action == 'deposit':
                wait_time = account.deposit(amount, client_id)
            else:
                wait_time = account.withdraw(amount, client_id)
            
            if wait_time == -1:
                timeouts += 1
                retry_count += 1
                print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} retrying transaction {transaction_num + 1} (attempt {retry_count + 1})")
                time.sleep(random.uniform(0.1, 0.3))
            else:
                wait_times.append(wait_time)
                break
        

        if timeouts > 0:

            sleep_time = random.uniform(0.2, 1.5)
        else:
            sleep_time = random.uniform(0.05, 0.3)
        
        time.sleep(sleep_time)
    
    if timeouts > 0:
        print(f"[INFO] Client {client_id} experienced {timeouts} timeouts")
    
    return wait_times

def analyze_transaction_history(account):
    print("\n===== TRANSACTION ANALYSIS =====")
    

    transactions = []
    

    time.sleep(0.5)
    
    try:
        with account.history_lock:
            while not account.transaction_history.empty():
                transactions.append(account.transaction_history.get())
    except Exception as e:
        print(f"Error extracting transaction history: {e}")
        return
    

    total_transactions = len(transactions)
    if total_transactions == 0:
        print("No transactions recorded.")
        return
    
    successful_transactions = [t for t in transactions if t.get('success', True)]
    failed_transactions = [t for t in transactions if not t.get('success', True)]
    
    if successful_transactions:
        total_wait_time = sum(t['wait_time'] for t in successful_transactions)
        avg_wait_time = total_wait_time / len(successful_transactions)
        max_wait_time = max(t['wait_time'] for t in successful_transactions)
        min_wait_time = min(t['wait_time'] for t in successful_transactions)
        
        print(f"Total transactions: {total_transactions}")
        print(f"Successful transactions: {len(successful_transactions)}")
        print(f"Failed transactions: {len(failed_transactions)}")
        print(f"Average wait time: {avg_wait_time:.3f} seconds")
        print(f"Maximum wait time: {max_wait_time:.3f} seconds")
        print(f"Minimum wait time: {min_wait_time:.3f} seconds")
        

        deposits = [t for t in successful_transactions if t['type'] == 'deposit']
        withdrawals = [t for t in successful_transactions if t['type'] == 'withdrawal']
        
        if deposits:
            avg_deposit_wait = sum(t['wait_time'] for t in deposits) / len(deposits)
            print(f"Average wait time for deposits: {avg_deposit_wait:.3f} seconds")
        
        if withdrawals:
            avg_withdrawal_wait = sum(t['wait_time'] for t in withdrawals) / len(withdrawals)
            print(f"Average wait time for withdrawals: {avg_withdrawal_wait:.3f} seconds")
        

        client_ids = set(t['client_id'] for t in successful_transactions)
        for client_id in sorted(client_ids):
            client_txns = [t for t in successful_transactions if t['client_id'] == client_id]
            avg_client_wait = sum(t['wait_time'] for t in client_txns) / len(client_txns)
            print(f"Client {client_id}: {len(client_txns)} successful transactions, avg wait {avg_client_wait:.3f} seconds")

def run_simulation(num_clients, transactions_per_client):
    print("===== STARTING OPTIMIZED BANK ACCOUNT SIMULATION =====")
    print(f"Number of clients: {num_clients}")
    print(f"Transactions per client: {transactions_per_client}")
    print("=====================================================\n")
    

    account = BankAccount()
    

    threads = []
    for i in range(1, num_clients + 1):
        t = threading.Thread(target=client_activity, args=(account, i, transactions_per_client))
        threads.append(t)
        t.start()
    

    for t in threads:
        t.join()
    
    print("\n===== SIMULATION COMPLETED =====")
    

    final_balance = account.get_balance_safely()
    if final_balance is not None:
        print(f"Final account balance: ${final_balance}")
    else:
        print("Could not read final balance due to lock contention")
    

    analyze_transaction_history(account)

if __name__ == "__main__":

    run_simulation(num_clients=10, transactions_per_client=5)