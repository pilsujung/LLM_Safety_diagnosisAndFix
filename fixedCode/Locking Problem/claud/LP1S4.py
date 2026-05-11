import threading
import time
import random
import queue
from datetime import datetime

class BankAccount:
    def __init__(self, balance=1000):
        self.balance = balance
        self.balance_lock = threading.RLock()
        self.transaction_history = queue.Queue()
        
    def deposit(self, client_id, amount):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to deposit ${amount}...")
        

        processing_time = random.uniform(0.1, 0.5)
        time.sleep(processing_time)
        

        with self.balance_lock:

            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time - processing_time
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} acquired lock after waiting {wait_time:.3f} seconds")
            

            old_balance = self.balance
            self.balance += amount
            new_balance = self.balance
            
            print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} deposited ${amount}. New balance: ${new_balance}")
        

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
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after deposit")
        return wait_time
        
    def withdraw(self, client_id, amount):

        start_time = time.time()
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} waiting to withdraw ${amount}...")
        

        processing_time = random.uniform(0.1, 0.5)
        time.sleep(processing_time)
        

        with self.balance_lock:

            lock_acquired_time = time.time()
            wait_time = lock_acquired_time - start_time - processing_time
            
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
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} released lock after withdrawal attempt")
        return wait_time

    def get_balance(self):
        """Thread-safe balance inquiry"""
        with self.balance_lock:
            return self.balance

def client_activity(account, client_id, num_transactions):
    wait_times = []
    
    for transaction_num in range(num_transactions):

        action = random.choice(['deposit', 'withdraw'])
        amount = random.randint(10, 200)
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} starting transaction {transaction_num + 1}/{num_transactions}: {action} ${amount}")
        

        if action == 'deposit':
            wait_time = account.deposit(client_id, amount)
        else:
            wait_time = account.withdraw(client_id, amount)
        
        wait_times.append(wait_time)
        

        inter_transaction_delay = random.uniform(0.1, 1.0)
        time.sleep(inter_transaction_delay)
    
    avg_wait_time = sum(wait_times) / len(wait_times) if wait_times else 0
    print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] Client {client_id} completed all transactions. Avg wait time: {avg_wait_time:.3f}s")
    return wait_times

def analyze_transaction_history(account):
    print("\n===== TRANSACTION ANALYSIS =====")
    

    transactions = []
    while not account.transaction_history.empty():
        try:
            transactions.append(account.transaction_history.get_nowait())
        except queue.Empty:
            break
    

    total_transactions = len(transactions)
    if total_transactions == 0:
        print("No transactions recorded.")
        return
    
    total_wait_time = sum(t['wait_time'] for t in transactions)
    total_processing_time = sum(t['processing_time'] for t in transactions)
    avg_wait_time = total_wait_time / total_transactions
    avg_processing_time = total_processing_time / total_transactions
    max_wait_time = max(t['wait_time'] for t in transactions)
    min_wait_time = min(t['wait_time'] for t in transactions)
    
    print(f"Total transactions: {total_transactions}")
    print(f"Average wait time: {avg_wait_time:.3f} seconds")
    print(f"Average processing time: {avg_processing_time:.3f} seconds")
    print(f"Maximum wait time: {max_wait_time:.3f} seconds")
    print(f"Minimum wait time: {min_wait_time:.3f} seconds")
    

    deposits = [t for t in transactions if t['type'] == 'deposit']
    withdrawals = [t for t in transactions if t['type'] == 'withdrawal']
    successful_withdrawals = [t for t in withdrawals if t.get('success', True)]
    failed_withdrawals = [t for t in withdrawals if not t.get('success', True)]
    
    if deposits:
        avg_deposit_wait = sum(t['wait_time'] for t in deposits) / len(deposits)
        print(f"Deposits: {len(deposits)} transactions, avg wait {avg_deposit_wait:.3f} seconds")
    
    if withdrawals:
        avg_withdrawal_wait = sum(t['wait_time'] for t in withdrawals) / len(withdrawals)
        print(f"Withdrawals: {len(withdrawals)} transactions, avg wait {avg_withdrawal_wait:.3f} seconds")
        print(f"  - Successful: {len(successful_withdrawals)}")
        print(f"  - Failed: {len(failed_withdrawals)}")
    

    client_ids = sorted(set(t['client_id'] for t in transactions))
    print(f"\nPer-client analysis:")
    for client_id in client_ids:
        client_txns = [t for t in transactions if t['client_id'] == client_id]
        avg_client_wait = sum(t['wait_time'] for t in client_txns) / len(client_txns)
        client_deposits = len([t for t in client_txns if t['type'] == 'deposit'])
        client_withdrawals = len([t for t in client_txns if t['type'] == 'withdrawal'])
        print(f"  Client {client_id}: {len(client_txns)} transactions ({client_deposits}D/{client_withdrawals}W), avg wait {avg_client_wait:.3f}s")

def run_simulation(num_clients, transactions_per_client):
    print("===== STARTING BANK ACCOUNT SIMULATION =====")
    print(f"Number of clients: {num_clients}")
    print(f"Transactions per client: {transactions_per_client}")
    print("============================================\n")
    

    account = BankAccount()
    print(f"Initial account balance: ${account.get_balance()}")
    

    simulation_start = time.time()
    

    threads = []
    for i in range(1, num_clients + 1):
        t = threading.Thread(target=client_activity, args=(account, i, transactions_per_client))
        t.daemon = False
        threads.append(t)
        t.start()

        time.sleep(0.01)
    
    print(f"All {num_clients} client threads started...\n")
    

    for i, t in enumerate(threads, 1):
        t.join()
        print(f"Client thread {i} completed.")
    
    simulation_end = time.time()
    total_simulation_time = simulation_end - simulation_start
    
    print(f"\n===== SIMULATION COMPLETED =====")
    print(f"Total simulation time: {total_simulation_time:.3f} seconds")
    print(f"Final account balance: ${account.get_balance()}")
    

    analyze_transaction_history(account)

if __name__ == "__main__":

    print("Running optimized simulation...")
    run_simulation(num_clients=10, transactions_per_client=5)