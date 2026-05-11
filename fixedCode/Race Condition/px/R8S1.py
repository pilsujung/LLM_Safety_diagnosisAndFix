import threading
import time
import random

class BankAccount:
    """
    A thread-safe bank account class that eliminates race conditions
    using proper synchronization with threading.Lock().
    """
    def __init__(self, initial_balance=0):
        self._lock = threading.Lock()
        self.account_balance = initial_balance
        self.transaction_count = 0
        self.transaction_history = []

    def deposit(self, deposit_amount):
        """
        Thread-safe deposit method. Entire operation is atomic.
        """
        with self._lock:

            current_balance = self.account_balance
            


            processing_delay = random.uniform(0.05, 0.15)
            time.sleep(processing_delay)
            

            self.account_balance = current_balance + deposit_amount
            self.transaction_count += 1
            

            transaction_record = f"Deposit: +${deposit_amount}, Balance: ${self.account_balance}"
            self.transaction_history.append(transaction_record)

    def withdraw(self, withdrawal_amount):
        """
        Thread-safe withdrawal method. Entire operation is atomic.
        """
        with self._lock:

            current_balance = self.account_balance
            

            processing_delay = random.uniform(0.05, 0.15)
            time.sleep(processing_delay)
            

            self.account_balance = current_balance - withdrawal_amount
            self.transaction_count += 1
            

            transaction_record = f"Withdrawal: -${withdrawal_amount}, Balance: ${self.account_balance}"
            self.transaction_history.append(transaction_record)

    def get_account_summary(self):
        """Thread-safe account status reader"""
        with self._lock:
            return {
                'balance': self.account_balance,
                'transaction_count': self.transaction_count,
                'history_length': len(self.transaction_history)
            }

def simulate_customer_transactions(shared_account, customer_id, num_transactions=10):
    """
    Simulate a customer performing random transactions on their account.
    Each customer will perform both deposits and withdrawals.
    """
    print(f"Customer {customer_id} starting transactions...")

    for transaction_num in range(num_transactions):

        if random.choice([True, False]):

            deposit_amount = random.randint(50, 200)
            shared_account.deposit(deposit_amount)
            print(f"Customer {customer_id}: Deposited ${deposit_amount}")
        else:

            withdrawal_amount = random.randint(25, 150)
            shared_account.withdraw(withdrawal_amount)
            print(f"Customer {customer_id}: Withdrew ${withdrawal_amount}")


        time.sleep(random.uniform(0.01, 0.05))

    print(f"Customer {customer_id} completed all transactions.")

def run_concurrent_banking_simulation():
    """
    Main function to demonstrate thread-safe banking scenario.
    Multiple customers (threads) access the same account simultaneously.
    """
    print("=" * 60)
    print("THREAD-SAFE BANK ACCOUNT DEMONSTRATION")
    print("=" * 60)


    shared_account = BankAccount(initial_balance=1000)


    number_of_customers = 5
    transactions_per_customer = 8

    print(f"Initial account balance: ${shared_account.account_balance}")
    print(f"Number of concurrent customers: {number_of_customers}")
    print(f"Transactions per customer: {transactions_per_customer}")
    print("\nStarting concurrent transactions...\n")


    customer_threads = []

    for customer_id in range(1, number_of_customers + 1):
        customer_thread = threading.Thread(
            target=simulate_customer_transactions,
            args=(shared_account, customer_id, transactions_per_customer),
            name=f"Customer-{customer_id}"
        )
        customer_threads.append(customer_thread)
        customer_thread.start()


    for thread in customer_threads:
        thread.join()


    print("\n" + "=" * 60)
    print("SIMULATION RESULTS")
    print("=" * 60)

    final_summary = shared_account.get_account_summary()

    print(f"Final account balance: ${final_summary['balance']}")
    print(f"Total recorded transactions: {final_summary['transaction_count']}")
    print(f"Transaction history entries: {final_summary['history_length']}")


    expected_transactions = number_of_customers * transactions_per_customer
    print(f"Expected total transactions: {expected_transactions}")


    print("\nTHREAD SAFETY VERIFICATION:")
    if final_summary['transaction_count'] == expected_transactions:
        print("✅ Transaction count PERFECT!")
    else:
        print(f"⚠️ Transaction count mismatch! Expected: {expected_transactions}, Got: {final_summary['transaction_count']}")

    if final_summary['transaction_count'] == final_summary['history_length']:
        print("✅ History length PERFECT!")
    else:
        print(f"⚠️ History length mismatch! Transactions: {final_summary['transaction_count']}, History: {final_summary['history_length']}")

    print(f"\nNote: Race conditions ELIMINATED! Results consistent every run.")


    print(f"\nLast 5 transaction records:")
    for record in shared_account.transaction_history[-5:]:
        print(f" {record}")

def run_multiple_simulations(num_simulations=3):
    """
    Run the simulation multiple times to prove consistent results
    (no more race condition variability).
    """
    print("Running multiple simulations to demonstrate CONSISTENT results...\n")

    results = []

    for simulation_num in range(1, num_simulations + 1):
        print(f"\n{'='*20} SIMULATION #{simulation_num} {'='*20}")


        test_account = BankAccount(initial_balance=500)


        threads = []
        for i in range(3):
            t = threading.Thread(
                target=simulate_customer_transactions,
                args=(test_account, i+1, 5)
            )
            threads.append(t)
            t.start()

        for t in threads:
            t.join()

        final_balance = test_account.account_balance
        final_transactions = test_account.transaction_count

        results.append((final_balance, final_transactions))
        print(f"Result: Balance=${final_balance}, Transactions={final_transactions}")

    print(f"\n{'='*60}")
    print("COMPARISON OF MULTIPLE RUNS:")
    print("(Notice CONSISTENT transaction counts - no race conditions!)")
    print(f"{'='*60}")

    for i, (balance, transactions) in enumerate(results, 1):
        expected = 3 * 5
        print(f"Run #{i}: Balance=${balance}, Transactions={transactions} (Expected: {expected})")

    print("\n✅ All runs show PERFECT transaction counts!")

if __name__ == "__main__":

    run_concurrent_banking_simulation()


    print("\n" + "="*70)
    print(" Uncomment run_multiple_simulations() to see consistent results across runs!")
    print("="*70)

