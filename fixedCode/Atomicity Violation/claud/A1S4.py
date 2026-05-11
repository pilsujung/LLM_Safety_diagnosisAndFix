import threading
import time
import random

class BankAccount:
    def __init__(self, account_id, balance):
        self.account_id = account_id
        self.balance = balance
                                                         
        self._lock = threading.Lock()
    
    def withdraw(self, amount):
        """Thread-safe withdraw operation"""
        with self._lock:
                                      
            current_balance = self.balance
            
                                           
            time.sleep(0.001)
            
                                                    
            if current_balance >= amount:
                                                  
                self.balance = current_balance - amount
                return True
            return False
    
    def deposit(self, amount):
        """Thread-safe deposit operation"""
        with self._lock:
                                      
            current_balance = self.balance
            
                                           
            time.sleep(0.001)
            
                                              
            self.balance = current_balance + amount
            return True

    def get_balance(self):
        """Thread-safe balance reading"""
        with self._lock:
            return self.balance


def transfer(from_account, to_account, amount):
    """Thread-safe atomic transfer between accounts"""
                                                                  
                                                  
    if from_account.account_id < to_account.account_id:
        first_lock = from_account._lock
        second_lock = to_account._lock
    else:
        first_lock = to_account._lock
        second_lock = from_account._lock
    
                                                           
    with first_lock:
        with second_lock:
                                                     
            if from_account.balance >= amount:
                                                 
                from_account.balance -= amount
                to_account.balance += amount
                return True
            return False


def run_transfers(from_account, to_account, num_transfers):
    """Perform multiple transfers between accounts"""
    successful_transfers = 0
    for _ in range(num_transfers):
        amount = random.randint(10, 50)
        if transfer(from_account, to_account, amount):
            successful_transfers += 1
    
    print(f"Thread completed: {successful_transfers}/{num_transfers} successful transfers")


def main():
                                                           
    account_a = BankAccount("A", 1000)
    account_b = BankAccount("B", 1000)
    
                         
    print(f"Initial balances:")
    print(f"Account A: ${account_a.get_balance()}")
    print(f"Account B: ${account_b.get_balance()}")
    print(f"Total money in system: ${account_a.get_balance() + account_b.get_balance()}")
    print("\nStarting transfers...\n")
    
                                                              
    threads = []
    
                                              
    for i in range(5):
        thread = threading.Thread(target=run_transfers, args=(account_a, account_b, 20))
        threads.append(thread)
    
                                              
    for i in range(5):
        thread = threading.Thread(target=run_transfers, args=(account_b, account_a, 20))
        threads.append(thread)
    
                       
    for thread in threads:
        thread.start()
    
                                      
    for thread in threads:
        thread.join()
    
                       
    print("\nAll transfers completed!")
    print(f"Final balances:")
    print(f"Account A: ${account_a.get_balance()}")
    print(f"Account B: ${account_b.get_balance()}")
    print(f"Total money in system: ${account_a.get_balance() + account_b.get_balance()}")
    
                                          
    initial_total = 2000                         
    final_total = account_a.get_balance() + account_b.get_balance()
    
    if final_total != initial_total:
        print(f"\n⚠️ ATOMIC VIOLATION DETECTED! ⚠️")
        print(f"Money was created or destroyed in the system.")
        print(f"Expected total: ${initial_total}")
        print(f"Actual total: ${final_total}")
        print(f"Discrepancy: ${final_total - initial_total}")
    else:
        print("\n✅ System integrity maintained!")
    
                                 
    if account_a.get_balance() < 0 or account_b.get_balance() < 0:
        print(f"\n⚠️ NEGATIVE BALANCE DETECTED! ⚠️")
        if account_a.get_balance() < 0:
            print(f"Account A has gone negative: ${account_a.get_balance()}")
        if account_b.get_balance() < 0:
            print(f"Account B has gone negative: ${account_b.get_balance()}")
    else:
        print("✅ No negative balances detected!")


if __name__ == "__main__":
    main()