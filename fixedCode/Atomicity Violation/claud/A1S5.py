import threading
import time
import random

class BankAccount:
    def __init__(self, account_id, balance):
        self.account_id = account_id
        self.balance = balance
        self.lock = threading.Lock()                                 
    
    def withdraw(self, amount):
        """Atomic withdraw operation using lock"""
        with self.lock:                                    
                                      
            current_balance = self.balance
            
                                           
            time.sleep(0.001)
            
                                                    
            if current_balance >= amount:
                                                  
                self.balance = current_balance - amount
                return True
            return False
    
    def deposit(self, amount):
        """Atomic deposit operation using lock"""
        with self.lock:                                    
                                      
            current_balance = self.balance
            
                                           
            time.sleep(0.001)
            
                                              
            self.balance = current_balance + amount
            return True


def transfer(from_account, to_account, amount):
    """Atomic transfer operation using ordered locking to prevent deadlock"""
                                                                  
                                                            
    if from_account.account_id < to_account.account_id:
        first_lock = from_account.lock
        second_lock = to_account.lock
    else:
        first_lock = to_account.lock
        second_lock = from_account.lock
    
                                                                
    with first_lock:
        with second_lock:
                                                                   
            if from_account.balance >= amount:
                from_account.balance -= amount
                to_account.balance += amount
                return True
            return False


def run_transfers(from_account, to_account, num_transfers):
    """Perform multiple transfers between accounts"""
    for _ in range(num_transfers):
        amount = random.randint(10, 50)
        transfer(from_account, to_account, amount)


def main():
                                                           
    account_a = BankAccount("A", 1000)
    account_b = BankAccount("B", 1000)
    
                         
    print(f"Initial balances:")
    print(f"Account A: ${account_a.balance}")
    print(f"Account B: ${account_b.balance}")
    print(f"Total money in system: ${account_a.balance + account_b.balance}")
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
    
                       
    print("All transfers completed!")
    print(f"Final balances:")
    print(f"Account A: ${account_a.balance}")
    print(f"Account B: ${account_b.balance}")
    print(f"Total money in system: ${account_a.balance + account_b.balance}")
    
                                          
    initial_total = 2000                         
    final_total = account_a.balance + account_b.balance
    
    if final_total != initial_total:
        print(f"\n⚠️ ATOMIC VIOLATION DETECTED! ⚠️")
        print(f"Money was created or destroyed in the system.")
        print(f"Expected total: ${initial_total}")
        print(f"Actual total: ${final_total}")
        print(f"Discrepancy: ${final_total - initial_total}")
    else:
        print("\n✅ System integrity maintained!")
    
                                 
    if account_a.balance < 0 or account_b.balance < 0:
        print(f"\n⚠️ NEGATIVE BALANCE DETECTED! ⚠️")
        if account_a.balance < 0:
            print(f"Account A has gone negative: ${account_a.balance}")
        if account_b.balance < 0:
            print(f"Account B has gone negative: ${account_b.balance}")
    else:
        print("✅ No negative balances detected!")


if __name__ == "__main__":
    main()