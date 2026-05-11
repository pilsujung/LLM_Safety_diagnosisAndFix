import threading
import time
import random


class InventorySystem:
    def __init__(self):
                                                              
        self.inventory = {
            "product_a": 1000,
            "product_b": 500,
            "product_c": 750
        }
                                               
        self.inventory_lock = threading.Lock()
        
    def update_inventory(self, product_id, change_amount):
        """
        Update the inventory for a specific product.
        Now uses proper synchronization to prevent atomic violations.
        """
                                                
        with self.inventory_lock:
                                                      
            current_quantity = self.inventory[product_id]
            
                                                                             
                                                             
            time.sleep(random.uniform(0.001, 0.005))
            
                                                       
            new_quantity = current_quantity + change_amount
            
                                            
            time.sleep(random.uniform(0.001, 0.005))
            
                                                               
            self.inventory[product_id] = new_quantity
            
                                                          
            print(f"Thread {threading.current_thread().name}: Updated {product_id} by {change_amount}, " 
                  f"Expected: {current_quantity + change_amount}, Set to: {new_quantity}")

    def get_inventory(self, product_id):
        """
        Thread-safe method to read inventory levels.
        """
        with self.inventory_lock:
            return self.inventory[product_id]

    def get_all_inventory(self):
        """
        Thread-safe method to read all inventory levels.
        """
        with self.inventory_lock:
            return self.inventory.copy()


def simulate_user_activity(inventory_system, product_id, num_operations):
    """Simulate a user performing multiple inventory updates."""
    for _ in range(num_operations):
                                                                   
        change_amount = random.choice([-5, -3, -1, 1, 3, 5])
        inventory_system.update_inventory(product_id, change_amount)


def run_simulation(num_users, operations_per_user):
    """Run the main simulation with multiple concurrent users."""
    inventory = InventorySystem()
    target_product = "product_a"
    
                              
    initial_quantity = inventory.get_inventory(target_product)
    print(f"\nSIMULATION START: {target_product} initial quantity: {initial_quantity}")
    
                                                                    
    threads = []
    for i in range(num_users):
        thread = threading.Thread(
            target=simulate_user_activity,
            args=(inventory, target_product, operations_per_user),
            name=f"User-{i+1}"
        )
        threads.append(thread)
    
                       
    for thread in threads:
        thread.start()
    
                                      
    for thread in threads:
        thread.join()
    
                            
    final_quantity = inventory.get_inventory(target_product)
    
                                            
    total_reported_changes = 0
    for line in logged_changes:
        if "Updated product_a by" in line:
            change = int(line.split("by ")[1].split(",")[0])
            total_reported_changes += change
    
                                
    print(f"\nSIMULATION COMPLETE")
    print(f"Initial quantity: {initial_quantity}")
    print(f"Final quantity: {final_quantity}")
    print(f"Total reported changes: {total_reported_changes}")
    print(f"Expected final quantity: {initial_quantity + total_reported_changes}")
    
    if final_quantity != (initial_quantity + total_reported_changes):
        print("\n*** ATOMIC VIOLATION DETECTED ***")
        print(f"Discrepancy: {final_quantity - (initial_quantity + total_reported_changes)}")
        print("This should not happen with proper synchronization!")
    else:
        print("\n*** SUCCESS: No atomic violation detected! ***")
        print("Proper synchronization has prevented data inconsistencies.")


                                                                             
class AdvancedInventorySystem:
    def __init__(self):
        self.inventory = {
            "product_a": 1000,
            "product_b": 500,
            "product_c": 750
        }
                                                                         
        self.inventory_lock = threading.RLock()
        
    def update_inventory(self, product_id, change_amount):
        """Update inventory with reentrant lock support."""
        with self.inventory_lock:
            current_quantity = self.inventory[product_id]
            time.sleep(random.uniform(0.001, 0.005))
            new_quantity = current_quantity + change_amount
            time.sleep(random.uniform(0.001, 0.005))
            self.inventory[product_id] = new_quantity
            print(f"Thread {threading.current_thread().name}: Updated {product_id} by {change_amount}, " 
                  f"Expected: {current_quantity + change_amount}, Set to: {new_quantity}")
    
    def batch_update(self, updates):
        """
        Perform multiple updates atomically.
        updates: list of tuples (product_id, change_amount)
        """
        with self.inventory_lock:
            for product_id, change_amount in updates:
                current_quantity = self.inventory[product_id]
                self.inventory[product_id] = current_quantity + change_amount
                print(f"Batch update: {product_id} changed by {change_amount}")
    
    def transfer_inventory(self, from_product, to_product, amount):
        """
        Atomically transfer inventory between products.
        """
        with self.inventory_lock:
            if self.inventory[from_product] >= amount:
                self.inventory[from_product] -= amount
                self.inventory[to_product] += amount
                print(f"Transferred {amount} from {from_product} to {to_product}")
                return True
            else:
                print(f"Insufficient inventory in {from_product} for transfer")
                return False


                                 
logged_changes = []
original_print = print

def log_print(*args, **kwargs):
    message = " ".join(map(str, args))
    logged_changes.append(message)
    original_print(*args, **kwargs)

print = log_print


                    
if __name__ == "__main__":
    print("===== FIXED INVENTORY SYNCHRONIZATION SIMULATION =====")
    print("This simulation demonstrates how proper thread synchronization")
    print("prevents atomic violations and ensures data consistency.\n")
    
                                     
    NUM_USERS = 10                                      
    OPERATIONS_PER_USER = 5                                 
    
                        
    run_simulation(NUM_USERS, OPERATIONS_PER_USER)
    
                                
    print("\n" + "="*60)
    print("===== ADVANCED INVENTORY SYSTEM DEMO =====")
    logged_changes.clear()
    
    advanced_inventory = AdvancedInventorySystem()
    
                                  
    def batch_operations():
        updates = [("product_a", -10), ("product_b", 5), ("product_c", -3)]
        advanced_inventory.batch_update(updates)
    
                                     
    def transfer_operations():
        advanced_inventory.transfer_inventory("product_a", "product_b", 50)
    
                                                  
    batch_thread = threading.Thread(target=batch_operations, name="BatchUser")
    transfer_thread = threading.Thread(target=transfer_operations, name="TransferUser")
    
    batch_thread.start()
    transfer_thread.start()
    
    batch_thread.join()
    transfer_thread.join()
    
    final_inventory = advanced_inventory.inventory
    print(f"\nFinal inventory state: {final_inventory}")