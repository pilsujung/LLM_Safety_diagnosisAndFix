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
                                                 
        self.lock = threading.Lock()
        
    def update_inventory(self, product_id, change_amount):
        """
        Update the inventory for a specific product in a thread-safe manner.
        """
                                                                       
        with self.lock:
                                                      
            current_quantity = self.inventory[product_id]
            
                                           
            time.sleep(random.uniform(0.001, 0.005))
            
                                                       
            new_quantity = current_quantity + change_amount
            
                                            
            time.sleep(random.uniform(0.001, 0.005))
            
                                                               
            self.inventory[product_id] = new_quantity
            
                                                          
            print(f"Thread {threading.current_thread().name}: Updated {product_id} by {change_amount}, " 
                  f"Expected: {current_quantity + change_amount}, Set to: {new_quantity}")


def simulate_user_activity(inventory_system, product_id, num_operations):
    """Simulate a user performing multiple inventory updates."""
    for _ in range(num_operations):
                                                                   
        change_amount = random.choice([-5, -3, -1, 1, 3, 5])
        inventory_system.update_inventory(product_id, change_amount)


def run_simulation(num_users, operations_per_user):
    """Run the main simulation with multiple concurrent users."""
    inventory = InventorySystem()
    target_product = "product_a"
    
                              
    initial_quantity = inventory.inventory[target_product]
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
    
                            
    final_quantity = inventory.inventory[target_product]

                                
    print(f"\nSIMULATION COMPLETE")
    print(f"Initial quantity: {initial_quantity}")
    print(f"Final quantity: {final_quantity}")

    return initial_quantity, final_quantity


                    
if __name__ == "__main__":
    print("===== INVENTORY ATOMIC VIOLATION SIMULATION =====")
    print("This simulation demonstrates how concurrent inventory updates")
    print("without proper synchronization can lead to data inconsistencies.\n")
    
                                     
    NUM_USERS = 10                                      
    OPERATIONS_PER_USER = 5                                 
    
                        
    run_simulation(NUM_USERS, OPERATIONS_PER_USER)
