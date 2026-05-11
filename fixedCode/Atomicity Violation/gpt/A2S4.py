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
                                                                               
        self._locks = {pid: threading.Lock() for pid in self.inventory}

    def update_inventory(self, product_id, change_amount):
        """
        Update the inventory for a specific product atomically.

        We protect the entire read-modify-write sequence with a lock so that
        concurrent threads cannot interleave and overwrite each other's updates.
        """
        if product_id not in self.inventory:
            raise KeyError(f"Unknown product_id: {product_id}")

        lock = self._locks[product_id]
        with lock:
                                                      
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

                                            
    total_reported_changes = 0
    for line in logged_changes:
        if "Updated product_a by" in line:
            try:
                change = int(line.split("by ")[1].split(",")[0])
                total_reported_changes += change
            except Exception:
                                             
                continue

                                
    print(f"\nSIMULATION COMPLETE")
    print(f"Initial quantity: {initial_quantity}")
    print(f"Final quantity: {final_quantity}")
    print(f"Total reported changes: {total_reported_changes}")
    print(f"Expected final quantity: {initial_quantity + total_reported_changes}")

    if final_quantity != (initial_quantity + total_reported_changes):
        print("\n*** ATOMIC VIOLATION DETECTED ***")
        print(f"Discrepancy: {final_quantity - (initial_quantity + total_reported_changes)}")
        print("This would indicate missing synchronization.")
    else:
        print("\nNo atomic violation detected. Updates are now atomic thanks to locking.")


                                 
logged_changes = []
original_print = print

def log_print(*args, **kwargs):
    message = " ".join(map(str, args))
    logged_changes.append(message)
    original_print(*args, **kwargs)

print = log_print

                    
if __name__ == "__main__":
    print("===== INVENTORY ATOMIC VIOLATION SIMULATION =====")
    print("This simulation demonstrates how proper synchronization prevents data inconsistencies.\n")

                                     
    NUM_USERS = 10                                       
    OPERATIONS_PER_USER = 5                                 

                        
    run_simulation(NUM_USERS, OPERATIONS_PER_USER)
