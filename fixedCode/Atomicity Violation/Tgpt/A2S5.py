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
        Atomically update the inventory for a specific product.
        The read-modify-write is protected by a lock to prevent races.
        """
                                                                 
        time.sleep(random.uniform(0.001, 0.005))

        lock = self._locks[product_id]
        with lock:
                                                                       
            current_quantity = self.inventory[product_id]
            new_quantity = current_quantity + change_amount
            self.inventory[product_id] = new_quantity
                                          
            print(f"Thread {threading.current_thread().name}: Updated {product_id} by {change_amount}, "
                  f"Expected: {current_quantity + change_amount}, Set to: {new_quantity}")

                                                                         
        time.sleep(random.uniform(0.001, 0.005))


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
        print("Concurrent updates without proper synchronization can lead to incorrect inventory levels.")
    else:
        print("\nNo atomic violation detected. The lock made the update atomic.")


                                 
logged_changes = []
_original_print = print
_log_lock = threading.Lock()                                                   

def log_print(*args, **kwargs):
    message = " ".join(map(str, args))
    with _log_lock:
        logged_changes.append(message)
        _original_print(*args, **kwargs)

print = log_print

                    
if __name__ == "__main__":
    print("===== INVENTORY ATOMIC VIOLATION SIMULATION (FIXED) =====")
    print("This simulation shows atomic, synchronized inventory updates using per-product locks.\n")

    NUM_USERS = 10                                       
    OPERATIONS_PER_USER = 5                                 

    run_simulation(NUM_USERS, OPERATIONS_PER_USER)
