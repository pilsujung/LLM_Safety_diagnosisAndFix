import threading
import time
import random
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Optional

                                                    
inventory = {
    "laptop": 10, "smartphone": 15, "tablet": 8, "headphones": 20,
    "keyboard": 12, "mouse": 18, "monitor": 6, "webcam": 9
}

                   
inventory_lock = threading.Lock()
stats_lock = threading.Lock()

                                                    
def reset_globals():
    global completed_orders, failed_orders, user_sessions, order_statistics
    completed_orders = []
    failed_orders = []
    user_sessions = {}
    order_statistics = {"total_attempts": 0, "successful_orders": 0, 
                       "failed_orders": 0, "revenue": 0.0}

                 
product_prices = {
    "laptop": 999.99, "smartphone": 699.99, "tablet": 399.99,
    "headphones": 199.99, "keyboard": 89.99, "mouse": 49.99,
    "monitor": 299.99, "webcam": 129.99
}

class OrderProcessor:
    def __init__(self):
        self.processing_delay_range = (0.05, 0.2)

    def generate_order_id(self) -> str:
        return f"ORD-{uuid.uuid4().hex[:8].upper()}"

    def log_user_session(self, user_id: int, product_id: str, quantity: int) -> None:
        with stats_lock:
            session_id = f"SESSION-{user_id}-{int(time.time())}"
            user_sessions[user_id] = {
                "session_id": session_id, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "requested_product": product_id, "requested_quantity": quantity,
                "user_agent": f"MobileApp-{random.choice(['iOS', 'Android'])}-{random.randint(1, 5)}.0"
            }

    def process_customer_order(self, user_id: int, product_id: str, quantity: int, priority: str = "normal") -> bool:
        order_id = self.generate_order_id()
        
        self.log_user_session(user_id, product_id, quantity)
        with stats_lock:
            order_statistics["total_attempts"] += 1

        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] User {user_id} ({priority}): Starting order {order_id}")
        print(f"[User {user_id}] Requesting {quantity}x {product_id}")

                                                           
        time.sleep(random.uniform(*self.processing_delay_range))
        if priority == "premium":
            time.sleep(0.02)

                                                                     
        with inventory_lock:
            current_stock = inventory.get(product_id, 0)
            print(f"[User {user_id}] Inventory check: {current_stock} units available")
            
            if current_stock < quantity:
                failed_order = {
                    "order_id": order_id, "user_id": user_id, "product_id": product_id,
                    "quantity": quantity, "failure_reason": "insufficient_inventory",
                    "available_quantity": current_stock, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                }
                failed_orders.append(failed_order)
                with stats_lock:
                    order_statistics["failed_orders"] += 1
                print(f"[User {user_id}] ❌ ORDER FAILED! Insufficient inventory")
                return False

                               
            inventory[product_id] = current_stock - quantity
            order_total = product_prices[product_id] * quantity
            remaining_stock = inventory[product_id]

                                                        
        order_details = {
            "order_id": order_id, "user_id": user_id, "product_id": product_id,
            "quantity": quantity, "total_amount": order_total, "priority": priority,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        completed_orders.append(order_details)
        with stats_lock:
            order_statistics["successful_orders"] += 1
            order_statistics["revenue"] += order_total

        print(f"[User {user_id}] ✅ ORDER SUCCESS! Order {order_id} | Remaining: {remaining_stock}")
        if remaining_stock <= 2:
            print(f"[SYSTEM] ⚠️ LOW STOCK: {remaining_stock} {product_id}(s)")
        return True

                                                                      
def simulate_high_traffic_scenario():
    reset_globals()
                                            
    processor = OrderProcessor()
    threads = []
                                                   
    for user_id in range(1, 8):
        priority = "premium" if user_id <= 2 else "normal"
        thread = threading.Thread(target=processor.process_customer_order, 
                                args=(user_id, "laptop", 2, priority))
        threads.append(thread)
                      
    shopping_list = [(101,"smartphone",1,"normal"), (102,"tablet",3,"premium"),
                    (103,"headphones",2,"normal"), (104,"keyboard",1,"normal"),
                    (105,"mouse",4,"premium"), (106,"monitor",2,"normal"),
                    (107,"webcam",1,"normal"), (108,"laptop",1,"premium"),
                    (109,"smartphone",2,"normal"), (110,"headphones",5,"normal")]
    for user_id, product, qty, priority in shopping_list:
        thread = threading.Thread(target=processor.process_customer_order, 
                                args=(user_id, product, qty, priority))
        threads.append(thread)
    
    print("Launching concurrent orders...")
    for i, thread in enumerate(threads):
        thread.start()
        time.sleep(0.01 if i < 7 else random.uniform(0.005, 0.03))
    for thread in threads:
        thread.join()
    print_final_report()

def print_final_report():
    print("\n📊 ATOMICALLY SAFE RESULTS")
    print("No overselling: Inventory never goes negative.")
    oversold = any(stock < 0 for stock in inventory.values())
    print(f"✅ Verification: {'PASS' if not oversold else 'FAIL'}")

if __name__ == "__main__":
    simulate_high_traffic_scenario()
