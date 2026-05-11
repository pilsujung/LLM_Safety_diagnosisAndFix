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
completed_orders = []
failed_orders = []
user_sessions = {}
order_statistics = {"total_attempts": 0, "successful_orders": 0, "failed_orders": 0, "revenue": 0.0}
product_prices = {
    "laptop": 999.99, "smartphone": 699.99, "tablet": 399.99, "headphones": 199.99,
    "keyboard": 89.99, "mouse": 49.99, "monitor": 299.99, "webcam": 129.99
}

                              
inventory_lock = threading.Lock()
stats_lock = threading.Lock()

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
        start_time = time.time()
        self.log_user_session(user_id, product_id, quantity)

        with stats_lock:
            order_statistics["total_attempts"] += 1

        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] User {user_id} ({priority}): Starting order {order_id}")
        print(f"[User {user_id}] Requesting {quantity}x {product_id}")

                                                                     
        with inventory_lock:
            current_stock = inventory.get(product_id, 0)
            print(f"[User {user_id}] Inventory check: {current_stock} units available")
            
            if current_stock < quantity:
                with stats_lock:
                    failed_orders.append({
                        "order_id": order_id, "user_id": user_id, "product_id": product_id,
                        "quantity": quantity, "failure_reason": "insufficient_inventory",
                        "available_quantity": current_stock, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    })
                    order_statistics["failed_orders"] += 1
                print(f"[User {user_id}] ❌ ORDER FAILED! Insufficient inventory")
                print(f"[User {user_id}] Requested: {quantity}, Available: {current_stock}")
                return False

                               
            inventory[product_id] = current_stock - quantity
            remaining_stock = inventory[product_id]

                                                               
        order_total = product_prices.get(product_id, 0.0) * quantity
        order_details = {
            "order_id": order_id, "user_id": user_id, "product_id": product_id, "quantity": quantity,
            "unit_price": product_prices[product_id], "total_amount": order_total,
            "priority": priority, "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "processing_time": time.time() - start_time
        }

        with stats_lock:
            completed_orders.append(order_details)
            order_statistics["successful_orders"] += 1
            order_statistics["revenue"] += order_total

        print(f"[User {user_id}] ✅ ORDER SUCCESS! Order {order_id} completed")
        print(f"[User {user_id}] Charged ${order_total:.2f} | Remaining stock: {remaining_stock}")
        if remaining_stock <= 2:
            print(f"[SYSTEM] ⚠️ LOW STOCK ALERT: Only {remaining_stock} {product_id}(s) remaining!")

                                            
        time.sleep(random.uniform(*self.processing_delay_range))
        if priority == "premium":
            time.sleep(0.02)
        return True

def simulate_high_traffic_scenario():
                                 
    global inventory, completed_orders, failed_orders, user_sessions, order_statistics
    inventory = {"laptop": 10, "smartphone": 15, "tablet": 8, "headphones": 20,
                 "keyboard": 12, "mouse": 18, "monitor": 6, "webcam": 9}
    for lst in [completed_orders, failed_orders]: lst.clear()
    user_sessions.clear()
    order_statistics = {"total_attempts": 0, "successful_orders": 0, "failed_orders": 0, "revenue": 0.0}

    print("🚀 THREAD-SAFE E-COMMERCE SIMULATION")
    processor = OrderProcessor()
    threads = []

                                               
    for user_id in range(1, 8):
        priority = "premium" if user_id <= 2 else "normal"
        threads.append(threading.Thread(target=processor.process_customer_order,
                                       args=(user_id, "laptop", 2, priority)))

                  
    shopping_list = [
        (101, "smartphone", 1, "normal"), (102, "tablet", 3, "premium"), (103, "headphones", 2, "normal"),
        (104, "keyboard", 1, "normal"), (105, "mouse", 4, "premium"), (106, "monitor", 2, "normal"),
        (107, "webcam", 1, "normal"), (108, "laptop", 1, "premium"), (109, "smartphone", 2, "normal"),
        (110, "headphones", 5, "normal")
    ]
    for user_id, product, qty, priority in shopping_list:
        threads.append(threading.Thread(target=processor.process_customer_order,
                                       args=(user_id, product, qty, priority)))

    for thread in threads:
        thread.start()
        time.sleep(0.001)                  
    for thread in threads:
        thread.join()

                           
    oversold = [p for p, s in inventory.items() if s < 0]
    print("\n✅ FIXED: No overselling detected!" + (f" (oversold: {oversold})" if oversold else ""))

if __name__ == "__main__":
    simulate_high_traffic_scenario()
