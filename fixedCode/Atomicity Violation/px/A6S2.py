import threading
import time
import random
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Optional
from threading import Lock

                                                    
inventory = {
    "laptop": 10,
    "smartphone": 15,
    "tablet": 8,
    "headphones": 20,
    "keyboard": 12,
    "mouse": 18,
    "monitor": 6,
    "webcam": 9
}

                                             
inventory_lock = Lock()

                                                                  
completed_orders = []
failed_orders = []
user_sessions = {}

                        
order_statistics_lock = Lock()
order_statistics = {
    "total_attempts": 0,
    "successful_orders": 0,
    "failed_orders": 0,
    "revenue": 0.0
}

                                          
product_prices = {
    "laptop": 999.99,
    "smartphone": 699.99,
    "tablet": 399.99,
    "headphones": 199.99,
    "keyboard": 89.99,
    "mouse": 49.99,
    "monitor": 299.99,
    "webcam": 129.99
}

class OrderProcessor:
    """Enhanced order processing system with atomic operations"""

    def __init__(self):
        self.processing_delay_range = (0.05, 0.2)                                 

    def generate_order_id(self) -> str:
        """Generate unique order ID"""
        return f"ORD-{uuid.uuid4().hex[:8].upper()}"

    def log_user_session(self, user_id: int, product_id: str, quantity: int) -> None:
        """Track user session information"""
        session_id = f"SESSION-{user_id}-{int(time.time())}"
        user_sessions[user_id] = {
            "session_id": session_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "requested_product": product_id,
            "requested_quantity": quantity,
            "user_agent": f"MobileApp-{random.choice(['iOS', 'Android'])}-{random.randint(1, 5)}.0"
        }

    def check_and_update_inventory(self, product_id: str, quantity: int) -> Tuple[bool, int]:
        """ATOMIC inventory check and update operation"""
        with inventory_lock:
            current_stock = inventory.get(product_id, 0)
            if current_stock >= quantity:
                inventory[product_id] = current_stock - quantity
                return True, current_stock
            return False, current_stock

    def calculate_order_total(self, product_id: str, quantity: int) -> float:
        """Calculate total order amount"""
        unit_price = product_prices.get(product_id, 0.0)
        return unit_price * quantity

    def update_statistics(self, success: bool, order_total: float = 0.0):
        """Thread-safe statistics update"""
        with order_statistics_lock:
            order_statistics["total_attempts"] += 1
            if success:
                order_statistics["successful_orders"] += 1
                order_statistics["revenue"] += order_total
            else:
                order_statistics["failed_orders"] += 1

    def process_customer_order(self, user_id: int, product_id: str, quantity: int, priority: str = "normal") -> bool:
        """
        Thread-safe order processing with atomic inventory operations
        """
        order_id = self.generate_order_id()
        start_time = time.time()

                                                                         
        self.log_user_session(user_id, product_id, quantity)

        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] User {user_id} ({priority}): Starting order {order_id}")
        print(f"[User {user_id}] Requesting {quantity}x {product_id}")

                                                            
        processing_delay = random.uniform(*self.processing_delay_range)
        print(f"[User {user_id}] Processing payment and validating order... ({processing_delay:.3f}s)")
        time.sleep(processing_delay)

        if priority == "premium":
            time.sleep(0.02)                                              

                                                   
        is_available, current_stock = self.check_and_update_inventory(product_id, quantity)
        print(f"[User {user_id}] Inventory check: {current_stock} units available")

        if is_available:
                                                                    
            order_total = self.calculate_order_total(product_id, quantity)

                               
            self.update_statistics(True, order_total)

                                     
            order_details = {
                "order_id": order_id,
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "unit_price": product_prices[product_id],
                "total_amount": order_total,
                "priority": priority,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "processing_time": time.time() - start_time
            }

            completed_orders.append(order_details)

            remaining_stock = inventory[product_id]
            print(f"[User {user_id}] ✅ ORDER SUCCESS! Order {order_id} completed")
            print(f"[User {user_id}] Charged ${order_total:.2f} | Remaining stock: {remaining_stock}")

                                                            
            if remaining_stock <= 2:
                print(f"[SYSTEM] ⚠️ LOW STOCK ALERT: Only {remaining_stock} {product_id}(s) remaining!")

            return True
        else:
                                                
            self.update_statistics(False)

                                 
            failed_order = {
                "order_id": order_id,
                "user_id": user_id,
                "product_id": product_id,
                "quantity": quantity,
                "failure_reason": "insufficient_inventory",
                "requested_quantity": quantity,
                "available_quantity": current_stock,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }

            failed_orders.append(failed_order)
            print(f"[User {user_id}] ❌ ORDER FAILED! Insufficient inventory")
            print(f"[User {user_id}] Requested: {quantity}, Available: {current_stock}")

            return False

    def simulate_high_traffic_scenario(self):
        """Simulate realistic e-commerce traffic with concurrent users"""

        print("🚀 Starting High-Traffic E-commerce Simulation - ATOMIC VERSION")
        print("=" * 70)
        print(f"Initial Inventory State:")
        for product, stock in inventory.items():
            print(f" {product}: {stock} units (${product_prices[product]:.2f} each)")
        print("=" * 70)

        threads = []

                                                                   
        print("\n📱 Scenario 1: Flash sale - Multiple users ordering laptops")
        for user_id in range(1, 8):
            priority = "premium" if user_id <= 2 else "normal"
            thread = threading.Thread(
                target=self.process_customer_order,
                args=(user_id, "laptop", 2, priority),
                name=f"User-{user_id}-Thread"
            )
            threads.append(thread)

                                                                 
        print("\n🛒 Scenario 2: Regular shopping - Various product orders")
        shopping_list = [
            (101, "smartphone", 1, "normal"),
            (102, "tablet", 3, "premium"),
            (103, "headphones", 2, "normal"),
            (104, "keyboard", 1, "normal"),
            (105, "mouse", 4, "premium"),
            (106, "monitor", 2, "normal"),
            (107, "webcam", 1, "normal"),
            (108, "laptop", 1, "premium"),
            (109, "smartphone", 2, "normal"),
            (110, "headphones", 5, "normal")
        ]

        for user_id, product, qty, priority in shopping_list:
            thread = threading.Thread(
                target=self.process_customer_order,
                args=(user_id, product, qty, priority),
                name=f"Shopper-{user_id}-Thread"
            )
            threads.append(thread)

                                                                           
        print(f"\n⏱️ Launching {len(threads)} concurrent order threads...")

        for i, thread in enumerate(threads):
            thread.start()
            time.sleep(random.uniform(0.005, 0.03))                                       

                                          
        for thread in threads:
            thread.join()

                                       
        self.generate_final_report()

    def generate_final_report(self):
        """Generate detailed analysis of the simulation results"""

        print("\n" + "=" * 80)
        print("📊 FINAL SIMULATION REPORT - ATOMIC VERSION")
        print("=" * 80)

                          
        print(f"\n📦 FINAL INVENTORY STATUS:")
        total_remaining = 0
        initial_stocks = {
            "laptop": 10, "smartphone": 15, "tablet": 8, "headphones": 20,
            "keyboard": 12, "mouse": 18, "monitor": 6, "webcam": 9
        }
        for product, remaining in inventory.items():
            sold = initial_stocks[product] - remaining
            total_remaining += remaining
            status = "🔴 OUT OF STOCK" if remaining <= 0 else "🟡 LOW STOCK" if remaining <= 2 else "🟢 IN STOCK"
            print(f" {product:<12}: {remaining:>2} remaining | {sold:>2} sold | {status}")

                                            
        with order_statistics_lock:
            stats = order_statistics.copy()
        
        print(f"\n📈 ORDER STATISTICS:")
        print(f" Total Order Attempts: {stats['total_attempts']}")
        print(f" Successful Orders: {stats['successful_orders']}")
        print(f" Failed Orders: {stats['failed_orders']}")
        print(f" Success Rate: {(stats['successful_orders']/stats['total_attempts']*100):.1f}%")
        print(f" Total Revenue: ${stats['revenue']:.2f}")

                                 
        if completed_orders:
            print(f"\n✅ SUCCESSFUL ORDERS ({len(completed_orders)}):")
            for order in sorted(completed_orders, key=lambda x: x['timestamp'])[:5]:              
                print(f" {order['order_id']} | User {order['user_id']:>3} | "
                      f"{order['quantity']}x {order['product_id']:<12} | "
                      f"${order['total_amount']:>7.2f} | {order['priority']}")

        if failed_orders:
            print(f"\n❌ FAILED ORDERS ({len(failed_orders)}):")
            for order in failed_orders[:3]:              
                print(f" {order['order_id']} | User {order['user_id'] :>3} | "
                      f"{order['quantity']}x {order['product_id']:<12} | "
                      f"Reason: {order['failure_reason']}")

                                   
        print(f"\n✅ ATOMIC VIOLATION ANALYSIS:")
        oversold_products = [product for product, stock in inventory.items() if stock < 0]
        if oversold_products:
            print(f" ⚠️ DETECTED OVERSELLING in: {', '.join(oversold_products)}")
        else:
            print(f" ✅ NO OVERSELLING DETECTED - Atomic operations working correctly!")

                              
        print(f"\n👥 USER SESSION SUMMARY:")
        print(f" Total Active Users: {len(user_sessions)}")
        mobile_users = sum(1 for session in user_sessions.values() if 'Mobile' in session['user_agent'])
        print(f" Mobile Users: {mobile_users}")
        print(f" Desktop Users: {len(user_sessions) - mobile_users}")

def main():
    """Main entry point"""
    print("🛍️ E-COMMERCE ATOMIC ORDER PROCESSING DEMONSTRATION")
    print("This simulation demonstrates thread-safe inventory management using locks.")
    print("No overselling will occur even under high concurrency!")
    print("\nPress Enter to start the simulation...")
    input()
    
    processor = OrderProcessor()
    processor.simulate_high_traffic_scenario()

if __name__ == "__main__":
    main()
