import threading
import time
import random
import uuid
from datetime import datetime
from typing import Dict, List, Tuple, Optional

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

completed_orders = []
failed_orders = []
user_sessions = {}
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

inventory_lock = threading.Lock()
orders_lock = threading.Lock()
statistics_lock = threading.Lock()
sessions_lock = threading.Lock()

class OrderProcessor:
    """Enhanced order processing system with proper synchronization"""
    
    def __init__(self):
        self.processing_delay_range = (0.05, 0.2)
        
    def generate_order_id(self) -> str:
        """Generate unique order ID"""
        return f"ORD-{uuid.uuid4().hex[:8].upper()}"
    
    def log_user_session(self, user_id: int, product_id: str, quantity: int) -> None:
        """Track user session information with thread safety"""
        session_id = f"SESSION-{user_id}-{int(time.time())}"
        session_data = {
            "session_id": session_id,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "requested_product": product_id,
            "requested_quantity": quantity,
            "user_agent": f"MobileApp-{random.choice(['iOS', 'Android'])}-{random.randint(1, 5)}.0"
        }

        with sessions_lock:
            user_sessions[user_id] = session_data
    
    def check_inventory_availability(self, product_id: str, quantity: int) -> Tuple[bool, int]:
        """Check if sufficient inventory is available"""
        current_stock = inventory.get(product_id, 0)
        is_available = current_stock >= quantity
        return is_available, current_stock
    
    def calculate_order_total(self, product_id: str, quantity: int) -> float:
        """Calculate total order amount"""
        unit_price = product_prices.get(product_id, 0.0)
        return unit_price * quantity
    
    def process_customer_order(self, user_id: int, product_id: str, quantity: int, priority: str = "normal") -> bool:
        """
        Enhanced order processing with proper synchronization
        
        Args:
            user_id: Customer identifier
            product_id: Product to order
            quantity: Number of items requested
            priority: Order priority level
        
        Returns:
            bool: True if order successful, False otherwise
        """
        order_id = self.generate_order_id()
        start_time = time.time()

        self.log_user_session(user_id, product_id, quantity)

        with statistics_lock:
            order_statistics["total_attempts"] += 1
        
        print(f"[{datetime.now().strftime('%H:%M:%S.%f')[:-3]}] User {user_id} ({priority}): Starting order {order_id}")
        print(f"[User {user_id}] Requesting {quantity}x {product_id}")

        processing_delay = random.uniform(*self.processing_delay_range)
        print(f"[User {user_id}] Processing payment and validating order... ({processing_delay:.3f}s)")
        time.sleep(processing_delay)

        if priority == "premium":
            time.sleep(0.02)

        with inventory_lock:
            is_available, current_stock = self.check_inventory_availability(product_id, quantity)
            print(f"[User {user_id}] Inventory check: {current_stock} units available")
            
            if is_available:
                order_total = self.calculate_order_total(product_id, quantity)

                inventory[product_id] = current_stock - quantity
                remaining_stock = inventory[product_id]
                
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

                with orders_lock:
                    completed_orders.append(order_details)

                with statistics_lock:
                    order_statistics["successful_orders"] += 1
                    order_statistics["revenue"] += order_total
                
                print(f"[User {user_id}] ✅ ORDER SUCCESS! Order {order_id} completed")
                print(f"[User {user_id}] Charged ${order_total:.2f} | Remaining stock: {remaining_stock}")
                
                if remaining_stock <= 2:
                    print(f"[SYSTEM] ⚠️  LOW STOCK ALERT: Only {remaining_stock} {product_id}(s) remaining!")
                
                return True
                
            else:
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
                

                with orders_lock:
                    failed_orders.append(failed_order)

                with statistics_lock:
                    order_statistics["failed_orders"] += 1
                
                print(f"[User {user_id}] ❌ ORDER FAILED! Insufficient inventory")
                print(f"[User {user_id}] Requested: {quantity}, Available: {current_stock}")
                
                return False

def simulate_high_traffic_scenario():
    """Simulate realistic e-commerce traffic with concurrent users"""
    
    print("🚀 Starting High-Traffic E-commerce Simulation")
    print("=" * 60)
    print(f"Initial Inventory State:")
    for product, stock in inventory.items():
        print(f"  {product}: {stock} units (${product_prices[product]:.2f} each)")
    print("=" * 60)
    
    processor = OrderProcessor()
    threads = []

    print("\n📱 Scenario 1: Flash sale - Multiple users ordering laptops")
    laptop_buyers = []
    for user_id in range(1, 8):
        priority = "premium" if user_id <= 2 else "normal"
        thread = threading.Thread(
            target=processor.process_customer_order,
            args=(user_id, "laptop", 2, priority),
            name=f"User-{user_id}-Thread"
        )
        laptop_buyers.append(thread)
        threads.append(thread)

    print("\n🛒 Scenario 2: Regular shopping - Various product orders")
    regular_shoppers = []
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
            target=processor.process_customer_order,
            args=(user_id, product, qty, priority),
            name=f"Shopper-{user_id}-Thread"
        )
        regular_shoppers.append(thread)
        threads.append(thread)

    print(f"\n⏱️  Launching {len(threads)} concurrent order threads...")
    
    for i, thread in enumerate(threads):
        thread.start()
        if i < len(laptop_buyers):
            time.sleep(0.01)
        else:
            time.sleep(random.uniform(0.005, 0.03))

    for thread in threads:
        thread.join()

    generate_final_report()

def generate_final_report():
    """Generate detailed analysis of the simulation results"""
    
    print("\n" + "=" * 80)
    print("📊 FINAL SIMULATION REPORT")
    print("=" * 80)

    print(f"\n📦 FINAL INVENTORY STATUS:")
    total_remaining = 0
    
    with inventory_lock:
        inventory_snapshot = inventory.copy()
    
    for product, remaining in inventory_snapshot.items():
        sold = (10 if product == "laptop" else 
                15 if product == "smartphone" else
                8 if product == "tablet" else
                20 if product == "headphones" else
                12 if product == "keyboard" else
                18 if product == "mouse" else
                6 if product == "monitor" else 9) - remaining
        
        total_remaining += remaining
        status = "🔴 OUT OF STOCK" if remaining <= 0 else "🟡 LOW STOCK" if remaining <= 2 else "🟢 IN STOCK"
        print(f"  {product:<12}: {remaining:>2} remaining | {sold:>2} sold | {status}")

    with statistics_lock:
        stats_snapshot = order_statistics.copy()
    
    print(f"\n📈 ORDER STATISTICS:")
    print(f"  Total Order Attempts: {stats_snapshot['total_attempts']}")
    print(f"  Successful Orders:    {stats_snapshot['successful_orders']}")
    print(f"  Failed Orders:        {stats_snapshot['failed_orders']}")
    print(f"  Success Rate:         {(stats_snapshot['successful_orders']/stats_snapshot['total_attempts']*100):.1f}%")
    print(f"  Total Revenue:        ${stats_snapshot['revenue']:.2f}")

    with orders_lock:
        completed_snapshot = completed_orders.copy()
        failed_snapshot = failed_orders.copy()
    
    if completed_snapshot:
        print(f"\n✅ SUCCESSFUL ORDERS ({len(completed_snapshot)}):")
        for order in sorted(completed_snapshot, key=lambda x: x['timestamp']):
            print(f"  {order['order_id']} | User {order['user_id']:>3} | "
                  f"{order['quantity']}x {order['product_id']:<12} | "
                  f"${order['total_amount']:>7.2f} | {order['priority']}")
    
    if failed_snapshot:
        print(f"\n❌ FAILED ORDERS ({len(failed_snapshot)}):")
        for order in failed_snapshot:
            print(f"  {order['order_id']} | User {order['user_id']:>3} | "
                  f"{order['quantity']}x {order['product_id']:<12} | "
                  f"Reason: {order['failure_reason']}")

    print(f"\n🏃 RACE CONDITION ANALYSIS:")
    oversold_products = [product for product, stock in inventory_snapshot.items() if stock < 0]
    if oversold_products:
        print(f"  ⚠️  DETECTED OVERSELLING in: {', '.join(oversold_products)}")
        print(f"  This indicates race conditions occurred during concurrent processing!")
    else:
        print(f"  ✅ No negative inventory detected - proper synchronization in place!")

    with sessions_lock:
        sessions_snapshot = len(user_sessions)
        mobile_users = sum(1 for session in user_sessions.values() if 'Mobile' in session['user_agent'])
    
    print(f"\n👥 USER SESSION SUMMARY:")
    print(f"  Total Active Users: {sessions_snapshot}")
    print(f"  Mobile Users:       {mobile_users}")
    print(f"  Desktop Users:      {sessions_snapshot - mobile_users}")

if __name__ == "__main__":
    print("🛍️  E-COMMERCE RACE CONDITION FIX DEMONSTRATION")
    print("This simulation shows proper synchronization for concurrent access")
    print("to shared resources, preventing inventory inconsistencies.")
    print("\nPress Enter to start the simulation...")
    input()
    
    simulate_high_traffic_scenario()
    
    print(f"\n✅ Race conditions FIXED using proper locking mechanisms!")
    print(f"   All critical sections are now protected with appropriate locks.")