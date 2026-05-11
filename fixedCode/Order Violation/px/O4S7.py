import threading
import time
import random


payment_event = threading.Event()

payment_completed = False
order_violation_occurred = False

def process_payment():
    """Simulates payment processing with a random delay."""
    global payment_completed
    print("Starting payment processing...")
    processing_time = random.uniform(1, 3)
    time.sleep(processing_time)
    payment_completed = True
    print(f"Payment completed successfully after {processing_time:.2f} seconds")
    payment_event.set()

def request_shipping():
    """Waits for payment completion before shipping."""
    global payment_completed, order_violation_occurred
    print("Processing shipping request...")
    shipping_time = random.uniform(0.5, 2)
    time.sleep(shipping_time)
    

    payment_event.wait()
    
    print(f"Payment verified. Order is being shipped after {shipping_time:.2f} seconds")

def detect_order_violation():
    """Checks if an order violation has occurred."""
    global order_violation_occurred
    if order_violation_occurred:
        print("\nORDER VIOLATION DETECTED!")
    else:
        print("\nNo order violation detected.")

def run_simulation():
    """Runs a single simulation."""
    global payment_completed, order_violation_occurred
    payment_completed = False
    order_violation_occurred = False
    payment_event.clear()

    print("\n===== Starting New Simulation =====")
    payment_thread = threading.Thread(target=process_payment)
    shipping_thread = threading.Thread(target=request_shipping)

    payment_thread.start()
    shipping_thread.start()

    payment_thread.join()
    shipping_thread.join()

    detect_order_violation()


if __name__ == "__main__":
    print("E-COMMERCE ORDER PROCESSING SIMULATION - FIXED")
    print("Using threading.Event to enforce payment-before-shipping order.\n")
    
    num_simulations = 5
    violation_count = 0
    
    for i in range(num_simulations):
        run_simulation()
        if order_violation_occurred:
            violation_count += 1
    
    print(f"\nCompleted {num_simulations} simulations")
    print(f"Order violations: {violation_count}/{num_simulations} (0% expected)")
