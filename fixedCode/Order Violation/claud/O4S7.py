import threading
import time
import random


payment_completed = False

order_violation_occurred = False

payment_lock = threading.Lock()

def process_payment():
    """Simulates payment processing with a random delay."""
    global payment_completed
    
    print("Starting payment processing...")

    processing_time = random.uniform(1, 3)
    time.sleep(processing_time)
    

    with payment_lock:
        payment_completed = True
    print(f"Payment completed successfully after {processing_time:.2f} seconds")

def request_shipping():
    """Attempts to ship an order, waiting for payment completion."""
    global payment_completed, order_violation_occurred
    
    print("Processing shipping request...")

    shipping_time = random.uniform(0.5, 2)
    time.sleep(shipping_time)
    

    with payment_lock:
        payment_status = payment_completed
    

    if payment_status:
        print(f"Payment verified. Order is being shipped after {shipping_time:.2f} seconds")
    else:
        print(f"ERROR: Order shipped without payment! (after {shipping_time:.2f} seconds)")
        order_violation_occurred = True

def detect_order_violation():
    """Checks if an order violation has occurred after both threads complete."""
    global order_violation_occurred
    if order_violation_occurred:
        print("\nORDER VIOLATION DETECTED: Shipping proceeded before payment was completed!")
    else:
        print("\nNo order violation detected in this run.")

def run_simulation():
    """Runs a single simulation of the payment and shipping process."""
    global payment_completed, order_violation_occurred
    

    payment_completed = False
    order_violation_occurred = False
    
    print("\n===== Starting New Simulation =====")
    

    payment_thread = threading.Thread(target=process_payment)
    shipping_thread = threading.Thread(target=request_shipping)
    

    payment_thread.start()
    shipping_thread.start()
    

    payment_thread.join()
    shipping_thread.join()
    

    detect_order_violation()


if __name__ == "__main__":
    print("E-COMMERCE ORDER PROCESSING SIMULATION (FIXED)")
    print("This program uses thread synchronization to prevent race conditions")
    
    num_simulations = 5
    
    violation_count = 0
    for i in range(num_simulations):
        run_simulation()
        if order_violation_occurred:
            violation_count += 1
    
    print(f"\nCompleted {num_simulations} simulations")
    print(f"Order violations detected: {violation_count}/{num_simulations} ({violation_count/num_simulations*100:.0f}%)")
    
    if violation_count > 0:
        print("\nWarning: Race conditions still occurred!")
    else:
        print("\nSuccess! No race conditions detected. Thread synchronization is working correctly.")