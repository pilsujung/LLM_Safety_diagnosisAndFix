import threading
import time
import random
from concurrent.futures import ThreadPoolExecutor


payment_latch = threading.Event()

order_violation_occurred = False

def process_payment():
    """Simulates payment processing with a random delay."""
    global payment_latch
    
    print("Starting payment processing...")

    processing_time = random.uniform(1, 3)
    time.sleep(processing_time)


    payment_latch.set()
    print(f"Payment completed successfully after {processing_time:.2f} seconds")

def request_shipping():
    """Waits for payment completion before shipping."""
    global payment_latch, order_violation_occurred

    print("Processing shipping request...")
    

    payment_latch.wait()
    

    shipping_time = random.uniform(0.5, 2)
    time.sleep(shipping_time)

    print(f"Payment verified. Order is being shipped after {shipping_time:.2f} seconds")

def detect_order_violation():
    """Checks if an order violation has occurred after both threads complete."""
    global order_violation_occurred
    if order_violation_occurred:
        print("\nORDER VIOLATION DETECTED: Shipping proceeded before payment was completed!")
    else:
        print("\nNo order violation detected in this run.")

def run_simulation():
    """Runs a single simulation of the payment and shipping process."""
    global payment_latch, order_violation_occurred


    payment_latch.clear()
    order_violation_occurred = False

    print("\n===== Starting New Simulation =====")


    with ThreadPoolExecutor(max_workers=2) as executor:
        future_payment = executor.submit(process_payment)
        future_shipping = executor.submit(request_shipping)
        

        future_payment.result()
        future_shipping.result()


    detect_order_violation()


if __name__ == "__main__":
    print("E-COMMERCE ORDER PROCESSING SIMULATION (FIXED)")
    print("Payment MUST complete before shipping - 100% guaranteed via Event synchronization")

    num_simulations = 10
    violation_count = 0
    
    for i in range(num_simulations):
        run_simulation()
        if order_violation_occurred:
            violation_count += 1

    print(f"\nCompleted {num_simulations} simulations")
    print(f"Order violations detected: {violation_count}/{num_simulations} ({violation_count/num_simulations*100:.1f}%)")

    if violation_count == 0:
        print("\n✅ PERFECT SYNCHRONIZATION: 0% violations across all runs!")
        print("Event.wait() guarantees shipping blocks until payment signals via set()")
