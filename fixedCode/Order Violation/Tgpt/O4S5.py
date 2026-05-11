import threading
import time
import random


payment_completed = False

order_violation_occurred = False


payment_completed_event = threading.Event()


def process_payment():
    """Simulates payment processing with a random delay."""
    global payment_completed

    print("Starting payment processing...")

    processing_time = random.uniform(1, 3)
    time.sleep(processing_time)


    payment_completed = True


    payment_completed_event.set()

    print(f"Payment completed successfully after {processing_time:.2f} seconds")


def request_shipping():
    """Ships an order only after payment completion."""
    global payment_completed, order_violation_occurred

    print("Waiting for payment to complete before shipping...")


    payment_completed_event.wait()

    print("Processing shipping request...")

    shipping_time = random.uniform(0.5, 2)
    time.sleep(shipping_time)


    if payment_completed:
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
    payment_completed_event.clear()

    print("\n===== Starting New Simulation =====")


    payment_thread = threading.Thread(target=process_payment)
    shipping_thread = threading.Thread(target=request_shipping)


    payment_thread.start()
    shipping_thread.start()


    payment_thread.join()
    shipping_thread.join()


    detect_order_violation()



if __name__ == "__main__":
    print("E-COMMERCE ORDER PROCESSING SIMULATION (FIXED ORDER)")

    num_simulations = 5
    violation_count = 0

    for i in range(num_simulations):
        run_simulation()
        if order_violation_occurred:
            violation_count += 1

    print(f"\nCompleted {num_simulations} simulations")
    print(f"Order violations detected: {violation_count}/{num_simulations} "
          f"({violation_count / num_simulations * 100:.0f}%)")

    if violation_count > 0:
        print("\nUnexpected order violations still occurred – check synchronization logic.")
    else:
        print("\nOrder violations were successfully prevented.")
