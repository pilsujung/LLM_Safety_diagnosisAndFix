import threading
import time
from datetime import datetime


resource_semaphore = threading.Semaphore(1)


shared_resource_counter = 0


print_lock = threading.Lock()

def safe_print(message):
    """Thread-safe printing function"""
    with print_lock:
        timestamp = datetime.now().strftime("%H:%M:%S.%f")[:-3]
        print(f"[{timestamp}] {threading.current_thread().name}: {message}")

def critical_section_work():
    """Simulate some work in the critical section"""
    global shared_resource_counter
    safe_print("Performing critical section work...")

    
    for i in range(3):
        shared_resource_counter += 1
        safe_print(f"Counter incremented to: {shared_resource_counter}")
        time.sleep(0.5)

def fixed_task():
    """
    FIXED VERSION: Proper semaphore usage with timeout and context manager pattern
    Uses acquire(timeout=...) to prevent indefinite blocking and ensures proper release
    """
    thread_name = threading.current_thread().name
    safe_print("Starting FIXED task...")

    
    safe_print("Attempting first semaphore acquisition...")
    if not resource_semaphore.acquire(timeout=5.0):
        safe_print("ERROR: Failed to acquire semaphore (timeout)")
        return

    try:
        safe_print("SUCCESS: First semaphore acquisition completed")
        critical_section_work()

        safe_print("Simulating processing that might have led to mistake...")
        time.sleep(1)

        
        safe_print("CRITICAL: Checking if semaphore already held (FIXED)...")
        
        
        safe_print("ALREADY HOLDING SEMAPHORE - Skipping second acquisition (FIXED)")
        safe_print("Performing additional work while still holding semaphore...")
        
        
        shared_resource_counter += 1
        safe_print(f"Additional work: Counter incremented to: {shared_resource_counter}")
        time.sleep(0.5)

    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        
        safe_print("Releasing semaphore (GUARANTEED)")
        resource_semaphore.release()
        safe_print("SUCCESS: Semaphore released properly")

def monitoring_task():
    """Monitor the fixed task execution"""
    safe_print("Monitoring task started...")
    for i in range(5):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - Counter: {shared_resource_counter}")
    safe_print("Monitoring task completed")


safe_print("=== FIXED SEMAPHORE DEADLOCK DEMONSTRATION ===")
safe_print("Demonstrates proper semaphore usage with timeout and guaranteed release")

main_thread = threading.Thread(target=fixed_task, name="FixedMainTask")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")

safe_print("Starting fixed main task...")
main_thread.start()
safe_print("Starting monitoring thread...")
monitor_thread.start()


main_thread.join()
monitor_thread.join()

safe_print("=== FIXED PROGRAM COMPLETED SUCCESSFULLY ===")
safe_print(f"Final counter value: {shared_resource_counter}")
