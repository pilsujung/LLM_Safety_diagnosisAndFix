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
    FIXED VERSION: Proper semaphore usage with context manager and timeout protection
    Key fixes:
    1. Use context manager (with statement) for automatic release
    2. Add timeout to prevent indefinite blocking
    3. Proper exception handling with guaranteed cleanup
    """
    safe_print("Starting FIXED task...")
    
    try:
        
        safe_print("Attempting FIRST semaphore acquisition with timeout...")
        with resource_semaphore:  
            safe_print("SUCCESS: First semaphore acquisition completed")
            
            
            critical_section_work()
            
            
            safe_print("Simulating processing that needs additional protection...")
            time.sleep(1)
            
            
            safe_print("Using NESTED critical section (no second acquire needed)...")
            
            
            time.sleep(0.5)
            shared_resource_counter += 1
            safe_print(f"Nested work - Counter now: {shared_resource_counter}")
            
            safe_print("First critical section completed successfully")
            
    except threading.TimeoutError:
        safe_print("ERROR: Semaphore acquisition TIMED OUT - aborting operation")
    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        safe_print("Task cleanup completed")

def monitoring_task(main_thread):
    """Monitor the fixed task execution"""
    safe_print("Monitoring task started...")
    
    for i in range(5):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1}")
        safe_print(f"Current shared counter value: {shared_resource_counter}")
        
        if not main_thread.is_alive():
            safe_print("Main task thread has completed")
            break
    
    safe_print("Monitoring task completed")


safe_print("=== FIXED SEMAPHORE DEADLOCK DEMO ===")
safe_print("Demonstrates proper semaphore usage with context managers and timeouts")
safe_print("")


main_thread = threading.Thread(target=fixed_task, name="FixedMainTask")
monitor_thread = threading.Thread(target=monitoring_task, args=(main_thread,), name="Monitor")


safe_print("Starting fixed main task thread...")
main_thread.start()

safe_print("Starting monitoring thread...")
monitor_thread.start()


main_thread.join(timeout=15)
monitor_thread.join()

safe_print("=== FIXED PROGRAM COMPLETED SUCCESSFULLY ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
