import threading
import time
import random
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

def problematic_task():
    thread_name = threading.current_thread().name
    safe_print("Starting problematic task...")
    
    try:

        safe_print("Attempting first semaphore acquisition...")
        resource_semaphore.acquire()
        safe_print("SUCCESS: First semaphore acquisition completed")

        critical_section_work()
        

        safe_print("Simulating accidental second acquisition attempt...")
        time.sleep(1)
        

        safe_print("CRITICAL: Attempting second semaphore acquisition...")
        safe_print("WARNING: This will cause deadlock - thread will block forever!")
        

        resource_semaphore.acquire()

        safe_print("SUCCESS: Second semaphore acquisition completed (NEVER REACHED)")

        safe_print("Performing additional work (NEVER REACHED)")
        critical_section_work()

        safe_print("Releasing first semaphore (NEVER REACHED)")
        resource_semaphore.release()
        safe_print("Releasing second semaphore (NEVER REACHED)")
        resource_semaphore.release()
        
    except Exception as error:
        safe_print(f"Exception occurred: {error}")
    finally:
        safe_print("Task cleanup (this may not execute due to deadlock)")

def monitoring_task():
    """A separate task to monitor the deadlock situation"""
    safe_print("Monitoring task started...")
    
    for i in range(10):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System still running...")
        safe_print(f"Current shared counter value: {shared_resource_counter}")

        if main_thread.is_alive():
            safe_print("Main task thread is still alive (likely deadlocked)")
        else:
            safe_print("Main task thread has completed")
            break
    
    safe_print("Monitoring task completed")

def timeout_handler():
    """Handler to demonstrate timeout-based deadlock detection"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)
    
    if main_thread.is_alive():
        safe_print("!!!!!!!!!!DEADLOCK DETECTED: Main thread still running after timeout!!!!!!!!!")
        safe_print("In a real application, this is where you'd implement recovery logic")
    else:
        safe_print("Main thread completed normally")

safe_print("=== SEMAPHORE DEADLOCK DEMONSTRATION ===")
safe_print("This program demonstrates how a thread can deadlock itself")
safe_print("by trying to acquire the same semaphore twice.")
safe_print("")

main_thread = threading.Thread(target=problematic_task, name="MainTask")
monitor_thread = threading.Thread(target=monitoring_task, name="Monitor")
timeout_thread = threading.Thread(target=timeout_handler, name="TimeoutHandler")

safe_print("Starting main task thread...")
main_thread.start()

safe_print("Starting monitoring thread...")
monitor_thread.start()

safe_print("Starting timeout handler thread...")
timeout_thread.start()

safe_print("Waiting for main thread to complete...")
main_thread.join(timeout=20)

if main_thread.is_alive():
    safe_print("CONFIRMED: Main thread is deadlocked!")
    safe_print("Program will continue with other threads...")
else:
    safe_print("Main thread completed successfully")

monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("Note: Main thread may still be deadlocked in the background")