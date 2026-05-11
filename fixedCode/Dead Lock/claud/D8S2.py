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

def resolved_task():
    """
    FIXED VERSION: Avoids deadlock by acquiring the semaphore only once.
    The key fix is to ensure we never try to acquire the same semaphore twice
    from the same thread.
    """
    thread_name = threading.current_thread().name
    safe_print("Starting resolved task...")
    
    try:
        
        safe_print("Attempting semaphore acquisition...")
        resource_semaphore.acquire()
        safe_print("SUCCESS: Semaphore acquisition completed")
        
        
        critical_section_work()
        
        
        safe_print("Continuing work without nested acquisition...")
        time.sleep(1)  
        
        
        safe_print("Performing additional work within single critical section...")
        critical_section_work()
        
        
        safe_print("Releasing semaphore...")
        resource_semaphore.release()
        safe_print("SUCCESS: Semaphore released - task completed normally")
        
    except Exception as error:
        safe_print(f"Exception occurred: {error}")
        
        try:
            resource_semaphore.release()
        except:
            pass
    finally:
        safe_print("Task cleanup completed successfully")

def monitoring_task():
    """A separate task to monitor the execution"""
    safe_print("Monitoring task started...")
    
    for i in range(10):
        time.sleep(2)
        safe_print(f"Monitor check #{i+1} - System running normally...")
        safe_print(f"Current shared counter value: {shared_resource_counter}")
        
        
        if main_thread.is_alive():
            safe_print("Main task thread is still running")
        else:
            safe_print("Main task thread has completed successfully")
            break
    
    safe_print("Monitoring task completed")

def timeout_handler():
    """Handler to verify no deadlock occurs"""
    safe_print("Timeout handler started - will wait 15 seconds...")
    time.sleep(15)
    
    if main_thread.is_alive():
        safe_print("WARNING: Main thread still running after timeout")
    else:
        safe_print("SUCCESS: Main thread completed normally - no deadlock!")


safe_print("=== SEMAPHORE DEADLOCK RESOLUTION DEMONSTRATION ===")
safe_print("This program demonstrates the FIXED version that avoids deadlock")
safe_print("by acquiring the semaphore only once per critical section.")
safe_print("")

main_thread = threading.Thread(target=resolved_task, name="MainTask")
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
    safe_print("ERROR: Main thread is still running (unexpected)")
else:
    safe_print("SUCCESS: Main thread completed successfully - no deadlock!")


monitor_thread.join()
timeout_thread.join()

safe_print("=== PROGRAM COMPLETED SUCCESSFULLY ===")
safe_print(f"Final shared counter value: {shared_resource_counter}")
safe_print("All threads completed without deadlock!")