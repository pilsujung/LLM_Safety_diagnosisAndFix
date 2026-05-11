import threading
import time
import sys
from datetime import datetime

class DeadlockDemonstration:
    """
    A demonstration of deadlock *prevention* in multithreaded programming.
    
    The original version created two threads that attempted to acquire locks on
    shared resources in opposite orders, leading to deadlock.
    
    This fixed version enforces a *consistent global lock order*:
    - Both threads always acquire database_connection_lock first
      and file_system_access_lock second.
    This removes the circular wait condition and prevents deadlock.
    """
    
    def __init__(self):
        
        
        self.database_connection_lock = threading.Lock()
        self.file_system_access_lock = threading.Lock()
        
        
        self.database_worker_thread = None
        self.file_system_worker_thread = None
        
        
        self.program_running = True
        self.deadlock_detected = False
        
        
        self.database_operations_completed = 0
        self.file_operations_completed = 0
        
        print("=" * 70)
        print("DEADLOCK PREVENTION DEMONSTRATION PROGRAM")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program shows how to prevent deadlocks by:")
        print("- Enforcing a single, consistent lock acquisition order")
        print("- Both threads now lock Database → then FileSystem")
        print("=" * 70)
    
    def database_worker_function(self):
        """
        Worker function for the database thread.
        
        This thread simulates a database operation that requires both
        database access and file system access. It first acquires the
        database lock, then acquires the file system lock (DB → FS).
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Database worker thread started")
        
        try:
            
            print(f"[{thread_name}] Attempting to acquire database connection lock...")
            
            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock")
                print(f"[{thread_name}] Now performing database operations...")
                
                
                time.sleep(2)
                self.database_operations_completed += 1
                
                print(f"[{thread_name}] Database operations completed. Now need file access...")
                print(f"[{thread_name}] Attempting to acquire file system access lock...")
                
                
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Successfully acquired file system access lock")
                    print(f"[{thread_name}] Performing combined database + file operations...")
                    
                    
                    time.sleep(1)
                    self.file_operations_completed += 1
                    
                    print(f"[{thread_name}] All operations completed successfully!")
                    
        except Exception as error:
            print(f"[{thread_name}] Error occurred: {error}")
        finally:
            print(f"[{thread_name}] Database worker thread finished")
    
    def file_system_worker_function(self):
        """
        Worker function for the file system thread.
        
        This thread simulates a file system operation that requires both
        file system access and database access.
        
        IMPORTANT FIX:
        --------------
        Originally this thread acquired locks in the opposite order (FS → DB),
        which, together with DB → FS in the other thread, caused deadlock.
        
        Now this thread also acquires locks in the *same* order as the
        database worker (DB → FS), removing the circular wait.
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")
        
        try:
            
            print(f"[{thread_name}] Attempting to acquire database connection lock (global order)...")
            
            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock")
                print(f"[{thread_name}] Now attempting to acquire file system access lock...")
                
                
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Successfully acquired file system access lock")
                    print(f"[{thread_name}] Now performing file system operations...")
                    
                    
                    time.sleep(2)
                    self.file_operations_completed += 1
                    
                    print(f"[{thread_name}] File system operations completed.")
                    print(f"[{thread_name}] Performing combined file + database operations...")
                    
                    
                    time.sleep(1)
                    self.database_operations_completed += 1
                    
                    print(f"[{thread_name}] All operations completed successfully!")
                    
        except Exception as error:
            print(f"[{thread_name}] Error occurred: {error}")
        finally:
            print(f"[{thread_name}] File system worker thread finished")
    
    def monitor_thread_activity(self):
        """
        Monitor thread for detecting potential deadlock situations.
        
        In this fixed version, deadlock should *not* occur because both
        threads follow the same lock acquisition order.
        
        This monitor now effectively verifies that the system keeps
        making progress rather than hanging.
        """
        print("[Monitor] Deadlock monitoring thread started")
        
        previous_db_operations = 0
        previous_file_operations = 0
        no_progress_cycles = 0
        max_no_progress_cycles = 5  
        
        while self.program_running:
            time.sleep(3)  
            
            current_db_operations = self.database_operations_completed
            current_file_operations = self.file_operations_completed
            
            
            if (current_db_operations == previous_db_operations and 
                current_file_operations == previous_file_operations):
                no_progress_cycles += 1
                print(f"[Monitor] No progress detected for {no_progress_cycles * 3} seconds...")
                
                if no_progress_cycles >= max_no_progress_cycles:
                    print("[Monitor] ⚠️  POTENTIAL DEADLOCK/STALL DETECTED! ⚠️")
                    print("[Monitor] Threads appear to be stuck. In the original version,")
                    print("[Monitor] this was caused by inconsistent lock ordering.")
                    self.deadlock_detected = True
                    break
            else:
                no_progress_cycles = 0  
                print(f"[Monitor] Progress detected - DB ops: {current_db_operations}, File ops: {current_file_operations}")
            
            previous_db_operations = current_db_operations
            previous_file_operations = current_file_operations
        
        print("[Monitor] Monitoring thread finished")
    
    def start_deadlock_demonstration(self):
        """
        Start the demonstration by creating and launching all threads.
        
        In this fixed version, all threads should complete successfully
        without deadlocking.
        """
        print("\n" + "=" * 50)
        print("STARTING DEADLOCK PREVENTION DEMONSTRATION")
        print("=" * 50)
        
        
        self.database_worker_thread = threading.Thread(
            target=self.database_worker_function,
            name="DatabaseWorker",
            daemon=True
        )
        
        self.file_system_worker_thread = threading.Thread(
            target=self.file_system_worker_function,
            name="FileSystemWorker", 
            daemon=True
        )
        
        
        monitor_thread = threading.Thread(
            target=self.monitor_thread_activity,
            name="DeadlockMonitor",
            daemon=True
        )
        
        
        print("Launching database worker thread...")
        self.database_worker_thread.start()
        
        print("Launching file system worker thread...")
        self.file_system_worker_thread.start()
        
        print("Launching monitoring thread...")
        monitor_thread.start()
        
        print("\nAll threads launched! Observing behavior...")
        print("In this fixed version, threads should complete and the program should exit.")
        
        try:
            
            while (self.database_worker_thread.is_alive() or 
                   self.file_system_worker_thread.is_alive()) and not self.deadlock_detected:
                time.sleep(1)
            
            
            self.program_running = False
            
            if self.deadlock_detected:
                print("\n" + "=" * 50)
                print("DEADLOCK/STALLED STATE DETECTED BY MONITOR")
                print("=" * 50)
                print("This should NOT normally happen with correct lock ordering.")
            else:
                print("\n" + "=" * 50)
                print("NO DEADLOCK OCCURRED")
                print("=" * 50)
                print("All threads completed successfully thanks to consistent lock ordering.")
                print("Key rule applied: always acquire multiple locks in the same global order.")
                
        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
            self.program_running = False
        
        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """
    Main function to run the deadlock prevention demonstration.
    """
    print("Python Multithreading Deadlock Prevention Demonstration")
    print("This program shows how proper lock ordering prevents deadlock.")
    
    try:
        
        deadlock_demo = DeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()
        
    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)
    
    print("\nDemonstration completed.")



if __name__ == "__main__":
    main()
