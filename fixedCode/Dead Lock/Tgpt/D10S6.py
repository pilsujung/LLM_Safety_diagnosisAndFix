import threading
import time
import sys
from datetime import datetime

class DeadlockDemonstration:
    """
    A demonstration of how to fix deadlock scenarios in multithreaded programming.

    Originally, this class created two threads that attempted to acquire locks on
    shared resources in opposite orders, leading to a classic deadlock where neither
    thread could proceed.

    In this fixed version, both threads acquire locks in the same global order to
    prevent deadlock.
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
        print("DEADLOCK FIX DEMONSTRATION PROGRAM")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program shows how to avoid a classic deadlock scenario where:")
        print("- Thread 1 and Thread 2 both need Database and FileSystem locks")
        print("- Deadlock is prevented by enforcing a global lock order:")
        print("    database_connection_lock -> file_system_access_lock")
        print("=" * 70)
    
    def database_worker_function(self):
        """
        Worker function for the database thread.
        
        This thread simulates a database operation that requires both
        database access and file system access. It acquires the locks in
        the global order: database lock first, then file system lock.
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Database worker thread started")
        
        try:
            
            print(f"[{thread_name}] Attempting to acquire database connection lock (global order: DB -> FS)...")
            
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
        file system access and database access. In the original code, this
        function acquired the locks in the opposite order (FS -> DB), which
        caused a deadlock when run together with database_worker_function.
        
        In this fixed version, it also acquires locks in the global order:
        database_connection_lock first, then file_system_access_lock.
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")
        
        try:
            
            print(f"[{thread_name}] Attempting to acquire database connection lock (global order: DB -> FS)...")
            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock")
                print(f"[{thread_name}] File system worker now needs file system access...")
                print(f"[{thread_name}] Attempting to acquire file system access lock...")
                
                
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Successfully acquired file system access lock")
                    print(f"[{thread_name}] Now performing file system operations...")
                    
                    
                    time.sleep(2)
                    self.file_operations_completed += 1
                    
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
        
        This thread runs in the background and periodically checks if the
        main worker threads are still making progress. If no progress is
        detected for a certain period, it assumes deadlock has occurred.
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
                    print("[Monitor] ⚠️  DEADLOCK DETECTED! ⚠️")
                    print("[Monitor] Both threads appear to be waiting for each other's locks")
                    print("[Monitor] This is a classic deadlock scenario!")
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
        
        This method creates the worker threads and monitoring thread, then
        starts them all. With the fixed lock ordering, no deadlock should occur.
        """
        print("\n" + "=" * 50)
        print("STARTING DEADLOCK-FREE DEMONSTRATION")
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
        print("With correct lock ordering, the threads should complete without deadlock.")
        
        try:
            
            while (self.database_worker_thread.is_alive() or 
                   self.file_system_worker_thread.is_alive()) and not self.deadlock_detected:
                time.sleep(1)
            
            
            self.program_running = False
            
            if self.deadlock_detected:
                print("\n" + "=" * 50)
                print("DEADLOCK CONFIRMATION")
                print("=" * 50)
                print("A deadlock was detected. In the fixed version, this should not happen.")
            else:
                print("\n" + "=" * 50)
                print("NO DEADLOCK OCCURRED")
                print("=" * 50)
                print("All threads completed successfully - deadlock was prevented by:")
                print("1. Enforcing a global lock acquisition order:")
                print("   database_connection_lock -> file_system_access_lock")
                print("2. Making sure every thread respects this order whenever")
                print("   it needs both resources.")
                
        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
            self.program_running = False
        
        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """
    Main function to run the deadlock fix demonstration.
    
    This function creates an instance of the DeadlockDemonstration class
    and starts the demonstration.
    """
    print("Python Multithreading Deadlock Fix Demonstration")
    print("This program shows how to prevent deadlock by using a consistent lock order.")
    
    try:
        
        deadlock_demo = DeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()
        
    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)
    
    print("\nDemonstration completed.")



if __name__ == "__main__":
    main()
