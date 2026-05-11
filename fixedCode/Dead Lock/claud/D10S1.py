import threading
import time
import sys
from datetime import datetime

class DeadlockDemonstration:
    """
    A demonstration of how to prevent deadlock using consistent lock ordering.
    
    This class creates two threads that safely acquire locks on shared resources
    by always acquiring them in the same order, preventing deadlock situations.
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
        print("DEADLOCK PREVENTION DEMONSTRATION")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program demonstrates deadlock prevention using lock ordering:")
        print("- Both threads acquire locks in the SAME order")
        print("- Order: Database lock FIRST, then FileSystem lock")
        print("- This prevents circular wait conditions")
        print("=" * 70)
    
    def database_worker_function(self):
        """
        Worker function for the database thread.
        
        FIXED: Acquires locks in consistent order (database → file system)
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
        
        FIXED: Now acquires locks in the SAME order as database thread
        (database → file system) instead of reverse order
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")
        
        try:
            
            print(f"[{thread_name}] Attempting to acquire database connection lock...")
            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock")
                
                
                print(f"[{thread_name}] Attempting to acquire file system access lock...")
                
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
        Start the demonstration with fixed lock ordering.
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
        print("With consistent lock ordering, no deadlock should occur!")
        
        try:
            
            while (self.database_worker_thread.is_alive() or 
                   self.file_system_worker_thread.is_alive()) and not self.deadlock_detected:
                time.sleep(1)
            
            if self.deadlock_detected:
                print("\n⚠️  Unexpected deadlock occurred!")
            else:
                print("\n" + "=" * 50)
                print("SUCCESS - NO DEADLOCK!")
                print("=" * 50)
                print("All threads completed successfully using consistent lock ordering!")
                
        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
            self.program_running = False
        
        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """
    Main function to run the fixed demonstration.
    """
    print("Python Multithreading Deadlock Prevention Demonstration")
    print("This program demonstrates how to prevent deadlock using consistent lock ordering")
    
    try:
        deadlock_demo = DeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()
        
    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)
    
    print("\nDemonstration completed successfully!")


if __name__ == "__main__":
    main()