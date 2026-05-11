import threading
import time
import sys
from datetime import datetime

class DeadlockResolved:
    """
    A demonstration of how to resolve deadlock by ensuring consistent lock ordering.
    
    This class creates two threads that acquire locks on shared resources
    in the SAME order, preventing the deadlock situation.
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
        print("DEADLOCK RESOLVED DEMONSTRATION PROGRAM")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program demonstrates how to prevent deadlock by:")
        print("- Thread 1 locks Database → then locks FileSystem")
        print("- Thread 2 locks Database → then locks FileSystem (SAME ORDER)")
        print("- Both threads acquire locks in consistent order, preventing deadlock")
        print("=" * 70)
    
    def database_worker_function(self):
        """
        Worker function for the database thread.
        
        This thread simulates a database operation that requires both
        database access and file system access. It first acquires the
        database lock, then attempts to acquire the file system lock.
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
        file system access and database access. It now acquires locks in
        the SAME ORDER as the database thread to prevent deadlock.
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")
        
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
            print(f"[{thread_name}] File system worker thread finished")
    
    def monitor_thread_activity(self):
        """
        Monitor thread for detecting potential deadlock situations.
        
        This thread runs in the background and periodically checks if the
        main worker threads are still making progress.
        """
        print("[Monitor] Thread activity monitoring started")
        
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
                print(f"[Monitor] Checking progress... ({no_progress_cycles * 3} seconds)")
                
                if no_progress_cycles >= max_no_progress_cycles:
                    print("[Monitor] ⚠️  No progress detected - possible issue")
                    self.deadlock_detected = True
                    break
            else:
                no_progress_cycles = 0
                print(f"[Monitor] Progress detected - DB ops: {current_db_operations}, File ops: {current_file_operations}")
            
            previous_db_operations = current_db_operations
            previous_file_operations = current_file_operations
        
        print("[Monitor] Monitoring thread finished")
    
    def start_demonstration(self):
        """
        Start the resolved deadlock demonstration.
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
            name="ActivityMonitor",
            daemon=True
        )
        
        
        print("Launching database worker thread...")
        self.database_worker_thread.start()
        
        print("Launching file system worker thread...")
        self.file_system_worker_thread.start()
        
        print("Launching monitoring thread...")
        monitor_thread.start()
        
        print("\nAll threads launched! Observing behavior...")
        print("With proper lock ordering, threads should complete successfully!")
        
        try:
            
            self.database_worker_thread.join(timeout=30)
            self.file_system_worker_thread.join(timeout=30)
            
            if not self.deadlock_detected:
                print("\n" + "=" * 50)
                print("SUCCESS - NO DEADLOCK!")
                print("=" * 50)
                print("All threads completed successfully!")
                print("By acquiring locks in the same order, we prevented deadlock.")
            
        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
        finally:
            self.program_running = False
        
        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """
    Main function to run the deadlock resolution demonstration.
    """
    print("Python Multithreading - Deadlock Resolution Demonstration")
    print("This program demonstrates how consistent lock ordering prevents deadlock")
    
    try:
        demo = DeadlockResolved()
        demo.start_demonstration()
        
    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)
    
    print("\nDemonstration completed successfully.")


if __name__ == "__main__":
    main()