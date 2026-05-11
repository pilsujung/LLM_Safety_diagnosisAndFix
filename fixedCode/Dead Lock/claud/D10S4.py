import threading
import time
import sys
from datetime import datetime

class DeadlockDemonstration:
    """
    A demonstration of deadlock resolution in multithreaded programming.
    
    This class has been FIXED to prevent deadlock by ensuring both threads
    always acquire locks in the same order (database first, then file system).
    This eliminates the circular wait condition that causes deadlock.
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
        print("DEADLOCK RESOLUTION DEMONSTRATION PROGRAM (FIXED)")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program demonstrates deadlock prevention by:")
        print("- Thread 1 locks Database → then locks FileSystem")
        print("- Thread 2 locks Database → then locks FileSystem")
        print("- BOTH threads acquire locks in THE SAME ORDER (no circular wait)")
        print("=" * 70)
    
    def database_worker_function(self):
        """
        Worker function for the database thread.
        
        FIXED: This thread now acquires locks in a consistent order:
        1. Database lock first
        2. File system lock second
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
        
        FIXED: This thread now acquires locks in the SAME consistent order:
        1. Database lock first (CHANGED - was file system first)
        2. File system lock second (CHANGED - was database second)
        
        This prevents circular wait and eliminates the deadlock condition.
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
        
        With the fix applied, both threads should complete successfully
        without deadlock.
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
        print("With the fix applied, threads should complete without deadlock")
        print("Press Ctrl+C to interrupt if needed")
        
        try:
            
            while (self.database_worker_thread.is_alive() or 
                   self.file_system_worker_thread.is_alive()) and not self.deadlock_detected:
                time.sleep(1)
            
            if self.deadlock_detected:
                print("\n" + "=" * 50)
                print("UNEXPECTED DEADLOCK")
                print("=" * 50)
                print("Deadlock was detected - the fix may not have worked!")
            else:
                print("\n" + "=" * 50)
                print("SUCCESS - NO DEADLOCK!")
                print("=" * 50)
                print("All threads completed successfully!")
                print("The deadlock has been prevented by ensuring both threads")
                print("acquire locks in the same order (database → file system)")
                print("\nKey deadlock prevention principle applied:")
                print("✓ Consistent lock ordering across all threads")
                
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
    print("This program demonstrates how to PREVENT deadlock in concurrent programming")
    
    try:
        
        deadlock_demo = DeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()
        
    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)
    
    print("\nDeadlock prevention demonstration completed.")



if __name__ == "__main__":
    main()