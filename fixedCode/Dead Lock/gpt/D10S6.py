import threading
import time
import sys
from datetime import datetime

class DeadlockDemonstration:
    """
    A demonstration of how to AVOID deadlock in multithreaded programming.

    In the original version, two threads acquired locks on shared resources
    in opposite orders, causing a classic deadlock. In this fixed version:

    - Both threads always acquire locks in the same global order:
      1) database_connection_lock
      2) file_system_access_lock

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
        print("DEADLOCK DEMONSTRATION PROGRAM - FIXED VERSION")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program used to demonstrate a classic deadlock scenario where:")
        print("- Thread 1 locked Database → then tried to lock FileSystem")
        print("- Thread 2 locked FileSystem → then tried to lock Database")
        print()
        print("In this FIXED version, both threads acquire locks in the SAME ORDER:")
        print("- Always lock Database first, then FileSystem")
        print("This consistent ordering removes the circular wait condition and avoids deadlock.")
        print("=" * 70)

    def database_worker_function(self):
        """
        Worker function for the database thread.

        This thread simulates a database operation that requires both
        database access and file system access. It acquires the locks in
        the global order:
        1) database_connection_lock
        2) file_system_access_lock
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
        database access and file system access. To avoid deadlock, it acquires
        the locks in the SAME order as the database worker:
        1) database_connection_lock
        2) file_system_access_lock
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")

        try:
            
            print(f"[{thread_name}] Attempting to acquire database connection lock (global order step 1)...")
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock (global order step 1)")

                print(f"[{thread_name}] Attempting to acquire file system access lock (global order step 2)...")
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Successfully acquired file system access lock (global order step 2)")
                    print(f"[{thread_name}] Now performing file system operations...")

                    
                    time.sleep(2)
                    self.file_operations_completed += 1

                    print(f"[{thread_name}] File system operations completed. "
                          f"Performing combined file + database operations...")

                    
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

        In the fixed version, this thread should see progress from both workers
        and should NOT report a deadlock.
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
                    print("[Monitor] This WOULD indicate a classic deadlock scenario.")
                    self.deadlock_detected = True
                    break
            else:
                no_progress_cycles = 0  
                print(f"[Monitor] Progress detected - DB ops: {current_db_operations}, "
                      f"File ops: {current_file_operations}")

            previous_db_operations = current_db_operations
            previous_file_operations = current_file_operations

        print("[Monitor] Monitoring thread finished")

    def start_deadlock_demonstration(self):
        """
        Start the (now fixed) demonstration by creating and launching all threads.
        """
        print("\n" + "=" * 50)
        print("STARTING DEADLOCK DEMONSTRATION (FIXED)")
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
        print("In the fixed version, the threads should complete without deadlock.")

        try:
            
            while (self.database_worker_thread.is_alive() or
                   self.file_system_worker_thread.is_alive()) and not self.deadlock_detected:
                time.sleep(1)

        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
            self.program_running = False

        
        self.program_running = False
        time.sleep(0.1)

        if self.deadlock_detected:
            print("\n" + "=" * 50)
            print("DEADLOCK DETECTED (should NOT happen in fixed version)")
            print("=" * 50)
        else:
            print("\nAll threads completed successfully - no deadlock occurred!")

        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    """
    Main function to run the (fixed) deadlock demonstration.
    """
    print("Python Multithreading Deadlock Demonstration (Fixed Version)")
    print("This program now demonstrates how to avoid deadlock by using a "
          "consistent lock acquisition order.\n")

    try:
        
        deadlock_demo = DeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()

    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)

    print("\nDeadlock demonstration completed.")



if __name__ == "__main__":
    main()
