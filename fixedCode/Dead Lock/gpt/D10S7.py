import threading
import time
import sys
from datetime import datetime

class DeadlockDemonstration:
    """
    Deadlock-free version of the original demonstration.

    The original program created a classic deadlock situation by acquiring
    the same pair of locks in opposite orders in two different threads.

    This fixed version prevents deadlock by enforcing a *global lock ordering*:
    - Both threads always acquire `database_connection_lock` first
      and `file_system_access_lock` second.
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
        print("DEADLOCK-FREE DEMONSTRATION PROGRAM")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program previously demonstrated a classic deadlock scenario.")
        print("In this fixed version, we avoid deadlock by:")
        print("- Enforcing a consistent lock acquisition order: Database → FileSystem")
        print("- Both threads now follow this same lock order")
        print("=" * 70)

    def database_worker_function(self):
        """
        Worker function for the database thread.

        This thread simulates a database operation that requires both
        database access and file system access. It acquires:
        1) database_connection_lock
        2) file_system_access_lock

        This order matches the file system worker and prevents deadlock.
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

        This thread simulates a file system operation that also requires
        database access. The FIX is here:

        It now acquires locks in the SAME order as the database worker:
        1) database_connection_lock
        2) file_system_access_lock

        This removes the circular wait condition and prevents deadlock.
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")

        try:
            
            print(f"[{thread_name}] Attempting to acquire database connection lock (global order DB → FS)...")
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock")
                print(f"[{thread_name}] Now preparing file system operations that require database access...")

                
                time.sleep(2)
                self.database_operations_completed += 1

                print(f"[{thread_name}] Now need file system access...")
                print(f"[{thread_name}] Attempting to acquire file system access lock...")

                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Successfully acquired file system access lock")
                    print(f"[{thread_name}] Performing combined file + database operations...")

                    
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

        In the fixed version, this will simply observe progress and confirm
        that both threads eventually complete without deadlock.
        """
        print("[Monitor] Deadlock monitoring thread started")

        previous_db_operations = 0
        previous_file_operations = 0
        no_progress_cycles = 0
        max_no_progress_cycles = 5  

        while self.program_running:
            time.sleep(3)  

            
            if (self.database_worker_thread is not None and
                self.file_system_worker_thread is not None and
                not self.database_worker_thread.is_alive() and
                not self.file_system_worker_thread.is_alive()):
                print("[Monitor] Worker threads have finished. No deadlock detected.")
                break

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
        Start the (now deadlock-free) demonstration by creating and launching all threads.
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
        print("In this fixed version, both threads should complete without deadlock.")

        try:
            
            while ((self.database_worker_thread.is_alive() or
                    self.file_system_worker_thread.is_alive()) and
                   not self.deadlock_detected):
                time.sleep(1)

            
            self.program_running = False

            if self.deadlock_detected:
                print("\n" + "=" * 50)
                print("DEADLOCK CONFIRMATION")
                print("=" * 50)
                print("Deadlock was detected by the monitoring thread.")
            else:
                print("\n" + "=" * 50)
                print("NO DEADLOCK OCCURRED")
                print("=" * 50)
                print("All threads completed successfully.")
                print("The global lock ordering (Database → FileSystem) prevented deadlock.")

        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
            self.program_running = False

        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """
    Main function to run the deadlock-free demonstration.
    """
    print("Python Multithreading Deadlock (Fixed) Demonstration")
    print("This program shows how to avoid deadlock by using a consistent lock order.\n")

    try:
        deadlock_demo = DeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()

    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)

    print("\nDeadlock demonstration completed.")


if __name__ == "__main__":
    main()
