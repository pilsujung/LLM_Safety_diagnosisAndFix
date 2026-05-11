import threading
import time
import sys
from datetime import datetime

class DeadlockResolvedDemonstration:
    """
    Fixed version of deadlock demonstration using consistent lock ordering.
    
    Both threads now acquire locks in the same fixed order: 
    database_connection_lock first, then file_system_access_lock.
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
        print("DEADLOCK RESOLVED DEMONSTRATION (FIXED VERSION)")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program demonstrates how to PREVENT deadlock by:")
        print("- Always acquiring locks in the SAME ORDER in ALL threads")
        print("- DatabaseWorker: database_lock → file_lock")
        print("- FileSystemWorker: database_lock → file_lock (same order!)")
        print("=" * 70)

    def database_worker_function(self):
        """Database worker - acquires locks in fixed order: DB → File"""
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Database worker thread started")

        try:
            print(f"[{thread_name}] Attempting to acquire locks in order: database → file...")

            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Acquired database connection lock")
                print(f"[{thread_name}] Performing database operations...")
                time.sleep(2)
                self.database_operations_completed += 1

                print(f"[{thread_name}] Now acquiring file system lock...")
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Acquired file system access lock")
                    print(f"[{thread_name}] Performing combined operations...")
                    time.sleep(1)
                    self.file_operations_completed += 1

            print(f"[{thread_name}] All operations completed successfully!")

        except Exception as error:
            print(f"[{thread_name}] Error occurred: {error}")
        finally:
            print(f"[{thread_name}] Database worker thread finished")

    def file_system_worker_function(self):
        """File system worker - FIXED to acquire locks in SAME order: DB → File"""
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")

        try:
            print(f"[{thread_name}] Attempting to acquire locks in order: database → file...")

            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Acquired database connection lock")
                print(f"[{thread_name}] Performing database operations...")
                time.sleep(2)
                self.database_operations_completed += 1

                print(f"[{thread_name}] Now acquiring file system lock...")
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Acquired file system access lock")
                    print(f"[{thread_name}] Performing combined operations...")
                    time.sleep(1)
                    self.file_operations_completed += 1

            print(f"[{thread_name}] All operations completed successfully!")

        except Exception as error:
            print(f"[{thread_name}] Error occurred: {error}")
        finally:
            print(f"[{thread_name}] File system worker thread finished")

    def monitor_thread_activity(self):
        """Simplified monitor - no deadlock expected in this fixed version"""
        print("[Monitor] Progress monitoring thread started")
        
        previous_db_operations = 0
        previous_file_operations = 0
        
        while self.program_running:
            time.sleep(2)
            
            current_db_operations = self.database_operations_completed
            current_file_operations = self.file_operations_completed
            
            if (current_db_operations > previous_db_operations or 
                current_file_operations > previous_file_operations):
                print(f"[Monitor] ✓ Progress: DB ops={current_db_operations}, File ops={current_file_operations}")
            
            previous_db_operations = current_db_operations
            previous_file_operations = current_file_operations
            
            
            if current_db_operations >= 1 and current_file_operations >= 2:
                break

    def start_demonstration(self):
        """Start the deadlock-free demonstration"""
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
            name="ProgressMonitor",
            daemon=True
        )

        print("Launching database worker thread...")
        self.database_worker_thread.start()

        print("Launching file system worker thread...")
        self.file_system_worker_thread.start()

        print("Launching progress monitor...")
        monitor_thread.start()

        print("\nAll threads launched! Watch them complete successfully...")

        try:
            
            self.database_worker_thread.join(timeout=10)
            self.file_system_worker_thread.join(timeout=10)

            print("\n" + "=" * 50)
            print("SUCCESS! NO DEADLOCK OCCURRED")
            print("=" * 50)
            print("✅ Both threads completed their operations without deadlock!")
            print("✅ Key fix: Consistent lock acquisition order across all threads")
            print("✅ Lock order: database_connection_lock → file_system_access_lock")

        except KeyboardInterrupt:
            print("\nProgram interrupted by user")
        finally:
            self.program_running = False

        print(f"\nFinal statistics:")
        print(f"Database operations: {self.database_operations_completed}")
        print(f"File operations: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    print("Python Multithreading DEADLOCK RESOLUTION Demonstration")
    print("Following Java examples: consistent lock ordering prevents deadlock")
    
    try:
        demo = DeadlockResolvedDemonstration()
        demo.start_demonstration()
    except Exception as error:
        print(f"Error: {error}")
        sys.exit(1)

if __name__ == "__main__":
    main()
