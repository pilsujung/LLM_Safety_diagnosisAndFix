import threading
import time
import sys
from datetime import datetime

class DeadlockResolved:
    """
    Deadlock-resolved version of the multithreaded programming demonstration.
    
    This class demonstrates how to prevent deadlock by ensuring both threads
    acquire locks in the SAME ORDER: database_connection_lock FIRST, then
    file_system_access_lock SECOND.
    """

    def __init__(self):
        
        self.database_connection_lock = threading.Lock()
        self.file_system_access_lock = threading.Lock()

        
        self.database_worker_thread = None
        self.file_system_worker_thread = None

        
        self.database_operations_completed = 0
        self.file_operations_completed = 0

        print("=" * 70)
        print("DEADLOCK RESOLVED DEMONSTRATION")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program demonstrates deadlock PREVENTION by:")
        print("- Both threads lock Database → then FileSystem (SAME ORDER)")
        print("- NO deadlock occurs - both threads complete successfully!")
        print("=" * 70)

    def database_worker_function(self):
        """
        Worker function for the database thread (UNCHANGED).
        Locks: database → file_system (same order as other thread)
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
        FIXED Worker function for the file system thread.
        NOW locks in SAME ORDER: database → file_system (instead of file_system → database)
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
                    print(f"[{thread_name}] Performing combined file + database operations...")
                    time.sleep(1)
                    self.file_operations_completed += 1
                    print(f"[{thread_name}] All operations completed successfully!")

        except Exception as error:
            print(f"[{thread_name}] Error occurred: {error}")
        finally:
            print(f"[{thread_name}] File system worker thread finished")

    def start_deadlock_resolution_demo(self):
        """
        Start the deadlock-resolved demonstration.
        Both threads now complete successfully!
        """
        print("\n" + "=" * 50)
        print("STARTING DEADLOCK-RESOLVED DEMONSTRATION")
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

        
        print("Launching database worker thread...")
        self.database_worker_thread.start()

        print("Launching file system worker thread...")
        self.file_system_worker_thread.start()

        print("\nAll threads launched! Watching them complete successfully...")

        try:
            
            self.database_worker_thread.join(timeout=10)
            self.file_system_worker_thread.join(timeout=10)

            print("\n" + "=" * 50)
            print("SUCCESS! NO DEADLOCK OCCURRED")
            print("=" * 50)
            print("Both threads completed successfully because they acquired locks in the SAME ORDER!")
            print("Key principle: Always lock resources in consistent order across all threads.")

        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")

        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """
    Main function to run the deadlock-resolved demonstration.
    """
    print("Python Multithreading Deadlock RESOLUTION Demonstration")
    print("Watch how consistent lock ordering prevents deadlock!")

    try:
        deadlock_resolved = DeadlockResolved()
        deadlock_resolved.start_deadlock_resolution_demo()

    except Exception as error:
        print(f"An error occurred: {error}")
        sys.exit(1)

    print("\nDeadlock resolution demonstration completed successfully!")


if __name__ == "__main__":
    main()
