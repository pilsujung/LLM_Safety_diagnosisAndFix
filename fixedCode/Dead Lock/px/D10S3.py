import threading
import time
import sys
from datetime import datetime

class FixedDeadlockDemonstration:
    """
    FIXED version of the deadlock demonstration.
    Both threads now acquire locks in the SAME ORDER: database_lock → file_lock.
    This eliminates the circular wait condition required for deadlock.
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
        print("✅ FIXED DEADLOCK DEMONSTRATION PROGRAM")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program demonstrates the FIXED deadlock scenario where:")
        print("- BOTH threads lock Database → then FileSystem (SAME ORDER)")
        print("- No circular wait = NO DEADLOCK!")
        print("=" * 70)

    def database_worker_function(self):
        """
        FIXED: Database worker acquires locks in order: Database → FileSystem
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Database worker thread started")

        try:
            print(f"[{thread_name}] Acquiring locks in CONSISTENT ORDER (DB → File)...")

            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock")
                print(f"[{thread_name}] Performing database operations...")

                
                time.sleep(2)
                self.database_operations_completed += 1

                print(f"[{thread_name}] Database operations completed. Now need file access...")
                
                
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
        FIXED: File system worker now acquires locks in SAME ORDER: Database → FileSystem
        (Previously: FileSystem → Database, which caused deadlock)
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")

        try:
            print(f"[{thread_name}] Acquiring locks in CONSISTENT ORDER (DB → File)...")

            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Successfully acquired database connection lock")
                print(f"[{thread_name}] Performing database operations...")

                
                time.sleep(2)
                self.database_operations_completed += 1

                print(f"[{thread_name}] Database operations completed. Now need file access...")
                
                
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
        Monitor thread for tracking progress (no deadlock detection needed now).
        """
        print("[Monitor] Progress monitoring thread started")

        previous_db_operations = 0
        previous_file_operations = 0

        while self.program_running:
            time.sleep(2)

            current_db_operations = self.database_operations_completed
            current_file_operations = self.file_operations_completed

            if (current_db_operations > previous_db_operations or 
                current_file_operations > previous_file_operations):
                print(f"[Monitor] ✓ Progress detected - DB ops: {current_db_operations}, File ops: {current_file_operations}")
                previous_db_operations = current_db_operations
                previous_file_operations = current_file_operations
            else:
                print(f"[Monitor] Waiting for progress...")

            
            if (not self.database_worker_thread.is_alive() and 
                not self.file_system_worker_thread.is_alive()):
                break

        print("[Monitor] Both worker threads completed successfully!")

    def start_deadlock_demonstration(self):
        """
        Start the FIXED demonstration by creating and launching all threads.
        """
        print("\n" + "=" * 50)
        print("🚀 STARTING FIXED DEADLOCK DEMONSTRATION")
        print("=" * 50)
        print("KEY FIX: Both threads acquire locks in SAME ORDER: Database → FileSystem")
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

        print("Launching monitoring thread...")
        monitor_thread.start()

        print("\nAll threads launched! Watching for successful completion...")
        print("With consistent lock ordering, no deadlock should occur!")

        try:
            
            self.database_worker_thread.join(timeout=15)
            self.file_system_worker_thread.join(timeout=15)

            print("\n" + "=" * 50)
            print("✅ SUCCESS! No deadlock occurred!")
            print("=" * 50)
            print("Both threads completed their operations successfully!")
            print("This demonstrates how consistent lock ordering prevents deadlock.")
            print(f"Final statistics:")
            print(f"  Database operations completed: {self.database_operations_completed}")
            print(f"  File operations completed: {self.file_operations_completed}")
            print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
            self.program_running = False

def main():
    """
    Main function to run the FIXED deadlock demonstration.
    """
    print("Python Multithreading DEADLOCK FIXED Demonstration")
    print("✅ Both threads now use CONSISTENT lock ordering!")

    try:
        
        deadlock_demo = FixedDeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()

    except Exception as error:
        print(f"An error occurred during demonstration: {error}")
        sys.exit(1)

    print("\n✅ FIXED Deadlock demonstration completed successfully!")


if __name__ == "__main__":
    main()
