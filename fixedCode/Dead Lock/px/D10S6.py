import threading
import time
import sys
from datetime import datetime

class DeadlockDemonstration:
    """
    A comprehensive demonstration of deadlock scenarios in multithreaded programming.
    
    FIXED VERSION: Both threads now acquire locks in CONSISTENT ORDER (DB -> File)
    This eliminates the circular wait condition that caused deadlock.
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
        print("FIXED DEADLOCK DEMONSTRATION PROGRAM")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("This program demonstrates the FIXED deadlock scenario where:")
        print("- BOTH threads lock Database → THEN FileSystem (CONSISTENT ORDER)")
        print("- No circular wait possible - threads complete successfully!")
        print("=" * 70)

    def acquire_locks_in_order(self):
        """
        CRITICAL FIX: Helper method to ALWAYS acquire locks in SAME ORDER
        Database lock FIRST, then File lock SECOND. Eliminates deadlock.
        """
        self.database_connection_lock.acquire()
        self.file_system_access_lock.acquire()
        return True

    def release_locks_in_reverse_order(self):
        """
        Release locks in reverse acquisition order (LIFO) for proper cleanup.
        """
        self.file_system_access_lock.release()
        self.database_connection_lock.release()

    def database_worker_function(self):
        """
        FIXED: Database worker now follows consistent lock ordering.
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Database worker thread started")

        try:
            print(f"[{thread_name}] Attempting to acquire BOTH locks in order (DB -> File)...")
            
            
            if self.acquire_locks_in_order():
                print(f"[{thread_name}] ✓ Successfully acquired BOTH locks (DB -> File)")
                
                
                print(f"[{thread_name}] Performing database operations...")
                time.sleep(2)
                self.database_operations_completed += 1

                
                print(f"[{thread_name}] Performing file operations...")
                time.sleep(1)
                self.file_operations_completed += 1

                print(f"[{thread_name}] ✓ All operations completed successfully!")
            else:
                print(f"[{thread_name}] Failed to acquire locks")

        except Exception as error:
            print(f"[{thread_name}] Error occurred: {error}")
        finally:
            
            try:
                self.release_locks_in_reverse_order()
            except:
                pass  
            print(f"[{thread_name}] Database worker thread finished")

    def file_system_worker_function(self):
        """
        FIXED: File system worker now follows SAME consistent lock ordering.
        """
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")

        try:
            print(f"[{thread_name}] Attempting to acquire BOTH locks in order (DB -> File)...")
            
            
            if self.acquire_locks_in_order():
                print(f"[{thread_name}] ✓ Successfully acquired BOTH locks (DB -> File)")
                
                
                print(f"[{thread_name}] Performing file system operations...")
                time.sleep(2)
                self.file_operations_completed += 1

                
                print(f"[{thread_name}] Performing database operations...")
                time.sleep(1)
                self.database_operations_completed += 1

                print(f"[{thread_name}] ✓ All operations completed successfully!")
            else:
                print(f"[{thread_name}] Failed to acquire locks")

        except Exception as error:
            print(f"[{thread_name}] Error occurred: {error}")
        finally:
            try:
                self.release_locks_in_reverse_order()
            except:
                pass
            print(f"[{thread_name}] File system worker thread finished")

    def monitor_thread_activity(self):
        """
        Monitor thread for detecting progress (now shows SUCCESS instead of deadlock).
        """
        print("[Monitor] Progress monitoring thread started")

        previous_db_operations = 0
        previous_file_operations = 0
        no_progress_cycles = 0
        max_no_progress_cycles = 10  

        while self.program_running:
            time.sleep(2)

            current_db_operations = self.database_operations_completed
            current_file_operations = self.file_operations_completed

            if (current_db_operations == previous_db_operations and
                current_file_operations == previous_file_operations):
                no_progress_cycles += 1
                print(f"[Monitor] No progress for {no_progress_cycles * 2} seconds...")

                if no_progress_cycles >= max_no_progress_cycles:
                    print("[Monitor] ⚠️ NO PROGRESS DETECTED ⚠️")
                    self.deadlock_detected = True
                    break
            else:
                no_progress_cycles = 0
                print(f"[Monitor] ✓ Progress: DB={current_db_operations}, File={current_file_operations}")

            previous_db_operations = current_db_operations
            previous_file_operations = current_file_operations

        print("[Monitor] Monitoring thread finished")

    def start_deadlock_demonstration(self):
        """
        Start the FIXED demonstration - both threads will complete successfully!
        """
        print("\n" + "=" * 50)
        print("STARTING FIXED DEADLOCK DEMONSTRATION")
        print("=" * 50)
        print("✅ Both threads use SAME lock order: Database -> FileSystem")
        print("✅ No circular wait possible!")

        
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
        print("Expected result: database_operations_completed=2, file_operations_completed=2")

        try:
            
            while (self.database_worker_thread.is_alive() or
                   self.file_system_worker_thread.is_alive()):
                time.sleep(1)

            print("\n" + "=" * 50)
            print("✅ SUCCESS! No Deadlock Occurred!")
            print("=" * 50)
            print("FIX SUMMARY:")
            print("• Both threads acquired locks in SAME ORDER: DB → File")
            print("• Eliminated circular wait condition")
            print("• Both threads completed their work successfully")
            print(f"Final counters: DB={self.database_operations_completed}, File={self.file_operations_completed}")

        except KeyboardInterrupt:
            print("\n\nProgram interrupted by user")
            self.program_running = False

        print(f"\nFinal statistics:")
        print(f"Database operations completed: {self.database_operations_completed}")
        print(f"File operations completed: {self.file_operations_completed}")
        print(f"Program ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """
    Main function to run the FIXED deadlock demonstration.
    """
    print("Python Multithreading DEADLOCK FIX Demonstration")
    print("Watch how consistent lock ordering eliminates deadlock!")

    try:
        deadlock_demo = DeadlockDemonstration()
        deadlock_demo.start_deadlock_demonstration()

    except Exception as error:
        print(f"An error occurred: {error}")
        sys.exit(1)

    print("\n✅ Deadlock demonstration FIXED and completed successfully!")

if __name__ == "__main__":
    main()
