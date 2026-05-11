import threading
import time
import sys
from datetime import datetime

class DeadlockFreeDemonstration:
    """
    Fixed version of the deadlock demonstration.
    
    SOLUTION: Always acquire locks in the SAME ORDER in all threads.
    Both threads now acquire database_lock FIRST, then file_lock SECOND.
    This prevents the circular wait condition that causes deadlock.
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
        print("DEADLOCK-FREE DEMONSTRATION PROGRAM (FIXED)")
        print("=" * 70)
        print(f"Program started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("FIX: Both threads now acquire locks in SAME ORDER:")
        print("- Thread 1 & Thread 2: database_lock → file_lock")
        print("This prevents circular wait → NO DEADLOCK!")
        print("=" * 70)

    def database_worker_function(self):
        """Database worker - FIXED: acquires locks in consistent order"""
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] Database worker thread started")

        try:
            print(f"[{thread_name}] Acquiring locks in order: DB → File...")
            
            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Acquired database connection lock")
                time.sleep(1)  
                self.database_operations_completed += 1
                
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Acquired file system lock")
                    time.sleep(1)  
                    self.file_operations_completed += 1
                    
                    print(f"[{thread_name}] ✓ All operations completed!")

        except Exception as error:
            print(f"[{thread_name}] Error: {error}")
        finally:
            print(f"[{thread_name}] Database worker finished")

    def file_system_worker_function(self):
        """File system worker - FIXED: acquires locks in SAME ORDER"""
        thread_name = threading.current_thread().name
        print(f"[{thread_name}] File system worker thread started")

        try:
            print(f"[{thread_name}] Acquiring locks in order: DB → File...")
            
            
            
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Acquired database connection lock")
                time.sleep(1)  
                self.database_operations_completed += 1
                
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ Acquired file system lock")
                    time.sleep(1)  
                    self.file_operations_completed += 1
                    
                    print(f"[{thread_name}] ✓ All operations completed!")

        except Exception as error:
            print(f"[{thread_name}] Error: {error}")
        finally:
            print(f"[{thread_name}] File system worker finished")

    def monitor_thread_activity(self):
        """Simplified monitor - no deadlock expected"""
        print("[Monitor] Progress monitoring started")
        start_time = time.time()
        
        while self.program_running and time.time() - start_time < 10:
            time.sleep(1)
            print(f"[Monitor] DB ops: {self.database_operations_completed}, "
                  f"File ops: {self.file_operations_completed}")
        
        print("[Monitor] Monitoring complete")

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

        self.database_worker_thread.start()
        self.file_system_worker_thread.start()
        monitor_thread.start()

        print("✓ All threads launched successfully!")
        print("Observing smooth execution (no deadlock)...")

        
        self.database_worker_thread.join(timeout=8)
        self.file_system_worker_thread.join(timeout=8)
        
        print("\n" + "=" * 50)
        print("✅ SUCCESS - NO DEADLOCK!")
        print("=" * 50)
        print("Key fix: Consistent lock acquisition order prevents circular wait")
        print(f"Final stats - DB ops: {self.database_operations_completed}, "
              f"File ops: {self.file_operations_completed}")
        print(f"Program ended: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

def main():
    """Main entry point"""
    print("Python Multithreading - Deadlock Fixed!")
    try:
        demo = DeadlockFreeDemonstration()
        demo.start_demonstration()
    except KeyboardInterrupt:
        print("\nProgram interrupted")
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
