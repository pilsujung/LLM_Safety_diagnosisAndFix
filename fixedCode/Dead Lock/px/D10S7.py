import threading
import time
import sys
from datetime import datetime

class NoDeadlockDemonstration:
    """
    Fixed version demonstrating deadlock prevention.
    Both threads acquire locks in SAME ORDER: database -> file
    """
    def __init__(self):
        self.database_connection_lock = threading.Lock()
        self.file_system_access_lock = threading.Lock()
        self.database_worker_thread = None
        self.file_system_worker_thread = None
        self.program_running = True
        self.operations_completed = 0

        print("=" * 70)
        print("NO DEADLOCK DEMONSTRATION (FIXED)")
        print("=" * 70)
        print("Both threads now acquire locks in SAME ORDER: database -> file")
        print("This prevents circular wait conditions!")

    def worker_function(self, thread_name):
        """Unified worker that always acquires locks in same order"""
        print(f"[{thread_name}] Worker started")
        
        try:
            
            print(f"[{thread_name}] Acquiring database lock...")
            with self.database_connection_lock:
                print(f"[{thread_name}] ✓ Database lock acquired")
                time.sleep(1)  
                
                print(f"[{thread_name}] Acquiring file lock...")
                with self.file_system_access_lock:
                    print(f"[{thread_name}] ✓ File lock acquired")
                    time.sleep(1)  
                    
                    self.operations_completed += 1
                    print(f"[{thread_name}] ✓ Operations completed!")
                    
        except Exception as e:
            print(f"[{thread_name}] Error: {e}")
        finally:
            print(f"[{thread_name}] Worker finished")

    def start_demonstration(self):
        print("\n" + "=" * 50)
        print("STARTING NO-DEADLOCK DEMONSTRATION")
        print("=" * 50)

        
        self.database_worker_thread = threading.Thread(
            target=self.worker_function, 
            name="DatabaseWorker", 
            args=("DatabaseWorker",),
            daemon=True
        )
        self.file_system_worker_thread = threading.Thread(
            target=self.worker_function, 
            name="FileSystemWorker", 
            args=("FileSystemWorker",),
            daemon=True
        )

        
        self.database_worker_thread.start()
        self.file_system_worker_thread.start()
        
        print("All threads launched! Both will complete successfully...")
        
        
        self.database_worker_thread.join(timeout=10)
        self.file_system_worker_thread.join(timeout=10)
        
        print(f"\n✓ SUCCESS! {self.operations_completed} operations completed")
        print("No deadlock occurred - locks acquired consistently!")

def main():
    print("Python Multithreading - DEADLOCK FIXED!")
    demo = NoDeadlockDemonstration()
    demo.start_demonstration()

if __name__ == "__main__":
    main()
