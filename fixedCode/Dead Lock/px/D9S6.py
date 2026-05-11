import multiprocessing
import time
import sys

def child_worker_process(child_connection_pipe):
    """
    Child worker process - unchanged, now receives data immediately from parent.
    """
    print("[CHILD PROCESS] Starting child worker process...")
    print(f"[CHILD PROCESS] Process ID: {multiprocessing.current_process().pid}")
    print("[CHILD PROCESS] Child process waiting for data from parent...")

    try:
        received_data_from_parent = child_connection_pipe.recv()
        print(f"[CHILD PROCESS] Received data from parent: {received_data_from_parent}")

        processed_result = f"PROCESSED: {received_data_from_parent['message'].upper()}"
        print(f"[CHILD PROCESS] Processed result: {processed_result}")

        acknowledgment_message = {
            "status": "success",
            "processed_data": processed_result,
            "timestamp": time.time(),
            "process_id": multiprocessing.current_process().pid
        }

        print("[CHILD PROCESS] Sending acknowledgment to parent...")
        child_connection_pipe.send(acknowledgment_message)
        print("[CHILD PROCESS] Acknowledgment sent!")

    except Exception as error_exception:
        print(f"[CHILD PROCESS] Error: {error_exception}")
        child_connection_pipe.send({"status": "error", "error": str(error_exception)})

    finally:
        child_connection_pipe.close()
        print("[CHILD PROCESS] Child process finished!")

def main():
    print("="*60)
    print("FIXED MULTIPROCESSING COMMUNICATION")
    print("="*60)
    print(f"Main process ID: {multiprocessing.current_process().pid}")

    parent_connection_pipe, child_connection_pipe = multiprocessing.Pipe()

    child_process = multiprocessing.Process(
        target=child_worker_process,
        args=(child_connection_pipe,),
        name="ChildWorkerProcess"
    )
    
    child_process.start()
    print(f"[MAIN PROCESS] Child PID: {child_process.pid}")
    
    time.sleep(1)  

    
    print("\n[MAIN PROCESS] Sending data to child...")
    initial_data = {
        "message": "Hello from parent!",
        "timestamp": time.time(),
        "parent_pid": multiprocessing.current_process().pid
    }
    parent_connection_pipe.send(initial_data)
    
    
    print("[MAIN PROCESS] Waiting for child response...")
    response = parent_connection_pipe.recv()
    print(f"[MAIN PROCESS] Child response: {response}")

    parent_connection_pipe.close()
    child_process.join()
    print(f"[MAIN PROCESS] Child exit code: {child_process.exitcode}")
    print("Communication completed successfully!")

if __name__ == "__main__":
    main()
