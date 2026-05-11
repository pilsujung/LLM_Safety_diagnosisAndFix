import multiprocessing
import time
import sys

def child_worker_process(child_connection_pipe):
    """
    Fixed child worker process - sends 'ready' signal FIRST to break deadlock.
    """
    print("[CHILD PROCESS] Starting child worker process...")
    print(f"[CHILD PROCESS] Process ID: {multiprocessing.current_process().pid}")
    
    try:
        
        ready_message = {
            "status": "ready",
            "message": "Child process ready to receive data",
            "process_id": multiprocessing.current_process().pid,
            "timestamp": time.time()
        }
        print("[CHILD PROCESS] ✓ Sending 'ready' signal to parent...")
        child_connection_pipe.send(ready_message)
        print("[CHILD PROCESS] ✓ 'Ready' signal sent!")
        
        
        print("[CHILD PROCESS] Waiting for data from parent...")
        received_data_from_parent = child_connection_pipe.recv()
        print(f"[CHILD PROCESS] ✓ Received data from parent: {received_data_from_parent}")

        
        if isinstance(received_data_from_parent, dict) and "message" in received_data_from_parent:
            processed_result = f"PROCESSED: {received_data_from_parent['message'].upper()}"
        else:
            processed_result = f"PROCESSED: {str(received_data_from_parent).upper()}"
        print(f"[CHILD PROCESS] Processed result: {processed_result}")

        
        acknowledgment_message = {
            "status": "success",
            "processed_data": processed_result,
            "timestamp": time.time(),
            "process_id": multiprocessing.current_process().pid
        }
        print("[CHILD PROCESS] Sending final acknowledgment to parent...")
        child_connection_pipe.send(acknowledgment_message)
        print("[CHILD PROCESS] ✓ Acknowledgment sent successfully!")

    except Exception as error_exception:
        print(f"[CHILD PROCESS] Error occurred: {error_exception}")
        try:
            error_message = {
                "status": "error",
                "error_details": str(error_exception),
                "timestamp": time.time()
            }
            child_connection_pipe.send(error_message)
        except:
            pass

    finally:
        print("[CHILD PROCESS] Closing child connection pipe...")
        child_connection_pipe.close()
        print("[CHILD PROCESS] Child worker process finished!")

def main():
    """
    Fixed main function - waits for child's 'ready' signal before proceeding.
    """
    print("="*60)
    print("MULTIPROCESSING DEADLOCK RESOLVED")
    print("="*60)
    print(f"Main process ID: {multiprocessing.current_process().pid}")

    
    print("\n[MAIN PROCESS] Creating bidirectional communication pipe...")
    parent_connection_pipe, child_connection_pipe = multiprocessing.Pipe()
    print("[MAIN PROCESS] ✓ Pipe created successfully!")

    
    print("\n[MAIN PROCESS] Creating and starting child worker process...")
    child_process = multiprocessing.Process(
        target=child_worker_process,
        args=(child_connection_pipe,),
        name="ChildWorkerProcess"
    )
    child_process.start()
    print(f"[MAIN PROCESS] ✓ Child process PID: {child_process.pid}")

    
    time.sleep(0.2)

    print("\n[MAIN PROCESS] Waiting for child's 'ready' signal...")
    try:
        
        ready_msg = parent_connection_pipe.recv()
        print(f"[MAIN PROCESS] ✓ Received ready signal: {ready_msg}")

        
        response_data = {
            "message": "Hello from parent process!",
            "timestamp": time.time(),
            "parent_pid": multiprocessing.current_process().pid,
            "instructions": "Process this and send acknowledgment"
        }
        print("[MAIN PROCESS] Sending data to child...")
        parent_connection_pipe.send(response_data)
        print("[MAIN PROCESS] ✓ Data sent to child!")

        
        ack_msg = parent_connection_pipe.recv()
        print(f"[MAIN PROCESS] ✓ Final acknowledgment: {ack_msg}")

    except Exception as e:
        print(f"[MAIN PROCESS] Error: {e}")
        child_process.terminate()

    finally:
        
        if not parent_connection_pipe.closed:
            parent_connection_pipe.close()
        child_process.join(timeout=2)
        if child_process.is_alive():
            child_process.terminate()
            child_process.join()
        print(f"[MAIN PROCESS] Child exit code: {child_process.exitcode}")
        print("="*60)
        print("✓ SUCCESS - No deadlock!")
        print("="*60)

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)  
    main()
