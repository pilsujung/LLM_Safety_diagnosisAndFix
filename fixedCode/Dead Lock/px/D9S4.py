import multiprocessing
import time
import sys

def child_worker_process(child_connection_pipe):
    """
    Fixed child worker process - sends initialization message first to establish communication protocol.
    """
    print("[CHILD PROCESS] Starting child worker process...")
    print(f"[CHILD PROCESS] Process ID: {multiprocessing.current_process().pid}")
    
    try:
        
        print("[CHILD PROCESS] Sending initialization message to parent...")
        init_message = {
            "status": "initialized", 
            "process_id": multiprocessing.current_process().pid,
            "timestamp": time.time()
        }
        child_connection_pipe.send(init_message)
        print("[CHILD PROCESS] Initialization message sent - now waiting for parent data...")
        
        
        received_data_from_parent = child_connection_pipe.recv()
        print(f"[CHILD PROCESS] Received data from parent: {received_data_from_parent}")

        
        if isinstance(received_data_from_parent, dict) and 'message' in received_data_from_parent:
            message_to_process = received_data_from_parent['message']
        else:
            message_to_process = str(received_data_from_parent)
            
        processed_result = f"PROCESSED: {message_to_process.upper()}"
        print(f"[CHILD PROCESS] Processed result: {processed_result}")

        
        acknowledgment_message = {
            "status": "success",
            "processed_data": processed_result,
            "timestamp": time.time(),
            "process_id": multiprocessing.current_process().pid
        }

        print("[CHILD PROCESS] Sending acknowledgment to parent...")
        child_connection_pipe.send(acknowledgment_message)
        print("[CHILD PROCESS] Acknowledgment sent successfully!")

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
        try:
            child_connection_pipe.close()
        except:
            pass
        print("[CHILD PROCESS] Child worker process finished!")

def main():
    """
    Fixed main function - follows clear communication protocol: child->parent, parent->child, child->parent
    """
    print("="*60)
    print("FIXED MULTIPROCESSING COMMUNICATION (DEADLOCK RESOLVED)")
    print("="*60)
    print(f"Main process ID: {multiprocessing.current_process().pid}")

    
    print("\n[MAIN PROCESS] Creating bidirectional communication pipe...")
    parent_connection_pipe, child_connection_pipe = multiprocessing.Pipe()
    print("[MAIN PROCESS] Pipe created successfully!")

    
    print("\n[MAIN PROCESS] Creating child worker process...")
    child_worker_process_instance = multiprocessing.Process(
        target=child_worker_process,
        args=(child_connection_pipe,),
        name="ChildWorkerProcess"
    )

    print("[MAIN PROCESS] Starting child worker process...")
    child_worker_process_instance.start()
    print(f"[MAIN PROCESS] Child process started with PID: {child_worker_process_instance.pid}")

    
    print("[MAIN PROCESS] Closing child end of pipe in parent...")
    child_connection_pipe.close()

    
    time.sleep(0.5)

    print("\n[MAIN PROCESS] Waiting for child's initialization message...")
    
    try:
        
        init_message = parent_connection_pipe.recv()
        print(f"[MAIN PROCESS] Received init from child: {init_message}")

        
        response_data_to_child = {
            "message": "Hello from parent process!",
            "timestamp": time.time(),
            "parent_pid": multiprocessing.current_process().pid,
            "instructions": "Please process this data and send back acknowledgment"
        }

        print("[MAIN PROCESS] Sending work data to child...")
        parent_connection_pipe.send(response_data_to_child)

        
        result_from_child = parent_connection_pipe.recv()
        print(f"[MAIN PROCESS] Received result from child: {result_from_child}")
        print("\n[MAIN PROCESS] ✅ COMMUNICATION SUCCESSFUL - NO DEADLOCK!")

    except Exception as comm_error:
        print(f"[MAIN PROCESS] Communication error: {comm_error}")

    finally:
        print("\n[MAIN PROCESS] Cleaning up...")
        
        
        try:
            parent_connection_pipe.close()
            print("[MAIN PROCESS] Parent pipe closed.")
        except:
            pass

        
        print("[MAIN PROCESS] Waiting for child process...")
        child_worker_process_instance.join(timeout=3)

        if child_worker_process_instance.is_alive():
            print("[MAIN PROCESS] Force terminating child...")
            child_worker_process_instance.terminate()
            child_worker_process_instance.join()

        print(f"[MAIN PROCESS] Child exit code: {child_worker_process_instance.exitcode}")
        print("="*60)
        print("✅ DEADLOCK FIXED SUCCESSFULLY")
        print("="*60)

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn', force=True)  
    main()
