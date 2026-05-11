import multiprocessing
import time
import sys


def child_worker_process(child_connection_pipe):
    """
    Child worker process that participates in a simple
    request/response protocol with the parent:
      1) recv data from parent
      2) process it
      3) send acknowledgment back
    """
    print("[CHILD PROCESS] Starting child worker process...")
    print(f"[CHILD PROCESS] Process ID: {multiprocessing.current_process().pid}")
    print("[CHILD PROCESS] Child process is now waiting to receive data from parent...")

    try:
        
        print("[CHILD PROCESS] Calling recv() to get data from parent...")
        received_data_from_parent = child_connection_pipe.recv()
        print(f"[CHILD PROCESS] Successfully received data from parent: {received_data_from_parent}")

        
        processed_result = f"PROCESSED: {str(received_data_from_parent).upper()}"
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
        error_message = {
            "status": "error",
            "error_details": str(error_exception),
            "timestamp": time.time()
        }
        
        try:
            child_connection_pipe.send(error_message)
        except Exception:
            pass

    finally:
        print("[CHILD PROCESS] Closing child connection pipe...")
        child_connection_pipe.close()
        print("[CHILD PROCESS] Child worker process finished!")


def main():
    """
    Main function demonstrating a correct request/response protocol.

    Communication order (fixed, like a consistent lock order):
      - Parent: send -> recv
      - Child : recv -> send
    This prevents both ends from blocking on recv() at the same time.
    """
    print("=" * 60)
    print("MULTIPROCESSING DEADLOCK RESOLVED DEMONSTRATION")
    print("=" * 60)
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

    
    time.sleep(1)

    try:
        
        request_data_to_child = {
            "message": "Hello from parent process!",
            "timestamp": time.time(),
            "parent_pid": multiprocessing.current_process().pid,
            "instructions": "Please process this data and send back acknowledgment"
        }

        print("\n[MAIN PROCESS] Sending request data to child (send first)...")
        parent_connection_pipe.send(request_data_to_child)
        print("[MAIN PROCESS] Request sent. Waiting for acknowledgment from child...")

        
        acknowledgment_from_child = parent_connection_pipe.recv()
        print(f"[MAIN PROCESS] Received message from child: {acknowledgment_from_child}")

    except KeyboardInterrupt:
        print("\n\n[MAIN PROCESS] KeyboardInterrupt received! Terminating child process...")
        child_worker_process_instance.terminate()
        print("[MAIN PROCESS] Child process terminated due to user interrupt!")

    except Exception as main_exception:
        print(f"\n[MAIN PROCESS] Exception occurred: {main_exception}")
        child_worker_process_instance.terminate()

    finally:
        print("\n[MAIN PROCESS] Cleaning up resources...")

        
        if not parent_connection_pipe.closed:
            parent_connection_pipe.close()
            print("[MAIN PROCESS] Parent connection pipe closed.")

        
        print("[MAIN PROCESS] Waiting for child process to finish...")
        child_worker_process_instance.join(timeout=5)

        if child_worker_process_instance.is_alive():
            print("[MAIN PROCESS] Child process still alive - forcing termination...")
            child_worker_process_instance.terminate()
            child_worker_process_instance.join()

        print(f"[MAIN PROCESS] Child process exit code: {child_worker_process_instance.exitcode}")
        print("[MAIN PROCESS] All resources cleaned up!")
        print("=" * 60)
        print("DEADLOCK RESOLVED DEMONSTRATION COMPLETED")
        print("=" * 60)


if __name__ == "__main__":
    main()
