import multiprocessing
import time
import sys


def child_worker_process(child_connection_pipe):
    """
    Child worker process that participates in a well-defined
    request/response protocol:

    1. Wait to receive data from parent.
    2. Process the data.
    3. Send acknowledgment back to parent.
    """
    print("[CHILD PROCESS] Starting child worker process...")
    print(f"[CHILD PROCESS] Process ID: {multiprocessing.current_process().pid}")
    print("[CHILD PROCESS] Child process is now waiting to receive data from parent...")

    try:
        
        
        print("[CHILD PROCESS] Calling recv() to get data from parent...")
        received_data_from_parent = child_connection_pipe.recv()

        print(f"[CHILD PROCESS] Successfully received data from parent: {received_data_from_parent}")

        
        payload = received_data_from_parent.get("message", "")
        processed_result = f"PROCESSED: {payload.upper()}"
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

    except EOFError:
        print("[CHILD PROCESS] Pipe was closed before any data was received.")
    except Exception as error_exception:
        print(f"[CHILD PROCESS] Error occurred: {error_exception}")
        error_message = {
            "status": "error",
            "error_details": str(error_exception),
            "timestamp": time.time(),
            "process_id": multiprocessing.current_process().pid
        }

        
        try:
            child_connection_pipe.send(error_message)
        except Exception:
            print("[CHILD PROCESS] Failed to send error message to parent.")

    finally:
        print("[CHILD PROCESS] Closing child connection pipe...")
        child_connection_pipe.close()
        print("[CHILD PROCESS] Child worker process finished!")


def main():
    """
    Main function demonstrating a correct multiprocessing communication pattern.
    Protocol:

    1. Parent sends work to child.
    2. Child receives it, processes it, and sends back acknowledgment.
    3. Parent receives acknowledgment and finishes.
    """
    print("=" * 60)
    print("MULTIPROCESSING COMMUNICATION (DEADLOCK RESOLVED)")
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

        print("\n[MAIN PROCESS] Sending request data to child...")
        parent_connection_pipe.send(request_data_to_child)
        print("[MAIN PROCESS] Request data sent successfully!")

        
        print("[MAIN PROCESS] Waiting to receive acknowledgment from child...")
        acknowledgment_from_child = parent_connection_pipe.recv()

        print(f"[MAIN PROCESS] Received acknowledgment from child: {acknowledgment_from_child}")

    except KeyboardInterrupt:
        print("\n\n[MAIN PROCESS] KeyboardInterrupt received! Terminating child process...")
        child_worker_process_instance.terminate()
        print("[MAIN PROCESS] Child process terminated due to user interruption!")

    except Exception as main_exception:
        print(f"\n[MAIN PROCESS] Exception occurred: {main_exception}")
        child_worker_process_instance.terminate()
        print("[MAIN PROCESS] Child process terminated due to exception!")

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
        print("COMMUNICATION DEMO COMPLETED (NO DEADLOCK)")
        print("=" * 60)


if __name__ == "__main__":
    main()
