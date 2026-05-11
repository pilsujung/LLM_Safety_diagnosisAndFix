import multiprocessing
import time

def child_worker_process(child_pipe):
    """Child sends 'ready' first to establish communication order."""
    print(f"[CHILD {multiprocessing.current_process().pid}] Started, sending ready signal...")
    
    
    child_pipe.send({"status": "ready", "pid": multiprocessing.current_process().pid})
    
    try:
        
        data = child_pipe.recv()
        print(f"[CHILD] Received: {data['message']}")
        
        
        result = data['message'].upper()
        child_pipe.send({"status": "processed", "result": result})
        print(f"[CHILD] Sent processed result: {result}")
        
    except Exception as e:
        print(f"[CHILD] Error: {e}")
        child_pipe.send({"status": "error", "details": str(e)})
    finally:
        child_pipe.close()

def main():
    """Parent waits for ready, then sends data and receives result."""
    print("MULTIPROCESSING PIPE COMMUNICATION (FIXED)")
    parent_pipe, child_pipe = multiprocessing.Pipe()
    
    child = multiprocessing.Process(target=child_worker_process, args=(child_pipe,))
    child.start()
    
    
    ready = parent_pipe.recv()
    print(f"[MAIN {multiprocessing.current_process().pid}] Child ready: {ready}")
    
    
    work = {"message": "hello from parent!"}
    parent_pipe.send(work)
    
    
    result = parent_pipe.recv()
    print(f"[MAIN] Result: {result['result']}")
    
    child.join()
    parent_pipe.close()
    print("Communication completed successfully!")

if __name__ == "__main__":
    multiprocessing.set_start_method('spawn')  
    main()
