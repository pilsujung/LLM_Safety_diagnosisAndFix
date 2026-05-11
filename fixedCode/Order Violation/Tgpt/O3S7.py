import asyncio
import time
import random
from typing import List, Tuple, Dict, Any


data_ready = False
action_log: List[Tuple[str, str, str]] = []
violations: List[Tuple[str, str, str]] = []

def now() -> str:
    """Return a timestamp with millisecond precision."""
    return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

def record_action(who: str, action: str):
    """Record an action for later review."""
    action_log.append((now(), who, action))

def record_violation(who: str, reason: str):
    """Record an order violation and print a warning."""
    ts = now()
    violations.append((ts, who, reason))
    print(f"[{time.strftime('%H:%M:%S')}] [OrderViolation] {who}: {reason}")


audit_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()

async def emit_event(who: str, action: str, meta: Dict[str, Any] = None):
    """Emit an event into the monitoring queue."""
    payload = {
        "ts": now(),
        "who": who,
        "action": action,
        "meta": meta or {}
    }
    record_action(who, action)
    await audit_queue.put(payload)

async def monitor_events(stop_event: asyncio.Event):
    """
    Simple state machine that enforces order:
      fetcher_start → fetcher_done → processor_start → processor_done
    Any processor activity before fetcher_done is considered a violation.
    """
    fetch_done = False
    processor_started = False

    while True:
        if stop_event.is_set() and audit_queue.empty():
            break
        try:
            evt = await asyncio.wait_for(audit_queue.get(), timeout=0.1)
        except asyncio.TimeoutError:
            continue

        who = evt["who"]
        action = evt["action"]


        if who == "Fetcher":
            if action == "start":
                fetch_done = False
            elif action == "done":
                fetch_done = True

        elif who == "Processor":
            if action == "start":
                processor_started = True
                if not fetch_done:
                    record_violation("Processor", "Started processing before data was fetched")
            elif action == "done":
                if not fetch_done:
                    record_violation("Processor", "Finished processing before data was fetched")


        if action == "violation_hint":
            record_violation(who, evt["meta"].get("reason", "Manual violation hint"))

    print(f"[{time.strftime('%H:%M:%S')}] [Monitor] Stopped. fetch_done={fetch_done}, processor_started={processor_started}")


async def fetch_data():
    """Simulate fetching data asynchronously."""
    global data_ready
    await emit_event("Fetcher", "start")
    start_time = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] [Fetcher] Fetching data...")


    if random.random() < 0.2:
        print(f"[{time.strftime('%H:%M:%S')}] [Fetcher] ⚠️ Random delay occurred!")
        await asyncio.sleep(3)
    else:
        await asyncio.sleep(2)

    end_time = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] [Fetcher] Data fetched in {end_time - start_time:.2f} seconds")
    data_ready = True
    await emit_event("Fetcher", "done")
    return "Server Response"

async def process_data():
    """Simulate processing that depends on fetch_data, with correct ordering."""
    global data_ready

    print(f"[{time.strftime('%H:%M:%S')}] [Processor] Waiting to fetch data before processing...")
    start_time = time.time()



    data = await fetch_data()


    await emit_event("Processor", "start")
    print(f"[{time.strftime('%H:%M:%S')}] [Processor] Processing data...")


    await asyncio.sleep(1)

    end_time = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] [Processor] Processed data: {data}")
    print(f"[{time.strftime('%H:%M:%S')}] [Processor] Processing took {end_time - start_time:.2f} seconds")
    await emit_event("Processor", "done", {"processed": bool(data)})

async def check_execution_order():
    """High-level orchestration of the workflow."""
    print(f"[{time.strftime('%H:%M:%S')}] [Checker] Checking execution order...")
    start_time = time.time()
    await process_data()
    end_time = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] [Checker] Execution completed in {end_time - start_time:.2f} seconds")


async def main():
    random.seed()
    stop_event = asyncio.Event()
    monitor_task = asyncio.create_task(monitor_events(stop_event))

    try:
        await check_execution_order()
    finally:
        stop_event.set()
        await monitor_task


        print("\n==== ACTION LOG ====")
        for ts, who, action in action_log:
            print(f"{ts} | {who:9s} | {action}")

        print("\n==== VIOLATIONS ====")
        if not violations:
            print("No violations detected ✅")
        else:
            for ts, who, reason in violations:
                print(f"{ts} | {who:9s} | {reason}")
        print("====================\n")

if __name__ == "__main__":
    asyncio.run(main())
