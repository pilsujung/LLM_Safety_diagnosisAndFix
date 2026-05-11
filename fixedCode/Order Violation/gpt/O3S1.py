import asyncio
import time
import random
from typing import List, Tuple, Dict, Any


action_log: List[Tuple[str, str, str]] = []
violations: List[Tuple[str, str, str]] = []

def now() -> str:
    return time.strftime("%H:%M:%S") + f".{int((time.time() % 1) * 1000):03d}"

def record_action(who: str, action: str):
    action_log.append((now(), who, action))

def record_violation(who: str, reason: str):
    ts = now()
    violations.append((ts, who, reason))
    print(f"[{time.strftime('%H:%M:%S')}] [OrderViolation] {who}: {reason}")


audit_queue: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()

async def emit_event(who: str, action: str, meta: Dict[str, Any] = None):
    payload = {"ts": now(), "who": who, "action": action, "meta": meta or {}}
    record_action(who, action)
    await audit_queue.put(payload)

async def monitor_events(stop_event: asyncio.Event):
    """
    Enforce: Fetcher start -> Fetcher done -> Processor start -> Processor done
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

    print(f"[{time.strftime('%H:%M:%S')}] [Monitor] Stopped. fetch_done={fetch_done}, processor_started={processor_started}]")


data_ready_event = asyncio.Event()


async def fetch_data():
    await emit_event("Fetcher", "start")
    start_time = time.time()
    print(f"[{time.strftime('%H:%M:%S')}] [Fetcher] Fetching data...")


    await asyncio.sleep(3 if random.random() < 0.2 else 2)

    print(f"[{time.strftime('%H:%M:%S')}] [Fetcher] Data fetched in {time.time() - start_time:.2f} seconds")
    data_ready_event.set()
    await emit_event("Fetcher", "done")
    return "Server Response"

async def process_data():
    print(f"[{time.strftime('%H:%M:%S')}] [Processor] Waiting for data...")
    await data_ready_event.wait()
    await emit_event("Processor", "start")

    start_time = time.time()

    await asyncio.sleep(1)

    data = "Processed(Server Response)"
    print(f"[{time.strftime('%H:%M:%S')}] [Processor] Processed data: {data}")
    print(f"[{time.strftime('%H:%M:%S')}] [Processor] Processing took {time.time() - start_time:.2f} seconds")
    await emit_event("Processor", "done", {"processed": True})

async def check_execution_order():
    print(f"[{time.strftime('%H:%M:%S')}] [Checker] Checking execution order...")
    start_time = time.time()

    await asyncio.gather(fetch_data(), process_data())
    print(f"[{time.strftime('%H:%M:%S')}] [Checker] Execution completed in {time.time() - start_time:.2f} seconds")


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
