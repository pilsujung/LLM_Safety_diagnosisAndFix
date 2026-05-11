import multiprocessing
import time
import random
import logging
from datetime import datetime


def configure_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(processName)s - %(message)s"
    )


def deposit_worker(
    shared_balances,
    account_indices,
    deposit_amount,
    operation_count,
    worker_id,
    stats_queue,
    lock,
    delay_range=(0.001, 0.02),
):
    configure_logging()
    logging.info(f"DEPOSIT Worker {worker_id} started (amount={deposit_amount})")

    applied = 0
    for i in range(operation_count):
        time.sleep(random.uniform(*delay_range))
        idx = random.choice(account_indices)

                                                        
        with lock:
            current = shared_balances[idx]
            time.sleep(0.001)                                                     
            shared_balances[idx] = current + deposit_amount

        applied += 1
        if (i + 1) % 25 == 0:
            logging.info(f"DEPOSIT Worker {worker_id}: {i+1}/{operation_count}")

    stats_queue.put({"type": "deposit", "worker": worker_id, "applied": applied})
    logging.info(f"DEPOSIT Worker {worker_id} finished")


def withdraw_worker(
    shared_balances,
    account_indices,
    withdraw_amount,
    operation_count,
    worker_id,
    stats_queue,
    lock,
    delay_range=(0.001, 0.02),
):
    configure_logging()
    logging.info(f"WITHDRAW Worker {worker_id} started (amount={withdraw_amount})")

    success = 0
    skipped = 0

    for i in range(operation_count):
        time.sleep(random.uniform(*delay_range))
        idx = random.choice(account_indices)

                                                                    
        with lock:
            current = shared_balances[idx]
            if current >= withdraw_amount:
                time.sleep(0.001)                                          
                shared_balances[idx] = current - withdraw_amount
                success += 1
            else:
                skipped += 1

        if (i + 1) % 25 == 0:
            logging.info(f"WITHDRAW Worker {worker_id}: {i+1}/{operation_count}")

    stats_queue.put(
        {"type": "withdraw", "worker": worker_id, "success": success, "skipped": skipped}
    )
    logging.info(f"WITHDRAW Worker {worker_id} finished")


def interest_worker(
    shared_balances,
    account_indices,
    interest_factor,
    operation_count,
    worker_id,
    stats_queue,
    lock,
    delay_range=(0.005, 0.015),
):
    configure_logging()
    logging.info(f"INTEREST Worker {worker_id} started (factor={interest_factor})")

    applied = 0
    for i in range(operation_count):
        time.sleep(random.uniform(*delay_range))
        idx = random.choice(account_indices)

                                        
        with lock:
            current = shared_balances[idx]
            shared_balances[idx] = current * interest_factor

        applied += 1
        if (i + 1) % 10 == 0:
            logging.info(f"INTEREST Worker {worker_id}: {i+1}/{operation_count}")

    stats_queue.put({"type": "interest", "worker": worker_id, "applied": applied})
    logging.info(f"INTEREST Worker {worker_id} finished")


def monitor_balances(shared_balances, stop_event, lock, sample_interval=0.5):
    configure_logging()
    logging.info("Monitor started")

    while not stop_event.is_set():
                                                                                  
        with lock:
            snapshot = list(shared_balances)
        logging.info(f"Snapshot balances: {snapshot}")
        time.sleep(sample_interval)

    logging.info("Monitor finished")


def print_final_report(account_names, shared_balances, initial_balances, stats, deposit_amount, withdraw_amount):
    final_balances = list(shared_balances)

    print("\n" + "=" * 70)
    print("FINAL WALLET REPORT")
    print("=" * 70)
    for name, bal in zip(account_names, final_balances):
        print(f"{name:>10}: {bal:.2f}")

    initial_total = sum(initial_balances)
    final_total = sum(final_balances)

    print("-" * 70)
    print(f"Initial total balance: {initial_total:.2f}")
    print(f"Final total balance  : {final_total:.2f}")
    print(f"Total change         : {final_total - initial_total:+.2f}")
    print("=" * 70)

    deposits = sum(s.get("applied", 0) for s in stats if s["type"] == "deposit")
    withdraw_success = sum(s.get("success", 0) for s in stats if s["type"] == "withdraw")
    withdraw_skipped = sum(s.get("skipped", 0) for s in stats if s["type"] == "withdraw")
    interests = sum(s.get("applied", 0) for s in stats if s["type"] == "interest")

    print("\nTRANSACTION SUMMARY (worker self-reports)")
    print("-" * 70)
    print(f"Deposits applied      : {deposits}")
    print(f"Withdrawals success   : {withdraw_success}")
    print(f"Withdrawals skipped   : {withdraw_skipped}")
    print(f"Interest applications : {interests}")
    print("-" * 70)

                                             
    expected_change_no_interest = deposits * deposit_amount - withdraw_success * withdraw_amount
    actual_change = final_total - initial_total
    print("ATOMICITY CHECK (ignoring interest)")
    print(f"Expected change (no interest): {expected_change_no_interest:+.2f}")
    print(f"Actual change                : {actual_change:+.2f}")
    print(f"Mismatch                     : {(actual_change - expected_change_no_interest):+.2f}")
    print("NOTE: If interest is enabled, expected != actual even without races.\n"
          "      For the cleanest atomicity check, set ENABLE_INTEREST=False.")
    print("-" * 70)


if __name__ == "__main__":
    multiprocessing.freeze_support()
    configure_logging()

    print("Starting Multiprocessing Atomicity Demo (Fixed Version)")
    print()

                                                    
    ENABLE_INTEREST = False                                                

    ACCOUNT_NAMES = ["Alice", "Bob", "Chris", "Dana", "Evan"]
    INITIAL_BALANCES = [200.0, 500.0, 150.0, 300.0, 400.0]

    NUM_DEPOSIT_WORKERS = 8
    NUM_WITHDRAW_WORKERS = 8
    NUM_INTEREST_WORKERS = 3 if ENABLE_INTEREST else 0

    OPS_PER_DEPOSIT_WORKER = 150
    OPS_PER_WITHDRAW_WORKER = 150
    OPS_PER_INTEREST_WORKER = 20

    DEPOSIT_AMOUNT = 5.0
    WITHDRAW_AMOUNT = 5.0
    INTEREST_FACTORS = [1.10, 0.90, 1.05]

    print("Configuration:")
    print(f"  - Accounts          : {ACCOUNT_NAMES}")
    print(f"  - Initial balances  : {INITIAL_BALANCES}")
    print(f"  - ENABLE_INTEREST   : {ENABLE_INTEREST}")
    print(f"  - Deposit workers   : {NUM_DEPOSIT_WORKERS}")
    print(f"  - Withdraw workers  : {NUM_WITHDRAW_WORKERS}")
    print(f"  - Interest workers  : {NUM_INTEREST_WORKERS}\n")

    manager = multiprocessing.Manager()
    shared_balances = manager.list([float(x) for x in INITIAL_BALANCES])

                                                     
    lock = manager.Lock()
    stats_queue = multiprocessing.Queue()
    account_indices = list(range(len(ACCOUNT_NAMES)))

    processes = []

    for w in range(NUM_DEPOSIT_WORKERS):
        p = multiprocessing.Process(
            target=deposit_worker,
            args=(shared_balances, account_indices, DEPOSIT_AMOUNT,
                  OPS_PER_DEPOSIT_WORKER, w + 1, stats_queue, lock),
            name=f"DepositWorker-{w+1}",
        )
        processes.append(p)

    for w in range(NUM_WITHDRAW_WORKERS):
        p = multiprocessing.Process(
            target=withdraw_worker,
            args=(shared_balances, account_indices, WITHDRAW_AMOUNT,
                  OPS_PER_WITHDRAW_WORKER, w + 1, stats_queue, lock),
            name=f"WithdrawWorker-{w+1}",
        )
        processes.append(p)

    for w in range(NUM_INTEREST_WORKERS):
        factor = INTEREST_FACTORS[w % len(INTEREST_FACTORS)]
        p = multiprocessing.Process(
            target=interest_worker,
            args=(shared_balances, account_indices, factor,
                  OPS_PER_INTEREST_WORKER, w + 1, stats_queue, lock),
            name=f"InterestWorker-{w+1}",
        )
        processes.append(p)

    stop_event = multiprocessing.Event()
    monitor = multiprocessing.Process(
        target=monitor_balances,
        args=(shared_balances, stop_event, lock, 0.5),
        name="BalanceMonitor",
    )

    print(f"Initial balances: {list(shared_balances)}")
    print(f"Starting {len(processes)} worker processes...\n")

    start = time.time()

    for p in processes:
        p.start()
        time.sleep(0.05)

    monitor.start()

    for p in processes:
        p.join()

    stop_event.set()
    monitor.join()

    elapsed = time.time() - start
    print(f"\nAll processes completed in {elapsed:.2f}s")

    expected_reports = NUM_DEPOSIT_WORKERS + NUM_WITHDRAW_WORKERS + NUM_INTEREST_WORKERS
    stats = []
    for _ in range(expected_reports):
        try:
            stats.append(stats_queue.get(timeout=5))
        except Exception:
            break

    print_final_report(ACCOUNT_NAMES, shared_balances, INITIAL_BALANCES,
                       stats, DEPOSIT_AMOUNT, WITHDRAW_AMOUNT)

    print(f"Program finished at: {datetime.now()}")
