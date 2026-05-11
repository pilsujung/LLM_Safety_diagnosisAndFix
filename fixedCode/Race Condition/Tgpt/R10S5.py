import threading, time, random
from contextlib import nullcontext


PRICES = {
    "Kimbap":1500,"Noodles":1800,"Water":1000,"Drink":2500,
    "Choco":1200,"Mask":800,"Hotbar":2200,"Milk":2000
}
INV0   = {
    "Kimbap":40,  "Noodles":35,  "Water":60,  "Drink":25,
    "Choco":50,   "Mask":80,     "Hotbar":30, "Milk":28
}

N_CUSTOMERS = 30
MAX_ITEMS_PER_CUSTOMER = 8
DELAY = (0.00005, 0.00030)

def run(use_locks: bool, seed: int = 12345):

    inv = dict(INV0)
    sales = 0
    receipt = 1
    txs = []


    L_inv   = threading.Lock()
    L_sales = threading.Lock()
    L_rcpt  = threading.Lock()
    L_txs   = threading.Lock()


    D_lock = threading.Lock()
    seen_receipts = set()
    dup_receipts = 0
    neg_inv_events = 0

    def cm(lock):
        return lock if use_locks else nullcontext()

    def tiny(rng):
        time.sleep(rng.uniform(*DELAY))

    def customer(cid: int):
        nonlocal sales, receipt, dup_receipts, neg_inv_events
        rng = random.Random(seed ^ (cid * 0x9E3779B1) ^ threading.get_ident())


        cart = []
        for _ in range(rng.randint(1, MAX_ITEMS_PER_CUSTOMER)):
            name = rng.choice(list(PRICES.keys()))
            cart.append((name, rng.randint(1, 3)))


        required = {}
        for name, qty in cart:
            required[name] = required.get(name, 0) + qty


        with cm(L_inv):

            for name, total_qty in required.items():
                if inv.get(name, 0) < total_qty:
                    return


            for name, total_qty in required.items():
                cur = inv[name]
                tiny(rng)
                inv[name] = cur - total_qty
                if inv[name] < 0:

                    with D_lock:
                        neg_inv_events += 1


        amount = sum(PRICES[n] * q for n, q in cart)


        with cm(L_rcpt):
            cur = receipt
            tiny(rng)
            receipt = cur + 1


        with D_lock:
            if cur in seen_receipts:
                dup_receipts += 1
            else:
                seen_receipts.add(cur)


        with cm(L_sales):
            cur_sales = sales
            tiny(rng)
            sales = cur_sales + amount


        with cm(L_txs):
            txs.append((cur, amount, cart))


    threads = [threading.Thread(target=customer, args=(i+1,)) for i in range(N_CUSTOMERS)]
    t0 = time.time()
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    dt = time.time() - t0


    sum_tx = sum(a for _, a, _ in txs)
    sales_delta = sum_tx - sales

    sold = {k: 0 for k in INV0}
    for _, _, cart in txs:
        for n, q in cart:
            sold[n] += q
    expected_inv = {k: INV0[k] - sold[k] for k in INV0}
    inv_mismatch = sum(1 for k in INV0 if inv.get(k, 0) != expected_inv[k])
    neg_items = {k: v for k, v in inv.items() if v < 0}


    mode = "SAFE" if use_locks else "UNSAFE"
    print("="*70)
    print(f"Self-checkout race simulation [{mode}]  tx={len(txs)}  time={dt:.4f}s")
    print(f"Sales: shared={sales}  sum(tx)={sum_tx}  delta={sales_delta}  -> "
          f"{'OK' if sales_delta == 0 else 'RACE'}")
    print(f"Receipts: duplicates={dup_receipts}  -> "
          f"{'OK' if dup_receipts == 0 else 'RACE'}")
    print(f"Inventory: neg_events={neg_inv_events}, "
          f"neg_items={neg_items if neg_items else 'none'}")
    print(f"Inventory mismatch items={inv_mismatch}  -> "
          f"{'OK' if (inv_mismatch == 0 and not neg_items) else 'RACE'}")
    print("="*70)


if __name__ == "__main__":

    run(use_locks=False, seed=12345)

    run(use_locks=True,  seed=12345)
