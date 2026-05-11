import threading, time, random


PRICES = {
    "Kimbap": 1500,
    "Noodles": 1800,
    "Water": 1000,
    "Drink": 2500,
    "Choco": 1200,
    "Mask": 800,
    "Hotbar": 2200,
    "Milk": 2000,
}
INV0 = {
    "Kimbap": 40,
    "Noodles": 35,
    "Water": 60,
    "Drink": 25,
    "Choco": 50,
    "Mask": 80,
    "Hotbar": 30,
    "Milk": 28,
}

N_CUSTOMERS = 30
MAX_ITEMS_PER_CUSTOMER = 8
DELAY = (0.00005, 0.00030)


def run(seed: int = 12345):

    inv = dict(INV0)
    sales = 0
    receipt = 1
    txs = []


    L_inv = threading.Lock()
    L_sales = threading.Lock()
    L_rcpt = threading.Lock()
    L_txs = threading.Lock()


    D_lock = threading.Lock()
    seen_receipts = set()
    dup_receipts = 0
    neg_inv_events = 0

    def tiny(rng):
        time.sleep(rng.uniform(*DELAY))

    def customer(cid: int):
        nonlocal sales, receipt, dup_receipts, neg_inv_events
        rng = random.Random(seed ^ (cid * 0x9E3779B1) ^ threading.get_ident())


        cart = []
        for _ in range(rng.randint(1, MAX_ITEMS_PER_CUSTOMER)):
            name = rng.choice(list(PRICES.keys()))
            cart.append((name, rng.randint(1, 3)))


        with L_inv:

            for name, qty in cart:
                if inv.get(name, 0) < qty:
                    return


            for name, qty in cart:
                cur = inv[name]
                tiny(rng)
                inv[name] = cur - qty
                if inv[name] < 0:


                    neg_inv_events += 1

        amount = sum(PRICES[n] * q for n, q in cart)


        with L_rcpt:
            cur = receipt
            tiny(rng)
            receipt = cur + 1


        with D_lock:
            if cur in seen_receipts:
                dup_receipts += 1
            else:
                seen_receipts.add(cur)


        with L_sales:
            cur_sales = sales
            tiny(rng)
            sales = cur_sales + amount


        with L_txs:
            txs.append((cur, amount, cart))


    threads = [threading.Thread(target=customer, args=(i + 1,)) for i in range(N_CUSTOMERS)]
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


    print("=" * 70)
    print(f"Self-checkout race simulation [SAFE]  tx={len(txs)}  time={dt:.4f}s")
    print(
        f"Sales: shared={sales}  sum(tx)={sum_tx}  delta={sales_delta}  "
        f"-> {'OK' if sales_delta == 0 else 'RACE'}"
    )
    print(f"Receipts: duplicates={dup_receipts}  -> {'OK' if dup_receipts == 0 else 'RACE'}")
    print(f"Inventory: neg_events={neg_inv_events}, neg_items={neg_items if neg_items else 'none'}")
    print(
        f"Inventory mismatch items={inv_mismatch}  "
        f"-> {'OK' if (inv_mismatch == 0 and not neg_items) else 'RACE'}"
    )
    print("=" * 70)


if __name__ == "__main__":

    run(seed=12345)
