import asyncio
import websockets
import json
import nest_asyncio

# --- CONFIGURATION ---
TOKEN = 'pE5s66EGDhOxOMh' 
SYMBOLS = ['R_10', 'R_25', 'R_50', 'R_75', 'R_100']
INITIAL_STAKE = 50.0
TP_PERCENTAGE = 0.70 
MARTINGALE_FACTOR = 2.4

INDEX_CONFIG = {
    'R_10':  0.0000486, 'R_25':  0.0001216, 'R_50':  0.0002431,
    'R_75':  0.0003647, 'R_100': 0.0004863
}

tracker = {symbol: {
    "last_runs": [], "current_ticks": 0, "last_price": None, 
    "stake": INITIAL_STAKE, "active_trade": False, "has_synced": False,
    "waiting_for_recovery": False 
} for symbol in SYMBOLS}

async def place_trade(ws, symbol, stake):
    try:
        # Calculate 70% profit amount
        tp_amount = round(stake * TP_PERCENTAGE, 2)
        
        # We send the TP instruction directly in the buy request
        buy_req = {
            "buy": 1, 
            "price": stake,
            "parameters": {
                "amount": stake, 
                "basis": "stake", 
                "contract_type": "ACCU",
                "currency": "USD", 
                "symbol": symbol, 
                "growth_rate": 0.05,
                "limit_order": {
                    "take_profit": tp_amount  # SERVER-SIDE TP
                }
            }
        }
        await ws.send(json.dumps(buy_req))
        
        contract_id = None
        while not contract_id:
            resp = json.loads(await ws.recv())
            if "buy" in resp:
                contract_id = resp['buy']['contract_id']
                print(f">>> [{symbol}] TRADE LIVE | Stake: ${stake} | TP Set: ${tp_amount}")
                break
            elif "error" in resp:
                print(f"!!! [{symbol}] REJECTED: {resp['error']['message']}")
                return "error"

        # Now we just wait for the contract to close itself
        while True:
            await ws.send(json.dumps({"proposal_open_contract": 1, "contract_id": contract_id}))
            msg = json.loads(await ws.recv())
            
            if "proposal_open_contract" in msg:
                poc = msg["proposal_open_contract"]
                status = poc.get("status")

                if status == "won":
                    print(f"$$$ [{symbol}] WIN | TP hit at ${poc.get('profit')}")
                    return "win"
                
                if status == "lost":
                    print(f"L [{symbol}] LOSS | Market Crashed.")
                    return "loss"
            
            await asyncio.sleep(0.5)

    except Exception as e:
        print(f"!!! [{symbol}] Error: {e}")
        return "error"

async def bot_manager():
    uri = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
    async with websockets.connect(uri, ping_interval=20, ping_timeout=60) as ws:
        await ws.send(json.dumps({"authorize": TOKEN}))
        auth_data = json.loads(await ws.recv())
        
        if "error" in auth_data:
            print(f"Auth Failed: {auth_data['error']['message']}")
            return

        print(f"SERVER-SIDE TP SNIPER ONLINE | Balance: {auth_data['authorize']['balance']}")

        for symbol in SYMBOLS:
            await ws.send(json.dumps({"ticks": symbol, "subscribe": 1}))

        async for message in ws:
            data = json.loads(message)
            if "tick" in data:
                tick = data["tick"]
                sym = tick["symbol"]
                price = float(tick["quote"])
                s = tracker[sym]

                if s["last_price"] is not None and not s["active_trade"]:
                    barrier = s["last_price"] * INDEX_CONFIG[sym]
                    move = abs(price - s["last_price"])

                    if move > barrier:
                        run_len = s["current_ticks"]
                        if not s["has_synced"]:
                            print(f"[SYNC] {sym} Ready.")
                            s["has_synced"] = True
                        elif s["waiting_for_recovery"]:
                            if run_len > 10:
                                print(f"[RECOVERY] {sym} Ready (Run: {run_len}).")
                                s["waiting_for_recovery"] = False
                                s["last_runs"] = []
                        else:
                            s["last_runs"].append(run_len)
                            if len(s["last_runs"]) > 5: s["last_runs"].pop(0)

                            if len(s["last_runs"]) == 5 and all(r < 10 for r in s["last_runs"]):
                                print(f"\n[PATTERN] {sym}: {s['last_runs']}")
                                s["active_trade"] = True
                                result = await place_trade(ws, sym, s["stake"])
                                
                                if result == "win":
                                    s["stake"] = INITIAL_STAKE
                                    s["last_runs"] = []
                                elif result == "loss":
                                    s["stake"] = round(s["stake"] * MARTINGALE_FACTOR, 2)
                                    s["waiting_for_recovery"] = True
                                
                                s["active_trade"] = False
                        s["current_ticks"] = 0
                    else:
                        s["current_ticks"] += 1
                s["last_price"] = price

if __name__ == "__main__":
    nest_asyncio.apply()
    try:
        asyncio.run(bot_manager())
    except KeyboardInterrupt:
        print("\nShutdown.")