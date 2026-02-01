
async def process_data(data):
    print(data)
    global symbol_data_state, future_prices
    
    symbol = data.get("InstrumentIdentifier")
    if not symbol or symbol not in symbol_data_state:
        return

    new_price = data.get("LastTradePrice")
    new_oi = data.get("OpenInterest")

    if new_price is None or new_oi is None:
        return

    state = symbol_data_state[symbol]
    is_simple_future = symbol.endswith("-I")

    # --- ROUTE 1: Logic for simple "-I" futures ---
    if is_simple_future:
        base_symbol = symbol.split("-")[0].upper()
        if new_price > 0: future_prices[base_symbol] = new_price
        
        prev_oi = state.get("oi", 0)
        prev_price = state.get("price", 0)

        state["price"], state["oi"] = new_price, new_oi

        if prev_oi == 0:
            print(f"🟢 [{now()}] {symbol}: First Data (P: {new_price}, OI: {new_oi})", flush=True)
            return

        oi_chg = new_oi - prev_oi
        if abs(oi_chg) == 0:
            return

        lots = lots_from_oi_change(symbol, oi_chg)
        if lots >= 50:
            price_chg = new_price - prev_price
            oi_roc = (oi_chg / prev_oi) * 100 if prev_oi != 0 else 0.0
            price_chg_percent = (price_chg / prev_price) * 100 if prev_price != 0 else 0.0

            direction_symbol = "↔️"
            if oi_chg > 0 and price_chg > 0: direction_symbol = "⬆️"
            elif oi_chg > 0 and price_chg < 0: direction_symbol = "⬇️"
            elif oi_chg < 0 and price_chg > 0: direction_symbol = "↗️"
            elif oi_chg < 0 and price_chg < 0: direction_symbol = "↘️"
            
            alert_msg = (f"🔔 FUTURE ALERT: {symbol} {direction_symbol}\n"
                         f"Existing OI: {prev_oi}\n"
                         f"OI Change: {oi_chg} ({lots} lots)\n"
                         f"OI RoC: {oi_roc:.2f}%\n"
                         f"Price: {new_price:.2f}\n"
                         f"Price Chg: {price_chg:+.2f} ({price_chg_percent:+.2f}%)\n"
                         f"Time: {now()}")
            await send_alert(alert_msg)
            print(f"🚀 Alert (Future): {symbol} Lot size >= 50 detected.", flush=True)
        return

    # --- ROUTE 2: Existing logic for Options ---
    if "FUT" in symbol: # This handles the ...FUT style symbols if any are left
        underlying = next((key for key in future_prices if key in symbol), None)
        if underlying and new_price > 0:
            future_prices[underlying] = new_price
        return

    # --- Standard processing for Option contracts ---
    state["price_prev"], state["oi_prev"] = state.get("price", 0), state.get("oi", 0)
    state["price"], state["oi"] = new_price, new_oi

    if state["oi_prev"] == 0:
        print(f"ℹ️ [{now()}] {symbol}: Initializing option state.", flush=True)
        return

    oi_chg = state["oi"] - state["oi_prev"]
    if oi_chg == 0:
        return

    price_chg = state["price"] - state["price_prev"]
    price_chg_percent = (price_chg / state["price_prev"]) * 100 if state["price_prev"] != 0 else 0.0
    oi_roc = (oi_chg / state["oi_prev"]) * 100 if state["oi_prev"] != 0 else 0.0

    if abs(oi_roc) > OI_ROC_THRESHOLD:
        lots = lots_from_oi_change(symbol, oi_chg)
        if lots > 100:
            moneyness = get_option_moneyness(symbol, future_prices)
            action = classify_option(oi_chg, price_chg, symbol)
            bucket = lot_bucket(lots)
            alert_msg = format_alert_message(symbol, action, bucket, lots, state, oi_chg, oi_roc, moneyness, future_prices, price_chg, price_chg_percent)
            await send_alert(alert_msg)
            print(f"📊 [{now()}] {symbol}: {moneyness}, lots: {lots}. TRIGGERING ALERT.", flush=True)
