from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.exceptions import ReadTimeout
from pytz import timezone

def get_performers_past(client, interval, start_date, end_date, n=1, worst=True, future=True):
    """
    Identifie les meilleurs ou pires performeurs dans chaque plage horaire.
    """
    paris_tz = timezone('Europe/Paris')
    if future : 
        info = client.futures_exchange_info()
    else: 
        info = client.get_exchange_info()
        
    symbols = [x['symbol'] for x in info['symbols']]
    exclude = ['UP', 'DOWN', 'BEAR', 'BULL']
    non_lev = [symbol for symbol in symbols if all(excludes not in symbol for excludes in exclude)]
    relevant_symbols = [symbol for symbol in non_lev if symbol.endswith('USDT')]

    all_results = []
    date_range = []
    current_time = start_date
    while current_time < end_date:
        date_range.append(current_time)
        current_time += timedelta(hours=1)
        
    print(f"start : {start_date}, end : {end_date}")
    print("Nombre de plage d'heure", len(date_range))

    for current_time in date_range : 
        print("Processing time range:", current_time, "to", current_time + timedelta(hours=1))
        next_time = current_time + timedelta(hours=1)

        # Initialisation des listes pour cette période
        returns, selected_symbols, dates = [], [], []
        klines_data = [None] * len(relevant_symbols)

        def _helper(klines_data, i):
            try:
                klines_data[i] = get_history(
                    client, relevant_symbols[i], interval,
                    start_str=current_time.strftime("%Y-%m-%d %H:%M:%S"),
                    end_str=next_time.strftime("%Y-%m-%d %H:%M:%S")
                )
            except ReadTimeout:
                print(f"Timeout for {relevant_symbols[i]} during {current_time} -> {next_time}. Retrying...")
            except Exception:
                pass

        threads = [threading.Thread(target=_helper, args=(klines_data, i)) for i in range(len(relevant_symbols))]
        for thread in threads:
            thread.start()
        for thread in tqdm(threads, desc=f"Processing {current_time} -> {next_time}"):
            thread.join()

        # Calculer les rendements
        for i, klines in enumerate(klines_data):
                # print(f"{relevant_symbols[i]}: {len(klines) if klines else 0} klines retrieved.")
            if relevant_symbols[i] == 'BTTCUSDT':  
                continue
            if klines and len(klines) > 0:
                df = pd.DataFrame(klines)
                df_close_prices = df[4].astype(float)  # Prix de clôture
                cumret = (df_close_prices.pct_change() + 1).prod() - 1

                # start_date = pd.to_datetime(df.iloc[0, 0], unit='ms')
                # end_date = pd.to_datetime(df.iloc[-1, 0], unit='ms')

                #to have paris timezone
                start_date = pd.to_datetime(df.iloc[0, 0], unit='ms').tz_localize('UTC').tz_convert(paris_tz)
                end_date = pd.to_datetime(df.iloc[-1, 0], unit='ms').tz_localize('UTC').tz_convert(paris_tz)



                returns.append(cumret)
                selected_symbols.append(relevant_symbols[i])
                dates.append((start_date, end_date))

        retdf = pd.DataFrame({
            'ret': returns,
            'start_date': [d[0] for d in dates],
            'end_date': [d[1] for d in dates]
        }, index=selected_symbols)

        if not retdf.empty:
            if worst:
                result = retdf.nsmallest(n, 'ret')
            else:
                result = retdf.nlargest(n, 'ret')

            result['start_period'] = current_time
            result['end_period'] = next_time
            all_results.append(result)
        else:
            print("WARNING -- df EMPTY")

        current_time = next_time
        # print("Finished processing:", current_time)

    return pd.concat(all_results).reset_index()
