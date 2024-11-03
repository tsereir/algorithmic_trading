import time as tm
from datetime import time, timedelta, datetime
import schedule
import pandas as pd
import threading
from tqdm import tqdm
from binance.client import Client

client = Client()

def get_history(client, symbol, interval, duration):
    tm.sleep(0.01)  #délai pour contourner la limite de l'API
    return client.get_historical_klines(symbol=symbol, interval=interval, start_str=duration)

def get_top_performer_multithreading(client, interval, duration, n=10):
    #informations sur les symboles
    info = client.get_exchange_info()
    symbols = [x['symbol'] for x in info['symbols']]
    #exclure les tokens à effet de levier
    exclude = ['UP', 'DOWN', 'BEAR', 'BULL']
    non_lev = [symbol for symbol in symbols if all(excludes not in symbol for excludes in exclude)]
    relevant_symbols = [symbol for symbol in non_lev if symbol.endswith('USDT')]

    #créer une liste pour stocker les données et une pour les rendements
    returns, selected_symbols, dates = [], [], []
    klines_data = [None] * len(relevant_symbols)
    
    #fonction pour récupérer les données en parallèle
    def _helper(i):
        klines_data[i] = get_history(client, relevant_symbols[i], interval, duration)
    
    #créer et lancer les threads
    threads = [threading.Thread(target=_helper, args=(i,)) for i in range(len(relevant_symbols))]
    for thread in threads:
        thread.start()
    for thread in tqdm(threads):
        thread.join()
    
    #calcul des rendements
    for i, klines in enumerate(klines_data):
        if relevant_symbols[i] == 'BTTCUSDT':  
            continue
        if klines and len(klines) > 0:
            df = pd.DataFrame(klines)
            df_close_prices = df[4].astype(float)  # La colonne des prix de clôture
            cumret = (df_close_prices.pct_change() + 1).prod() - 1
            
            start_date = pd.to_datetime(df.iloc[0, 0], unit='ms')
            end_date = pd.to_datetime(df.iloc[-1, 0], unit='ms')
            
            returns.append(cumret)
            selected_symbols.append(relevant_symbols[i])
            dates.append((start_date, end_date))

    
    retdf = pd.DataFrame({
        'ret': returns,
        'start_date': [d[0] for d in dates],
        'end_date': [d[1] for d in dates]
    }, index=selected_symbols)

    result = retdf.nlargest(n, 'ret')
    
    result = retdf.nlargest(n, 'ret')
    
    top_end_date = result['end_date'].iloc[0]
    # date_str = datetime.now().strftime("%Y-%m-%d")
    date_str = top_end_date.strftime("%Y-%m-%d_%H-%M-%S")
    file_path = f"C:/Users/tariq/algorithmic_trading/data/topPerformers/top_performers_{date_str}.parquet"
    # Sauvegarder le résultat en parquet
    result.to_parquet(file_path)    
    print("ajout effectué pour la date : ", date_str)


def job():
    get_top_performer_multithreading(client, '1m',  '4 minutes ago UTC', n=1)

schedule.every(5).minutes.do(job)

while True:
    schedule.run_pending()
    tm.sleep(1)
