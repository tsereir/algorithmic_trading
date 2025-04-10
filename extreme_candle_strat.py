import pandas as pd
import numpy as np
import os

def backtest_extreme_candle_pair_vectorized(filepath, threshold=-0.05):
    df = pd.read_parquet(filepath)
    df["date"] = pd.to_datetime(df["date"])
    
    df["ret"] = np.log(df["close"] / df["close"].shift(1))
    df["logreturn_1"] = df["ret"].shift(1) 
    df["buy_hold_ret"] = (1 + df["ret"]).cumprod() - 1

    # signaux
    signal_mask = df["ret"] < threshold

    # Shift pour avoir accès aux données futures
    df["open_next"] = df["open"].shift(-1)
    df["close_after"] = df["close"].shift(-1)
    # df["close_plus_2"] = df["close"].shift(-2)

    # Calcul du return du trade
    df["return"] = np.log(df["close_after"] / df["open_next"]) - 0.0001
    # df["trade_return"] = np.log(df["close_plus_2"] / df["close_after"]) - 0.0001  # alternative

    # Ne garder que les lignes avec signal
    trades = df[signal_mask].copy()
    trades["pair"] = filepath.split("_")[0]
    trades["timestamp"] = trades["date"]

    trades = trades[["pair", "timestamp", "open_next", "close_after", "return", "buy_hold_ret"]]

    if not trades.empty:
        trades["cumulative_return"] = (1 + trades["return"]).cumprod() - 1
    else:
        print(f"Aucun trade détecté pour {filepath}")

    return trades

import pandas as pd
import numpy as np

# TODO: DEBUGGER
def backtest_extreme_candle_adaptative(filepath, threshold=-0.05, holding_period=5, entry_delay=0):
    df = pd.read_parquet(filepath)
    df["date"] = pd.to_datetime(df["date"])

    df["ret"] = np.log(df["close"] / df["close"].shift(1))
    df["logreturn_1"] = df["ret"].shift(1)
    df["buy_hold_ret"] = (1 + df["ret"]).cumprod() - 1

    signal_mask = df["ret"] < threshold
    signal_indices = np.where(signal_mask)[0]

    entry_indices = signal_indices + entry_delay + 1
    exit_indices = entry_indices + holding_period - 1

    valid_mask = (exit_indices < len(df)) & (entry_indices < len(df))
    signal_indices = signal_indices[valid_mask]
    entry_indices = entry_indices[valid_mask]
    exit_indices = exit_indices[valid_mask]

    # Extraction vectorisée des valeurs
    entry_open = df.iloc[entry_indices]["open"].values
    exit_close = df.iloc[exit_indices]["close"].values
    timestamps = df.iloc[signal_indices]["date"].values
    buy_hold_ret = df.iloc[signal_indices]["buy_hold_ret"].values

    # Calcul du retour de chaque trade
    trade_returns = np.log(exit_close / entry_open) - 0.0001  # slippage

    # Construction du DataFrame résultat
    trades = pd.DataFrame({
        "pair": filepath.split("_")[0],
        "timestamp": timestamps,
        "open_entry": entry_open,
        "close_exit": exit_close,
        "return": trade_returns,
        "buy_hold_ret": buy_hold_ret
    })

    if not trades.empty:
        trades["cumulative_return"] = (1 + trades["return"]).cumprod() - 1
    else:
        print(f"Aucun trade détecté pour {filepath}")

    return trades
