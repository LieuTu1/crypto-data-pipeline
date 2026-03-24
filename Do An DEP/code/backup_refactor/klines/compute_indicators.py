"""Tinh SMA20, SMA50, RSI14 va Bollinger Bands tu file btcusdt_5m.json."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def load_klines(path: Path) -> pd.DataFrame:
  """Doc file JSON lines thanh DataFrame, giu nguyen dinh dang chuoi timestamp."""
  return pd.read_json(path, lines=True, convert_dates=False)


def add_indicators(df: pd.DataFrame) -> pd.DataFrame:
  """Them cac chi bao ky thuat co ban vao DataFrame."""
  out = df.copy()
  closes = out["close"]

  # Trung binh dong gia 20 va 50 nen
  out["sma20"] = closes.rolling(window=20, min_periods=20).mean()
  out["sma50"] = closes.rolling(window=50, min_periods=50).mean()

  # RSI14 voi Wilder smoothing
  delta = closes.diff()
  gain = delta.clip(lower=0)
  loss = -delta.clip(upper=0)
  avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
  avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
  rs = avg_gain / avg_loss
  out["rsi14"] = 100 - (100 / (1 + rs))

  # Bollinger Bands 20 ky voi do lech chuan 2
  rolling20 = closes.rolling(window=20, min_periods=20)
  middle = rolling20.mean()
  std = rolling20.std(ddof=0)
  out["bb_middle"] = middle
  out["bb_upper"] = middle + 2 * std
  out["bb_lower"] = middle - 2 * std

  return out


def main() -> None:
  parser = argparse.ArgumentParser(
      description="Tinh SMA20, SMA50, RSI14, Bollinger Bands tu btcusdt_5m.json"
  )
  parser.add_argument(
      "-i",
      "--input",
      type=Path,
      default=Path("klines/btcusdt_5m.json"),
      help="Duong dan file JSON lines dau vao (mac dinh: klines/btcusdt_5m.json)",
  )
  parser.add_argument(
      "-o",
      "--output",
      type=Path,
      default=Path("klines/btcusdt_5m_indicators.json"),
      help="File JSON lines chua ket qua (mac dinh: klines/btcusdt_5m_indicators.json)",
  )
  args = parser.parse_args()

  df = load_klines(args.input)
  enriched = add_indicators(df)
  # Ghi tung ban ghi tren 1 dong de giu dinh dang giong file goc.
  enriched.to_json(args.output, orient="records", lines=True, force_ascii=False)
  print(f"Da tinh chi bao cho {len(enriched)} dong, luu tai {args.output}")


if __name__ == "__main__":
  main()
