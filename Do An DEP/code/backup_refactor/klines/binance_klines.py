"""Utilities to fetch OHLCV candlesticks from Binance."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from typing import Iterable, List
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd


BINANCE_API = "https://api.binance.com/api/v3/klines"
INTERVAL_5M_MS = 5 * 60 * 1000
MAX_LIMIT = 1000  # Binance hard limit per request


@dataclass
class Kline:
  """Container for a single candlestick row."""

  open_time: int
  open: float
  high: float
  low: float
  close: float
  volume: float


def _fetch_raw_klines(
    symbol: str, interval: str, limit: int, start_time: int | None = None
) -> List[List]:
  """Call Binance public API and return raw kline payload."""
  url = f"{BINANCE_API}?symbol={symbol}&interval={interval}&limit={limit}"
  if start_time is not None:
    url += f"&startTime={start_time}"
  request = Request(url, headers={"User-Agent": "kline-fetcher/1.0"})
  try:
    with urlopen(request, timeout=15) as resp:
      payload = resp.read()
  except (HTTPError, URLError) as exc:
    raise RuntimeError(f"Failed to fetch klines: {exc}") from exc

  try:
    data = json.loads(payload)
  except json.JSONDecodeError as exc:
    raise RuntimeError("Unable to decode Binance response") from exc

  if not isinstance(data, list):
    raise RuntimeError(f"Unexpected response: {data}")

  return data


def _parse_klines(rows: Iterable[List]) -> List[Kline]:
  """Convert raw rows into typed Kline instances."""
  parsed: List[Kline] = []
  for row in rows:
    if len(row) < 6:
      raise RuntimeError(f"Incomplete kline row: {row}")
    open_time, open_, high, low, close, volume = row[:6]
    parsed.append(
        Kline(
            open_time=int(open_time),
            open=float(open_),
            high=float(high),
            low=float(low),
            close=float(close),
            volume=float(volume),
        )
    )
  return parsed


def fetch_btcusdt_5m(limit: int = 150) -> pd.DataFrame:
  """
  Lay 150 nen khung 5 phut BTC/USDT tu Binance va tra ve DataFrame.

  Columns: timestamp (UTC+7), open, high, low, close, volume.
  """
  raw = _fetch_raw_klines(symbol="BTCUSDT", interval="5m", limit=limit)
  klines = _parse_klines(raw)
  df = pd.DataFrame(
      [
          {
              "timestamp": k.open_time,
              "open": k.open,
              "high": k.high,
              "low": k.low,
              "close": k.close,
              "volume": k.volume,
          }
          for k in klines
      ]
  )
  # Convert timestamp to human-readable string in UTC+7 (Asia/Ho_Chi_Minh)
  df["timestamp"] = (
      pd.to_datetime(df["timestamp"], unit="ms", utc=True)
      .dt.tz_convert("Asia/Ho_Chi_Minh")
      .dt.strftime("%Y-%m-%d %H:%M:%S")
  )
  return df


def fetch_btcusdt_5m_history(total: int = 10_000) -> pd.DataFrame:
  """
  Thu thap toi thieu `total` nen 5m BTC/USDT (mac dinh 10k) theo thu tu thoi gian tang dan.
  """
  rows: list[list] = []
  # Ước tính start_time lùi đủ xa để lấy đủ số nến yêu cầu.
  lookback_ms = total * INTERVAL_5M_MS + INTERVAL_5M_MS
  start_time = int(time.time() * 1000) - lookback_ms

  while len(rows) < total:
    batch = _fetch_raw_klines(
        symbol="BTCUSDT",
        interval="5m",
        limit=MAX_LIMIT,
        start_time=start_time,
    )
    if not batch:
      break
    rows.extend(batch)
    # Đi tiếp từ sau cây nến cuối cùng đã nhận.
    start_time = int(batch[-1][0]) + INTERVAL_5M_MS
    if len(batch) < MAX_LIMIT:
      break

  klines = _parse_klines(rows[:total])
  df = pd.DataFrame(
      [
          {
              "timestamp": k.open_time,
              "open": k.open,
              "high": k.high,
              "low": k.low,
              "close": k.close,
              "volume": k.volume,
          }
          for k in klines
      ]
  )
  df["timestamp"] = (
      pd.to_datetime(df["timestamp"], unit="ms", utc=True)
      .dt.tz_convert("Asia/Ho_Chi_Minh")
      .dt.strftime("%Y-%m-%d %H:%M:%S")
  )
  return df


if __name__ == "__main__":
  # Example usage: fetch 10k rows and store to JSON file.
  df = fetch_btcusdt_5m_history(10_000)
  # Danh so thu tu (STT) bat dau tu 1 cho tung moc thoi gian.
  df.insert(0, "stt", range(1, len(df) + 1))
  output_path = "klines/btcusdt_5m.json"
  # Ghi moi ban ghi tren 1 dong, thu thuan doc bang log.
  df.to_json(output_path, orient="records", lines=True, force_ascii=False)
  print(f"Saved {len(df)} rows to {output_path}")
  # In 5 dong moi nhat (thoi gian moi nhat o cuoi DataFrame)
  print("\n5 dong moi nhat:")
  print(df.tail(5).to_string(index=False))
