"""Utilities to fetch OHLCV candlesticks from Binance."""

from __future__ import annotations

from dags import pipeline_tasks

fetch_btcusdt_5m_history = pipeline_tasks.fetch_btcusdt_5m_history


def main() -> None:
  output_path = fetch_btcusdt_5m_history()
  print(f"Saved klines to {output_path}")


if __name__ == "__main__":
  main()
