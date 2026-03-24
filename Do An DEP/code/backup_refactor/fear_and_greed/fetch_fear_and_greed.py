"""Fetch CoinMarketCap Fear & Greed historical data and save to JSONL."""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List

import requests

API_URL = "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical"
# Default API key provided by user; override via CMC_API_KEY env var if needed.
DEFAULT_API_KEY = "0501efdbbe614d69a4b21edb0518c43c"


def fetch_history(
    api_key: str, time_start: int | None = None, time_end: int | None = None
) -> List[Dict[str, Any]]:
  """Call CoinMarketCap API and return raw records."""
  headers = {
      "X-CMC_PRO_API_KEY": api_key,
      "Accept": "application/json",
  }
  params: Dict[str, Any] = {}
  if time_start is not None:
    params["time_start"] = int(time_start)
  if time_end is not None:
    params["time_end"] = int(time_end)

  response = requests.get(API_URL, headers=headers, params=params, timeout=30)
  response.raise_for_status()
  payload = response.json()

  status = payload.get("status", {})
  error_code = status.get("error_code")
  if error_code not in (0, "0", None):
    raise RuntimeError(f"API error: {status}")

  data = payload.get("data")
  if not isinstance(data, list):
    raise RuntimeError(f"Unexpected response shape: {payload}")

  return data


def _enrich(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
  """Normalize timestamps and keep useful fields only."""
  enriched: List[Dict[str, Any]] = []
  for item in records:
    ts = int(item["timestamp"])
    dt_utc = datetime.fromtimestamp(ts, tz=timezone.utc)
    enriched.append(
        {
            "timestamp": ts,
            "timestamp_iso": dt_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "value": int(item.get("value", 0)),
            "value_classification": item.get("value_classification", ""),
        }
    )
  return enriched


def save_jsonl(records: Iterable[Dict[str, Any]], output_path: str) -> None:
  """Write records to a JSONL file."""
  os.makedirs(os.path.dirname(output_path), exist_ok=True)
  with open(output_path, "w", encoding="utf-8") as f:
    for row in records:
      json.dump(row, f, ensure_ascii=False)
      f.write("\n")


def main() -> None:
  api_key = os.getenv("CMC_API_KEY", DEFAULT_API_KEY)
  raw = fetch_history(api_key=api_key)
  enriched = _enrich(raw)
  output_path = os.path.join("fear_and_greed", "fear_and_greed_historical.jsonl")
  save_jsonl(enriched, output_path)
  print(f"Saved {len(enriched)} rows to {output_path}")
  if enriched:
    print(f"Most recent record: {enriched[0]}")


if __name__ == "__main__":
  main()
