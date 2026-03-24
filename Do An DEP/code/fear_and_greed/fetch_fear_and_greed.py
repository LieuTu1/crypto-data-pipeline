"""Fetch CoinMarketCap Fear & Greed data into the shared data directory."""

from __future__ import annotations

from dags import pipeline_tasks

fetch_history = pipeline_tasks.fetch_history
enrich_fear_and_greed = pipeline_tasks.enrich_fear_and_greed
fetch_fear_and_greed_history = pipeline_tasks.fetch_fear_and_greed_history


def main() -> None:
  output_path = fetch_fear_and_greed_history()
  print(f"Saved Fear & Greed history to {output_path}")


if __name__ == "__main__":
  main()
