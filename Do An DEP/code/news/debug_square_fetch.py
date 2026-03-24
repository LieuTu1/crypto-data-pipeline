"""
Script debug: thu html Binance Square, luu ra file va in thong tin time_raw.
"""

from __future__ import annotations

import json
import os
from collections import Counter

from news_bitcoin import (
    OUTPUT_JSON,
    extract_cards,
    extract_dom_titles,
    extract_news,
    fetch_html,
)

RAW_HTML_PATH = os.path.join("news", "raw_square_debug.html")


def main() -> None:
    html = fetch_html()
    os.makedirs(os.path.dirname(RAW_HTML_PATH), exist_ok=True)
    with open(RAW_HTML_PATH, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"Saved raw HTML to {RAW_HTML_PATH}")

    cards = extract_cards(html)
    pairs_json = extract_news(html)
    pairs_dom = extract_dom_titles(html)

    print(f"Cards found: {len(cards)}")
    print(f"Title pairs from JSON: {len(pairs_json)}; from DOM: {len(pairs_dom)}")

    # Thong ke time_raw trong cards
    counter = Counter(card.get("time_raw") for card in cards)
    print("time_raw distribution (cards):")
    for key, count in counter.most_common():
        print(f"  {repr(key)}: {count}")

    # In 5 card bi mat time_raw hoac '--'
    missing = [c for c in cards if not c.get("time_raw") or c.get("time_raw") == "--"]
    print(f"Missing/placeholder time_raw: {len(missing)}")
    for card in missing[:5]:
        print(json.dumps(card, ensure_ascii=False, indent=2))

    # In 5 card co time_raw hop le
    has_time = [c for c in cards if c.get("time_raw") not in (None, "", "--")]
    print(f"Cards with time_raw: {len(has_time)}")
    for card in has_time[:5]:
        print(json.dumps(card, ensure_ascii=False, indent=2))

    # Ghi toan bo cards ra file JSON tam de so sanh
    tmp_cards_path = os.path.join("news", "debug_cards.json")
    with open(tmp_cards_path, "w", encoding="utf-8") as f:
        json.dump(cards, f, ensure_ascii=False, indent=2)
    print(f"Wrote all cards to {tmp_cards_path}")

    # Ghi pairs tu JSON block vao file
    tmp_pairs_path = os.path.join("news", "debug_pairs_json.json")
    with open(tmp_pairs_path, "w", encoding="utf-8") as f:
        json.dump(pairs_json, f, ensure_ascii=False, indent=2)
    print(f"Wrote pairs from JSON blocks to {tmp_pairs_path}")


if __name__ == "__main__":
    main()
