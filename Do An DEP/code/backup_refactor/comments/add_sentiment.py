"""
Tinh diem sentiment cho file JSONL posts & comments va luu ket qua vao thu muc comments/.

Nguon model: distilbert-base-uncased-finetuned-sst-2-english (cached tai models/distilbert-sst2).
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable, Iterator, List

from transformers import (
    AutoModelForSequenceClassification,
    AutoTokenizer,
    pipeline,
)


ROOT = Path(__file__).resolve().parents[1]
INPUT_DIR = ROOT / "comments"
OUTPUT_COMMENTS = INPUT_DIR / "reddit_bitcoin_comments_scored.jsonl"
OUTPUT_POSTS = INPUT_DIR / "reddit_bitcoin_posts_scored.jsonl"

MODEL_ID = "distilbert-base-uncased-finetuned-sst-2-english"
CACHE_DIR = ROOT / "models" / "distilbert-sst2"
BATCH_SIZE = 32


def _read_jsonl(path: Path) -> list[dict]:
  with path.open("r", encoding="utf-8") as f:
    return [json.loads(line) for line in f if line.strip()]


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
  with path.open("w", encoding="utf-8") as f:
    for row in rows:
      f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _chunk(seq: List[str], size: int) -> Iterator[list[str]]:
  for i in range(0, len(seq), size):
    yield seq[i : i + size]


def _add_sentiment(rows: list[dict], text_field: str, clf) -> list[dict]:
  texts = [row.get(text_field, "") or "" for row in rows]
  results: list[dict | None] = [None] * len(rows)

  # Xu ly theo batch de tiet kiem bo nho.
  start = 0
  while start < len(texts):
    batch_texts = texts[start : start + BATCH_SIZE]
    # Map chi so trong batch -> chi so trong danh sach goc (de giu thu tu).
    indices = list(range(start, min(start + BATCH_SIZE, len(texts))))

    # Tach nhung text rong -> bo qua scoring.
    non_empty_pairs = [(i, t) for i, t in zip(indices, batch_texts) if t.strip()]
    if non_empty_pairs:
      batch_indices, batch_inputs = zip(*non_empty_pairs)
      preds = clf(list(batch_inputs), batch_size=len(batch_inputs))
      for idx, pred in zip(batch_indices, preds):
        results[idx] = {
            "sentiment_label": pred["label"],
            "sentiment_score": float(pred["score"]),
        }

    # Gan None cho text rong.
    for idx, text in zip(indices, batch_texts):
      if not text.strip():
        results[idx] = {"sentiment_label": None, "sentiment_score": None}

    start += BATCH_SIZE

  # Tron ket qua vao row goc.
  enriched: list[dict] = []
  for row, pred in zip(rows, results):
    out = dict(row)
    out.update(pred or {"sentiment_label": None, "sentiment_score": None})
    enriched.append(out)
  return enriched


def main() -> None:
  comments_path = INPUT_DIR / "reddit_bitcoin_comments.jsonl"
  posts_path = INPUT_DIR / "reddit_bitcoin_posts.jsonl"
  if not comments_path.exists() or not posts_path.exists():
    raise FileNotFoundError("Khong tim thay file JSONL dau vao trong thu muc comments/")

  CACHE_DIR.mkdir(parents=True, exist_ok=True)
  tokenizer = AutoTokenizer.from_pretrained(MODEL_ID, cache_dir=CACHE_DIR)
  model = AutoModelForSequenceClassification.from_pretrained(
      MODEL_ID, cache_dir=CACHE_DIR
  )
  clf = pipeline(
      task="sentiment-analysis",
      model=model,
      tokenizer=tokenizer,
      device="cpu",
  )

  comments_rows = _read_jsonl(comments_path)
  posts_rows = _read_jsonl(posts_path)

  comments_scored = _add_sentiment(comments_rows, text_field="body", clf=clf)
  posts_scored = _add_sentiment(posts_rows, text_field="title", clf=clf)

  _write_jsonl(OUTPUT_COMMENTS, comments_scored)
  _write_jsonl(OUTPUT_POSTS, posts_scored)

  print(
      f"Da ghi {len(comments_scored)} dong co diem sentiment -> {OUTPUT_COMMENTS}"
  )
  print(f"Da ghi {len(posts_scored)} dong co diem sentiment -> {OUTPUT_POSTS}")


if __name__ == "__main__":
  main()
