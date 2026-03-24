"""Tinh diem sentiment cho post/comment da duoc tai ve trong thu muc data/comments."""

from __future__ import annotations

from dags import pipeline_tasks

score_reddit_sentiment = pipeline_tasks.score_reddit_sentiment


def main() -> None:
  outputs = score_reddit_sentiment()
  print(f"Da ghi sentiment comments -> {outputs['comments']}")
  print(f"Da ghi sentiment posts -> {outputs['posts']}")


if __name__ == "__main__":
  main()
