"""Fetch posts and comments from r/Bitcoin into the shared data directory."""

from __future__ import annotations

from dags import pipeline_tasks

fetch_reddit_bitcoin = pipeline_tasks.fetch_reddit_bitcoin
fetch_posts = pipeline_tasks.fetch_posts
fetch_comments_for_post = pipeline_tasks.fetch_comments_for_post


def main() -> None:
  outputs = fetch_reddit_bitcoin()
  print(f"Wrote posts to {outputs['posts']}")
  print(f"Wrote comments to {outputs['comments']}")


if __name__ == "__main__":
  main()
