"""Fetch posts and comments from r/Bitcoin and save to JSONL files in comments/."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Iterable
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


USER_AGENT = "codex-cli/1.0 (contact: example@example.com)"
POST_LIMIT = 30  # number of posts to pull from /hot
COMMENT_LIMIT = 100  # max comments per post (Reddit may trim)
OUTPUT_DIR = Path("comments")
POSTS_FILE = OUTPUT_DIR / "reddit_bitcoin_posts.jsonl"
COMMENTS_FILE = OUTPUT_DIR / "reddit_bitcoin_comments.jsonl"


def _get_json(url: str) -> dict:
  request = Request(url, headers={"User-Agent": USER_AGENT})
  try:
    with urlopen(request, timeout=15) as resp:
      return json.loads(resp.read())
  except (HTTPError, URLError) as exc:
    raise RuntimeError(f"Request failed for {url}: {exc}") from exc


def fetch_posts(limit: int = POST_LIMIT) -> list[dict]:
  url = f"https://www.reddit.com/r/Bitcoin/hot.json?limit={limit}"
  data = _get_json(url)
  children = data.get("data", {}).get("children", [])
  posts = []
  for child in children:
    d = child.get("data", {})
    posts.append(
        {
            "id": d.get("id"),
            "title": d.get("title"),
            "author": d.get("author"),
            "created_utc": d.get("created_utc"),
            "score": d.get("score"),
            "num_comments": d.get("num_comments"),
            "url": d.get("url"),
            "permalink": d.get("permalink"),
            "subreddit": d.get("subreddit"),
        }
    )
  return posts


def _flatten_comments(listing: dict, post_id: str) -> Iterable[dict]:
  """Yield a flat stream of comments for a listing payload."""
  stack = listing.get("data", {}).get("children", [])
  while stack:
    child = stack.pop()
    kind = child.get("kind")
    data = child.get("data", {})
    if kind != "t1":  # only comment objects
      continue
    replies = data.get("replies")
    if isinstance(replies, dict):
      stack.extend(replies.get("data", {}).get("children", []))
    yield {
        "post_id": post_id,
        "comment_id": data.get("id"),
        "author": data.get("author"),
        "body": data.get("body"),
        "created_utc": data.get("created_utc"),
        "score": data.get("score"),
        "parent_id": data.get("parent_id"),
        "permalink": data.get("permalink"),
    }


def fetch_comments_for_post(permalink: str, post_id: str) -> list[dict]:
  url = f"https://www.reddit.com{permalink}.json?limit={COMMENT_LIMIT}"
  payload = _get_json(url)
  if not isinstance(payload, list) or len(payload) < 2:
    return []
  comment_listing = payload[1]
  return list(_flatten_comments(comment_listing, post_id))


def write_jsonl(path: Path, rows: Iterable[dict]) -> None:
  with path.open("w", encoding="utf-8") as f:
    for row in rows:
      f.write(json.dumps(row, ensure_ascii=False) + "\n")


def main() -> None:
  OUTPUT_DIR.mkdir(exist_ok=True)
  posts = fetch_posts()
  write_jsonl(POSTS_FILE, posts)
  print(f"Wrote {len(posts)} posts to {POSTS_FILE}")

  all_comments: list[dict] = []
  for post in posts:
    permalink = post.get("permalink")
    post_id = post.get("id")
    if not permalink or not post_id:
      continue
    comments = fetch_comments_for_post(permalink, post_id)
    all_comments.extend(comments)
    time.sleep(0.5)  # be gentle with Reddit
  write_jsonl(COMMENTS_FILE, all_comments)
  print(f"Wrote {len(all_comments)} comments to {COMMENTS_FILE}")


if __name__ == "__main__":
  main()
