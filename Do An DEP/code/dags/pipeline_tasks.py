"""Reusable data tasks for Airflow DAGs and local scripts."""

from __future__ import annotations

import json
import os
import time
from http.client import RemoteDisconnected
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, Iterator, List
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

import pandas as pd
import requests


DEFAULT_API_KEY = "0501efdbbe614d69a4b21edb0518c43c"
USER_AGENT = "codex-cli/1.0 (contact: example@example.com)"
POST_LIMIT = 30
COMMENT_LIMIT = 100
BINANCE_API = "https://api.binance.com/api/v3/klines"
INTERVAL_5M_MS = 5 * 60 * 1000
MAX_LIMIT = 1000


def _symbol_lower(symbol: str) -> str:
  return symbol.lower()


def get_data_dir() -> Path:
  """Return the base data directory, creating it if needed."""
  configured = os.getenv("DATA_DIR")
  if configured:
    base = Path(configured)
    if not base.is_absolute():
      base = (Path(__file__).resolve().parents[1] / configured).resolve()
  else:
    base = Path(__file__).resolve().parents[1] / "data"
  base.mkdir(parents=True, exist_ok=True)
  return base


# ----------------------- Fear & Greed ----------------------- #
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

  response = requests.get(
      "https://pro-api.coinmarketcap.com/v3/fear-and-greed/historical",
      headers=headers,
      params=params,
      timeout=30,
  )
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


def enrich_fear_and_greed(records: Iterable[Dict[str, Any]]) -> List[Dict[str, Any]]:
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


def _save_jsonl(path: Path, rows: Iterable[Dict[str, Any]]) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("w", encoding="utf-8") as f:
    for row in rows:
      json.dump(row, f, ensure_ascii=False)
      f.write("\n")


def fetch_fear_and_greed_history(
    api_key: str | None = None, time_start: int | None = None, time_end: int | None = None
) -> Path:
  """Fetch Fear & Greed data and save to the shared data directory."""
  api_key = api_key or os.getenv("CMC_API_KEY", DEFAULT_API_KEY)
  raw = fetch_history(api_key=api_key, time_start=time_start, time_end=time_end)
  enriched = enrich_fear_and_greed(raw)
  output_path = get_data_dir() / "fear_and_greed" / "fear_and_greed_historical.jsonl"
  _save_jsonl(output_path, enriched)
  return str(output_path)


def load_fear_and_greed_into_mysql(mysql_conn_id: str = "mysql_default") -> int:
  """
  Load Fear & Greed JSONL into MySQL (table: fear_and_greed).

  Returns the number of rows written.
  """
  data_path = get_data_dir() / "fear_and_greed" / "fear_and_greed_historical.jsonl"
  if not data_path.exists():
    raise FileNotFoundError(
        f"Missing {data_path}. Run fetch_fear_and_greed_history first."
    )

  try:
    from airflow.hooks.mysql_hook import MySqlHook  # Imported here to avoid hard dependency when unused.
  except ImportError as exc:
    raise ImportError(
        "apache-airflow-providers-mysql not installed; install it in the image to load MySQL."
    ) from exc

  with data_path.open("r", encoding="utf-8") as f:
    rows = [json.loads(line) for line in f if line.strip()]

  if not rows:
    return 0

  hook = MySqlHook(mysql_conn_id=mysql_conn_id)
  hook.run(
      """
      CREATE TABLE IF NOT EXISTS fear_and_greed (
        timestamp BIGINT PRIMARY KEY,
        timestamp_iso VARCHAR(32),
        value INT,
        value_classification VARCHAR(32)
      )
      """
  )
  prepared_rows = [
      (
          int(row.get("timestamp", 0)),
          row.get("timestamp_iso", ""),
          int(row.get("value", 0)),
          row.get("value_classification", ""),
      )
      for row in rows
  ]
  hook.insert_rows(
      table="fear_and_greed",
      rows=prepared_rows,
      target_fields=[
          "timestamp",
          "timestamp_iso",
          "value",
          "value_classification",
      ],
      replace=True,
  )
  return len(prepared_rows)


# ----------------------- Reddit ----------------------- #
def _get_json(url: str, retries: int = 6, backoff: float = 5.0) -> dict:
  """GET JSON with retry/backoff for rate limits (429) and transient errors."""
  request = Request(url, headers={"User-Agent": USER_AGENT})
  attempt = 0
  while True:
    try:
      with urlopen(request, timeout=20) as resp:
        return json.loads(resp.read())
    except HTTPError as exc:
      attempt += 1
      if exc.code == 429 and attempt < retries:
        time.sleep(backoff)
        backoff *= 2
        continue
      raise RuntimeError(f"Request failed for {url}: {exc}") from exc
    except (URLError, RemoteDisconnected) as exc:
      attempt += 1
      if attempt < retries:
        time.sleep(backoff)
        backoff *= 2
        continue
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


def _flatten_comments(listing: dict, post_id: str) -> Iterator[dict]:
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


def fetch_comments_for_post(permalink: str, post_id: str, comment_limit: int) -> list[dict]:
  url = f"https://www.reddit.com{permalink}.json?limit={comment_limit}"
  payload = _get_json(url)
  if not isinstance(payload, list) or len(payload) < 2:
    return []
  comment_listing = payload[1]
  return list(_flatten_comments(comment_listing, post_id))


def fetch_reddit_bitcoin(
    post_limit: int = POST_LIMIT, comment_limit: int = COMMENT_LIMIT
) -> dict[str, Path]:
  """Fetch Reddit posts/comments and save to the shared data directory."""
  output_dir = get_data_dir() / "comments"
  output_dir.mkdir(parents=True, exist_ok=True)
  posts_file = output_dir / "reddit_bitcoin_posts.jsonl"
  comments_file = output_dir / "reddit_bitcoin_comments.jsonl"

  posts = fetch_posts(limit=post_limit)
  with posts_file.open("w", encoding="utf-8") as f:
    for row in posts:
      f.write(json.dumps(row, ensure_ascii=False) + "\n")

  all_comments: list[dict] = []
  for post in posts:
    permalink = post.get("permalink")
    post_id = post.get("id")
    if not permalink or not post_id:
      continue
    comments = fetch_comments_for_post(
        permalink=permalink, post_id=post_id, comment_limit=comment_limit
    )
    all_comments.extend(comments)
    time.sleep(2.0)  # throttle to avoid 429

  with comments_file.open("w", encoding="utf-8") as f:
    for row in all_comments:
      f.write(json.dumps(row, ensure_ascii=False) + "\n")

  return {"posts": str(posts_file), "comments": str(comments_file)}


def _read_jsonl(path: Path) -> list[dict]:
  with path.open("r", encoding="utf-8") as f:
    return [json.loads(line) for line in f if line.strip()]


def _write_jsonl(path: Path, rows: Iterable[dict]) -> None:
  path.parent.mkdir(parents=True, exist_ok=True)
  with path.open("w", encoding="utf-8") as f:
    for row in rows:
      f.write(json.dumps(row, ensure_ascii=False) + "\n")


def _chunk(seq: List[str], size: int) -> Iterator[list[str]]:
  for i in range(0, len(seq), size):
    yield seq[i : i + size]


def _add_sentiment(rows: list[dict], text_field: str, clf) -> list[dict]:
  texts = [row.get(text_field, "") or "" for row in rows]
  results: list[dict | None] = [None] * len(rows)

  start = 0
  while start < len(texts):
    batch_texts = texts[start : start + 32]
    indices = list(range(start, min(start + 32, len(texts))))

    non_empty_pairs = [(i, t) for i, t in zip(indices, batch_texts) if t.strip()]
    if non_empty_pairs:
      batch_indices, batch_inputs = zip(*non_empty_pairs)
      preds = clf(
          list(batch_inputs),
          batch_size=len(batch_inputs),
          truncation=True,
          max_length=512,
      )
      for idx, pred in zip(batch_indices, preds):
        results[idx] = {
            "sentiment_label": pred["label"],
            "sentiment_score": float(pred["score"]),
        }

    for idx, text in zip(indices, batch_texts):
      if not text.strip():
        results[idx] = {"sentiment_label": None, "sentiment_score": None}

    start += 32

  enriched: list[dict] = []
  for row, pred in zip(rows, results):
    out = dict(row)
    out.update(pred or {"sentiment_label": None, "sentiment_score": None})
    enriched.append(out)
  return enriched


def score_reddit_sentiment() -> dict[str, Path]:
  """Add sentiment scores to Reddit posts/comments stored in the data directory."""
  base_dir = get_data_dir() / "comments"
  comments_path = base_dir / "reddit_bitcoin_comments.jsonl"
  posts_path = base_dir / "reddit_bitcoin_posts.jsonl"
  if not comments_path.exists() or not posts_path.exists():
    raise FileNotFoundError(
        f"Missing input files in {base_dir}. Run fetch_reddit_bitcoin first."
    )

  try:
    from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
  except ImportError as exc:
    raise ImportError(
        "transformers not installed; ensure the image is built with ML dependencies."
    ) from exc

  cache_dir = get_data_dir() / "models" / "distilbert-sst2"
  cache_dir.mkdir(parents=True, exist_ok=True)
  tokenizer = AutoTokenizer.from_pretrained(
      "distilbert-base-uncased-finetuned-sst-2-english", cache_dir=cache_dir
  )
  model = AutoModelForSequenceClassification.from_pretrained(
      "distilbert-base-uncased-finetuned-sst-2-english", cache_dir=cache_dir
  )
  clf = pipeline(task="sentiment-analysis", model=model, tokenizer=tokenizer, device="cpu")

  comments_rows = _read_jsonl(comments_path)
  posts_rows = _read_jsonl(posts_path)

  comments_scored = _add_sentiment(comments_rows, text_field="body", clf=clf)
  posts_scored = _add_sentiment(posts_rows, text_field="title", clf=clf)

  output_comments = base_dir / "reddit_bitcoin_comments_scored.jsonl"
  output_posts = base_dir / "reddit_bitcoin_posts_scored.jsonl"

  _write_jsonl(output_comments, comments_scored)
  _write_jsonl(output_posts, posts_scored)

  return {"comments": str(output_comments), "posts": str(output_posts)}


# ----------------------- Binance Klines ----------------------- #
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


def _parse_klines(rows: Iterable[List]) -> List[Dict[str, Any]]:
  """Convert raw rows into dictionaries."""
  parsed: List[Dict[str, Any]] = []
  for row in rows:
    if len(row) < 6:
      raise RuntimeError(f"Incomplete kline row: {row}")
    open_time, open_, high, low, close, volume = row[:6]
    parsed.append(
        {
            "timestamp": int(open_time),
            "open": float(open_),
            "high": float(high),
            "low": float(low),
            "close": float(close),
            "volume": float(volume),
        }
    )
  return parsed


def _kline_paths(symbol: str) -> tuple[Path, Path]:
  """Return input/output paths for a symbol."""
  base_dir = get_data_dir() / "klines"
  symbol_lower = symbol.lower()
  raw_path = base_dir / f"{symbol_lower}_5m.jsonl"
  ind_path = base_dir / f"{symbol_lower}_5m_indicators.jsonl"
  return raw_path, ind_path


def fetch_symbol_5m_history(symbol: str, total: int = 10_000) -> str:
  """Collect 5m klines for a symbol and store them in the data directory."""
  rows: list[list] = []
  lookback_ms = total * INTERVAL_5M_MS + INTERVAL_5M_MS
  start_time = int(time.time() * 1000) - lookback_ms

  while len(rows) < total:
    batch = _fetch_raw_klines(
        symbol=symbol,
        interval="5m",
        limit=MAX_LIMIT,
        start_time=start_time,
    )
    if not batch:
      break
    rows.extend(batch)
    start_time = int(batch[-1][0]) + INTERVAL_5M_MS
    if len(batch) < MAX_LIMIT:
      break

  parsed = _parse_klines(rows[:total])
  df = pd.DataFrame(parsed)
  df["timestamp"] = (
      pd.to_datetime(df["timestamp"], unit="ms", utc=True)
      .dt.tz_convert("Asia/Ho_Chi_Minh")
      .dt.strftime("%Y-%m-%d %H:%M:%S")
  )

  output_path, _ = _kline_paths(symbol)
  output_path.parent.mkdir(parents=True, exist_ok=True)
  df.to_json(output_path, orient="records", lines=True, force_ascii=False)
  return str(output_path)


def fetch_btcusdt_5m_history(total: int = 10_000) -> str:
  """Backward-compatible wrapper for BTCUSDT."""
  return fetch_symbol_5m_history(symbol="BTCUSDT", total=total)


def compute_kline_indicators(
    symbol: str = "BTCUSDT", input_path: Path | None = None, output_path: Path | None = None
) -> str:
  """
  Compute SMA20, SMA50, RSI14, Bollinger Bands for klines JSONL.

  Defaults to data/klines/<symbol>_5m.jsonl -> data/klines/<symbol>_5m_indicators.jsonl.
  """
  default_raw, default_ind = _kline_paths(symbol)
  src = input_path or default_raw
  dst = output_path or default_ind

  if not Path(src).exists():
    raise FileNotFoundError(f"Missing input klines file: {src}")

  df = pd.read_json(src, lines=True, convert_dates=False)
  closes = df["close"]

  enriched = df.copy()
  enriched["sma20"] = closes.rolling(window=20, min_periods=20).mean()
  enriched["sma50"] = closes.rolling(window=50, min_periods=50).mean()

  delta = closes.diff()
  gain = delta.clip(lower=0)
  loss = -delta.clip(upper=0)
  avg_gain = gain.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
  avg_loss = loss.ewm(alpha=1 / 14, adjust=False, min_periods=14).mean()
  rs = avg_gain / avg_loss
  enriched["rsi14"] = 100 - (100 / (1 + rs))

  rolling20 = closes.rolling(window=20, min_periods=20)
  middle = rolling20.mean()
  std = rolling20.std(ddof=0)
  enriched["bb_middle"] = middle
  enriched["bb_upper"] = middle + 2 * std
  enriched["bb_lower"] = middle - 2 * std

  # Loại bỏ các dòng chưa đủ dữ liệu để tránh chỉ báo null.
  enriched = enriched.dropna(
      subset=["sma20", "sma50", "rsi14", "bb_middle", "bb_upper", "bb_lower"]
  )

  dst.parent.mkdir(parents=True, exist_ok=True)
  enriched.to_json(dst, orient="records", lines=True, force_ascii=False)
  return str(dst)


# ----------------------- MySQL loaders for other datasets ----------------------- #
def _get_mysql_hook(mysql_conn_id: str = "mysql_default"):
  try:
    from airflow.hooks.mysql_hook import MySqlHook
  except ImportError as exc:
    raise ImportError(
        "apache-airflow-providers-mysql not installed; install it in the image to load MySQL."
    ) from exc
  return MySqlHook(mysql_conn_id=mysql_conn_id)


def _ensure_column_exists(hook, table: str, column: str, ddl: str) -> None:
  exists = hook.get_first(
      """
      SELECT COUNT(*)
      FROM information_schema.columns
      WHERE table_schema = DATABASE()
        AND table_name = %s
        AND column_name = %s
      """,
      parameters=(table, column),
  )
  if not exists or not exists[0]:
    hook.run(f"ALTER TABLE {table} ADD COLUMN {ddl}")


def load_reddit_posts(mysql_conn_id: str = "mysql_default") -> int:
  base_dir = get_data_dir() / "comments"
  path = base_dir / "reddit_bitcoin_posts.jsonl"
  rows = _read_jsonl(path)
  if not rows:
    return 0

  hook = _get_mysql_hook(mysql_conn_id)
  hook.run(
      """
      CREATE TABLE IF NOT EXISTS reddit_posts (
        id VARCHAR(32) PRIMARY KEY,
        title TEXT,
        author VARCHAR(255),
        created_utc BIGINT,
        score INT,
        num_comments INT,
        url TEXT,
        permalink TEXT,
        subreddit VARCHAR(255)
      )
      """
  )
  prepared = [
      (
          row.get("id"),
          row.get("title"),
          row.get("author"),
          row.get("created_utc"),
          row.get("score"),
          row.get("num_comments"),
          row.get("url"),
          row.get("permalink"),
          row.get("subreddit"),
      )
      for row in rows
  ]
  hook.insert_rows(
      table="reddit_posts",
      rows=prepared,
      target_fields=[
          "id",
          "title",
          "author",
          "created_utc",
          "score",
          "num_comments",
          "url",
          "permalink",
          "subreddit",
      ],
      replace=True,
  )
  return len(prepared)


def load_reddit_comments(mysql_conn_id: str = "mysql_default") -> int:
  base_dir = get_data_dir() / "comments"
  path = base_dir / "reddit_bitcoin_comments.jsonl"
  rows = _read_jsonl(path)
  if not rows:
    return 0

  hook = _get_mysql_hook(mysql_conn_id)
  hook.run(
      """
      CREATE TABLE IF NOT EXISTS reddit_comments (
        comment_id VARCHAR(64) PRIMARY KEY,
        post_id VARCHAR(32),
        author VARCHAR(255),
        body TEXT,
        created_utc BIGINT,
        score INT,
        parent_id VARCHAR(64),
        permalink TEXT
      )
      """
  )
  prepared = [
      (
          row.get("comment_id"),
          row.get("post_id"),
          row.get("author"),
          row.get("body"),
          row.get("created_utc"),
          row.get("score"),
          row.get("parent_id"),
          row.get("permalink"),
      )
      for row in rows
  ]
  hook.insert_rows(
      table="reddit_comments",
      rows=prepared,
      target_fields=[
          "comment_id",
          "post_id",
          "author",
          "body",
          "created_utc",
          "score",
          "parent_id",
          "permalink",
      ],
      replace=True,
  )
  return len(prepared)


def load_reddit_posts_scored(mysql_conn_id: str = "mysql_default") -> int:
  base_dir = get_data_dir() / "comments"
  path = base_dir / "reddit_bitcoin_posts_scored.jsonl"
  rows = _read_jsonl(path)
  if not rows:
    return 0

  hook = _get_mysql_hook(mysql_conn_id)
  hook.run(
      """
      CREATE TABLE IF NOT EXISTS reddit_posts_scored (
        id VARCHAR(32) PRIMARY KEY,
        title TEXT,
        author VARCHAR(255),
        created_utc BIGINT,
        score INT,
        num_comments INT,
        url TEXT,
        permalink TEXT,
        subreddit VARCHAR(255),
        sentiment_label VARCHAR(32),
        sentiment_score DOUBLE
      )
      """
  )
  prepared = [
      (
          row.get("id"),
          row.get("title"),
          row.get("author"),
          row.get("created_utc"),
          row.get("score"),
          row.get("num_comments"),
          row.get("url"),
          row.get("permalink"),
          row.get("subreddit"),
          row.get("sentiment_label"),
          row.get("sentiment_score"),
      )
      for row in rows
  ]
  hook.insert_rows(
      table="reddit_posts_scored",
      rows=prepared,
      target_fields=[
          "id",
          "title",
          "author",
          "created_utc",
          "score",
          "num_comments",
          "url",
          "permalink",
          "subreddit",
          "sentiment_label",
          "sentiment_score",
      ],
      replace=True,
  )
  return len(prepared)


def load_reddit_comments_scored(mysql_conn_id: str = "mysql_default") -> int:
  base_dir = get_data_dir() / "comments"
  path = base_dir / "reddit_bitcoin_comments_scored.jsonl"
  rows = _read_jsonl(path)
  if not rows:
    return 0

  hook = _get_mysql_hook(mysql_conn_id)
  hook.run(
      """
      CREATE TABLE IF NOT EXISTS reddit_comments_scored (
        comment_id VARCHAR(64) PRIMARY KEY,
        post_id VARCHAR(32),
        author VARCHAR(255),
        body TEXT,
        created_utc BIGINT,
        score INT,
        parent_id VARCHAR(64),
        permalink TEXT,
        sentiment_label VARCHAR(32),
        sentiment_score DOUBLE
      )
      """
  )
  prepared = [
      (
          row.get("comment_id"),
          row.get("post_id"),
          row.get("author"),
          row.get("body"),
          row.get("created_utc"),
          row.get("score"),
          row.get("parent_id"),
          row.get("permalink"),
          row.get("sentiment_label"),
          row.get("sentiment_score"),
      )
      for row in rows
  ]
  hook.insert_rows(
      table="reddit_comments_scored",
      rows=prepared,
      target_fields=[
          "comment_id",
          "post_id",
          "author",
          "body",
          "created_utc",
          "score",
          "parent_id",
          "permalink",
          "sentiment_label",
          "sentiment_score",
      ],
      replace=True,
  )
  return len(prepared)


def load_klines(mysql_conn_id: str = "mysql_default") -> int:
  return load_klines_for_symbol(symbol="BTCUSDT", mysql_conn_id=mysql_conn_id)


def load_klines_for_symbol(symbol: str, mysql_conn_id: str = "mysql_default") -> int:
  """Load raw klines for a symbol into a symbol-specific staging table."""
  raw_path, _ = _kline_paths(symbol)
  rows = _read_jsonl(raw_path)
  if not rows:
    return 0

  hook = _get_mysql_hook(mysql_conn_id)
  symbol_lower = _symbol_lower(symbol)
  table = f"klines_{symbol_lower}_5m"
  try:
    hook.run(f"DROP TABLE IF EXISTS {table}")
  except Exception:
    pass
  hook.run(
      f"""
      CREATE TABLE IF NOT EXISTS {table} (
        symbol_code VARCHAR(16),
        ts_local VARCHAR(32),
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        PRIMARY KEY (symbol_code, ts_local)
      )
      """
  )
  _ensure_column_exists(hook, table, "symbol_code", "symbol_code VARCHAR(16)")
  _ensure_column_exists(hook, table, "ts_local", "ts_local VARCHAR(32)")
  _ensure_column_exists(hook, table, "symbol_code", "symbol_code VARCHAR(16)")
  _ensure_column_exists(hook, table, "ts_local", "ts_local VARCHAR(32)")
  prepared = [
      (
          symbol,
          row.get("timestamp"),
          row.get("open"),
          row.get("high"),
          row.get("low"),
          row.get("close"),
          row.get("volume"),
      )
      for row in rows
  ]
  hook.insert_rows(
      table=table,
      rows=prepared,
      target_fields=["symbol_code", "ts_local", "open", "high", "low", "close", "volume"],
      replace=True,
  )
  return len(prepared)


def load_kline_indicators(mysql_conn_id: str = "mysql_default") -> int:
  return load_kline_indicators_for_symbol(symbol="BTCUSDT", mysql_conn_id=mysql_conn_id)


def load_kline_indicators_for_symbol(symbol: str, mysql_conn_id: str = "mysql_default") -> int:
  """Load indicator klines for a symbol into a symbol-specific staging table."""
  _, ind_path = _kline_paths(symbol)
  rows = _read_jsonl(ind_path)
  if not rows:
    return 0

  hook = _get_mysql_hook(mysql_conn_id)
  symbol_lower = _symbol_lower(symbol)
  table = f"klines_{symbol_lower}_5m_indicators"
  try:
    hook.run(f"DROP TABLE IF EXISTS {table}")
  except Exception:
    pass
  hook.run(
      f"""
      CREATE TABLE IF NOT EXISTS {table} (
        symbol_code VARCHAR(16),
        ts_local VARCHAR(32),
        open DOUBLE,
        high DOUBLE,
        low DOUBLE,
        close DOUBLE,
        volume DOUBLE,
        sma20 DOUBLE,
        sma50 DOUBLE,
        rsi14 DOUBLE,
        bb_middle DOUBLE,
        bb_upper DOUBLE,
        bb_lower DOUBLE,
        PRIMARY KEY (symbol_code, ts_local)
      )
      """
  )
  # Đảm bảo các cột tồn tại trong trường hợp bảng cũ chưa đúng schema.
  _ensure_column_exists(hook, table, "symbol_code", "symbol_code VARCHAR(16)")
  _ensure_column_exists(hook, table, "ts_local", "ts_local VARCHAR(32)")
  _ensure_column_exists(hook, table, "open", "open DOUBLE")
  _ensure_column_exists(hook, table, "high", "high DOUBLE")
  _ensure_column_exists(hook, table, "low", "low DOUBLE")
  _ensure_column_exists(hook, table, "close", "close DOUBLE")
  _ensure_column_exists(hook, table, "volume", "volume DOUBLE")
  _ensure_column_exists(hook, table, "sma20", "sma20 DOUBLE")
  _ensure_column_exists(hook, table, "sma50", "sma50 DOUBLE")
  _ensure_column_exists(hook, table, "rsi14", "rsi14 DOUBLE")
  _ensure_column_exists(hook, table, "bb_middle", "bb_middle DOUBLE")
  _ensure_column_exists(hook, table, "bb_upper", "bb_upper DOUBLE")
  _ensure_column_exists(hook, table, "bb_lower", "bb_lower DOUBLE")
  prepared = [
      (
          symbol,
          row.get("timestamp"),
          row.get("open"),
          row.get("high"),
          row.get("low"),
          row.get("close"),
          row.get("volume"),
          row.get("sma20"),
          row.get("sma50"),
          row.get("rsi14"),
          row.get("bb_middle"),
          row.get("bb_upper"),
          row.get("bb_lower"),
      )
      for row in rows
  ]
  hook.insert_rows(
      table=table,
      rows=prepared,
      target_fields=[
          "symbol_code",
          "ts_local",
          "open",
          "high",
          "low",
          "close",
          "volume",
          "sma20",
          "sma50",
          "rsi14",
          "bb_middle",
          "bb_upper",
          "bb_lower",
      ],
      replace=True,
  )
  return len(prepared)


# ----------------------- Dimensional model loaders ----------------------- #
def _ensure_symbol(hook, symbol_code: str, base_asset: str, quote_asset: str) -> int:
  hook.run(
      """
      CREATE TABLE IF NOT EXISTS dim_symbol (
        symbol_id INT AUTO_INCREMENT PRIMARY KEY,
        symbol_code VARCHAR(20) UNIQUE,
        base_asset VARCHAR(10),
        quote_asset VARCHAR(10)
      )
      """
  )
  hook.run(
      """
      INSERT INTO dim_symbol (symbol_code, base_asset, quote_asset)
      VALUES (%s, %s, %s)
      ON DUPLICATE KEY UPDATE base_asset=VALUES(base_asset), quote_asset=VALUES(quote_asset)
      """,
      parameters=(symbol_code, base_asset, quote_asset),
  )
  row = hook.get_first(
      "SELECT symbol_id FROM dim_symbol WHERE symbol_code=%s", parameters=(symbol_code,)
  )
  return int(row[0])


def _ensure_interval(hook, interval_code: str, interval_minutes: int) -> int:
  hook.run(
      """
      CREATE TABLE IF NOT EXISTS dim_interval (
        interval_id INT AUTO_INCREMENT PRIMARY KEY,
        interval_code VARCHAR(10) UNIQUE,
        interval_minutes INT
      )
      """
  )
  hook.run(
      """
      INSERT INTO dim_interval (interval_code, interval_minutes)
      VALUES (%s, %s)
      ON DUPLICATE KEY UPDATE interval_minutes=VALUES(interval_minutes)
      """,
      parameters=(interval_code, interval_minutes),
  )
  row = hook.get_first(
      "SELECT interval_id FROM dim_interval WHERE interval_code=%s",
      parameters=(interval_code,),
  )
  return int(row[0])


def etl_dim_time_from_klines(symbol: str = "BTCUSDT", mysql_conn_id: str = "mysql_default") -> int:
  """Load dim_time from klines timestamps of a given symbol."""
  hook = _get_mysql_hook(mysql_conn_id)
  raw_path, _ = _kline_paths(symbol)
  rows = _read_jsonl(raw_path)
  if not rows:
    return 0

  hook.run(
      """
      CREATE TABLE IF NOT EXISTS dim_time (
        time_id BIGINT PRIMARY KEY,
        timestamp DATETIME,
        date DATE,
        hour INT,
        minute INT
      )
      """
  )

  # Ensure desired columns and drop legacy minute column if present.
  def _column_exists(name: str) -> bool:
    row = hook.get_first(
        """
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = DATABASE()
          AND table_name = 'dim_time'
          AND column_name = %s
        """,
        parameters=(name,),
    )
    return bool(row and row[0])

  for col_name, ddl in [
      ("hour", "hour INT"),
      ("date", "date DATE"),
      ("timestamp", "timestamp DATETIME"),
      ("minute", "minute INT"),
  ]:
    if not _column_exists(col_name):
      hook.run(f"ALTER TABLE dim_time ADD COLUMN {ddl}")

  # Drop wrongly named column if it exists.
  if _column_exists("minunte"):
    try:
      hook.run("ALTER TABLE dim_time DROP COLUMN minunte")
    except Exception:
      pass

  # Ensure time_id has sufficient width (older schema may be INT).
  try:
    hook.run("ALTER TABLE dim_time MODIFY COLUMN time_id BIGINT")
  except Exception:
    pass

  seen = set()
  prepared = []
  for r in rows:
    dt = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
    time_id = int(dt.strftime("%Y%m%d%H%M"))
    if time_id in seen:
      continue
    seen.add(time_id)
    prepared.append((time_id, dt, dt.date(), dt.hour, dt.minute))

  if prepared:
    hook.insert_rows(
        table="dim_time",
        rows=prepared,
        target_fields=["time_id", "timestamp", "date", "hour", "minute"],
        replace=True,
    )
  return len(prepared)


def etl_fact_ohlv(
    symbol_code: str = "BTCUSDT",
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
    mysql_conn_id: str = "mysql_default",
) -> int:
  """Load OHLCV fact from klines JSONL into fact_ohlv for a symbol."""
  hook = _get_mysql_hook(mysql_conn_id)
  raw_path, _ = _kline_paths(symbol_code)
  rows = _read_jsonl(raw_path)
  if not rows:
    return 0

  symbol_id = _ensure_symbol(hook, symbol_code, base_asset, quote_asset)
  interval_id = _ensure_interval(hook, "5m", 5)
  _ = etl_dim_time_from_klines(symbol=symbol_code, mysql_conn_id=mysql_conn_id)

  hook.run(
      """
      CREATE TABLE IF NOT EXISTS fact_ohlv (
        ohlv_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol_id INT,
        interval_id INT,
        time_id BIGINT,
        open DECIMAL(18,8),
        high DECIMAL(18,8),
        low DECIMAL(18,8),
        close DECIMAL(18,8),
        volume DECIMAL(28,8),
        UNIQUE(symbol_id, interval_id, time_id)
      )
      """
  )

  prepared = []
  for r in rows:
    dt = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
    time_id = int(dt.strftime("%Y%m%d%H%M"))
    prepared.append(
        (
            symbol_id,
            interval_id,
            time_id,
            float(r["open"]),
            float(r["high"]),
            float(r["low"]),
            float(r["close"]),
            float(r["volume"]),
        )
    )

  hook.insert_rows(
      table="fact_ohlv",
      rows=prepared,
      target_fields=[
          "symbol_id",
          "interval_id",
          "time_id",
          "open",
          "high",
          "low",
          "close",
          "volume",
      ],
      replace=True,
  )
  return len(prepared)


def etl_fact_indicator(mysql_conn_id: str = "mysql_default") -> int:
  """Load indicator fact from indicator JSONL into fact_indicator."""
  return etl_fact_indicator_for_symbol(mysql_conn_id=mysql_conn_id, truncate_table=False)


def etl_fact_indicator_for_symbol(
    symbol_code: str = "BTCUSDT",
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
    mysql_conn_id: str = "mysql_default",
    truncate_table: bool = False,
) -> int:
  """Load indicator fact for a specific symbol."""
  hook = _get_mysql_hook(mysql_conn_id)
  _, ind_path = _kline_paths(symbol_code)
  rows = _read_jsonl(ind_path)
  if not rows:
    return 0

  symbol_id = _ensure_symbol(hook, symbol_code, base_asset, quote_asset)
  interval_id = _ensure_interval(hook, "5m", 5)
  _ = etl_dim_time_from_klines(symbol=symbol_code, mysql_conn_id=mysql_conn_id)

  hook.run(
      """
      CREATE TABLE IF NOT EXISTS fact_indicator (
        indicator_id BIGINT AUTO_INCREMENT PRIMARY KEY,
        symbol_id INT,
        interval_id INT,
        time_id BIGINT,
        sma20 DECIMAL(18,8),
        sma50 DECIMAL(18,8),
        rsi14 DECIMAL(10,4),
        bb_upper DECIMAL(18,8),
        bb_middle DECIMAL(18,8),
        bb_lower DECIMAL(18,8),
        UNIQUE(symbol_id, interval_id, time_id)
      )
      """
  )

  # Optionally reset table when doing a full rebuild.
  if truncate_table:
    hook.run("TRUNCATE TABLE fact_indicator")
    hook.run("ALTER TABLE fact_indicator AUTO_INCREMENT = 1")

  prepared = []
  for r in rows:
    dt = datetime.strptime(r["timestamp"], "%Y-%m-%d %H:%M:%S")
    time_id = int(dt.strftime("%Y%m%d%H%M"))
    prepared.append(
        (
            symbol_id,
            interval_id,
            time_id,
            float(r["sma20"]) if r.get("sma20") is not None else None,
            float(r["sma50"]) if r.get("sma50") is not None else None,
            float(r["rsi14"]) if r.get("rsi14") is not None else None,
            float(r["bb_upper"]) if r.get("bb_upper") is not None else None,
            float(r["bb_middle"]) if r.get("bb_middle") is not None else None,
            float(r["bb_lower"]) if r.get("bb_lower") is not None else None,
        )
    )

  hook.insert_rows(
      table="fact_indicator",
      rows=prepared,
      target_fields=[
          "symbol_id",
          "interval_id",
          "time_id",
          "sma20",
          "sma50",
          "rsi14",
          "bb_upper",
          "bb_middle",
          "bb_lower",
      ],
      replace=True,
  )
  return len(prepared)


def etl_dim_symbol(
    symbol_code: str = "BTCUSDT",
    base_asset: str = "BTC",
    quote_asset: str = "USDT",
    mysql_conn_id: str = "mysql_default",
) -> int:
  """Ensure dim_symbol has the given entry."""
  hook = _get_mysql_hook(mysql_conn_id)
  return _ensure_symbol(hook, symbol_code, base_asset, quote_asset)


def etl_dim_interval(mysql_conn_id: str = "mysql_default") -> int:
  """Ensure dim_interval has 5m entry."""
  hook = _get_mysql_hook(mysql_conn_id)
  return _ensure_interval(hook, "5m", 5)


# ----------------------- Reddit news/comments dims & facts ----------------------- #
def etl_dim_news(mysql_conn_id: str = "mysql_default") -> int:
  """Load dim_news from reddit posts."""
  hook = _get_mysql_hook(mysql_conn_id)
  posts_path = get_data_dir() / "comments" / "reddit_bitcoin_posts.jsonl"
  rows = _read_jsonl(posts_path)
  if not rows:
    return 0

  # Clear dependent tables to avoid FK conflicts on refresh.
  try:
    hook.run("SET FOREIGN_KEY_CHECKS=0")
    for table in ("fact_comment_sentiment", "dim_comments"):
      try:
        hook.run(f"TRUNCATE TABLE {table}")
      except Exception:
        pass
  finally:
    try:
      hook.run("SET FOREIGN_KEY_CHECKS=1")
    except Exception:
      pass

  hook.run(
      """
      CREATE TABLE IF NOT EXISTS dim_news (
        news_id VARCHAR(64) PRIMARY KEY,
        author VARCHAR(255),
        title TEXT,
        created_time DATETIME,
        url TEXT,
        num_comments INT
      )
      """
  )

  prepared = []
  for r in rows:
    created_utc = r.get("created_utc")
    created_dt = (
        datetime.utcfromtimestamp(created_utc) if isinstance(created_utc, (int, float)) else None
    )
    prepared.append(
        (
            r.get("id"),
            r.get("author"),
            r.get("title"),
            created_dt,
            r.get("url"),
            r.get("num_comments"),
        )
    )

  # Upsert without deleting parent rows (avoid FK issues with dim_comments).
  upsert_sql = """
    INSERT INTO dim_news (news_id, author, title, created_time, url, num_comments)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
      author=VALUES(author),
      title=VALUES(title),
      created_time=VALUES(created_time),
      url=VALUES(url),
      num_comments=VALUES(num_comments)
  """
  for row in prepared:
    hook.run(upsert_sql, parameters=row)
  return len(prepared)


def etl_dim_comments(mysql_conn_id: str = "mysql_default") -> int:
  """Load dim_comments from reddit comments."""
  hook = _get_mysql_hook(mysql_conn_id)
  comments_path = get_data_dir() / "comments" / "reddit_bitcoin_comments.jsonl"
  rows = _read_jsonl(comments_path)
  if not rows:
    return 0

  hook.run(
      """
      CREATE TABLE IF NOT EXISTS dim_comments (
        comment_id VARCHAR(64) PRIMARY KEY,
        news_id VARCHAR(64),
        author VARCHAR(255),
        body_text TEXT,
        created_time DATETIME,
        parent_id VARCHAR(64),
        FOREIGN KEY (news_id) REFERENCES dim_news(news_id)
      )
      """
  )

  # Clear dependent fact table to avoid FK conflicts when refreshing dim_comments.
  try:
    hook.run("TRUNCATE TABLE fact_comment_sentiment")
  except Exception:
    pass

  # Ensure all news_ids referenced by comments exist in dim_news (insert stubs if missing).
  comment_news_ids = {r.get("post_id") for r in rows if r.get("post_id")}
  if comment_news_ids:
    existing = hook.get_records(
        """
        SELECT news_id FROM dim_news WHERE news_id IN %s
        """,
        parameters=(tuple(comment_news_ids),),
    )
    existing_ids = {row[0] for row in existing} if existing else set()
    missing_ids = comment_news_ids - existing_ids
    if missing_ids:
      stub_rows = [(nid, None, None, None, None, None) for nid in missing_ids]
      hook.insert_rows(
          table="dim_news",
          rows=stub_rows,
          target_fields=["news_id", "author", "title", "created_time", "url", "num_comments"],
          replace=False,
      )

  prepared = []
  for r in rows:
    created_utc = r.get("created_utc")
    created_dt = (
        datetime.utcfromtimestamp(created_utc) if isinstance(created_utc, (int, float)) else None
    )
    prepared.append(
        (
            r.get("comment_id"),
            r.get("post_id"),
            r.get("author"),
            r.get("body"),
            created_dt,
            r.get("parent_id"),
        )
    )

  hook.insert_rows(
      table="dim_comments",
      rows=prepared,
      target_fields=[
          "comment_id",
          "news_id",
          "author",
          "body_text",
          "created_time",
          "parent_id",
      ],
      replace=True,
  )
  return len(prepared)


def etl_fact_news_sentiment(mysql_conn_id: str = "mysql_default") -> int:
  """Load fact_news_sentiment from scored posts."""
  hook = _get_mysql_hook(mysql_conn_id)
  posts_path = get_data_dir() / "comments" / "reddit_bitcoin_posts_scored.jsonl"
  rows = _read_jsonl(posts_path)
  if not rows:
    return 0

  hook.run(
      """
      CREATE TABLE IF NOT EXISTS fact_news_sentiment (
        news_sentiment_id INT AUTO_INCREMENT PRIMARY KEY,
        news_id VARCHAR(64),
        sentiment_label VARCHAR(32),
        sentiment_score DECIMAL(6,4),
        FOREIGN KEY (news_id) REFERENCES dim_news(news_id),
        UNIQUE(news_id)
      )
      """
  )

  prepared = []
  for r in rows:
    prepared.append(
        (
            r.get("id"),
            r.get("sentiment_label"),
            float(r["sentiment_score"]) if r.get("sentiment_score") is not None else None,
        )
    )

  hook.insert_rows(
      table="fact_news_sentiment",
      rows=prepared,
      target_fields=["news_id", "sentiment_label", "sentiment_score"],
      replace=True,
  )
  return len(prepared)


def etl_fact_comment_sentiment(mysql_conn_id: str = "mysql_default") -> int:
  """Load fact_comment_sentiment from scored comments."""
  hook = _get_mysql_hook(mysql_conn_id)
  comments_path = get_data_dir() / "comments" / "reddit_bitcoin_comments_scored.jsonl"
  rows = _read_jsonl(comments_path)
  if not rows:
    return 0

  hook.run(
      """
      CREATE TABLE IF NOT EXISTS fact_comment_sentiment (
        comment_sentiment_id INT AUTO_INCREMENT PRIMARY KEY,
        comment_id VARCHAR(64) NOT NULL,
        sentiment_label VARCHAR(20),
        sentiment_score DECIMAL(6,4),
        CONSTRAINT fk_comment_sentiment_comment
          FOREIGN KEY (comment_id) REFERENCES dim_comments(comment_id),
        UNIQUE(comment_id)
      )
      """
  )

  prepared = []
  for r in rows:
    prepared.append(
        (
            r.get("comment_id"),
            r.get("sentiment_label"),
            float(r["sentiment_score"]) if r.get("sentiment_score") is not None else None,
        )
    )

  hook.insert_rows(
      table="fact_comment_sentiment",
      rows=prepared,
      target_fields=["comment_id", "sentiment_label", "sentiment_score"],
      replace=True,
  )
  return len(prepared)
