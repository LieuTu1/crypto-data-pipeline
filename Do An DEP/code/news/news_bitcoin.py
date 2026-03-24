from __future__ import annotations

import os
import re
import json
from datetime import datetime, timezone
import sys
from typing import Iterable, Optional, Sequence
import html as html_lib

import cloudscraper
from bs4 import BeautifulSoup

# Đảm bảo stdout/stderr dùng UTF-8 để tránh lỗi encode trên Windows
try:
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

URL = "https://www.binance.com/vi/square/news/bitcoin-news"
OUTPUT_FILE = os.path.join("news", "binance_square_bitcoin_titles.txt")
OUTPUT_JSON = os.path.join("news", "binance_square_bitcoin.json")
API_URL = os.getenv(
    "BINANCE_API_URL",
    "https://www.binance.com/bapi/composite/v4/friendly/pgc/feed/news/list",
)
# Cho phép bật/tắt lọc BTC/Bitcoin cho dữ liệu API (0 để tắt lọc)
FILTER_API_BTC = os.getenv("BINANCE_FILTER_BTC", "1") != "0"
# Một số header mặc định (có thể override qua biến môi trường)
CSRF_FALLBACK = "ad053db0554ade4a7bac786a4c2b9e10"
TRACE_FALLBACK = "515b2772-a011-4528-9900-0c1e64ee68db"
DEVICE_INFO_FALLBACK = (
    "eyJzY3JlZW5fcmVzb2x1dGlvbiI6Ijg2NCwxNTM2IiwiYXZhaWxhYmxlX3NjcmVlbl9yZXNv"
    "bHV0aW9uIjoiODE2LDE1MzYiLCJzeXN0ZW1fdmVyc2lvbiI6IldpbmRvd3MgMTAiLCJicmFu"
    "ZF9tb2RlbCI6InVua25vd24iLCJzeXN0ZW1fbGFuZyI6InZpIiwidGltZXpvbmUiOiJHTVQr"
    "MDc6MDAiLCJ0aW1lem9uZU9mZnNldCI6LTQyMCwidXNlcl9hZ2VudCI6Ik1vemlsbGEvNS4w"
    "IChXaW5kb3dzIE5UIDEwLjA7IFdpbjY0OyB4NjQpIEFwcGxlV2ViS2l0LzUzNy4zNiAoS0hU"
    "TUwsIGxpa2UgR2Vja28pIENocm9tZS8xNDIuMC4wLjAgU2FmYXJpLzUzNy4zNiIsImxpc3Rf"
    "cGx1Z2luIjoiUERGIFZpZXdlcixDaHJvbWUgUERGIFZpZXdlcixDaHJvbWl1bSBQREYgVmlld2VyLE1pY3Jvc29mdCBFZGdlIFBERiBWaWV3ZXIsV2ViS2l0IGJ1aWx0LWluIFBERiIsImNhbnZhc19jb2RlIjoiOTEwMzlmZDEiLCJ3ZWJnbF92ZW5kb3IiOiJHb29nbGUgSW5jLiAoQU1EKSIsIndlYmdsX3JlbmRlcmVyIjoiQU5HTEUgKEFNRCwgQU1EIFJhZGVvbihUTSkgR3JhcGhpY3MgKDB4MDAwMDE2NEMpIERpcmVjdDNEMTEgdnNfNV8wIHBzXzVfMCwgRDNEMTEpIiwiYXVkaW8iOiIxMjQuMDQzNDc1Mjc1MTYwNzQiLCJwbGF0Zm9ybSI6IldpbjMyIiwid2ViX3RpbWV6b25lIjoiQXNpYS9TYWlnb24iLCJkZXZpY2VfbmFtZSI6IkNocm9tZSBWMTQyLjAuMC4wIChXaW5kb3dzKSIsImZpbmdlcnByaW50IjoiZDg0YjdhNTJlODBiYzU3MmQ4Y2ViOGVkM2IzYzkyNWUiLCJkZXZpY2VfaWQiOiIiLCJyZWxhdGVkX2RldmljZV9pZHMiOiIifQ=="
)
FVIDEO_TOKEN_FALLBACK = (
    "zmycjQbew0FSf2rXOggEKwULnlWexItnhVJb/Ul3aViFr80PdqLzT7NIrNIr1KmL03gxPFxq"
    "2ruUxSPL1ctP8T1i2F2CEAgvQ/P2BnKgQxtHLhx64hgh4uaCpixN6zU3SSsV9pbhBP1wS/PI"
    "20BC7gEvMYtW3n0twmwPx+snC+fNiG8jYud/QDwlcYUyhN43A=17"
)
FVIDEO_ID_FALLBACK = "337fda9588baa609abf6b2c144ddec97003ad50b"
# Cai dat mac dinh: lay 5 trang x 20 ban ghi tu API (co the thay doi qua env)
API_PAGE_SIZE = int(os.getenv("BINANCE_NEWS_PAGE_SIZE", "20"))
API_PAGES = int(os.getenv("BINANCE_NEWS_PAGES", "30"))
# Cho phép đổi tagId (mặc định 16 nếu không đặt env; muốn Bitcoin có thể đặt BINANCE_TAG_ID=7)
API_TAG_ID = os.getenv("BINANCE_TAG_ID", "16")

# Nếu muốn cố định các giá trị header khi không set env, đặt tại đây
DEFAULT_BINANCE_COOKIE = os.getenv("BINANCE_COOKIE_DEFAULT", "")
DEFAULT_CSRF = os.getenv("BINANCE_CSRF_DEFAULT", "ad053db0554ade4a7bac786a4c2b9e10")
DEFAULT_TRACE = os.getenv("BINANCE_TRACE_ID_DEFAULT", "52a1d351-67bb-43c0-8506-e7ac3d038bbb")
DEFAULT_DEVICE_INFO = os.getenv("BINANCE_DEVICE_INFO_DEFAULT", DEVICE_INFO_FALLBACK)
DEFAULT_FVIDEO_TOKEN = os.getenv("BINANCE_FVIDEO_TOKEN_DEFAULT", FVIDEO_TOKEN_FALLBACK)
DEFAULT_FVIDEO_ID = os.getenv("BINANCE_FVIDEO_ID_DEFAULT", FVIDEO_ID_FALLBACK)
DEFAULT_PASSTHROUGH = os.getenv("BINANCE_PASSTHROUGH_TOKEN_DEFAULT", "")


def _is_waf_challenge(html: str) -> bool:
    """Detect AWS WAF challenge page (no real content)."""
    markers = (
        "AwsWafIntegration",
        "challenge-container",
        "awsWafCookieDomainList",
        "gokuProps",
    )
    return all(m in html for m in markers)


def fetch_html() -> str:
    """Lấy HTML trang Square, kèm cookie nếu cung cấp."""
    cookie = os.getenv("BINANCE_COOKIE", "") or DEFAULT_BINANCE_COOKIE
    scraper = cloudscraper.create_scraper()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi,en;q=0.9",
        "Accept-Encoding": "identity",
    }
    if cookie:
        headers["Cookie"] = cookie
    resp = scraper.get(URL, headers=headers, timeout=30)
    resp.raise_for_status()
    html = resp.text
    if _is_waf_challenge(html):
        if os.getenv("BINANCE_SKIP_WAF", "") != "1":
            raise RuntimeError(
                "Nhan duoc trang AWS WAF challenge (khong co noi dung). "
                "Hay lay cookie hop le tu trinh duyet va dat BINANCE_COOKIE hoac su dung HTML da luu."
            )
        # Cho phép bỏ qua WAF nếu đặt BINANCE_SKIP_WAF=1
        return ""
    return html


def _fetch_api_page(page_index: int, page_size: int) -> list[dict]:
    """Gọi API danh sách news và trả về list item thô. Yêu cầu BINANCE_COOKIE."""
    cookie = os.getenv("BINANCE_COOKIE", "") or DEFAULT_BINANCE_COOKIE
    if not cookie:
        raise RuntimeError("Thiếu BINANCE_COOKIE để gọi API news.")
    # cố gắng lấy uuid từ cookie nếu có
    def _uuid_from_cookie() -> str:
        m = re.search(r"bnc-uuid=([^;]+)", cookie)
        return m.group(1) if m else ""

    scraper = cloudscraper.create_scraper()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "Accept": "application/json, text/plain, */*",
        "Referer": "https://www.binance.com/vi/square/news/bitcoin-news",
        "Cookie": cookie,
        "content-type": "application/json",
        "clienttype": "web",
        "bnc-level": "0",
        "bnc-location": "BINANCE",
        "bnc-time-zone": "Asia/Saigon",
        "bnc-uuid": os.getenv("BINANCE_UUID", "") or _uuid_from_cookie(),
    }
    csrf = os.getenv("BINANCE_CSRF", "") or DEFAULT_CSRF
    headers["x-csrf-token"] = csrf

    trace = os.getenv("BINANCE_TRACE_ID", "") or DEFAULT_TRACE
    headers["x-trace-id"] = trace
    headers["x-ui-request-trace"] = trace

    passthrough = os.getenv("BINANCE_PASSTHROUGH_TOKEN", "") or DEFAULT_PASSTHROUGH
    if passthrough:
        headers["x-passthrough-token"] = passthrough

    device_info = os.getenv("BINANCE_DEVICE_INFO", "") or DEFAULT_DEVICE_INFO
    headers["device-info"] = device_info

    fvideo_token = os.getenv("BINANCE_FVIDEO_TOKEN", "") or DEFAULT_FVIDEO_TOKEN
    fvideo_id = os.getenv("BINANCE_FVIDEO_ID", "") or DEFAULT_FVIDEO_ID
    headers["fvideo-token"] = fvideo_token
    headers["fvideo-id"] = fvideo_id
    params = {
        "pageIndex": page_index,
        "pageSize": page_size,
        "strategy": 5,
        "tagId": API_TAG_ID,
        "featured": "false",
    }
    resp = scraper.get(API_URL, params=params, headers=headers, timeout=30)
    if resp.status_code == 403:
        raise RuntimeError("API bị 403 (cookie/headers không hợp lệ). Kiểm tra lại BINANCE_COOKIE.")
    resp.raise_for_status()
    data = resp.json()
    # Tìm list item trong nhiều cấu trúc lồng nhau
    def _find_list(obj):
        if isinstance(obj, list):
            return obj
        if isinstance(obj, dict):
            # thử các khóa phổ biến trước
            for k in ("data", "items", "list", "records", "posts", "rows"):
                if k in obj:
                    found = _find_list(obj[k])
                    if found:
                        return found
            # sau đó duyệt tất cả value
            for v in obj.values():
                found = _find_list(v)
                if found:
                    return found
        return None

    candidates = _find_list(data) or []
    if not candidates:
        debug_path = os.path.join("news", f"debug_api_response_page_{page_index}.json")
        os.makedirs(os.path.dirname(debug_path), exist_ok=True)
        with open(debug_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        raise RuntimeError(
            f"Không tìm thấy danh sách bài trong API data (page {page_index}). "
            f"Đã lưu response debug tại {debug_path}"
        )
    return candidates


def _normalize_ts(raw: str) -> Optional[str]:
    """Chuẩn hóa chuỗi thời gian về ISO UTC nếu là epoch giây/mili."""
    raw = raw.strip()
    if not raw:
        return None
    # Xử lý chuỗi epoch 10 hoặc 13 chữ số
    if raw.isdigit() and len(raw) in (10, 13):
        ts = int(raw)
        if len(raw) == 13:
            ts = ts / 1000
        if ts < 100000000:  # tránh timestamp quá nhỏ
            return None
        dt = datetime.fromtimestamp(ts, tz=timezone.utc)
        return dt.strftime("%Y-%m-%d %H:%M:%S")
    # Xử lý ISO/RCF
    try:
        if re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", raw):
            dt = datetime.strptime(raw[:19], "%Y-%m-%dT%H:%M:%S").replace(tzinfo=timezone.utc)
            return dt.strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        pass
    return raw


def parse_vietnamese_date(raw: str, reference: datetime = None) -> Optional[str]:
    """Chuyển '10 thg 12' sang ISO YYYY-MM-DD"""
    if not raw:
        return None
    raw = raw.strip().lower()
    if reference is None:
        reference = datetime.now()

    m = re.match(r"(\d{1,2})\s*thg\s*(\d{1,2})", raw)
    if m:
        day, month = int(m.group(1)), int(m.group(2))
        year = reference.year
        dt = datetime(year, month, day)
        return dt.strftime("%Y-%m-%d")
    return None


def extract_news(html: str) -> list[tuple[str, Optional[str]]]:
    """Lấy tiêu đề và thời gian từ các block JSON hydrat trong HTML."""
    results: list[tuple[str, Optional[str]]] = []
    seen: set[str] = set()

    blocks = re.findall(r'id="__APP_DATA__"\s*>(\{.*?\})</div>', html, re.DOTALL)

    for block in blocks:
        items = re.findall(
            r'"title"\s*:\s*"([^"]{3,400})".*?"(?:publishTime|createTime|time|releaseTime)"\s*:\s*"?(?P<t>[^",}]{6,20})"?', 
            block, 
            re.DOTALL
        )
        for title, time_raw in items:
            title = title.encode("utf-8", "ignore").decode("utf-8", "ignore").strip()
            if not title or not re.search(r"btc|bitcoin", title, re.IGNORECASE):
                continue
            time_str = _normalize_ts(time_raw)
            key = f"{title}|{time_str or time_raw}"
            if key in seen:
                continue
            seen.add(key)
            results.append((title, time_str))
    return results


def _extract_time(card) -> str:
    """Lấy thời gian bài viết từ card"""
    # 1. Node .css-vyak18
    time_node = card.select_one(".css-vyak18") or card.find(attrs={"class": re.compile(r"css-vyak18")})
    if time_node:
        raw = time_node.get_text(" ", strip=True)
        dt = parse_vietnamese_date(raw)
        return dt or raw

    # 2. Regex trong text card
    text_all = card.get_text(" ", strip=True)
    m = re.search(r"\d{1,2}\s*thg\s*\d{1,2}", text_all)
    if m:
        dt = parse_vietnamese_date(m.group(0))
        if dt:
            return dt

    # 3. Trích xuất từ URL nếu có
    link_node = card.find("a", href=True)
    if link_node:
        m_url = re.search(r"/(\d{2}-\d{2}-\d{4})-", link_node["href"])
        if m_url:
            try:
                dt = datetime.strptime(m_url.group(1), "%m-%d-%Y")
                return dt.strftime("%Y-%m-%d")
            except:
                pass

    # 4. Nếu tất cả thất bại
    return "--"


def extract_dom_titles(html: str) -> list[tuple[str, Optional[str]]]:
    """Lấy tiêu đề từ h3 và thời gian gắn kèm nếu có."""
    soup = BeautifulSoup(html, "html.parser")
    results: list[tuple[str, Optional[str]]] = []
    seen: set[str] = set()
    
    for h3 in soup.find_all("h3", {"class": re.compile(r"css-yxpvu")}):
        title = " ".join(h3.get_text(" ", strip=True).split())
        if not title or title in seen:
            continue
        if not re.search(r"btc|bitcoin", title, re.IGNORECASE):
            continue
        seen.add(title)
        card = h3.find_parent("div", class_=re.compile(r"css-vurnku"))
        time_val = _extract_time(card) if card else None
        results.append((title, time_val)) 
    return results


def _parse_date_from_link(link: str) -> Optional[str]:
    """Trích ngày (YYYY-MM-DD) từ URL dạng .../mm-dd-yyyy-..."""
    if not link:
        return None
    m_url = re.search(r"/(\d{2})-(\d{2})-(\d{4})-", link)
    if not m_url:
        return None
    month, day, year = m_url.groups()
    try:
        dt = datetime.strptime(f"{year}-{month}-{day}", "%Y-%m-%d")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return None


def _extract_from_api_item(item: dict) -> Optional[dict]:
    """Chuyển 1 item API thành dict card chuẩn, bỏ qua nếu không chứa BTC/Bitcoin."""
    if not isinstance(item, dict):
        return None
    # item có thể nằm trong key "vos" với nội dung ở item["title"], "subTitle"...
    # Ưu tiên title, fallback subtitle/translated title.
    title = (item.get("title") or "").strip()
    if not title:
        title = (item.get("subTitle") or item.get("translatedData", {}).get("title", "")).strip()
    if not title:
        return None
    if FILTER_API_BTC and not re.search(r"btc|bitcoin", title, re.IGNORECASE):
        return None

    time_raw = None
    for key in ("publishTime", "releaseTime", "createTime", "time", "date"):
        val = item.get(key)
        if val is not None:
            time_raw = _normalize_ts(str(val))
            break

    link = ""
    post_id = item.get("id") or item.get("postId") or item.get("contentId")
    if post_id:
        link = f"https://www.binance.com/vi/square/post/{post_id}"
    elif item.get("webLink"):
        link = item["webLink"]
    elif item.get("link"):
        link = item["link"]

    content = (
        (item.get("content") or item.get("subTitle") or item.get("summary") or "")
        .strip()
    )

    return {
        "time_raw": time_raw or "",
        "title": title,
        "content": content,
        "link": link,
    }


def fetch_api_cards(pages: int = API_PAGES, page_size: int = API_PAGE_SIZE) -> list[dict]:
    """Lấy nhiều trang news qua API (pagination), lọc bài liên quan Bitcoin."""
    cards: list[dict] = []
    debug_api_dumped = False
    for idx in range(1, pages + 1):
        items = _fetch_api_page(idx, page_size)
        if not items:
            break
        for it in items:
            card = _extract_from_api_item(it)
            if card:
                cards.append(card)
        # nếu không lọc BTC và muốn xem raw, dump một lần
        if not FILTER_API_BTC and not debug_api_dumped:
            path = os.path.join("news", f"debug_api_items_page_{idx}.json")
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                json.dump(items, f, ensure_ascii=False, indent=2)
            debug_api_dumped = True
    return cards


def extract_cards(html: str) -> list[dict]:
    """Trích thời gian, tiêu đề, nội dung, link từ các card."""
    soup = BeautifulSoup(html, "html.parser")
    cards: list[dict] = []

    def _clean(text: str) -> str:
        text = re.sub(r"<[^>]+>", " ", text)
        text = html_lib.unescape(text)
        return " ".join(text.split()).strip()

    for card in soup.find_all("div", class_=re.compile(r"css-vurnku")):
        title_node = card.find("h3", class_=re.compile(r"css-yxpvu"))
        content_node = card.find("div", class_=re.compile(r"css-10lrpzu"))
        link_node = card.find("a", href=True)

        title = _clean(title_node.get_text(" ", strip=True)) if title_node else ""
        if not title or not re.search(r"btc|bitcoin", title, re.IGNORECASE):
            continue

        time_raw = _extract_time(card)
        content = _clean(content_node.get_text(" ", strip=True)) if content_node else ""
        href = link_node["href"] if link_node else ""
        link = href if href.startswith("http") else f"https://www.binance.com{href}"

        if time_raw in (None, "", "--"):
            parsed = _parse_date_from_link(link)
            if parsed:
                time_raw = parsed

        cards.append({
            "time_raw": time_raw,
            "title": title,
            "content": content,
            "link": link,
        })
    return cards


def save_titles(items: Iterable[tuple[str, Optional[str]]]) -> None:
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        for idx, (title, t) in enumerate(items, start=1):
            prefix = t if t else ""
            f.write(f"{idx}\t{prefix}\t{title}\n")


def save_json(cards: list[dict]) -> None:
    os.makedirs(os.path.dirname(OUTPUT_JSON), exist_ok=True)
    with open(OUTPUT_JSON, "w", encoding="utf-8") as f:
        json.dump(cards, f, ensure_ascii=False, indent=2)


def _dedupe_pairs(pairs: Sequence[tuple[str, Optional[str]]]) -> list[tuple[str, Optional[str]]]:
    seen: set[tuple[str, Optional[str]]] = set()
    out: list[tuple[str, Optional[str]]] = []
    for pair in pairs:
        key = pair if pair[1] is not None else pair[0]
        if key in seen:
            continue
        seen.add(key)
        out.append(pair)
    return out


def main() -> None:
    # Chỉ lấy dữ liệu từ API để tránh trùng lặp DOM/JSON hydrat
    cards: list[dict] = []
    try:
        api_cards = fetch_api_cards()
        print(f"API trả {len(api_cards)} tin (sau lọc BTC={'on' if FILTER_API_BTC else 'off'}).")
        cards.extend(api_cards)
    except Exception as exc:
        raise RuntimeError(f"Không thể gọi API pagination: {exc}") from exc

    if not cards:
        raise RuntimeError("API không trả dữ liệu.")

    # dedupe cards theo (title, time_raw, link)
    seen_cards: set[tuple[str, str, str]] = set()
    deduped_cards: list[dict] = []
    for card in cards:
        key = (
            card.get("title", ""),
            card.get("time_raw", ""),
            card.get("link", ""),
        )
        if key in seen_cards:
            continue
        seen_cards.add(key)
        deduped_cards.append(card)
    cards = deduped_cards

    # Build title_pairs từ API cards
    title_pairs = _dedupe_pairs([(c.get("title", ""), c.get("time_raw")) for c in cards])
    if title_pairs:
        save_titles(title_pairs)
        print(f"Đã lưu {len(title_pairs)} tin vào {OUTPUT_FILE}")

    save_json(cards)
    print(f"Đã lưu {len(cards)} tin vào {OUTPUT_JSON}")


if __name__ == "__main__":
    main()
