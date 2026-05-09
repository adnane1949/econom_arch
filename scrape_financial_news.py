#!/usr/bin/env python3
"""
Scrape financial news from:
  - L'Economiste: https://www.leconomiste.com/
  - Reuters Business: https://www.reuters.com/business/
  - Yahoo Finance News: https://finance.yahoo.com/news/
  - Investing.com News: https://www.investing.com/news/

The output is JSON Lines, with one record per article using the requested
metadata/article/entities/analytics/quality/governance schema.

Usage:
  python scrape_financial_news.py --output data/news.jsonl
  python scrape_financial_news.py --max-per-source 100 --content-max-chars 1200 --output data/news.jsonl
  python scrape_financial_news.py --pretty --max-per-source 3
  python scrape_financial_news.py --mode batch-hourly --output-dir data/batch
  python scrape_financial_news.py --mode stream --event-output data/events.jsonl --poll-seconds 60
  python scrape_financial_news.py --mode stream --stream-once --kafka-bootstrap-servers localhost:9092
"""

from __future__ import annotations

import argparse
import hashlib
import html
import json
import re
import sys
import time
import uuid
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urljoin, urlparse, urlunparse
from urllib.request import Request, urlopen
from xml.etree import ElementTree


PIPELINE_VERSION = "v1.2"
COLLECTOR_NODE = "scraper-node-01"
RETENTION_POLICY = "365_days"

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/124.0 Safari/537.36"
)


@dataclass(frozen=True)
class Source:
    name: str
    country: str
    language: str
    homepage_url: str
    listing_url: str
    feed_url: str | None = None


SOURCES = [
    Source(
        name="LEconomiste",
        country="MA",
        language="fr",
        homepage_url="https://www.leconomiste.com/",
        listing_url="https://www.leconomiste.com/categorie/economie/",
    ),
    Source(
        name="Reuters",
        country="UK",
        language="en",
        homepage_url="https://www.reuters.com/",
        listing_url="https://www.reuters.com/business/",
    ),
    Source(
        name="YahooFinance",
        country="US",
        language="en",
        homepage_url="https://finance.yahoo.com/",
        listing_url="https://finance.yahoo.com/news/",
        feed_url="https://finance.yahoo.com/news/rssindex",
    ),
    Source(
        name="Investing",
        country="US",
        language="en",
        homepage_url="https://www.investing.com/",
        listing_url="https://www.investing.com/news/",
        feed_url="https://www.investing.com/rss/news.rss",
    ),
]

EXTRA_FEED_URLS = {
    "YahooFinance": [
        "https://finance.yahoo.com/news/rssindex",
        "https://finance.yahoo.com/rss/headline?s=AAPL",
        "https://finance.yahoo.com/rss/headline?s=MSFT",
        "https://finance.yahoo.com/rss/headline?s=NVDA",
        "https://finance.yahoo.com/rss/headline?s=TSLA",
        "https://finance.yahoo.com/rss/headline?s=AMZN",
        "https://finance.yahoo.com/rss/headline?s=META",
        "https://finance.yahoo.com/rss/headline?s=GOOGL",
        "https://finance.yahoo.com/rss/headline?s=JPM",
        "https://finance.yahoo.com/rss/headline?s=XOM",
    ],
    "Investing": [
        "https://www.investing.com/rss/news.rss",
        "https://www.investing.com/rss/news_1.rss",
        "https://www.investing.com/rss/news_11.rss",
        "https://www.investing.com/rss/news_25.rss",
        "https://www.investing.com/rss/news_95.rss",
        "https://www.investing.com/rss/news_287.rss",
        "https://www.investing.com/rss/central_banks.rss",
        "https://www.investing.com/rss/market_overview.rss",
    ],
}

EXTRA_LISTING_URLS = {
    "LEconomiste": [
        "https://www.leconomiste.com/",
        "https://www.leconomiste.com/flash-infos/",
        "https://www.leconomiste.com/rubrique/economie",
        "https://www.leconomiste.com/rubrique/entreprises",
        "https://www.leconomiste.com/rubrique/maroc",
    ],
    "YahooFinance": [
        "https://finance.yahoo.com/news/",
        "https://finance.yahoo.com/topic/stock-market-news/",
        "https://finance.yahoo.com/topic/economic-news/",
        "https://finance.yahoo.com/topic/earnings/",
        "https://finance.yahoo.com/topic/crypto/",
    ],
    "Investing": [
        "https://www.investing.com/news/",
        "https://www.investing.com/news/stock-market-news",
        "https://www.investing.com/news/stock-market-news/2",
        "https://www.investing.com/news/stock-market-news/3",
        "https://www.investing.com/news/economy",
        "https://www.investing.com/news/economy/2",
        "https://www.investing.com/news/economy/3",
        "https://www.investing.com/news/economy/4",
        "https://www.investing.com/news/economic-indicators",
        "https://www.investing.com/news/latest-news/2",
        "https://www.investing.com/news/latest-news/3",
        "https://www.investing.com/news/commodities-news",
        "https://www.investing.com/news/commodities-news/2",
        "https://www.investing.com/news/forex-news",
        "https://www.investing.com/news/forex-news/2",
    ],
}


POSITIVE_WORDS = {
    "beat",
    "boost",
    "bullish",
    "gain",
    "gains",
    "growth",
    "higher",
    "improve",
    "improves",
    "positive",
    "profit",
    "rally",
    "record",
    "rise",
    "rises",
    "strong",
    "surge",
    "up",
}

NEGATIVE_WORDS = {
    "bearish",
    "crisis",
    "cut",
    "decline",
    "declines",
    "drop",
    "drops",
    "fall",
    "falls",
    "fear",
    "inflation",
    "loss",
    "lower",
    "negative",
    "recession",
    "risk",
    "slump",
    "tensions",
    "weak",
}

COUNTRIES = {
    "Morocco",
    "Maroc",
    "USA",
    "United States",
    "US",
    "UK",
    "Britain",
    "France",
    "Germany",
    "China",
    "Japan",
    "Russia",
    "Saudi Arabia",
    "Spain",
    "Italy",
    "India",
    "Canada",
    "Brazil",
    "UAE",
    "Qatar",
}

CURRENCIES = {
    "USD",
    "EUR",
    "GBP",
    "JPY",
    "MAD",
    "CNY",
    "dollar",
    "dollars",
    "euro",
    "euros",
    "dirham",
    "dirhams",
}

COMPANY_SUFFIXES = (
    "Inc",
    "Corp",
    "Corporation",
    "Group",
    "Holdings",
    "Bank",
    "Energy",
    "Airlines",
    "Motors",
    "Technologies",
    "TotalEnergies",
    "Shell",
    "Apple",
    "Microsoft",
    "Amazon",
    "Tesla",
    "Nvidia",
    "Meta",
    "Google",
)

STOPWORDS = {
    "the",
    "and",
    "for",
    "with",
    "from",
    "that",
    "this",
    "into",
    "over",
    "under",
    "about",
    "after",
    "before",
    "than",
    "its",
    "sur",
    "les",
    "des",
    "une",
    "dans",
    "pour",
    "avec",
    "plus",
    "aux",
    "par",
    "est",
}


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str]] = []
        self._href: str | None = None
        self._text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        attrs_dict = dict(attrs)
        href = attrs_dict.get("href")
        if href:
            self._href = href
            self._text = []

    def handle_data(self, data: str) -> None:
        if self._href:
            self._text.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._href:
            title = clean_text(" ".join(self._text))
            self.links.append((self._href, title))
            self._href = None
            self._text = []


class ArticleParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.title_parts: list[str] = []
        self.h1_parts: list[str] = []
        self.h2_parts: list[str] = []
        self.paragraphs: list[str] = []
        self.meta: dict[str, str] = {}
        self.json_ld_blocks: list[str] = []
        self._tag_stack: list[str] = []
        self._current_script_type = ""
        self._script_buffer: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attrs_dict = {k.lower(): v or "" for k, v in attrs}
        self._tag_stack.append(tag)

        if tag == "meta":
            key = attrs_dict.get("property") or attrs_dict.get("name")
            content = attrs_dict.get("content")
            if key and content:
                self.meta[key.lower()] = html.unescape(content)

        if tag == "script":
            self._current_script_type = attrs_dict.get("type", "")
            self._script_buffer = []

    def handle_data(self, data: str) -> None:
        if not self._tag_stack:
            return
        tag = self._tag_stack[-1]
        if tag == "title":
            self.title_parts.append(data)
        elif tag == "h1":
            self.h1_parts.append(data)
        elif tag == "h2":
            self.h2_parts.append(data)
        elif tag == "p":
            self.paragraphs.append(data)
        elif tag == "script" and self._current_script_type == "application/ld+json":
            self._script_buffer.append(data)

    def handle_endtag(self, tag: str) -> None:
        if tag == "script" and self._current_script_type == "application/ld+json":
            block = "".join(self._script_buffer).strip()
            if block:
                self.json_ld_blocks.append(block)
            self._script_buffer = []
            self._current_script_type = ""

        if self._tag_stack:
            self._tag_stack.pop()


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def isoformat_z(value: datetime | None = None) -> str:
    value = value or utc_now()
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    value = html.unescape(value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def fetch_text(url: str, timeout: int = 20) -> str:
    url = normalize_url(url)
    request = Request(
        url,
        headers={
            "User-Agent": USER_AGENT,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9,fr;q=0.8",
        },
    )
    with urlopen(request, timeout=timeout) as response:
        body = response.read()
        charset = (response.headers.get_content_charset() or "").lower()

    html_charset = ""
    header = body[:4096].decode("ascii", errors="ignore")
    match = re.search(r"charset=[\"']?([\w.-]+)", header, re.I)
    if match:
        html_charset = match.group(1).lower()

    for candidate in dedupe(["utf-8", html_charset, charset, "windows-1252", "iso-8859-1"]):
        if not candidate:
            continue
        try:
            decoded = body.decode(candidate)
        except (LookupError, UnicodeDecodeError):
            continue
        if "Ã" not in decoded and "â€" not in decoded:
            return decoded

    return body.decode(charset or "utf-8", errors="replace")


def normalize_url(url: str) -> str:
    parsed = urlparse(html.unescape(clean_text(url)))
    return urlunparse(
        (
            parsed.scheme,
            parsed.netloc.encode("idna").decode("ascii") if parsed.netloc else "",
            quote(parsed.path, safe="/:%"),
            quote(parsed.params, safe=""),
            quote(parsed.query, safe="=&?/%:+,"),
            quote(parsed.fragment, safe=""),
        )
    )


def parse_date(value: str | None) -> str | None:
    value = clean_text(value)
    if not value:
        return None
    try:
        return isoformat_z(parsedate_to_datetime(value))
    except (TypeError, ValueError, IndexError):
        pass
    try:
        normalized = value.replace("Z", "+00:00")
        return isoformat_z(datetime.fromisoformat(normalized))
    except ValueError:
        return None


def parse_json_ld(parser: ArticleParser) -> dict[str, Any]:
    for block in parser.json_ld_blocks:
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        items = data if isinstance(data, list) else [data]
        for item in items:
            if not isinstance(item, dict):
                continue
            graph = item.get("@graph")
            candidates = graph if isinstance(graph, list) else [item]
            for candidate in candidates:
                if isinstance(candidate, dict) and is_news_candidate(candidate):
                    return candidate
    return {}


def is_news_candidate(data: dict[str, Any]) -> bool:
    item_type = data.get("@type", "")
    if isinstance(item_type, list):
        item_type = " ".join(str(value) for value in item_type)
    return bool(re.search(r"NewsArticle|Article|ReportageNewsArticle", str(item_type), re.I))


def read_json_ld_text(value: Any) -> str:
    if isinstance(value, str):
        return clean_text(value)
    if isinstance(value, dict):
        return clean_text(value.get("name") or value.get("headline") or "")
    if isinstance(value, list):
        return ", ".join(filter(None, [read_json_ld_text(item) for item in value]))
    return ""


def discover_from_feed(source: Source, max_items: int, feed_url: str | None = None) -> list[dict[str, str]]:
    feed_url = feed_url or source.feed_url
    if not feed_url:
        return []
    try:
        xml_text = fetch_text(feed_url)
    except (HTTPError, URLError, TimeoutError):
        return []

    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return []

    items: list[dict[str, str]] = []
    for item in root.findall(".//item"):
        title = clean_text(item.findtext("title"))
        link = clean_text(item.findtext("link"))
        pub_date = parse_date(item.findtext("pubDate")) or ""
        if title and link:
            items.append({"title": title, "url": link, "publication_timestamp": pub_date})
        if len(items) >= max_items:
            break
    return items


def discover_from_page(source: Source, max_items: int, listing_url: str | None = None) -> list[dict[str, str]]:
    try:
        page = fetch_text(listing_url or source.listing_url)
    except (HTTPError, URLError, TimeoutError):
        return []

    parser = LinkParser()
    parser.feed(page)

    seen: set[str] = set()
    items: list[dict[str, str]] = []
    base_domain = urlparse(source.homepage_url).netloc.replace("www.", "")

    for href, title in parser.links:
        url = urljoin(source.homepage_url, href)
        parsed = urlparse(url)
        if parsed.netloc.replace("www.", "") != base_domain:
            continue
        if not is_article_like_url(source, url):
            continue
        if url in seen:
            continue
        seen.add(url)
        items.append({"title": title, "url": url, "publication_timestamp": ""})
        if len(items) >= max_items:
            break

    return items


def discover_reuters_from_sitemap(max_items: int) -> list[dict[str, str]]:
    sitemap_urls = discover_reuters_sitemap_urls(max_items)
    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for sitemap_url in sitemap_urls:
        if len(items) >= max_items:
            break
        try:
            sitemap = fetch_text(sitemap_url)
        except (HTTPError, URLError, TimeoutError):
            continue

        urls = re.findall(r"<loc>(.*?)</loc>", sitemap)
        for url in urls:
            url = clean_text(url)
            if url in seen:
                continue
            if "/business/" not in url or not re.search(r"-\d{4}-\d{2}-\d{2}/?$", url):
                continue
            seen.add(url)
            items.append(
                {
                    "title": title_from_url(url),
                    "url": url,
                    "publication_timestamp": date_from_reuters_url(url) or "",
                }
            )
            if len(items) >= max_items:
                break
    return items


def discover_reuters_sitemap_urls(max_items: int) -> list[str]:
    index_url = "https://www.reuters.com/arc/outboundfeeds/sitemap-index/?outputType=xml"
    try:
        sitemap_index = fetch_text(index_url)
    except (HTTPError, URLError, TimeoutError):
        return ["https://www.reuters.com/arc/outboundfeeds/sitemap/?outputType=xml"]

    urls = [html.unescape(clean_text(url)) for url in re.findall(r"<loc>(.*?)</loc>", sitemap_index)]
    return urls[: max(1, min(len(urls), (max_items // 20) + 8))]


def discover_leconomiste_from_sitemap(max_items: int) -> list[dict[str, str]]:
    try:
        sitemap_index = fetch_text("https://www.leconomiste.com/sitemap_index.xml")
    except (HTTPError, URLError, TimeoutError):
        return []

    sitemap_pairs = re.findall(
        r"<sitemap>\s*<loc>(.*?)</loc>\s*<lastmod>(.*?)</lastmod>",
        sitemap_index,
    )
    post_sitemaps = [
        (clean_text(url), parse_date(lastmod) or "")
        for url, lastmod in sitemap_pairs
        if "post-sitemap" in url
    ]
    post_sitemaps.sort(key=lambda pair: pair[1], reverse=True)

    items: list[dict[str, str]] = []
    seen: set[str] = set()
    for sitemap_url, _ in post_sitemaps[:10]:
        if len(items) >= max_items:
            break
        try:
            sitemap = fetch_text(sitemap_url)
        except (HTTPError, URLError, TimeoutError):
            continue

        url_pairs = re.findall(
            r"<url>\s*<loc>(.*?)</loc>\s*<lastmod>(.*?)</lastmod>",
            sitemap,
        )
        url_pairs = [(clean_text(url), parse_date(lastmod) or "") for url, lastmod in url_pairs]
        url_pairs.sort(key=lambda pair: pair[1], reverse=True)

        for url, lastmod in url_pairs:
            if url in seen or not url.startswith("https://www.leconomiste.com/"):
                continue
            if any(part in url for part in ["/wp-content/", "/author/", "/tag/", "/category/"]):
                continue
            seen.add(url)
            items.append(
                {
                    "title": title_from_url(url),
                    "url": url,
                    "publication_timestamp": lastmod,
                }
            )
            if len(items) >= max_items:
                break
    return items


def is_article_like_url(source: Source, url: str) -> bool:
    path = urlparse(url).path.lower()
    if source.name == "Reuters":
        return path.startswith("/business/") and bool(re.search(r"-\d{4}-\d{2}-\d{2}/?$", path))
    if source.name == "LEconomiste":
        if "/archive/" in path or "/newsletter" in path or "/abonnement" in path:
            return False
        if path.rstrip("/") == "/flash-infos":
            return False
        return bool(re.search(r"/article/|/flash-infos/.+", path))
    if source.name == "YahooFinance":
        return (
            (path.endswith(".html") and "/articles/" in path)
            or (path.startswith("/news/") and len(path.strip("/").split("/")) >= 2)
        )
    if source.name == "Investing":
        parts = path.strip("/").split("/")
        if len(parts) < 3 or parts[-1].isdigit():
            return False
        return path.startswith("/news/") and "-" in parts[-1]
    return False


def title_from_url(url: str) -> str:
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1]
    slug = re.sub(r"-\d{4}-\d{2}-\d{2}$", "", slug)
    return clean_text(slug.replace("-", " ").title())


def date_from_reuters_url(url: str) -> str | None:
    match = re.search(r"-(\d{4})-(\d{2})-(\d{2})/?$", url)
    if not match:
        return None
    year, month, day = map(int, match.groups())
    return isoformat_z(datetime(year, month, day, tzinfo=timezone.utc))


def scrape_article(
    source: Source,
    item: dict[str, str],
    content_max_chars: int,
    ingestion_mode: str,
) -> dict[str, Any]:
    url = item["url"]
    try:
        page = fetch_text(url)
    except (HTTPError, URLError, TimeoutError, UnicodeError) as exc:
        return build_record(source, item, error=str(exc), ingestion_mode=ingestion_mode)

    parser = ArticleParser()
    parser.feed(page)
    json_ld = parse_json_ld(parser)

    title = (
        clean_text(read_json_ld_text(json_ld.get("headline")))
        or clean_text(parser.meta.get("og:title"))
        or clean_text(" ".join(parser.h1_parts))
        or item.get("title", "")
    )
    subtitle = (
        clean_text(read_json_ld_text(json_ld.get("description")))
        or clean_text(parser.meta.get("og:description"))
        or clean_text(" ".join(parser.h2_parts))
    )
    author = read_json_ld_text(json_ld.get("author")) or parser.meta.get("author") or default_author(source)
    publication_timestamp = (
        parse_date(read_json_ld_text(json_ld.get("datePublished")))
        or parse_date(parser.meta.get("article:published_time"))
        or item.get("publication_timestamp")
        or None
    )
    category = (
        clean_text(read_json_ld_text(json_ld.get("articleSection")))
        or clean_text(parser.meta.get("article:section"))
        or infer_topic(title + " " + subtitle)
    )
    tags = extract_tags(title, subtitle, " ".join(parser.paragraphs))
    paragraphs = [clean_text(paragraph) for paragraph in parser.paragraphs]
    content = "\n\n".join(dedupe([p for p in paragraphs if len(p) > 40]))
    if not content:
        content = clean_text(parser.meta.get("og:description")) or subtitle
    display_content = truncate_to_sentence(content, content_max_chars)

    article = {
        "title": title,
        "subtitle": subtitle,
        "author": author,
        "publication_timestamp": publication_timestamp,
        "category": category,
        "tags": tags,
        "language": source.language,
        "url": url,
        "content": display_content,
        "summary": summarize(content or subtitle or title),
        "reading_time_minutes": reading_time(display_content),
    }
    return build_record(
        source,
        item,
        article=article,
        analytics_text=text_for_analysis(article, content),
        ingestion_mode=ingestion_mode,
    )


def build_record(
    source: Source,
    item: dict[str, str],
    article: dict[str, Any] | None = None,
    error: str | None = None,
    analytics_text: str | None = None,
    ingestion_mode: str = "streaming",
) -> dict[str, Any]:
    scraped_at = utc_now()
    article = article or {
        "title": item.get("title", ""),
        "subtitle": "",
        "author": default_author(source),
        "publication_timestamp": item.get("publication_timestamp") or None,
        "category": "",
        "tags": [],
        "language": source.language,
        "url": item.get("url", ""),
        "content": "",
        "summary": "",
        "reading_time_minutes": 0,
    }

    text_blob = analytics_text or text_for_analysis(article, article.get("content", ""))
    entities = extract_entities(text_blob)
    analytics = build_analytics(text_blob)
    quality = build_quality(article, error)
    article_id = make_article_id(source.name, article.get("url", ""), scraped_at)

    return {
        "metadata": {
            "article_id": article_id,
            "source": source.name,
            "source_country": source.country,
            "source_type": "financial_news",
            "scraping_timestamp": isoformat_z(scraped_at),
            "ingestion_mode": ingestion_mode,
            "pipeline_version": PIPELINE_VERSION,
            "collector_node": COLLECTOR_NODE,
        },
        "article": article,
        "entities": entities,
        "analytics": analytics,
        "quality": quality,
        "governance": {
            "raw_storage_path": raw_storage_path(source.name, scraped_at, article_id),
            "silver_processed": quality["is_valid"],
            "gold_aggregated": False,
            "lineage_id": "lineage_" + hashlib.sha1(article_id.encode()).hexdigest()[:12],
            "retention_policy": RETENTION_POLICY,
        },
    }


def default_author(source: Source) -> str:
    if source.name == "Reuters":
        return "Reuters Staff"
    if source.name == "LEconomiste":
        return "L'Economiste"
    if source.name == "YahooFinance":
        return "Yahoo Finance"
    if source.name == "Investing":
        return "Investing.com"
    return source.name


def make_article_id(source_name: str, url: str, scraped_at: datetime) -> str:
    slug = re.sub(r"[^a-z0-9]+", "_", source_name.lower()).strip("_")
    digest = hashlib.sha1(url.encode("utf-8")).hexdigest()[:6]
    return f"{slug}_{scraped_at:%Y%m%d}_{digest}"


def raw_storage_path(source_name: str, scraped_at: datetime, article_id: str) -> str:
    source_slug = re.sub(r"[^a-z0-9]+", "_", source_name.lower()).strip("_")
    return (
        f"/bronze/{source_slug}/{scraped_at:%Y/%m/%d/%H}/"
        f"{article_id}.json"
    )


def dedupe(values: list[str]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        key = value.lower()
        if key in seen:
            continue
        seen.add(key)
        output.append(value)
    return output


def summarize(content: str, max_sentences: int = 2) -> str:
    content = clean_text(content)
    if not content:
        return ""
    sentences = re.split(r"(?<=[.!?])\s+", content)
    return " ".join(sentences[:max_sentences])[:500]


def truncate_to_sentence(content: str, max_chars: int) -> str:
    content = clean_text(content)
    if max_chars <= 0 or len(content) <= max_chars:
        return content

    excerpt = content[:max_chars].rstrip()
    sentence_end = max(excerpt.rfind("."), excerpt.rfind("!"), excerpt.rfind("?"))
    if sentence_end >= max_chars * 0.55:
        excerpt = excerpt[: sentence_end + 1]
    else:
        word_end = excerpt.rfind(" ")
        if word_end > 0:
            excerpt = excerpt[:word_end].rstrip()
        excerpt += "..."
    return excerpt


def text_for_analysis(article: dict[str, Any], content: str) -> str:
    return " ".join(
        [
            article.get("title", ""),
            article.get("subtitle", ""),
            content,
        ]
    )


def reading_time(content: str) -> int:
    words = re.findall(r"\w+", content)
    return max(1, round(len(words) / 220)) if words else 0


def extract_tags(*parts: str, limit: int = 8) -> list[str]:
    text = " ".join(parts).lower()
    words = re.findall(r"[a-zA-ZÀ-ÿ]{4,}", text)
    counts = Counter(word for word in words if word not in STOPWORDS)
    return [word for word, _ in counts.most_common(limit)]


def extract_entities(text: str) -> dict[str, list[str]]:
    countries = sorted({country for country in COUNTRIES if re.search(rf"\b{re.escape(country)}\b", text, re.I)})
    currencies = sorted({currency.upper() for currency in CURRENCIES if re.search(rf"\b{re.escape(currency)}\b", text, re.I)})

    company_pattern = re.compile(
        r"\b([A-Z][A-Za-z&.-]+(?:\s+[A-Z][A-Za-z&.-]+){0,3}\s+"
        + r"(?:"
        + "|".join(re.escape(suffix) for suffix in COMPANY_SUFFIXES)
        + r"))\b"
    )
    companies = sorted(set(company_pattern.findall(text)))

    people_pattern = re.compile(r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\b")
    people = []
    for match in people_pattern.findall(text):
        if match not in companies and match not in COUNTRIES and not any(suffix in match for suffix in COMPANY_SUFFIXES):
            people.append(match)

    return {
        "countries_mentioned": countries[:20],
        "companies_mentioned": companies[:20],
        "currencies_mentioned": currencies[:20],
        "people_mentioned": sorted(set(people))[:20],
    }


def build_analytics(text: str) -> dict[str, Any]:
    text_lower = text.lower()
    words = re.findall(r"[a-zA-ZÀ-ÿ]{3,}", text_lower)
    counts = Counter(word for word in words if word not in STOPWORDS)

    positive = sum(counts[word] for word in POSITIVE_WORDS)
    negative = sum(counts[word] for word in NEGATIVE_WORDS)
    total = positive + negative
    sentiment_score = 0.0 if total == 0 else round((positive - negative) / total, 2)
    if sentiment_score > 0.15:
        sentiment = "positive"
    elif sentiment_score < -0.15:
        sentiment = "negative"
    else:
        sentiment = "neutral"

    content_words = len(words)
    return {
        "sentiment": sentiment,
        "sentiment_score": sentiment_score,
        "topic": infer_topic(text),
        "keyword_frequency": dict(counts.most_common(15)),
        "importance_score": round(min(1.0, 0.25 + content_words / 1200 + total / 20), 2),
    }


def infer_topic(text: str) -> str:
    topic_keywords = {
        "Oil Market": ["oil", "petrol", "pétrole", "energy", "brent", "crude"],
        "Stock Market": ["stocks", "shares", "equities", "bourse", "market", "nasdaq", "dow"],
        "Currencies": ["dollar", "euro", "currency", "forex", "dirham", "yen"],
        "Inflation": ["inflation", "prices", "cpi", "rate", "rates"],
        "Banking": ["bank", "fed", "central bank", "loan", "credit"],
        "Technology": ["ai", "chip", "software", "technology", "cloud"],
        "Economy": ["economy", "economic", "growth", "gdp", "pib"],
    }
    text_lower = text.lower()
    scores = {
        topic: sum(1 for keyword in keywords if keyword in text_lower)
        for topic, keywords in topic_keywords.items()
    }
    best_topic, best_score = max(scores.items(), key=lambda item: item[1])
    return best_topic if best_score else "Business"


def build_quality(article: dict[str, Any], error: str | None) -> dict[str, Any]:
    required = [
        "title",
        "publication_timestamp",
        "url",
        "content",
    ]
    missing = [field for field in required if not article.get(field)]
    if error:
        missing.append("fetch_error")
    content_length = len(article.get("content", ""))
    duplicate_score = 0.0
    completeness = 1 - (len(set(missing)) / (len(required) + 1))
    length_score = min(1.0, content_length / 1500) if content_length else 0.0
    quality_score = round(max(0.0, 0.6 * completeness + 0.4 * length_score), 2)

    return {
        "is_valid": not missing,
        "missing_fields": sorted(set(missing)),
        "content_length": content_length,
        "duplicate_score": duplicate_score,
        "quality_score": quality_score,
    }


def discover_items(source: Source, max_items: int) -> list[dict[str, str]]:
    items: list[dict[str, str]] = []
    seen: set[str] = set()

    feed_urls = EXTRA_FEED_URLS.get(source.name, [source.feed_url] if source.feed_url else [])
    for feed_url in feed_urls:
        if len(items) >= max_items:
            break
        for item in discover_from_feed(source, max_items, feed_url):
            if item["url"] not in seen:
                seen.add(item["url"])
                items.append(item)

    listing_urls = EXTRA_LISTING_URLS.get(source.name, [source.listing_url])
    for listing_url in listing_urls:
        if len(items) >= max_items:
            break
        for item in discover_from_page(source, max_items, listing_url):
            if item["url"] not in seen:
                seen.add(item["url"])
                items.append(item)

    if source.name == "Reuters" and len(items) < max_items:
        extra = discover_reuters_from_sitemap(max_items - len(items))
        items.extend(item for item in extra if item["url"] not in seen)
    if source.name == "LEconomiste" and len(items) < max_items:
        extra = discover_leconomiste_from_sitemap(max_items - len(items))
        items.extend(item for item in extra if item["url"] not in seen)
    return items[:max_items]


def scrape_sources(
    max_per_source: int,
    delay_seconds: float,
    content_max_chars: int,
    ingestion_mode: str,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for source in SOURCES:
        items = discover_items(source, max_per_source)
        for item in items:
            records.append(scrape_article(source, item, content_max_chars, ingestion_mode))
            time.sleep(delay_seconds)
    return records


def load_seen_urls(path: Path) -> set[str]:
    if not path.exists():
        return set()
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError):
        return set()
    if isinstance(data, list):
        return {str(url) for url in data}
    return set()


def save_seen_urls(path: Path, seen_urls: set[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(sorted(seen_urls), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def append_jsonl(path: Path, record: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as file:
        file.write(json.dumps(record, ensure_ascii=False) + "\n")


def post_event(webhook_url: str, record: dict[str, Any]) -> None:
    payload = json.dumps(record, ensure_ascii=False).encode("utf-8")
    request = Request(
        webhook_url,
        data=payload,
        headers={
            "Content-Type": "application/json; charset=utf-8",
            "User-Agent": USER_AGENT,
        },
        method="POST",
    )
    with urlopen(request, timeout=20) as response:
        response.read()


def create_kafka_producer(bootstrap_servers: str) -> Any:
    try:
        from kafka import KafkaProducer
    except ImportError as exc:
        raise RuntimeError("Kafka ingestion requires kafka-python. Install it with: pip install -r requirements.txt") from exc

    return KafkaProducer(
        bootstrap_servers=[server.strip() for server in bootstrap_servers.split(",") if server.strip()],
        key_serializer=lambda value: value.encode("utf-8"),
        value_serializer=lambda value: json.dumps(value, ensure_ascii=False).encode("utf-8"),
        api_version_auto_timeout_ms=5000,
        request_timeout_ms=10000,
        linger_ms=100,
    )


def publish_kafka_event(producer: Any, topic: str, record: dict[str, Any]) -> None:
    article_id = record["metadata"]["article_id"]
    producer.send(topic, key=article_id, value=record).get(timeout=30)


def run_streaming_poll(args: argparse.Namespace, seen_urls: set[str], kafka_producer: Any | None = None) -> int:
    emitted = 0
    for source in SOURCES:
        items = discover_items(source, args.max_per_source)
        for item in items:
            url = item["url"]
            if url in seen_urls:
                continue

            record = scrape_article(source, item, args.content_max_chars, "streaming")
            append_jsonl(args.event_output, record)
            if args.webhook_url:
                try:
                    post_event(args.webhook_url, record)
                except (HTTPError, URLError, TimeoutError, UnicodeError) as exc:
                    print(f"webhook_failed url={url} error={exc}", file=sys.stderr)
            if kafka_producer:
                try:
                    publish_kafka_event(kafka_producer, args.kafka_topic, record)
                except Exception as exc:
                    print(f"kafka_publish_failed url={url} topic={args.kafka_topic} error={exc}", file=sys.stderr)
                    continue

            seen_urls.add(url)
            emitted += 1
            time.sleep(args.delay_seconds)
    save_seen_urls(args.seen_state, seen_urls)
    return emitted


def timestamped_batch_output(output_dir: Path, output_format: str) -> Path:
    suffix = "json" if output_format == "json" else "jsonl"
    return output_dir / f"financial_news_batch_{utc_now():%Y%m%dT%H%M%SZ}.{suffix}"


def run_batch_once(args: argparse.Namespace, output: Path | None = None) -> int:
    records = scrape_sources(args.max_per_source, args.delay_seconds, args.content_max_chars, "batch")
    write_records(records, output or args.output, args.pretty, args.output_format)
    return len(records)


def write_records(records: list[dict[str, Any]], output: Path | None, pretty: bool, output_format: str) -> None:
    if output:
        output.parent.mkdir(parents=True, exist_ok=True)
        with output.open("w", encoding="utf-8") as file:
            if output_format == "json":
                json.dump(records, file, ensure_ascii=False, indent=2)
                file.write("\n")
            else:
                for record in records:
                    file.write(json.dumps(record, ensure_ascii=False) + "\n")
        return

    if pretty:
        print(json.dumps(records, ensure_ascii=False, indent=2))
        return

    for record in records:
        print(json.dumps(record, ensure_ascii=False))


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Scrape financial news into the requested JSON schema.")
    parser.add_argument(
        "--mode",
        choices=["once", "batch-hourly", "stream"],
        default="once",
        help="once runs one batch; batch-hourly repeats every hour; stream emits each newly discovered article as an event.",
    )
    parser.add_argument("--max-per-source", type=int, default=100, help="Maximum articles per source.")
    parser.add_argument(
        "--content-max-chars",
        type=int,
        default=1200,
        help="Maximum characters stored in article.content. Use 0 to keep full content.",
    )
    parser.add_argument("--delay-seconds", type=float, default=1.0, help="Delay between article requests.")
    parser.add_argument("--output", type=Path, help="Output JSONL file. Defaults to stdout.")
    parser.add_argument("--output-dir", type=Path, default=Path("data/batch"), help="Directory for hourly batch files.")
    parser.add_argument("--event-output", type=Path, default=Path("data/streaming_events.jsonl"), help="Streaming event JSONL output.")
    parser.add_argument("--seen-state", type=Path, default=Path("data/seen_articles.json"), help="State file for streaming deduplication.")
    parser.add_argument("--poll-seconds", type=int, default=60, help="Polling interval for stream mode.")
    parser.add_argument("--stream-once", action="store_true", help="Run one streaming poll and exit.")
    parser.add_argument("--webhook-url", help="Optional endpoint that receives each streaming article event as JSON.")
    parser.add_argument("--kafka-bootstrap-servers", help="Optional Kafka bootstrap servers, for example localhost:9092.")
    parser.add_argument("--kafka-topic", default="financial-news-raw", help="Kafka topic for streaming article events.")
    parser.add_argument(
        "--output-format",
        choices=["jsonl", "json"],
        default="jsonl",
        help="Use json for a single JSON array file, or jsonl for one JSON object per line.",
    )
    parser.add_argument("--pretty", action="store_true", help="Pretty-print JSON array to stdout.")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    try:
        if args.mode == "batch-hourly":
            while True:
                output = timestamped_batch_output(args.output_dir, args.output_format)
                count = run_batch_once(args, output)
                print(f"batch_written records={count} output={output}")
                time.sleep(3600)

        if args.mode == "stream":
            seen_urls = load_seen_urls(args.seen_state)
            kafka_producer = None
            if args.kafka_bootstrap_servers:
                kafka_producer = create_kafka_producer(args.kafka_bootstrap_servers)
            while True:
                emitted = run_streaming_poll(args, seen_urls, kafka_producer)
                print(f"stream_poll emitted={emitted} event_output={args.event_output}")
                if args.stream_once:
                    if kafka_producer:
                        kafka_producer.flush()
                        kafka_producer.close()
                    return 0
                time.sleep(args.poll_seconds)

        records = scrape_sources(args.max_per_source, args.delay_seconds, args.content_max_chars, "batch")
    except KeyboardInterrupt:
        return 130
    write_records(records, args.output, args.pretty, args.output_format)
    return 0


if __name__ == "__main__":
    sys.exit(main())
