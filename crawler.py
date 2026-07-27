#!/usr/bin/env python3
import argparse
import asyncio
import re
import sqlite3
import time
import urllib.robotparser
from collections import Counter, defaultdict
from urllib.parse import urljoin, urlparse

import aiohttp
from bs4 import BeautifulSoup

DB_PATH = "search_index.db"
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"

# Vyanzo mbalimbali vya kimataifa na vya Kiswahili vya kuanzia kama SQLite ipo tupu
DEFAULT_SEEDS = [
    "https://sw.wikipedia.org/wiki/Mwanzo",
    "https://en.wikipedia.org/wiki/Main_Page",
    "https://www.bbc.com/swahili",
    "https://www.un.org/sw/",
    "https://www.dw.com/sw/",
    "https://news.un.org/sw/",
    "https://www.voaswahili.com/",
]

STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "and", "or", "of", "to",
    "in", "on", "for", "with", "this", "that", "it", "as", "at", "by",
    "na", "ya", "wa", "za", "la", "kwa", "kwenye", "ni", "au", "katika", "pia"
}


def init_db(path=DB_PATH):
    conn = sqlite3.connect(path)
    cur = conn.cursor()
    
    # 1. Unda meza za msingi kama hazipo
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pages (
            url TEXT PRIMARY KEY,
            domain TEXT,
            title TEXT,
            snippet TEXT
        )
    """)
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS word_index (
            word TEXT,
            url TEXT,
            freq INTEGER,
            PRIMARY KEY (word, url)
        )
    """)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_word ON word_index(word)")
    
    cur.execute("""
        CREATE TABLE IF NOT EXISTS queue (
            url TEXT PRIMARY KEY,
            domain TEXT,
            status TEXT DEFAULT 'pending',
            depth INTEGER DEFAULT 0
        )
    """)

    # 2. Hapa tumerkebisha: Ongeza column ya 'domain' kiotomatiki kama database ilikuwa ya zamani
    try:
        cur.execute("ALTER TABLE queue ADD COLUMN domain TEXT")
    except sqlite3.OperationalError:
        pass  # Column tayari ipo

    try:
        cur.execute("ALTER TABLE pages ADD COLUMN domain TEXT")
    except sqlite3.OperationalError:
        pass  # Column tayari ipo

    # 3. Jaza domain kwenye rows zote za zamani ambazo domain ni NULL
    cur.execute("SELECT url FROM queue WHERE domain IS NULL")
    rows = cur.fetchall()
    for row in rows:
        u = row[0]
        d = urlparse(u).netloc
        cur.execute("UPDATE queue SET domain = ? WHERE url = ?", (d, u))

    cur.execute("SELECT url FROM pages WHERE domain IS NULL")
    rows_pages = cur.fetchall()
    for row in rows_pages:
        u = row[0]
        d = urlparse(u).netloc
        cur.execute("UPDATE pages SET domain = ? WHERE url = ?", (d, u))

    # 4. Unda Index mpya kwa ajili ya usambazaji wa haraka (Domain Balancing)
    cur.execute("CREATE INDEX IF NOT EXISTS idx_queue_domain_status ON queue(domain, status)")
    
    conn.commit()
    return conn


def tokenize(text):
    words = re.findall(r"[a-zA-Z\u00c0-\u024f]+", text.lower())
    return [w for w in words if len(w) > 2 and w not in STOPWORDS]


def extract_page(html_content, url):
    try:
        soup = BeautifulSoup(html_content, "lxml")
    except Exception:
        soup = BeautifulSoup(html_content, "html.parser")

    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()

    title = soup.title.string.strip() if soup.title and soup.title.string else url
    body_text = soup.get_text(separator=" ", strip=True)
    snippet = body_text[:200]

    links = set()
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if href.startswith(("mailto:", "javascript:", "tel:", "#")):
            continue
            
        full_url = urljoin(url, href)
        parsed = urlparse(full_url)
        if parsed.scheme in ("http", "https"):
            full_url = full_url.split("#")[0]
            links.add(full_url)

    return title, body_text, snippet, links


class FastGlobalCrawler:
    def __init__(self, seed_urls, max_pages=5000, max_depth=5, concurrency=15, delay=0.1, any_domain=True):
        self.conn = init_db()
        self.max_pages = max_pages
        self.max_depth = max_depth
        self.concurrency = concurrency
        self.delay = delay
        self.any_domain = any_domain
        self.pages_crawled = 0
        self.robots_cache = {}

        # Weka Seed URLs kama queue ipo tupu
        if seed_urls or DEFAULT_SEEDS:
            seeds = seed_urls if seed_urls else DEFAULT_SEEDS
            cur = self.conn.cursor()
            for u in seeds:
                dom = urlparse(u).netloc
                cur.execute("INSERT OR IGNORE INTO queue (url, domain, status, depth) VALUES (?, ?, 'pending', 0)", (u, dom))
            self.conn.commit()

        self.allowed_domains = {urlparse(u).netloc for u in seed_urls} if seed_urls else set()

    def get_balanced_batch(self, batch_size):
        """ Inachagua URLs kutoka domain tofauti tofauti ili kuzuia kukwama kwenye tovuti moja """
        cur = self.conn.cursor()
        
        # Pata domain zenye pending URLs
        cur.execute("SELECT DISTINCT domain FROM queue WHERE status = 'pending' LIMIT 50")
        domains = [r[0] for r in cur.fetchall()]
        
        if not domains:
            return []

        selected_rows = []
        per_domain_limit = max(1, batch_size // len(domains))

        for dom in domains:
            cur.execute(
                "SELECT url, domain, depth FROM queue WHERE domain = ? AND status = 'pending' LIMIT ?",
                (dom, per_domain_limit)
            )
            rows = cur.fetchall()
            selected_rows.extend(rows)
            if len(selected_rows) >= batch_size:
                break

        # Badilisha status kuwa 'processing'
        for u, d, dp in selected_rows:
            cur.execute("UPDATE queue SET status = 'processing' WHERE url = ?", (u,))
        self.conn.commit()

        return selected_rows

    async def fetch_page(self, session, url, depth):
        domain = urlparse(url).netloc
        
        if not self.any_domain and domain not in self.allowed_domains:
            return url, domain, False, None, None, None, set(), depth

        headers = {"User-Agent": USER_AGENT}
        try:
            async with session.get(url, headers=headers, timeout=aiohttp.ClientTimeout(total=6)) as response:
                if response.status != 200 or "text/html" not in response.headers.get("Content-Type", ""):
                    return url, domain, False, None, None, None, set(), depth

                html = await response.text()
                title, body_text, snippet, links = extract_page(html, url)
                words = tokenize(title + " " + body_text)
                freqs = Counter(words)

                return url, domain, True, title, snippet, freqs, links, depth
        except Exception:
            return url, domain, False, None, None, None, set(), depth

    def save_results(self, results):
        cur = self.conn.cursor()
        for url, domain, success, title, snippet, freqs, links, depth in results:
            if success:
                # Hifadhi ukurasa
                cur.execute("INSERT OR REPLACE INTO pages (url, domain, title, snippet) VALUES (?, ?, ?, ?)", (url, domain, title, snippet))

                # Hifadhi maneno
                word_data = [(w, url, f) for w, f in freqs.items()]
                cur.executemany("INSERT OR REPLACE INTO word_index (word, url, freq) VALUES (?, ?, ?)", word_data)

                # Hifadhi viungo vipya (Queue)
                if depth < self.max_depth:
                    link_data = [(l, urlparse(l).netloc, depth + 1) for l in links]
                    cur.executemany("INSERT OR IGNORE INTO queue (url, domain, status, depth) VALUES (?, ?, 'pending', ?)", link_data)

                self.pages_crawled += 1
                print(f"[{self.pages_crawled}/{self.max_pages}] 🌐 [{domain[:15]}] ⚡ {url[:70]}")

            cur.execute("UPDATE queue SET status = 'visited' WHERE url = ?", (url,))
        
        self.conn.commit()

    async def run(self):
        print(f"🚀 Crawler ya Kimataifa Inaanza... Workers: {self.concurrency}")
        
        connector = aiohttp.TCPConnector(limit=self.concurrency, ssl=False)
        async with aiohttp.ClientSession(connector=connector) as session:
            while self.pages_crawled < self.max_pages:
                batch_size = min(self.concurrency, self.max_pages - self.pages_crawled)
                batch = self.get_balanced_batch(batch_size)

                if not batch:
                    print("\n✅ Foleni imekwisha! Hakuna URLs mpya za kuchukua.")
                    break

                tasks = [self.fetch_page(session, url, depth) for url, domain, depth in batch]
                results = await asyncio.gather(*tasks)

                self.save_results(results)

                if self.delay > 0:
                    await asyncio.sleep(self.delay)

        self.conn.close()
        print(f"\n🏁 Upekuzi Umekamilika! Jumla ya kurasa zilizohifadhiwa: {self.pages_crawled}")


def search(query, limit=10):
    conn = init_db()
    cur = conn.cursor()
    terms = tokenize(query)

    if not terms:
        print("Tafadhali andika neno la kutafuta.")
        return

    placeholders = ",".join("?" * len(terms))
    cur.execute(
        f"""
        SELECT p.url, p.title, p.snippet, SUM(w.freq) as score
        FROM word_index w
        JOIN pages p ON p.url = w.url
        WHERE w.word IN ({placeholders})
        GROUP BY w.url
        ORDER BY score DESC
        LIMIT ?
        """,
        (*terms, limit),
    )
    results = cur.fetchall()
    conn.close()

    if not results:
        print(f"Hakuna matokeo yaliyopatikana kwa: '{query}'")
        return

    print(f"\n🔍 Matokeo ya '{query}':\n")
    for i, (url, title, snippet, score) in enumerate(results, 1):
        print(f"{i}. {title}  (score: {score})")
        print(f"   {url}")
        print(f"   {snippet}...\n")


def main():
    parser = argparse.ArgumentParser(description="Global Balanced Speed Crawler")
    sub = parser.add_subparsers(dest="command", required=True)

    p_crawl = sub.add_parser("crawl", help="Tembelea kurasa na uunde index")
    p_crawl.add_argument("urls", nargs="*", help="Seed URL(s)")
    p_crawl.add_argument("--max-pages", type=int, default=5000)
    p_crawl.add_argument("--max-depth", type=int, default=5)
    p_crawl.add_argument("--delay", type=float, default=0.1)
    p_crawl.add_argument("--any-domain", action="store_true")
    p_crawl.add_argument("--workers", type=int, default=15)

    p_search = sub.add_parser("search", help="Tafuta kwenye index")
    p_search.add_argument("query")
    p_search.add_argument("--limit", type=int, default=10)

    args = parser.parse_args()

    if args.command == "crawl":
        crawler = FastGlobalCrawler(
            seed_urls=args.urls,
            max_pages=args.max_pages,
            max_depth=args.max_depth,
            concurrency=args.workers,
            delay=args.delay,
            any_domain=args.any_domain
        )
        asyncio.run(crawler.run())
    elif args.command == "search":
        search(args.query, limit=args.limit)


if __name__ == "__main__":
    main()