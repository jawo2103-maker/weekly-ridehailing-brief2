import os, json, requests, re, time
from datetime import datetime, timedelta, timezone

# --- ENV ---
OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
NEWSAPI_KEY        = os.environ.get("NEWSAPI_KEY", "")  # optional

# --- COMPANY QUERIES (used by RSS fallback) ---
COMPANIES = [
    ("Uber",    '"Uber" OR "Uber Technologies"'),
    ("DiDi",    '"Didi Chuxing" OR "DiDi" OR 滴滴'),
    ("Bolt",    '"Bolt" OR "Taxify"'),
    ("inDrive", '"inDrive" OR "inDriver"'),
    ("Cabify",  '"Cabify"'),
    ("Yassir",  '"Yassir"'),
    ("Heetch",  '"Heetch"'),
    ("Grab",    '"Grab"'),
    ("Gojek",   '"Gojek"'),
]

# --- TIME / COVERAGE (Europe/Helsinki with DST awareness if possible) ---
def helsinki_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Helsinki"))
    except Exception:
        # Fallback: UTC +3 (summer). Edit if you want +2 in winter manually.
        return datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=3)

def last_full_week_mon_sun(now_local: datetime):
    """
    Most recent FULL Monday–Sunday week BEFORE 'today' local date.
    If today is Monday, last week ended yesterday (Sunday).
    If today is Sunday, we go back to previous Sunday (exclude today).
    """
    today = now_local.date()
    wd = today.weekday()          # Mon=0..Sun=6
    last_sun = today - timedelta(days=wd + 1)  # go back to previous Sunday
    last_mon = last_sun - timedelta(days=6)    # Monday of that week
    return last_mon, last_sun

def to_display(d): return d.strftime("%d/%m/%Y")
def to_iso(d):     return d.strftime("%Y-%m-%d")

# --- FETCH: NewsAPI (if key present) ---
def fetch_news(from_iso: str, to_iso: str):
    if not NEWSAPI_KEY:
        return []
    url = "https://newsapi.org/v2/everything"
    # Strict for most brands + looser branch for Grab/Gojek to catch SEA items
    q = (
        '("Uber" OR "Uber Technologies" OR "DiDi" OR "Didi Chuxing" OR 滴滴 OR '
        '"Bolt" OR "Taxify" OR "inDrive" OR "inDriver" OR "Cabify" OR "Yassir" OR "Heetch") '
        'AND (ride OR driver OR mobility OR taxi OR regulation OR pricing OR safety OR expansion '
        'OR partnership OR investment OR funding OR strike)'
        ' OR ("Grab" OR "Gojek")'
    )
    params = {
        "q": q,
        "from": from_iso,
        "to": to_iso,
        "searchIn": "title,description,content",
        "sortBy": "publishedAt",
        "pageSize": 100,
        "language": "en",  # remove if you want multilingual from NewsAPI too
    }
    headers = {"X-Api-Key": NEWSAPI_KEY}
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    arts = data.get("articles", [])
    out = []
    for a in arts:
        url_ = (a.get("url") or "").split("?")[0].rstrip("/")
        title = (a.get("title") or "").strip()
        if not url_ or not title:
            continue
        out.append({
            "title": title,
            "source": (a.get("source") or {}).get("name"),
            "published_at": a.get("publishedAt"),
            "url": url_,
            "description": (a.get("description") or "").strip(),
        })
    return out

# --- FETCH: Google News RSS (no key). Skips silently if feedparser missing. ---
def fetch_google_news_rss(from_dt_utc, to_dt_utc):
    try:
        import feedparser  # type: ignore
    except Exception:
        return []

    results = []
    # SEA-friendly locales first (SG/ID), then a general US/EN feed
    locales = [
        ("en-SG", "SG"),   # Singapore
        ("en-ID", "ID"),   # Indonesia
        ("en-US", "US"),   # Global-ish
    ]
    base = "https://news.google.com/rss/search?q={query}%20when:7d&hl={hl}&gl={gl}&ceid={gl}:{hl_code}"

    for name, query in COMPANIES:
        q_enc = requests.utils.quote(query)
        for hl, gl in locales:
            hl_code = hl.split("-")[-1]
            url = base.format(query=q_enc, hl=hl, gl=gl, hl_code=hl_code)
            feed = feedparser.parse(url)

            for e in feed.entries:
                # Published time
                pub = None
                if getattr(e, "published_parsed", None):
                    pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                elif getattr(e, "updated_parsed", None):
                    pub = datetime(*e.updated_parsed[:6], tzinfo=timezone.utc)
                if not pub:
                    continue
                # Filter to coverage window
                if not (from_dt_utc <= pub <= to_dt_utc + timedelta(days=1)):
                    continue

                link = (getattr(e, "link", "") or "").split("?")[0].rstrip("/")
                title = (getattr(e, "title", "") or "").strip()
                if not link or not title:
                    continue

                # outlet name: try 'source.title', else domain fallback
                src = None
                src_tag = getattr(e, "source", None)
                if src_tag and hasattr(src_tag, "title"):
                    src = src_tag.title
                if not src:
                    try:
                        src = link.split("/")[2]
                    except Exception:
                        src = "Source"

                results.append({
                    "title": title,
                    "source": src,
                    "published_at": pub.isoformat(),
                    "url": link,
                    "description": "",
                })
            time.sleep(0.15)  # be polite

    return results

# --- MERGE / DEDUPE ---
def merge_dedupe(articles):
    seen = set()
    out = []
    for a in articles:
        url = (a.get("url") or "").split("?")[0].rstrip("/")
        title = (a.get("title") or "").strip().lower()
        key = url or title
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(a)
    # newest first, cap to 60 (token budget)
    out.sort(key=lambda x: x.get("published_at") or "", reverse=True)
    return out[:60]

def fetch_articles(from_iso: str, to_iso: str):
    arts = []
    # NewsAPI
    try:
        arts += fetch_news(from_iso, to_iso)
    except Exception:
        pass
    # RSS fallback (UTC window)
    try:
        from_dt_utc = datetime.fromisoformat(from_iso + "T00:00:00+00:00")
        to_dt_utc   = datetime.fromisoformat(to_iso   + "T23:59:59+00:00")
        arts += fetch_google_news_rss(from_dt_utc, to_dt_utc)
    except Exception:
        pass
    return merge_dedupe(arts)

# --- Post-processor: inline links if model ever outputs URL on next line ---
def inline_bullet_links(text: str) -> str:
    """
    Converts:
      ➡️ Headline — Source
      https://example.com
    into:
      ➡️ <a href="https://example.com">Headline</a> — Source
    Also converts [text](url) to HTML anchors.
    """
    lines = text.splitlines()
    out = []
    i = 0
    url_re = re.compile(r'^https?://\S+$')
    md_re  = re.compile(r'\[([^\]]+)\]\((https?://[^)]+)\)')

    while i < len(lines):
        line = md_re.sub(r'<a href="\2">\1</a>', lines[i])
        if line.strip().startswith('➡️') and i + 1 < len(lines) and url_re.match(lines[i+1].strip()):
            url = lines[i+1].strip()
            if ' — ' in line:
                head, tail = line.split(' — ', 1)
                bullet = '➡️ '
                headline = head[len(bullet):].strip() if head.startswith(bullet) else head.strip()
                line = f'➡️ <a href="{url}">{headline}</a> — {tail}'
                i += 2
                out.append(line)
                continue
        out.append(line)
        i += 1
    return "\n".join(out)

# --- OPENAI ---
def chatgpt_brief(coverage_start_disp, coverage_end_disp, articles):
    system = "You are a concise industry analyst for ride-hailing."

    user = f"""
Generate a weekly competitor news brief for ride-hailing. Companies: Uber, DiDi (滴滴), Bolt, inDrive, Cabify, Yassir, Heetch, Grab, Gojek.

Coverage Window: {coverage_start_disp} – {coverage_end_disp}

Use ONLY the articles in the JSON array below. Do not invent links. Remove duplicates.
If there are very few items, fewer than 15 is fine.

ARTICLES (JSON array):
{json.dumps(articles, ensure_ascii=False)}

Output EXACTLY:

<b>📌 Title:</b>
Weekly Competitor Brief — {coverage_end_disp}

<b>📌 Coverage Window:</b>
{coverage_start_disp} – {coverage_end_disp}

<b>📌 Top 15</b>
- Select up to 15 important, unique items from the article list (aim for 15 if available; fewer is OK if there aren’t enough credible items).
- If a company has no coverage, omit it (do NOT add a “no news” line).
- Each item must be a single line using HTML link format (no separate "Link" line, no Markdown):
  ➡️ <a href="URL">News in one sentence</a> — Source

<b>📌 Trend Takeaway</b>
One sentence capturing the dominant theme of the week.

Rules:
- Prefer original/authoritative outlets when duplicates exist, but aggregators (e.g., Biztoc) are allowed if that is the only or timeliest available link.
- Do not invent links.
- Keep headlines one sentence and neutral.
- Use the exact URLs from the JSON (no shortening or changing domains).
- Output ONLY the sections above, in this order, with the same bold HTML headers.
"""
    payload = {
        "model": "gpt-4o-mini",
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ],
        "temperature": 0.2,
        "max_tokens": 2500,
    }
    resp = requests.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json=payload, timeout=90
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]

# --- TELEGRAM ---
TG_API = "https://api.telegram.org/bot{token}/{method}"

def tg_send_text(text: str):
    # Telegram max 4096 chars; split gracefully on paragraph boundaries
    def chunks(s, n=4096):
        i = 0
        while i < len(s):
            j = s.rfind("\n\n", i, i+n)
            if j == -1 or j <= i:
                j = min(i+n, len(s))
            else:
                j += 2
            yield s[i:j]
            i = j

    for part in chunks(text):
        r = requests.post(
            TG_API.format(token=TELEGRAM_BOT_TOKEN, method="sendMessage"),
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": part,
                "parse_mode": "HTML",
                "disable_web_page_preview": False  # set True if you prefer no previews
            },
            timeout=30
        )
        r.raise_for_status()

# --- ENTRYPOINT ---
def main():
    now_local = helsinki_now()
    cov_start, cov_end = last_full_week_mon_sun(now_local)
    cov_start_iso, cov_end_iso = to_iso(cov_start), to_iso(cov_end)
    cov_start_disp, cov_end_disp = to_display(cov_start), to_display(cov_end)

    articles = fetch_articles(cov_start_iso, cov_end_iso)
    print(f"Coverage: {cov_start_disp} – {cov_end_disp} | Articles fetched: {len(articles)}")

    brief = chatgpt_brief(cov_start_disp, cov_end_disp, articles)
    brief = inline_bullet_links(brief)  # safety: convert any stray raw URLs into inline anchors

    # Send as a single message (chunked if over Telegram limit)
    tg_send_text(brief.strip())

if __name__ == "__main__":
    main()
