import os, json, requests, re, time
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher

# --- ENV ---
OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
NEWSAPI_KEY        = os.environ.get("NEWSAPI_KEY", "")  # optional

# --- COMPANIES & PATTERNS ---
COMPANY_PATTERNS = {
    "Uber":    [r"\buber\b", r"\buber technologies\b"],
    "DiDi":    [r"\bdidi\b", r"\bdidi chuxing\b", r"滴滴", r"\b99 app\b", r"\b99\b"],
    "Bolt":    [r"\bbolt\b", r"\btaxify\b"],
    "inDrive": [r"\bindrive\b", r"\bindriver\b"],
    "Cabify":  [r"\bcabify\b"],
    "Yassir":  [r"\byassir\b"],
    "Heetch":  [r"\bheetch\b"],
    "Grab":    [r"\bgrab\b"],
    "Gojek":   [r"\bgojek\b", r"\bgo-jek\b"],
}
COMPANIES = list(COMPANY_PATTERNS.keys())

# A few aggregator domains (we still allow them; we just prefer non-aggregators when de-duping)
AGGREGATOR_DOMAINS = {
    "biztoc.com",
    "news.google.com",
    "news.yahoo.com", "finance.yahoo.com",
    "flipboard.com",
}

# --- TIME / COVERAGE ---
def helsinki_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Helsinki"))
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=3)

def last_full_week_mon_sun(now_local: datetime):
    """
    Most recent FULL Monday–Sunday week BEFORE 'today' local date.
    If today is Monday, last week ended yesterday (Sunday).
    """
    today = now_local.date()
    wd = today.weekday()  # Mon=0..Sun=6
    last_sun = today - timedelta(days=wd + 1)
    last_mon = last_sun - timedelta(days=6)
    return last_mon, last_sun

def to_display(d): return d.strftime("%d/%m/%Y")
def to_iso(d):     return d.strftime("%Y-%m-%d")

# --- FETCH: NewsAPI (if key present) ---
def fetch_news(from_iso: str, to_iso: str):
    if not NEWSAPI_KEY:
        return []
    url = "https://newsapi.org/v2/everything"
    # Strict for most brands + looser branch for Grab/Gojek to catch SEA items.
    # Multilingual (no 'language' param).
    q = (
        '("Uber" OR "Uber Technologies" OR "DiDi" OR "Didi Chuxing" OR 滴滴 OR '
        '"Bolt" OR "Taxify" OR "inDrive" OR "inDriver" OR "Cabify" OR "Yassir" OR "Heetch") '
        'AND (ride OR driver OR mobility OR taxi OR regulation OR pricing OR safety OR expansion '
        'OR partnership OR investment OR funding OR strike OR launch OR city OR country OR rollout '
        'OR product OR feature OR EV OR electric OR autonomous OR robotaxi OR licensing OR licence)'
        ' OR ("Grab" OR "Gojek")'
    )
    params = {
        "q": q,
        "from": from_iso,
        "to": to_iso,
        "searchIn": "title,description,content",
        "sortBy": "publishedAt",
        "pageSize": 100,
        # no "language": allow multi-lingual
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

# --- FETCH: Google News RSS (no key). Requires 'feedparser' in workflow. ---
def fetch_google_news_rss(from_dt_utc, to_dt_utc):
    try:
        import feedparser  # type: ignore
    except Exception:
        return []

    results = []
    # SEA/Africa/EU/LatAm broad locales
    locales = [
        ("en-SG", "SG"),  # Singapore
        ("en-ID", "ID"),  # Indonesia
        ("en-GB", "GB"),
        ("en-ZA", "ZA"),
        ("en-KE", "KE"),
        ("en-NG", "NG"),
        ("es-ES", "ES"),
        ("pt-BR", "BR"),
        ("fr-FR", "FR"),
        ("ru-RU", "RU"),
    ]
    base = "https://news.google.com/rss/search?q={query}%20when:7d&hl={hl}&gl={gl}&ceid={gl}:{hl_code}"

    company_queries = [
        '"Uber" OR "Uber Technologies"',
        '"Didi Chuxing" OR "DiDi" OR 滴滴 OR "99 App" OR "99"',
        '"Bolt" OR "Taxify"',
        '"inDrive" OR "inDriver"',
        '"Cabify"',
        '"Yassir"',
        '"Heetch"',
        '"Grab"',
        '"Gojek" OR "Go-Jek"',
    ]

    for q in company_queries:
        q_enc = requests.utils.quote(q)
        for hl, gl in locales:
            hl_code = hl.split("-")[-1]
            url = base.format(query=q_enc, hl=hl, gl=gl, hl_code=hl_code)
            feed = feedparser.parse(url)
            for e in feed.entries:
                pub = None
                if getattr(e, "published_parsed", None):
                    pub = datetime(*e.published_parsed[:6], tzinfo=timezone.utc)
                elif getattr(e, "updated_parsed", None):
                    pub = datetime(*e.updated_parsed[:6], tzinfo=timezone.utc)
                if not pub:
                    continue
                if not (from_dt_utc <= pub <= to_dt_utc + timedelta(days=1)):
                    continue
                link = (getattr(e, "link", "") or "").split("?")[0].rstrip("/")
                title = (getattr(e, "title", "") or "").strip()
                if not link or not title:
                    continue
                # source
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
            time.sleep(0.1)  # polite pause
    return results

# --- CLASSIFY & FILTER ---
VIOLENCE_BLACKLIST = [
    "assault", "punched", "punch", "stab", "stabbing", "murder", "killed", "kill",
    "rape", "sexual assault", "molest", "robbery", "beaten", "beating",
]

def text_of(a):
    return f"{a.get('title','')} {a.get('description','')} {a.get('url','')}".lower()

def has_any(text, keywords):
    return any(k in text for k in keywords)

def tag_companies(a):
    txt = text_of(a)
    tags = []
    for cname, patterns in COMPANY_PATTERNS.items():
        for pat in patterns:
            if re.search(pat, txt, flags=re.I):
                tags.append(cname)
                break
    return tags

def is_business_relevant(a):
    t = text_of(a)
    if has_any(t, VIOLENCE_BLACKLIST):
        return False
    # Keep other topics; fetching already biases toward business items.
    return True

def domain_of(url):
    try:
        return url.split("/")[2].lower()
    except Exception:
        return ""

def norm_title(s):
    s = s.lower()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def similar(a, b, threshold=0.86):
    return SequenceMatcher(None, a, b).ratio() >= threshold

def sort_authority_first(arts):
    # Prefer non-aggregators; then newer items
    def key(a):
        d = domain_of(a.get("url",""))
        is_agg = 1 if d in AGGREGATOR_DOMAINS else 0
        ts = a.get("published_at") or ""
        # Non-aggregator (0) sorts before aggregator (1); then by published desc
        return (is_agg, -int("".join(filter(str.isdigit, ts)) or "0"))
    # Can't convert ts reliably to int for all formats; we will just return is_agg only.
    # But to keep a stable result, we'll do a two-step: non-agg first; then preserve original order.
    non_agg = [a for a in arts if domain_of(a.get("url","")) not in AGGREGATOR_DOMAINS]
    agg     = [a for a in arts if domain_of(a.get("url","")) in AGGREGATOR_DOMAINS]
    return non_agg + agg

def merge_dedupe_with_similarity(articles):
    # Prefer non-aggregators first
    articles = sort_authority_first(articles)

    kept = []
    seen_keys = set()

    for a in articles:
        url = (a.get("url") or "").split("?")[0].rstrip("/")
        title = (a.get("title") or "").strip()
        tnorm = norm_title(title)

        key = url or tnorm
        if key in seen_keys:
            continue

        dup = False
        for b in kept:
            burl = (b.get("url") or "").split("?")[0].rstrip("/")
            btitle = (b.get("title") or "").strip()
            btnorm = norm_title(btitle)
            # If same domain and titles very similar OR titles just very similar overall, treat as dup
            if domain_of(url) == domain_of(burl) and similar(tnorm, btnorm, 0.83):
                dup = True; break
            if similar(tnorm, btnorm, 0.90):
                dup = True; break
        if dup:
            continue

        seen_keys.add(key)
        kept.append(a)

    # newest-first order is already approximate; keep current
    return kept

def limit_per_company(articles, max_per=7):
    counts = {c: 0 for c in COMPANIES}
    selected = []
    for a in articles:
        tags = a.get("companies") or []
        # If no tags, keep it without affecting caps
        if not tags:
            selected.append(a)
            continue
        # Skip if any tagged company already reached cap
        if any(counts.get(c, 0) >= max_per for c in tags):
            continue
        # Increment caps for tagged companies
        for c in tags:
            if c in counts:
                counts[c] += 1
        selected.append(a)
    return selected

def fetch_articles(from_iso: str, to_iso: str):
    arts = []
    # NewsAPI
    try:
        arts += fetch_news(from_iso, to_iso)
    except Exception:
        pass
    # RSS fallback
    try:
        from_dt_utc = datetime.fromisoformat(from_iso + "T00:00:00+00:00")
        to_dt_utc   = datetime.fromisoformat(to_iso   + "T23:59:59+00:00")
        arts += fetch_google_news_rss(from_dt_utc, to_dt_utc)
    except Exception:
        pass

    # Classify, filter, dedupe
    # 1) business filter + tag companies
    filtered = []
    for a in arts:
        if not is_business_relevant(a):
            continue
        a["companies"] = tag_companies(a)
        filtered.append(a)

    # 2) stronger dedupe (URL + fuzzy titles, prefer non-aggregators)
    filtered = merge_dedupe_with_similarity(filtered)

    # 3) enforce per-company cap
    capped = limit_per_company(filtered, max_per=7)

    # 4) cap overall to protect tokens
    capped = capped[:100]
    return capped

# --- INLINE LINK SAFETY (kept, just in case) ---
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

# --- FIT TO ONE TELEGRAM MESSAGE ---
def truncate_to_one_message(text: str, hard_limit=4096):
    """
    Ensures a single Telegram message:
    1) If text <= limit, return as is.
    2) Else, progressively shorten anchor inner text for bullets.
    3) If still too long, drop bullets from the end until it fits.
    """
    if len(text) <= hard_limit:
        return text

    lines = text.splitlines()
    # Identify bullet lines (start with '➡️ ')
    bullet_idx = [i for i, ln in enumerate(lines) if ln.strip().startswith('➡️ ')]

    # Step 1: shrink anchor inner text on bullets
    def shrink_line(line, max_head_len):
        # Find anchor <a href="...">HEADLINE</a>
        m = re.search(r'(<a href="[^"]+">)(.+?)(</a>)(\s+—\s+.+)$', line)
        if not m:
            return line
        pre, head, post, tail = m.groups()
        if len(head) <= max_head_len:
            return line
        head = head[:max_head_len].rstrip() + "…"
        return pre + head + post + tail

    max_len = 140
    while len("\n".join(lines)) > hard_limit and max_len >= 80:
        for i in bullet_idx:
            lines[i] = shrink_line(lines[i], max_len)
        max_len -= 10

    # Step 2: drop bullets from the end if still too long
    while len("\n".join(lines)) > hard_limit and bullet_idx:
        drop_i = bullet_idx.pop()  # last bullet
        lines.pop(drop_i)

    return "\n".join(lines)

# --- OPENAI ---
def chatgpt_brief(coverage_end_disp, articles):
    system = "You are a concise industry analyst for ride-hailing."

    user = f"""
Generate a weekly competitor news brief for ride-hailing. Companies: Uber, DiDi (滴滴), Bolt, inDrive, Cabify, Yassir, Heetch, Grab, Gojek.

Use ONLY the articles in the JSON array below. Do not invent links. Remove duplicates before writing.
Prioritize business-relevant items: launches, new cities/countries, expansion, partnerships, M&A, funding/financing, pricing/regulatory changes, product/feature rollouts, EV/AV/robotaxi.
Avoid personal violent incidents (e.g., assault, murder). Regulatory/antitrust cases are allowed.

ARTICLES (JSON array; each item may include a "companies" array with detected tags):
{json.dumps(articles, ensure_ascii=False)}

Output EXACTLY:

<b>📌 Weekly Competitor Brief — {coverage_end_disp}</b>
––––
<b>📌 Top 15</b>
- Select up to 15 important, unique items from the article list (aim for 15 if available; fewer is OK if there aren’t enough credible items).
- Limit to at most 7 items per single company in this section.
- Each item must be ONE line using HTML link format (no raw URLs, no Markdown), and end with the company tag:
  ➡️ <a href="URL">News in one sentence</a> — Source — Company
  (If multiple companies apply, join with "/" — e.g., Uber/Grab)
––––
<b>📌 Trend Takeaway</b>
One sentence capturing the dominant theme of the week.

Rules:
- Prefer original/authoritative outlets when duplicates exist, but aggregators (e.g., Biztoc) are allowed if that is the only or timeliest available link.
- Do not invent links.
- Keep headlines one sentence and neutral.
- Use the exact URLs from the JSON (no shortening or changing domains).
- Output ONLY the sections above, in this order, with the same bold HTML headers and separators.
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

def tg_send_text_single(text: str):
    # Always send AS ONE MESSAGE (after truncation if needed)
    r = requests.post(
        TG_API.format(token=TELEGRAM_BOT_TOKEN, method="sendMessage"),
        json={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
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
    cov_end_disp = to_display(cov_end)  # date in title only

    articles = fetch_articles(cov_start_iso, cov_end_iso)
    print(f"Coverage: {to_display(cov_start)} – {to_display(cov_end)} | Articles fetched (pre-model): {len(articles)}")

    brief = chatgpt_brief(cov_end_disp, articles)
    brief = inline_bullet_links(brief)              # safety: convert stray URLs to anchors
    brief = truncate_to_one_message(brief.strip())  # ensure a single Telegram message

    tg_send_text_single(brief)

if __name__ == "__main__":
    main()
