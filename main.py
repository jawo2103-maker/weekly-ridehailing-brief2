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

# Aggregators are allowed, but we prefer original outlets when de-duping
AGGREGATOR_DOMAINS = {
    "biztoc.com", "news.google.com", "news.yahoo.com", "finance.yahoo.com", "flipboard.com"
}

# --- TIME / COVERAGE ---
def helsinki_now():
    try:
        from zoneinfo import ZoneInfo
        return datetime.now(ZoneInfo("Europe/Helsinki"))
    except Exception:
        return datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=3)

def last_full_week_mon_sun(now_local: datetime):
    today = now_local.date()
    wd = today.weekday()  # Mon=0..Sun=6
    last_sun = today - timedelta(days=wd + 1)
    last_mon = last_sun - timedelta(days=6)
    return last_mon, last_sun

def to_display(d): return d.strftime("%d/%m/%Y")
def to_iso(d):     return d.strftime("%Y-%m-%d")

# --- FETCH: NewsAPI (multilingual; if key present) ---
def fetch_news(from_iso: str, to_iso: str):
    if not NEWSAPI_KEY:
        return []
    url = "https://newsapi.org/v2/everything"
    # Strict for most brands + looser branch for Grab/Gojek to catch SEA items. Multilingual (no 'language' param).
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

# --- FETCH: Google News RSS (no key; requires feedparser) ---
def fetch_google_news_rss(from_dt_utc, to_dt_utc):
    try:
        import feedparser  # type: ignore
    except Exception:
        return []
    results = []
    # SEA/Africa/EU/LatAm/India
    locales = [
        ("en-SG", "SG"), ("en-ID", "ID"), ("en-GB", "GB"), ("en-IN", "IN"),
        ("en-ZA", "ZA"), ("en-KE", "KE"), ("en-NG", "NG"),
        ("es-ES", "ES"), ("es-MX", "MX"), ("es-CO", "CO"),
        ("pt-BR", "BR"), ("fr-FR", "FR"), ("fr-MA", "MA"),
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
            time.sleep(0.1)
    return results

# --- CLASSIFY & FILTER ---
INCIDENT_BLACKLIST = [
    # accidents/crimes/personal incidents
    "assault", "punched", "punch", "stab", "stabbing", "murder", "killed", "kill",
    "rape", "sexual assault", "molest", "robbery", "beaten", "beating",
    "accident", "crash", "collision", "injured", "injury", "fatal", "death", "dead",
    "explosion", "fire", "shooting", "homicide",
]

STUDY_REPORT_TERMS = [
    "study", "studies", "research", "report", "whitepaper", "survey", "analysis",
    "finds", "reveals", "indicates"
]

COMMERCIAL_WHITELIST = [
    "launch", "expansion", "expand", "opens", "opening", "enters", "entry", "rollout",
    "city", "cities", "country", "countries", "region", "market",
    "merger", "acquisition", "acquires", "m&a", "deal", "contract",
    "funding", "raises", "raise", "investment", "invests", "round",
    "partnership", "partners", "alliance", "collaboration",
    "ipo", "listing", "valuation", "revenue", "profit", "earnings",
    "pricing", "fare", "regulation", "license", "licence", "permit", "approval",
    "product", "feature", "safety feature", "ev", "electric", "autonomous", "robotaxi"
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
    # Exclude incidents & personal-violence items
    if has_any(t, INCIDENT_BLACKLIST):
        return False
    # Exclude generic studies/reports unless there is a commercial/regulatory action
    if has_any(t, STUDY_REPORT_TERMS) and not has_any(t, COMMERCIAL_WHITELIST):
        return False
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

def similar(a, b, threshold=0.80):
    return SequenceMatcher(None, a, b).ratio() >= threshold

def merge_dedupe_with_similarity(articles):
    # Prefer non-aggregators first, then newest-ish order (we don't enforce strict date sort here)
    non_agg = [a for a in articles if domain_of(a.get("url","")) not in AGGREGATOR_DOMAINS]
    agg     = [a for a in articles if domain_of(a.get("url","")) in AGGREGATOR_DOMAINS]
    ordered = non_agg + agg

    kept = []
    for a in ordered:
        url = (a.get("url") or "").split("?")[0].rstrip("/")
        title = (a.get("title") or "").strip()
        tnorm = norm_title(title)

        # Drop if essentially duplicate of a kept item (by URL or by similar title)
        dup = False
        for b in kept:
            burl = (b.get("url") or "").split("?")[0].rstrip("/")
            btitle = (b.get("title") or "").strip()
            btnorm = norm_title(btitle)
            # Same-domain near-duplicate (looser threshold)
            if domain_of(url) == domain_of(burl) and similar(tnorm, btnorm, 0.75):
                dup = True; break
            # Cross-domain near-duplicate
            if similar(tnorm, btnorm, 0.85):
                dup = True; break
        if dup:
            continue

        kept.append(a)
    return kept

def limit_per_company(articles, max_per=7):
    counts = {c: 0 for c in COMPANIES}
    selected = []
    for a in articles:
        tags = a.get("companies") or []
        if not tags:
            # drop items without a known company tag to avoid model guessing
            continue
        if any(counts.get(c, 0) >= max_per for c in tags):
            continue
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

    # Filter business relevance & tag companies
    filtered = []
    for a in arts:
        if not is_business_relevant(a):
            continue
        a["companies"] = tag_companies(a)
        filtered.append(a)

    # Stronger de-dup
    filtered = merge_dedupe_with_similarity(filtered)

    # Enforce per-company cap BEFORE model
    capped = limit_per_company(filtered, max_per=7)

    # Protect tokens
    return capped[:120]

# --- OUTPUT POST-PROCESS (extra safety) ---
ANCHOR_RE = re.compile(r'<a href="([^"]+)">', re.IGNORECASE)

def fix_bullet_prefixes(text: str) -> str:
    # Replace any leading "- ➡️" with "➡️ "
    lines = text.splitlines()
    for i, ln in enumerate(lines):
        s = ln.lstrip()
        if s.startswith("- ➡️"):
            # keep original left-space indentation minimal; produce canonical
            lines[i] = "➡️ " + s[len("- ➡️"):].lstrip()
    return "\n".join(lines)

def dedupe_output_bullets(text: str) -> str:
    lines = text.splitlines()
    out, seen_urls = [], set()
    for ln in lines:
        s = ln.strip()
        if s.startswith("➡️"):
            m = ANCHOR_RE.search(s)
            if m:
                url = m.group(1)
                if url in seen_urls:
                    continue
                seen_urls.add(url)
        out.append(ln)
    return "\n".join(out)

def enforce_output_company_cap(text: str, max_per=7) -> str:
    """
    Inside the Top 15 block, ensure no company exceeds cap.
    We detect company tags at the end of the line: " — Source — Company[/Company]"
    Excess bullets (beyond cap) are dropped from the end-first.
    """
    lines = text.splitlines()
    # locate Top block by headers/separators
    try:
        top_idx = next(i for i, ln in enumerate(lines) if ln.strip().startswith("<b>📌 Top"))
    except StopIteration:
        return text
    try:
        # Top block ends at next separator "––––" or Trend header
        end_idx = next(i for i, ln in enumerate(lines[top_idx+1:], start=top_idx+1)
                       if ln.strip() == "––––" or ln.strip().startswith("<b>📌 Trend"))
    except StopIteration:
        end_idx = len(lines)

    company_counts = {c: 0 for c in COMPANIES}
    new_section = []
    for i in range(top_idx+1, end_idx):
        ln = lines[i]
        s = ln.strip()
        if not s.startswith("➡️"):
            new_section.append(ln)
            continue
        # extract company label after last " — "
        parts = s.split(" — ")
        if len(parts) < 3:
            # malformed; keep but don't count
            new_section.append(ln)
            continue
        comp_field = parts[-1]
        comps = [c.strip() for c in comp_field.split("/") if c.strip()]
        # If company unknown, drop (avoid guessing)
        if not any(c in COMPANIES for c in comps):
            continue
        # Enforce caps
        over = any(company_counts.get(c, 0) >= max_per for c in comps if c in COMPANIES)
        if over:
            continue
        for c in comps:
            if c in company_counts:
                company_counts[c] += 1
        new_section.append(ln)

    # rebuild text
    rebuilt = lines[:top_idx+1] + new_section + lines[end_idx:]
    return "\n".join(rebuilt)

def filter_out_incident_and_study_bullets(text: str) -> str:
    """Extra safety on final text: drop bullets that look like incidents or generic studies."""
    lines = text.splitlines()
    out = []
    for ln in lines:
        s = ln.strip().lower()
        if s.startswith("➡️"):
            # incident filter
            if any(k in s for k in INCIDENT_BLACKLIST):
                continue
            # study/report filter unless commercial whitelist present
            if any(k in s for k in STUDY_REPORT_TERMS) and not any(k in s for k in COMMERCIAL_WHITELIST):
                continue
        out.append(ln)
    return "\n".join(out)

# --- FIT TO ONE TELEGRAM MESSAGE ---
def truncate_to_one_message(text: str, hard_limit=4096):
    """
    Single Telegram message guarantee:
    1) If <= limit, return as is.
    2) Shrink anchor headline text for bullets progressively.
    3) If still long, drop bullets from the end.
    """
    if len(text) <= hard_limit:
        return text

    lines = text.splitlines()
    bullet_idx = [i for i, ln in enumerate(lines) if ln.strip().startswith('➡️ ')]

    def shrink_line(line, max_head_len):
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

    while len("\n".join(lines)) > hard_limit and bullet_idx:
        drop_i = bullet_idx.pop()  # drop last bullet
        lines.pop(drop_i)

    return "\n".join(lines)

# --- OPENAI ---
def chatgpt_brief(coverage_end_disp, articles):
    system = "You are a concise industry analyst for ride-hailing."

    user = f"""
Generate a weekly competitor news brief for ride-hailing. Companies: Uber, DiDi (滴滴), Bolt, inDrive, Cabify, Yassir, Heetch, Grab, Gojek.

Use ONLY the articles in the JSON array below. Do not invent links. Remove duplicates before writing.
Include only commercial/strategic items: launches, new cities/countries, expansion, partnerships, M&A, funding/financing, pricing/regulatory changes, product/feature rollouts, EV/AV/robotaxi.
Exclude accidents, crimes, and personal incidents. Exclude generic studies/reports unless they announce a specific commercial/regulatory action.
Use the provided "companies" tags for the Company label; do NOT guess. If an item has no company tag, skip it.

ARTICLES (JSON array; each item may include a "companies" array with detected tags):
{json.dumps(articles, ensure_ascii=False)}

Output EXACTLY:

<b>📌 Weekly Competitor Brief — {coverage_end_disp}</b>
––––
<b>📌 Top 15</b>
- Select up to 15 important, unique items (aim for 15; fewer is OK if not enough credible items).
- Do NOT exceed 7 items for any single company.
- Each item must be ONE line using HTML link format (no raw URLs, no Markdown), and end with the company tag(s):
  ➡️ <a href="URL">News in one sentence</a> — Source — Company
  (If multiple companies apply, join with "/" — e.g., Uber/Grab)
––––
<b>📌 Trend Takeaway</b>
One sentence capturing the dominant theme of the week; reference at least two different companies from Top 15.

Rules:
- Prefer original/authoritative outlets when duplicates exist; aggregators (e.g., Biztoc) are allowed if they are the only or timeliest link.
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
    cov_end_disp = to_display(cov_end)  # date for title

    articles = fetch_articles(cov_start_iso, cov_end_iso)
    print(f"Coverage: {to_display(cov_start)} – {to_display(cov_end)} | Articles fetched (pre-model): {len(articles)}")

    # Generate brief
    brief = chatgpt_brief(cov_end_disp, articles)

    # Output safeties
    brief = fix_bullet_prefixes(brief)                 # ensure bullets use "➡️ " only
    brief = dedupe_output_bullets(brief)               # remove duplicate URL bullets
    brief = enforce_output_company_cap(brief, 7)       # enforce max 7 per company in final list
    brief = filter_out_incident_and_study_bullets(brief)  # drop any stray incident/study bullets
    # (Model already asked to inline anchors; keep fallback just in case)
    brief = truncate_to_one_message(brief.strip())     # ensure single Telegram message

    tg_send_text_single(brief)

if __name__ == "__main__":
    main()
