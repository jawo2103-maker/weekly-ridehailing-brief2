import os, json, requests
from datetime import datetime, timedelta, timezone

OPENAI_API_KEY     = os.environ["OPENAI_API_KEY"]
TELEGRAM_BOT_TOKEN = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID   = os.environ["TELEGRAM_CHAT_ID"]
NEWSAPI_KEY        = os.environ.get("NEWSAPI_KEY", "")  # optional

def helsinki_now():
    # Simple DST-safe enough for summer runs; see README note to adjust cron in winter.
    return datetime.utcnow().replace(tzinfo=timezone.utc) + timedelta(hours=3)

def last_full_week_mon_sun(now_hki: datetime):
    day = now_hki.weekday()  # Mon=0..Sun=6
    last_sun = now_hki - timedelta(days=(day + 1))  # back to Sunday
    last_sun = last_sun.replace(hour=0, minute=0, second=0, microsecond=0)
    last_mon = last_sun - timedelta(days=6)
    return last_mon.date(), last_sun.date()

def to_display(d): return d.strftime("%d/%m/%Y")
def to_iso(d):     return d.strftime("%Y-%m-%d")

def fetch_news(from_iso: str, to_iso: str):
    if not NEWSAPI_KEY:
        return []
    url = "https://newsapi.org/v2/everything"
    q = ('("Uber" OR "Uber Technologies" OR "DiDi" OR "Didi Chuxing" OR 滴滴 OR '
         '"Bolt" OR "Taxify" OR "inDrive" OR "inDriver" OR "Cabify" OR "Yassir" OR '
         '"Heetch" OR "Grab" OR "Gojek") AND '
         '(ride OR driver OR mobility OR taxi OR regulation OR pricing OR safety OR expansion '
         'OR partnership OR investment OR funding OR strike)')
    params = {
        "q": q,
        "from": from_iso,
        "to": to_iso,
        "searchIn": "title,description,content",
        "sortBy": "publishedAt",
        "pageSize": 100,
        "language": "en",
    }
    headers = {"X-Api-Key": NEWSAPI_KEY}
    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    data = r.json()
    arts = data.get("articles", [])
    seen, out = set(), []
    for a in arts:
        url = (a.get("url") or "").split("?")[0].rstrip("/")
        title = (a.get("title") or "").strip()
        if not url or not title: continue
        if url in seen: continue
        seen.add(url)
        out.append({
            "title": title,
            "source": (a.get("source") or {}).get("name"),
            "published_at": a.get("publishedAt"),
            "url": (a.get("url") or "").strip(),
            "description": (a.get("description") or "").strip(),
        })
    out.sort(key=lambda x: x["published_at"] or "", reverse=True)
    return out[:60]

def chatgpt_brief(coverage_start_disp, coverage_end_disp, articles):
    import requests as rq
    system = "You are a concise industry analyst for ride-hailing."
    user = f"""
Generate a weekly competitor news brief for ride-hailing. Companies: Uber, DiDi (滴滴), Bolt, inDrive, Cabify, Yassir, Heetch, Grab, Gojek.

Coverage Window: {coverage_start_disp} – {coverage_end_disp}

If the JSON article list below is non-empty, use ONLY those links/headlines (do not invent). If empty, do your best, but prefer reputable media. Remove duplicates.

ARTICLES (JSON array):
{json.dumps(articles, ensure_ascii=False)}

Output structure EXACTLY:

<b>📌 Title:</b>
Weekly Competitor Brief — {coverage_end_disp}

<b>📌 Coverage Window:</b>
{coverage_start_disp} – {coverage_end_disp}

<b>📌 Top 10</b>
- Select the 10 most important, unique items from the article list.
- If a company has no coverage that week, simply omit them from the Top 10 (do NOT add a line saying "no significant news").
- Each item must be exactly two lines:
  News in one sentence — Source
  Link

<b>📌 Trend Takeaway</b>
One sentence capturing the dominant theme of the week.

Rules:
- No “Impact” scores.
- Keep headlines one sentence and neutral.
- Use real, working links (exact URLs provided if ARTICLES non-empty).
- If a company has no credible articles in the window, state that.
"""
    payload = {
        "model": "gpt-4o-mini",
        "messages": [{"role":"system","content":system},{"role":"user","content":user}],
        "temperature": 0.2,
        "max_tokens": 2500,
    }
    resp = rq.post(
        "https://api.openai.com/v1/chat/completions",
        headers={"Authorization": f"Bearer {OPENAI_API_KEY}"},
        json=payload, timeout=60
    )
    resp.raise_for_status()
    return resp.json()["choices"][0]["message"]["content"]

TG_API = "https://api.telegram.org/bot{token}/{method}"

def tg_send_text(text: str):
    def chunks(s, n=4096):
        i = 0
        while i < len(s):
            j = s.rfind("\n\n", i, i+n)
            if j == -1 or j <= i: j = min(i+n, len(s))
            else: j += 2
            yield s[i:j]; i = j
    for part in chunks(text):
        r = requests.post(
            TG_API.format(token=TELEGRAM_BOT_TOKEN, method="sendMessage"),
            json={"chat_id": TELEGRAM_CHAT_ID, "text": part, "parse_mode": "HTML", "disable_web_page_preview": False},
            timeout=30
        )
        r.raise_for_status()

def main():
    now_hki = helsinki_now()
    cov_start, cov_end = last_full_week_mon_sun(now_hki)
    cov_start_iso, cov_end_iso = to_iso(cov_start), to_iso(cov_end)
    cov_start_disp, cov_end_disp = to_display(cov_start), to_display(cov_end)
    articles = fetch_news(cov_start_iso, cov_end_iso)  # [] is fine
    brief = chatgpt_brief(cov_start_disp, cov_end_disp, articles)
    if "### Appendix" in brief:
        head, appendix = brief.split("### Appendix", 1)
        tg_send_text(head.strip())
        tg_send_text("### Appendix" + appendix.strip())
    else:
        tg_send_text(brief.strip())

if __name__ == "__main__":
    main()
