import os, re, json, time
import numpy as np
import pandas as pd
from jobspy import scrape_jobs
from pathlib import Path
import requests
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")

def tg_send(text: str, disable_web_page_preview=True) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,
        "disable_web_page_preview": disable_web_page_preview,
    }, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Telegram send failed: {r.status_code} {r.text}")

# ── iterate keywords ──
keywords = [
    "data engineer",
    "machine learning engineer",
    "analytics engineer",
    "ai engineer",
    "ml engineer",
    "ml/ai engineer",
    "data backend engineer",
]

all_jobs = []
for kw in keywords:
    df_kw = scrape_jobs(
        site_name=["linkedin"],
        search_term=kw,          # ← iterate here
        location="Germany",
        results_wanted=10,
        hours_old=48,
        country_indeed="Germany",
    )
    if not df_kw.empty:
        df_kw = df_kw.copy()
        df_kw["matched_keyword"] = kw
        all_jobs.append(df_kw)
    time.sleep(1)  # be polite / avoid bursts

jobs = pd.concat(all_jobs, ignore_index=True) if all_jobs else pd.DataFrame()
print(f"Found {len(jobs)} jobs across {len(keywords)} keywords")

# ── filters: English-only titles ──
def is_ascii(s: str) -> bool:
    try:
        (s or "").encode("ascii")
        return True
    except UnicodeEncodeError:
        return False

gender_marker_re = re.compile(r"\((?:m\/w\/d|w\/m\/d|d\/m\/w|gn\*?|all genders)\)", re.IGNORECASE)

if not jobs.empty:
    # keep only rows whose title contains ANY of the keywords (safety),
    # though we already searched per keyword
    patt = "|".join([f"\\b{re.escape(k.lower())}\\b" for k in keywords])
    df = jobs[jobs["title"].str.lower().str.contains(patt, regex=True, na=False)]

    df = df[df["title"].apply(is_ascii)]
    df = df[~df["title"].str.contains(gender_marker_re, na=False)]

    # de-dup (prefer job_url, fallback to id)
    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"])
    elif "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])
    df = df.reset_index(drop=True)

    # ── send to Telegram ──
    if df.empty:
        tg_send("No matching English job postings found.")
    else:
        for _, row in df.iterrows():
            title    = str(row.get("title") or "")
            company  = str(row.get("company") or "")
            location = str(row.get("location") or "")
            url      = str(row.get("job_url") or "")
            kw       = str(row.get("matched_keyword") or "")
            tg_send(f"{title}\n{company} — {location}\n{url}\n[{kw}]")

    # save raw combined results (dates→str)
    records = jobs.where(jobs.notna(), None).to_dict(orient="records")
    Path("/Users/hermann/Documents/personal-training/linkedin job/jobs_linkedin.json").write_text(
        json.dumps(records, indent=4, ensure_ascii=False, default=str),
        encoding="utf-8"
    )
else:
    tg_send("No jobs returned by JobSpy for the given keywords.")