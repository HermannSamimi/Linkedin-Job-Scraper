#!/usr/bin/env python3
import os, re, time, requests
import pandas as pd
from jobspy import scrape_jobs
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID   = os.getenv("TELEGRAM_CHAT_ID")   # @channelusername OR -100xxxxxxxxxx

def tg_send(text: str, disable_web_page_preview: bool = True) -> None:
    if not BOT_TOKEN or not CHAT_ID:
        raise RuntimeError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.")
    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    r = requests.post(url, json={
        "chat_id": CHAT_ID,
        "text": text,  # plain text (no Markdown headaches)
        "disable_web_page_preview": disable_web_page_preview,
    }, timeout=30)
    if not r.ok:
        raise RuntimeError(f"Telegram send failed: {r.status_code} {r.text}")

KEYWORDS = [
    "data engineer",
    "machine learning engineer",
    "analytics engineer",
    "ai engineer",
    "ml engineer",
    "ml/ai engineer",
    "data backend engineer",
    "data backend",
    "big data",
]
KW_LC = [k.lower() for k in KEYWORDS]

SEARCH_LOCATION = "Germany"
RESULTS_WANTED_PER_TERM = 10
HOURS_OLD = 1

# Simple German-language markers to exclude
GERMAN_PATTERNS = re.compile(
    r"(m/w/d|w/m/d|d/m/w|gn|werkstudent|doktorand|praktikum|gesundheitswesen|entwicklung|technik|berater|"
    r"ä|ö|ü|ß)",
    re.IGNORECASE,
)

def main():
    frames = []

    for kw in KEYWORDS:
        try:
            df_kw = scrape_jobs(
                site_name=["linkedin", "glassdoor", "indeed"],
                search_term=kw,
                location=SEARCH_LOCATION,
                results_wanted=RESULTS_WANTED_PER_TERM,
                hours_old=HOURS_OLD,
                country_indeed="Germany",
            )
        except Exception as e:
            print(f"[warn] scrape failed for '{kw}': {e}")
            continue

        if not df_kw.empty:
            df_kw = df_kw.copy()
            df_kw.columns = [str(c).strip().lower() for c in df_kw.columns]
            df_kw["matched_keyword"] = kw
            frames.append(df_kw)

        time.sleep(1)

    if not frames:
        print("No jobs returned by JobSpy for the given keywords.")
        return

    jobs = pd.concat(frames, ignore_index=True)

    # 1) Drop obvious German-language titles
    if "title" in jobs.columns:
        jobs = jobs.loc[~jobs["title"].str.contains(GERMAN_PATTERNS, na=False)].copy()

    # 2) Keep ONLY titles that contain ANY of the keywords (substring, case-insensitive)
    if "title" in jobs.columns:
        jobs = jobs[
            jobs["title"].astype(str).str.lower().apply(lambda t: any(k in t for k in KW_LC))
        ].copy()

    # 3) Select only the fields you want to send
    wanted_cols = ["title", "company", "location", "job_url", "site", "matched_keyword", "date_posted"]
    present_cols = [c for c in wanted_cols if c in jobs.columns]
    df = jobs[present_cols].reset_index(drop=True)

    # 4) De-dup on URL if available, else on id (optional)
    if "job_url" in df.columns:
        df = df.drop_duplicates(subset=["job_url"]).reset_index(drop=True)

    if df.empty:
        print("No matching English job postings found after filters.")
        return

    # 5) Send each as a Telegram message (only these 7 fields)
    for _, row in df.iterrows():
        title   = str(row.get("title") or "")
        company = str(row.get("company") or "")
        loc     = str(row.get("location") or "")
        url     = str(row.get("job_url") or "")
        site    = str(row.get("site") or "")
        kw      = str(row.get("matched_keyword") or "")
        posted  = str(row.get("date_posted") or "")

        msg = (
            f"{title}\n"
            f"{company} — {loc}\n"
            f"Site: {site} | Keyword: {kw}\n"
            f"Posted: {posted}\n"
            f"{url}"
        )
        tg_send(msg)

if __name__ == "__main__":
    main()