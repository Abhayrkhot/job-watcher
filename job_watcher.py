#!/usr/bin/env python3
"""Poll public job feeds and email newly discovered matching roles."""

from __future__ import annotations

import hashlib
import html
import json
import os
import re
import smtplib
import sqlite3
import ssl
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from email.message import EmailMessage
from datetime import datetime, timezone
from pathlib import Path


BASE_DIR = Path(__file__).resolve().parent
STATE_DIR = BASE_DIR / "state"
DB_PATH = STATE_DIR / "jobs.sqlite3"
BOARDS_PATH = STATE_DIR / "ats_boards.json"
ATS_BATCH_SIZE = 200
AGGREGATOR_INTERVAL_SECONDS = 15 * 60
HEARTBEAT_EXPECTED_SECONDS = 5 * 60
HEARTBEAT_GRACE_SECONDS = 10 * 60
MAX_JOB_AGE_SECONDS = 12 * 60 * 60
SUPPORTED_PROVIDERS = {"ashby", "greenhouse", "lever", "lever-eu", "smartrecruiters"}
JOB_FAMILY_TERMS = (
    "software engineer",
    "software developer",
    "software development engineer",
    "backend engineer",
    "back-end engineer",
    "frontend engineer",
    "front-end engineer",
    "full stack engineer",
    "full-stack engineer",
    "data engineer",
    "analytics engineer",
    "data analyst",
    "machine learning engineer",
    "ml engineer",
    "mlops engineer",
    "machine learning operations",
    "ai engineer",
    "ai platform engineer",
    "llm engineer",
    "generative ai engineer",
    "inference engineer",
    "nlp engineer",
    "computer vision engineer",
    "perception engineer",
    "robotics software engineer",
    "gpu engineer",
    "cuda engineer",
    "compiler engineer",
    "performance engineer",
    "data scientist",
    "decision scientist",
    "applied scientist",
    "research engineer",
    "research scientist",
    "platform engineer",
    "cloud engineer",
    "infrastructure engineer",
    "devops engineer",
    "site reliability engineer",
    "database engineer",
    "storage engineer",
    "distributed systems engineer",
    "security engineer",
    "qa automation engineer",
    "software development engineer in test",
    "sdet",
    "forward deployed engineer",
    "forward-deployed engineer",
    "solutions engineer",
    "technical solutions engineer",
    "business intelligence engineer",
    "product data analyst",
)
ENTRY_TITLE_TERMS = (
    "new grad",
    "new college grad",
    "university grad",
    "early career",
    "entry level",
    "entry-level",
    "junior",
    "associate software",
    "engineer i",
    "engineer 1",
    "developer i",
    "level 1",
    "level i",
    "analyst i",
    "analyst 1",
    "scientist i",
    "scientist 1",
    "early talent",
    "campus hire",
)
EXCLUDED_TERMS = (
    "intern",
    "internship",
    "senior",
    "staff",
    "principal",
    "manager",
    "director",
    "professor",
    "technician",
)
US_STATE_CODES = (
    "AL|AK|AZ|AR|CA|CO|CT|DE|FL|GA|HI|ID|IL|IN|IA|KS|KY|LA|ME|MD|MA|MI|"
    "MN|MS|MO|MT|NE|NV|NH|NJ|NM|NY|NC|ND|OH|OK|OR|PA|RI|SC|SD|TN|TX|UT|"
    "VT|VA|WA|WV|WI|WY|DC"
)
US_LOCATION_RE = re.compile(rf"(?:,|\s)\s*(?:{US_STATE_CODES})(?:\b|$)", re.I)
US_LOCATION_TERMS = (
    "united states",
    "usa",
    "u.s.",
    "remote in us",
    "remote - us",
    "us remote",
    "new york city",
    "nyc",
    "san francisco",
    "sf",
    "washington, dc",
)
CANADA_LOCATION_TERMS = (
    "canada",
    "remote in canada",
    "remote - canada",
    "canadian",
)
ENTRY_DESCRIPTION_RE = re.compile(
    r"\b(?:new|recent) (?:college )?graduate\b|\bearly career\b|"
    r"\bentry[ -]level\b|\b0\s*(?:-|to)\s*2 years\b|"
    r"\b1\s*(?:-|to)\s*2 years\b|\b(?:up to|less than) 2 years\b|"
    r"\bno (?:prior |professional )?experience (?:is )?required\b",
    re.I,
)
CITIZENSHIP_REQUIREMENT_RE = re.compile(
    r"\b(?:us\s+citizen(?:s|ship)?|u\.s\.\s+citizen(?:s|ship)?|"
    r"citizenship\s+required|citizenship\s+is\s+required|"
    r"must\s+be\s+(?:a\s+|an\s+)?citizen(?:s|ship)?|"
    r"only\s+(?:us\s+)?citizens?|"
    r"must\s+be\s+(?:a\s+|an\s+)?us\s+citizen(?:s|ship)?)\b",
    re.I | re.S,
)


def is_us_location(locations: list[str]) -> bool:
    lowered_locations = [str(value).lower() for value in locations]
    if any(
        any(term == location or term in location for term in CANADA_LOCATION_TERMS)
        for location in lowered_locations
    ):
        return False
    return any(
        US_LOCATION_RE.search(location)
        or any(term == location or term in location for term in US_LOCATION_TERMS)
        for location in lowered_locations
    )


def description_mentions_citizenship_requirement(description: str) -> bool:
    if CITIZENSHIP_REQUIREMENT_RE.search(description):
        return False
    return True


def potential_match(job: dict) -> bool:
    if not job.get("active") or not job.get("is_visible", True):
        return False
    if not job_is_recent(job):
        return False
    title = str(job.get("title", "")).lower()
    if any(term in title for term in EXCLUDED_TERMS):
        return False
    locations = job.get("locations") or []
    description = str(job.get("description", ""))
    role_match = any(term in title for term in JOB_FAMILY_TERMS)
    entry_match = any(term in title for term in ENTRY_TITLE_TERMS) or bool(
        ENTRY_DESCRIPTION_RE.search(description)
    )
    return role_match and entry_match and is_us_location(locations)


def matches(job: dict) -> bool:
    return potential_match(job) and description_mentions_citizenship_requirement(
        str(job.get("description", ""))
    )


def stable_id(job: dict) -> str:
    value = str(job.get("id") or job.get("url") or json.dumps(job, sort_keys=True))
    return hashlib.sha256(value.encode()).hexdigest()


def canonical_url(job: dict) -> str:
    raw_url = str(job.get("url", ""))
    if not raw_url:
        return ""
    parsed = urllib.parse.urlsplit(raw_url)
    path = parsed.path.rstrip("/")
    if path.endswith("/application"):
        path = path.removesuffix("/application")
    query = urllib.parse.parse_qsl(parsed.query, keep_blank_values=True)
    query = [(key, value) for key, value in query if not key.lower().startswith("utm_")]
    return urllib.parse.urlunsplit(
        (parsed.scheme.lower(), parsed.netloc.lower(), path, urllib.parse.urlencode(query), "")
    )


def job_fingerprint(job: dict) -> str:
    fields = [
        str(job.get("company_name", "")),
        str(job.get("title", "")),
        *sorted(str(location) for location in job.get("locations") or []),
    ]
    normalized = "|".join(re.sub(r"[^a-z0-9]+", " ", field.lower()).strip() for field in fields)
    return hashlib.sha256(normalized.encode()).hexdigest()


def connect_db() -> sqlite3.Connection:
    STATE_DIR.mkdir(exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.execute(
        "CREATE TABLE IF NOT EXISTS seen_jobs "
        "(id TEXT PRIMARY KEY, first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS seen_urls "
        "(url TEXT PRIMARY KEY, first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS initialized_boards "
        "(board_key TEXT PRIMARY KEY, initialized_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS seen_fingerprints "
        "(fingerprint TEXT PRIMARY KEY, first_seen TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS board_stats ("
        "board_key TEXT PRIMARY KEY, last_checked INTEGER NOT NULL, "
        "relevant_count INTEGER NOT NULL DEFAULT 0, match_count INTEGER NOT NULL DEFAULT 0, "
        "failure_count INTEGER NOT NULL DEFAULT 0)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS source_checks "
        "(source TEXT PRIMARY KEY, last_checked INTEGER NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS heartbeat "
        "(id INTEGER PRIMARY KEY CHECK (id = 1), last_run INTEGER NOT NULL)"
    )
    return connection


def job_seen(connection: sqlite3.Connection, job: dict) -> bool:
    if connection.execute(
        "SELECT 1 FROM seen_jobs WHERE id = ?", (stable_id(job),)
    ).fetchone():
        return True
    if connection.execute(
        "SELECT 1 FROM seen_fingerprints WHERE fingerprint = ?", (job_fingerprint(job),)
    ).fetchone():
        return True
    url = canonical_url(job)
    return bool(url and connection.execute(
        "SELECT 1 FROM seen_urls WHERE url = ?", (url,)
    ).fetchone())


def record_jobs(connection: sqlite3.Connection, jobs: list[dict]) -> None:
    connection.executemany(
        "INSERT OR IGNORE INTO seen_jobs(id) VALUES (?)",
        [(stable_id(job),) for job in jobs],
    )
    connection.executemany(
        "INSERT OR IGNORE INTO seen_urls(url) VALUES (?)",
        [(url,) for job in jobs if (url := canonical_url(job))],
    )
    connection.executemany(
        "INSERT OR IGNORE INTO seen_fingerprints(fingerprint) VALUES (?)",
        [(job_fingerprint(job),) for job in jobs],
    )


def load_boards() -> list[dict]:
    boards = json.loads(BOARDS_PATH.read_text()) if BOARDS_PATH.exists() else []
    return [board for board in boards if board.get("provider") in SUPPORTED_PROVIDERS]


def board_interval_seconds(relevant_count: int, match_count: int) -> int:
    if match_count > 0 or relevant_count >= 3:
        return 5 * 60
    if relevant_count > 0:
        return 30 * 60
    return 2 * 60 * 60


def select_due_boards(
    connection: sqlite3.Connection,
    boards: list[dict],
    now: int | None = None,
) -> list[dict]:
    current_time = int(time.time()) if now is None else now
    stats = {
        row[0]: {"last_checked": row[1], "relevant_count": row[2], "match_count": row[3]}
        for row in connection.execute(
            "SELECT board_key, last_checked, relevant_count, match_count FROM board_stats"
        )
    }
    due = []
    for board in boards:
        board_key = f'{board["provider"]}:{board["slug"]}'
        board_stats = stats.get(board_key)
        if board_stats is None:
            next_check = 0
        else:
            next_check = board_stats["last_checked"] + board_interval_seconds(
                board_stats["relevant_count"], board_stats["match_count"]
            )
        if next_check <= current_time:
            due.append((next_check, board_key, board))
    due.sort(key=lambda item: (item[0], item[1]))
    return [item[2] for item in due[:ATS_BATCH_SIZE]]


def record_board_success(
    connection: sqlite3.Connection,
    board: dict,
    relevant_count: int,
    match_count: int,
    checked_at: int,
) -> None:
    board_key = f'{board["provider"]}:{board["slug"]}'
    connection.execute(
        "INSERT INTO board_stats(board_key, last_checked, relevant_count, match_count, failure_count) "
        "VALUES (?, ?, ?, ?, 0) ON CONFLICT(board_key) DO UPDATE SET "
        "last_checked=excluded.last_checked, relevant_count=excluded.relevant_count, "
        "match_count=excluded.match_count, failure_count=0",
        (board_key, checked_at, relevant_count, match_count),
    )


def record_board_failure(
    connection: sqlite3.Connection, board: dict, checked_at: int
) -> None:
    board_key = f'{board["provider"]}:{board["slug"]}'
    connection.execute(
        "INSERT INTO board_stats(board_key, last_checked, failure_count) VALUES (?, ?, 1) "
        "ON CONFLICT(board_key) DO UPDATE SET "
        "last_checked=excluded.last_checked, failure_count=failure_count+1",
        (board_key, checked_at),
    )


def plain_text(value: str) -> str:
    decoded = html.unescape(html.unescape(value or ""))
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", " ", decoded)).strip()


def parse_job_datetime(value: object) -> int | None:
    if value in (None, ""):
        return None
    if isinstance(value, (int, float)):
        return int(value / 1000 if value > 10**12 else value)
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        numeric = int(text)
        return int(numeric / 1000 if numeric > 10**12 else numeric)
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return int(parsed.timestamp())


def experience_summary(job: dict) -> str:
    title = str(job.get("title", "")).lower()
    description = str(job.get("description", "")).lower()
    text = f"{title} {description}"
    if any(term in text for term in ("new grad", "new graduate", "graduate", "entry level", "entry-level", "early career", "campus hire")):
        return "New grad / entry level"
    match = re.search(
        r"\b(?:0\s*(?:-|to)\s*2|1\s*(?:-|to)\s*2|2\s*(?:-|to)\s*4)\s*years?\b",
        text,
    )
    if match:
        return match.group(0).replace("  ", " ")
    if "junior" in text or "associate" in text or "i" in title.split():
        return "Junior / entry level"
    return "Entry level"


def relative_posted_time(job: dict, now: int | None = None) -> str:
    current_time = int(time.time()) if now is None else now
    posted_at = job_posted_timestamp(job)
    if posted_at is None:
        return "Unknown"
    age = max(current_time - posted_at, 0)
    if age < 60:
        return "Just now"
    minutes = age // 60
    if minutes < 60:
        return f"{minutes}m ago"
    hours = minutes // 60
    return f"{hours}h ago"


def role_category(job: dict) -> str:
    title = str(job.get("title", "")).lower()
    description = str(job.get("description", "")).lower()
    text = f"{title} {description}"
    categories = [
        ("robotics", (
            "robotics", "perception engineer", "autonomy", "control systems",
        )),
        ("ml", (
            "machine learning engineer", "ml engineer", "mlops", "ml ops",
            "ml platform", "ml infrastructure", "ml research",
        )),
        ("ai", (
            "ai engineer", "artificial intelligence", "genai", "gen ai",
            "llm", "inference engineer", "nlp engineer", "prompt", "rag",
        )),
        ("data_science", (
            "data scientist", "decision scientist", "applied scientist",
            "research scientist", "scientist", "experiment", "ab test",
        )),
        ("data", (
            "data engineer", "analytics engineer", "data analyst",
            "business intelligence", "bi engineer", "analytics",
        )),
        ("sde", (
            "software engineer", "software developer", "backend engineer",
            "back-end engineer", "frontend engineer", "front-end engineer",
            "full stack engineer", "full-stack engineer", "platform engineer",
            "infra engineer", "infrastructure engineer", "devops engineer",
            "site reliability engineer", "distributed systems engineer",
            "security engineer", "qa automation engineer", "sdet",
        )),
    ]
    for label, terms in categories:
        if any(term in text for term in terms):
            return label
    return "other"


def job_posted_timestamp(job: dict) -> int | None:
    for key in (
        "posted_at",
        "published_at",
        "publishedAt",
        "updated_at",
        "updatedAt",
        "created_at",
        "createdAt",
        "date_posted",
        "datePosted",
        "released_at",
        "releasedAt",
        "created",
        "releasedDate",
        "releaseDate",
        "postedDate",
    ):
        timestamp = parse_job_datetime(job.get(key))
        if timestamp is not None:
            return timestamp
    return None


def job_is_recent(job: dict, now: int | None = None) -> bool:
    current_time = int(time.time()) if now is None else now
    posted_at = job_posted_timestamp(job)
    if posted_at is None:
        return False
    age = current_time - posted_at
    return 0 <= age <= MAX_JOB_AGE_SECONDS


def company_from_slug(slug: str) -> str:
    return re.sub(r"[-_]+", " ", slug).title()


def direct_job(
    board: dict,
    job_id: object,
    title: str,
    locations: list[str],
    url: str,
    description: str,
    posted_at: object | None = None,
) -> dict:
    board_key = f'{board["provider"]}:{board["slug"]}'
    return {
        "source": "direct-ats",
        "board_key": board_key,
        "id": f"{board_key}:{job_id}",
        "title": title,
        "company_name": company_from_slug(board["slug"]),
        "locations": [location for location in locations if location],
        "url": url,
        "description": description,
        "posted_at": posted_at,
        "active": True,
        "is_visible": True,
    }


def fetch_json(url: str, data: bytes | None = None) -> object:
    headers = {
        "Accept": "application/json",
        "User-Agent": "personal-new-grad-job-watcher/1.0",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        url,
        data=data,
        headers=headers,
        method="POST" if data is not None else "GET",
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)


def fetch_direct_board(board: dict) -> tuple[dict, list[dict]]:
    slug = urllib.parse.quote(board["slug"], safe="")
    provider = board["provider"]
    normalized = []
    if provider == "greenhouse":
        payload = fetch_json(
            f"https://boards-api.greenhouse.io/v1/boards/{slug}/jobs?content=true"
        )
        for item in payload.get("jobs", []):
            normalized.append(direct_job(
                board,
                item.get("id"),
                str(item.get("title", "")),
                [str((item.get("location") or {}).get("name", ""))],
                str(item.get("absolute_url", "")),
                plain_text(str(item.get("content", ""))),
                item.get("updated_at"),
            ))
    elif provider in {"lever", "lever-eu"}:
        domain = "api.eu.lever.co" if provider == "lever-eu" else "api.lever.co"
        payload = fetch_json(f"https://{domain}/v0/postings/{slug}?mode=json")
        for item in payload:
            categories = item.get("categories") or {}
            locations = categories.get("allLocations") or [categories.get("location", "")]
            if item.get("country") == "US":
                locations = [*locations, "United States"]
            description = " ".join(filter(None, [
                str(item.get("openingPlain", "")),
                str(item.get("descriptionPlain", "")),
                str(item.get("additionalPlain", "")),
            ]))
            normalized.append(direct_job(
                board,
                item.get("id"),
                str(item.get("text", "")),
                [str(location) for location in locations],
                str(item.get("hostedUrl", "")),
                description,
                item.get("createdAt") or item.get("created_at") or item.get("updatedAt"),
            ))
    elif provider == "ashby":
        payload = fetch_json(f"https://api.ashbyhq.com/posting-api/job-board/{slug}")
        for item in payload.get("jobs", []):
            if not item.get("isListed", True):
                continue
            secondary = [
                str(location.get("location", ""))
                for location in item.get("secondaryLocations") or []
            ]
            addresses = [item.get("address") or {}] + [
                location.get("address") or {}
                for location in item.get("secondaryLocations") or []
            ]
            countries = [
                str((address.get("postalAddress") or address).get("addressCountry", ""))
                for address in addresses
            ]
            url = str(item.get("jobUrl") or item.get("applyUrl") or "")
            job_id = urllib.parse.urlsplit(url).path.rstrip("/").split("/")[-1]
            normalized.append(direct_job(
                board,
                job_id,
                str(item.get("title", "")),
                [str(item.get("location", "")), *secondary, *countries],
                url,
                str(item.get("descriptionPlain", "")),
                item.get("publishedAt") or item.get("published_at"),
            ))
    elif provider == "smartrecruiters":
        offset = 0
        while offset < 300:
            payload = fetch_json(
                "https://api.smartrecruiters.com/v1/companies/"
                f"{slug}/postings?limit=100&offset={offset}&country=us"
            )
            items = payload.get("content", [])
            for item in items:
                title = str(item.get("name", ""))
                lowered_title = title.lower()
                if not any(term in lowered_title for term in JOB_FAMILY_TERMS):
                    continue
                if any(term in lowered_title for term in EXCLUDED_TERMS):
                    continue
                posting_id = str(item.get("id", ""))
                detail = fetch_json(
                    "https://api.smartrecruiters.com/v1/companies/"
                    f"{slug}/postings/{urllib.parse.quote(posting_id, safe='')}"
                )
                sections = ((detail.get("jobAd") or {}).get("sections") or {}).values()
                experience = str((detail.get("experienceLevel") or {}).get("label", ""))
                description = " ".join(
                    [experience, *[plain_text(str(section.get("text", ""))) for section in sections]]
                )
                location = detail.get("location") or item.get("location") or {}
                locations = [str(location.get("fullLocation", ""))]
                if str(location.get("country", "")).lower() == "us":
                    locations.append("United States")
                job = direct_job(
                    board,
                    detail.get("id") or posting_id,
                    str(detail.get("name") or title),
                    locations,
                    str(detail.get("applyUrl") or f"https://jobs.smartrecruiters.com/{slug}/{posting_id}"),
                    description,
                    detail.get("releasedDate")
                    or detail.get("releaseDate")
                    or detail.get("createdOn")
                    or item.get("releasedDate")
                    or item.get("releaseDate")
                    or item.get("createdOn"),
                )
                job["company_name"] = str(
                    (detail.get("company") or item.get("company") or {}).get("name")
                    or company_from_slug(board["slug"])
                )
                normalized.append(job)
            offset += len(items)
            if not items or offset >= int(payload.get("totalFound", 0)):
                break
    return board, normalized


def aggregator_job(
    source: str,
    job_id: object,
    title: str,
    company: str,
    location: str,
    url: str,
    description: str,
    posted_at: object | None = None,
) -> dict:
    return {
        "source": source,
        "board_key": f"aggregator:{source}",
        "id": f"{source}:{job_id}",
        "title": title,
        "company_name": company,
        "locations": [location, "United States"],
        "url": url,
        "description": description,
        "posted_at": posted_at,
        "active": True,
        "is_visible": True,
    }


def fetch_adzuna_jobs(app_id: str, app_key: str) -> list[dict]:
    query = urllib.parse.urlencode({
        "app_id": app_id,
        "app_key": app_key,
        "results_per_page": 50,
        "what": "software engineer OR data engineer OR machine learning engineer OR AI engineer OR data scientist OR new grad OR entry level",
        "where": "United States",
        "sort_by": "date",
        "content-type": "application/json",
    })
    payload = fetch_json(f"https://api.adzuna.com/v1/api/jobs/us/search/1?{query}")
    jobs = []
    for item in payload.get("results", []):
        location = item.get("location") or {}
        company = item.get("company") or {}
        jobs.append(aggregator_job(
            "adzuna",
            item.get("id"),
            str(item.get("title", "")),
            str(company.get("display_name", "Unknown company")),
            str(location.get("display_name", "United States")),
            str(item.get("redirect_url", "")),
            str(item.get("description", "")),
            item.get("created"),
        ))
    return jobs


def fetch_jooble_jobs(api_key: str) -> list[dict]:
    body = json.dumps({
        "keywords": (
            "software engineer, data engineer, machine learning engineer, "
            "AI engineer, data scientist, new grad, entry level"
        ),
        "location": "United States",
        "page": "1",
        "ResultOnPage": "50",
        "companysearch": "false",
    }).encode()
    payload = fetch_json(
        f"https://jooble.org/api/{urllib.parse.quote(api_key, safe='')}", data=body
    )
    return [
        aggregator_job(
            "jooble",
            item.get("id"),
            str(item.get("title", "")),
            str(item.get("company", "Unknown company")),
            str(item.get("location", "United States")),
            str(item.get("link", "")),
            str(item.get("snippet", "")),
            item.get("updated") or item.get("created") or item.get("date"),
        )
        for item in payload.get("jobs", [])
    ]


def fetch_due_aggregators(
    connection: sqlite3.Connection, now: int
) -> list[tuple[str, list[dict]]]:
    configured = []
    adzuna_id = os.environ.get("JOB_WATCHER_ADZUNA_APP_ID")
    adzuna_key = os.environ.get("JOB_WATCHER_ADZUNA_APP_KEY")
    if adzuna_id and adzuna_key:
        configured.append(("adzuna", lambda: fetch_adzuna_jobs(adzuna_id, adzuna_key)))
    jooble_key = os.environ.get("JOB_WATCHER_JOOBLE_API_KEY")
    if jooble_key:
        configured.append(("jooble", lambda: fetch_jooble_jobs(jooble_key)))

    last_checks = dict(connection.execute("SELECT source, last_checked FROM source_checks"))
    results = []
    for source, fetcher in configured:
        if now - int(last_checks.get(source, 0)) < AGGREGATOR_INTERVAL_SECONDS:
            continue
        try:
            results.append((source, fetcher()))
        except Exception as exc:
            print(f"Aggregator source failed ({source}): {exc}", file=sys.stderr)
        finally:
            connection.execute(
                "INSERT INTO source_checks(source, last_checked) VALUES (?, ?) "
                "ON CONFLICT(source) DO UPDATE SET last_checked=excluded.last_checked",
                (source, now),
            )
    return results


def heartbeat_message(
    connection: sqlite3.Connection, now: int
) -> tuple[str, int | None]:
    row = connection.execute("SELECT last_run FROM heartbeat WHERE id = 1").fetchone()
    if not row:
        return "Heartbeat initialized", None
    last_run = int(row[0])
    gap = now - last_run
    if gap > HEARTBEAT_GRACE_SECONDS:
        return (
            f"Heartbeat gap detected: last run was {gap // 60} minutes ago "
            f"(expected every {HEARTBEAT_EXPECTED_SECONDS // 60} minutes)",
            last_run,
        )
    return (
        f"Heartbeat OK: last run was {max(gap // 60, 0)} minutes ago",
        last_run,
    )


def record_heartbeat(connection: sqlite3.Connection, now: int) -> None:
    connection.execute(
        "INSERT INTO heartbeat(id, last_run) VALUES (1, ?) "
        "ON CONFLICT(id) DO UPDATE SET last_run=excluded.last_run",
        (now,),
    )


def fetch_direct_batch(
    boards: list[dict],
) -> tuple[list[tuple[dict, list[dict]]], list[dict]]:
    if not boards:
        return [], []
    results = []
    failures = []
    with ThreadPoolExecutor(max_workers=8) as executor:
        futures = {executor.submit(fetch_direct_board, board): board for board in boards}
        for future in as_completed(futures):
            board = futures[future]
            try:
                results.append(future.result())
            except Exception as exc:
                failures.append(board)
                print(
                    f'ATS source failed ({board["provider"]}:{board["slug"]}): {exc}',
                    file=sys.stderr,
                )
    return results, failures


def send_email(jobs: list[dict]) -> None:
    recipient = os.environ.get("JOB_WATCHER_TO")
    if not recipient:
        raise RuntimeError("JOB_WATCHER_TO is not configured")
    subject = f"{len(jobs)} new US tech job{'s' if len(jobs) != 1 else ''}"
    rows = []
    text_rows = ["Category | Role | Company | Experience | Posted", "-" * 88]
    for job in jobs:
        category = html.escape(role_category(job))
        role = html.escape(str(job.get("title", "Untitled role")))
        company = html.escape(str(job.get("company_name", "Unknown company")))
        experience = html.escape(experience_summary(job))
        posted = html.escape(relative_posted_time(job))
        url = html.escape(str(job.get("url", "")), quote=True)
        text_rows.append(
            f"{role_category(job)} | "
            f"{plain_text(str(job.get('title', 'Untitled role')))} | "
            f"{plain_text(str(job.get('company_name', 'Unknown company')))} | "
            f"{plain_text(experience_summary(job))} | {plain_text(relative_posted_time(job))}"
        )
        rows.append(
            "<tr>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\"><strong><a href=\"{}\">{}</a></strong></td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "<td style=\"padding:10px 8px;border-bottom:1px solid #e5e7eb;vertical-align:top;\">{}</td>"
            "</tr>".format(
                category,
                url,
                role,
                company,
                experience,
                posted,
            )
        )
    html_body = (
        "<div style=\"font-family:Arial,sans-serif;line-height:1.4;max-width:720px;margin:0 auto;\">"
        "<h2 style=\"margin:0 0 12px 0;\">New matching jobs</h2>"
        "<table role=\"presentation\" cellpadding=\"0\" cellspacing=\"0\" style=\"width:100%;border-collapse:collapse;table-layout:fixed;\">"
        "<thead><tr>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:12%;\">Cat</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:34%;\">Role</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:22%;\">Company</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:18%;\">Experience</th>"
        "<th align=\"left\" style=\"padding:8px;border-bottom:2px solid #111827;width:14%;\">Posted</th>"
        "</tr></thead>"
        "<tbody>"
        + "".join(rows)
        + "</tbody></table></div>"
    )
    text_body = "\n".join(text_rows)
    resend_key = os.environ.get("JOB_WATCHER_RESEND_API_KEY")
    if resend_key:
        payload = json.dumps({
            "from": "Job Watcher <onboarding@resend.dev>",
            "to": [recipient],
            "subject": subject,
            "text": text_body,
            "html": html_body,
        }).encode()
        idempotency_key = hashlib.sha256(
            "|".join(sorted(stable_id(job) for job in jobs)).encode()
        ).hexdigest()
        request = urllib.request.Request(
            "https://api.resend.com/emails",
            data=payload,
            headers={
                "Authorization": f"Bearer {resend_key}",
                "Content-Type": "application/json",
                "Idempotency-Key": idempotency_key,
                "User-Agent": "personal-new-grad-job-watcher/1.0",
            },
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=30):
            return

    sender = os.environ.get("JOB_WATCHER_GMAIL", recipient)
    password = os.environ.get("JOB_WATCHER_GMAIL_APP_PASSWORD")
    if not password:
        raise RuntimeError("JOB_WATCHER_RESEND_API_KEY is not configured")
    message = EmailMessage()
    message["From"], message["To"], message["Subject"] = sender, recipient, subject
    message.set_content(text_body)
    message.add_alternative(html_body, subtype="html")
    with smtplib.SMTP_SSL("smtp.gmail.com", 465, context=ssl.create_default_context()) as smtp:
        smtp.login(sender, password.replace(" ", ""))
        smtp.send_message(message)


def source_candidates(
    connection: sqlite3.Connection, source_key: str, jobs: list[dict]
) -> tuple[list[dict], bool]:
    matching = [job for job in jobs if matches(job)]
    initialized = connection.execute(
        "SELECT 1 FROM initialized_boards WHERE board_key = ?", (source_key,)
    ).fetchone()
    if not initialized:
        record_jobs(connection, matching)
        connection.execute(
            "INSERT OR IGNORE INTO initialized_boards(board_key) VALUES (?)", (source_key,)
        )
        return [], True
    return [job for job in matching if not job_seen(connection, job)], False


def main() -> int:
    if "--test-email" in sys.argv:
        send_email([{
            "id": f"delivery-test-{time.time_ns()}",
            "title": "Job Watcher Delivery Test",
            "company_name": "Job Watcher",
            "locations": ["United States / Remote"],
            "url": "https://developers.greenhouse.io/job-board",
        }])
        print("Test email accepted by the email provider")
        return 0
    boards = load_boards()
    if not boards:
        raise RuntimeError("No approved ATS boards are configured")
    with connect_db() as connection:
        checked_at = int(time.time())
        heartbeat, last_run = heartbeat_message(connection, checked_at)
        print(heartbeat)
        if last_run is not None and checked_at - last_run > HEARTBEAT_GRACE_SECONDS:
            print(
                f"job-watcher: missed at least one 5-minute run; gap={checked_at - last_run}s",
                file=sys.stderr,
            )
        due_boards = select_due_boards(connection, boards, checked_at)
        direct_results, failed_boards = fetch_direct_batch(due_boards)
        aggregator_results = fetch_due_aggregators(connection, checked_at)
        candidates = []
        initialized_count = 0
        for board, direct_jobs in direct_results:
            board_key = f'{board["provider"]}:{board["slug"]}'
            relevant_count = sum(1 for job in direct_jobs if potential_match(job))
            match_count = sum(1 for job in direct_jobs if matches(job))
            record_board_success(
                connection, board, relevant_count, match_count, checked_at
            )
            new_candidates, was_initialized = source_candidates(
                connection, board_key, direct_jobs
            )
            candidates.extend(new_candidates)
            initialized_count += int(was_initialized)
        for board in failed_boards:
            record_board_failure(connection, board, checked_at)
        for source, jobs in aggregator_results:
            new_candidates, was_initialized = source_candidates(
                connection, f"aggregator:{source}", jobs
            )
            candidates.extend(new_candidates)
            initialized_count += int(was_initialized)

        new_jobs = []
        candidate_keys = set()
        for job in candidates:
            key = canonical_url(job) or stable_id(job)
            if key not in candidate_keys:
                candidate_keys.add(key)
                new_jobs.append(job)
        if new_jobs and "--initialize" not in sys.argv:
            send_email(new_jobs)
        record_jobs(connection, new_jobs)
        record_heartbeat(connection, checked_at)
    print(
        f"Selected {len(due_boards)} ATS boards; checked {len(direct_results)}; "
        f"failed {len(failed_boards)}; aggregators {len(aggregator_results)}; "
        f"initialized {initialized_count}; recorded {len(new_jobs)} new matching jobs"
    )
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as exc:
        print(f"job-watcher: {exc}", file=sys.stderr)
        raise SystemExit(1)
