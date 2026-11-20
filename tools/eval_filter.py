#!/usr/bin/env python3
"""Measure the filter against SimplifyJobs' curated new-grad list.

Recall: of the Software and AI/ML/Data roles humans listed as new grad in the last
N days, how many does rejection_reason() accept when run on the live posting?
Precision check: what share of *all* postings on the same boards it accepts, plus a
random sample of accepted titles that are not on the curated list.

    python3 tools/eval_filter.py [--days 30] [--sample 25]
"""
from __future__ import annotations

import argparse
import collections
import json
import random
import re
import sys
import time
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import job_watcher as jw  # noqa: E402

LISTINGS_URL = (
    "https://raw.githubusercontent.com/SimplifyJobs/New-Grad-Positions/dev/"
    ".github/scripts/listings.json"
)
RELEVANT_CATEGORIES = {"Software", "AI/ML/Data"}
CACHE = jw.STATE_DIR / "eval_cache" / "listings.json"
TOKEN_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}|\d{6,}"
)


def load_listings(max_age_hours: float = 6) -> list[dict]:
    if not CACHE.exists() or time.time() - CACHE.stat().st_mtime > max_age_hours * 3600:
        CACHE.parent.mkdir(parents=True, exist_ok=True)
        request = urllib.request.Request(LISTINGS_URL, headers={"User-Agent": "job-watcher-eval/1.0"})
        with urllib.request.urlopen(request, timeout=60) as response:
            CACHE.write_bytes(response.read())
    return json.loads(CACHE.read_text())


def board_for(url: str) -> dict | None:
    board = jw.board_from_url(url) if hasattr(jw, "board_from_url") else None
    if board:
        return board
    parts = urllib.parse.urlsplit(url)
    host, path = parts.netloc.lower(), [p for p in parts.path.split("/") if p]
    if "greenhouse.io" in host and path and path[0] != "embed":
        return {"provider": "greenhouse", "slug": path[0]}
    if host.endswith("lever.co") and path:
        return {"provider": "lever", "slug": path[0]}
    if "ashbyhq.com" in host and path:
        return {"provider": "ashby", "slug": path[0]}
    return None


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=30)
    parser.add_argument("--sample", type=int, default=25)
    args = parser.parse_args()

    now = time.time()
    rows = [
        listing for listing in load_listings()
        if listing.get("active") and listing.get("category") in RELEVANT_CATEGORIES
        and now - listing.get("date_posted", 0) <= args.days * 86400
    ]
    targets: dict[tuple, list] = collections.defaultdict(list)
    for listing in rows:
        board = board_for(listing["url"])
        token = TOKEN_RE.findall(listing["url"].lower())
        if board and token:
            targets[(board["provider"], board["slug"])].append((listing, token[-1], board))

    original = jw.source_timestamp_is_recent
    jw.source_timestamp_is_recent = lambda *a, **k: True  # judge content, not the clock

    def fetch(key):
        board = targets[key][0][2]
        try:
            return key, jw.fetch_direct_board(board)[1]
        except Exception:
            return key, None

    with ThreadPoolExecutor(8) as executor:
        boards = dict(executor.map(fetch, targets))
    jw.source_timestamp_is_recent = original

    def verdict(job):
        posted = jw.job_posted_timestamp(job) or int(now)
        return jw.rejection_reason(job, now=posted + 60)

    reasons = collections.Counter()
    examples = collections.defaultdict(list)
    curated_ids = set()
    for key, items in targets.items():
        for listing, token, _ in items:
            job = next((
                j for j in boards.get(key) or []
                if token in str(j.get("url", "")).lower() or token in str(j.get("id", "")).lower()
            ), None)
            if job is None:
                continue
            curated_ids.add(job["id"])
            reason = verdict(job)
            reasons[reason] += 1
            examples[reason].append(f'{listing["company_name"]} | {job["title"]}')

    found = sum(reasons.values())
    print(f"curated roles (last {args.days}d): {len(rows)}; on supported ATS with a job id: "
          f"{sum(map(len, targets.values()))} across {len(targets)} boards; matched live: {found}")
    if not found:
        return 1
    print(f"\nRECALL: {reasons['matched']}/{found} = {reasons['matched'] / found:.0%}")
    out_of_scope = reasons["excluded_company"] + reasons["outside_us"]
    in_scope = found - out_of_scope
    print(f"IN-SCOPE RECALL (US roles at companies you have not excluded): "
          f"{reasons['matched']}/{in_scope} = {reasons['matched'] / max(in_scope, 1):.0%}")
    for reason, count in reasons.most_common():
        print(f"  {reason:36s}{count:4d}  {count / found:5.1%}")
    for reason in reasons:
        if reason != "matched":
            print(f"  e.g. {reason}: " + "; ".join(examples[reason][:5]))

    others = [
        job for jobs in boards.values() for job in jobs or []
        if job["id"] not in curated_ids
    ]
    accepted = [job for job in others if verdict(job) == "matched"]
    print(f"\nPRECISION CHECK: accepts {len(accepted)}/{len(others)} = "
          f"{len(accepted) / max(len(others), 1):.1%} of all other postings on those boards")
    random.seed(7)
    for job in random.sample(accepted, min(args.sample, len(accepted))):
        print(f"  + {job['company_name']} | {job['title']} | {', '.join(job['locations'][:2])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
