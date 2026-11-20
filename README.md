# New-Grad Job Watcher

Checks approved employer ATS boards every five minutes and emails new US and
US-remote entry-level technical roles that do not include an explicit citizenship
requirement. SQLite state prevents duplicate mail and records a heartbeat so
missed runs are visible in the logs.

## Filter

The goal is to be among the first applicants, so the filter favors recall. It
accepts anything in the software, data, and ML area and rejects only clear
disqualifiers:

- **Seniority and gigs in the title**: senior, staff (except "Member of Technical
  Staff"), principal, lead, manager, III and above, plus internships, co-ops,
  contract, part-time, temporary, and annotation work. Words are matched whole,
  so "Internal Tools" is not an internship.
- **Role family**: engineer and developer titles need a software, data, or ML word
  ("Software Development Engineer 1", "Forward Deployed Engineer"). Analyst,
  associate, and scientist titles need a data or ML word next to them. Hardware,
  mechanical, and manufacturing titles are dropped unless they also say software.
- **Experience**: rejected only when a hard requirement asks for 3+ years, or for
  2+ years with no new-grad signal. "Or a Master's", "degree or 4 years",
  "including internships", and "is a plus" don't count as requirements, and
  numbers that aren't about experience ("20-40 years behind") are ignored.
- **Location**: at least one US location, or only "Remote"/"Hybrid". A role open
  in both the US and Canada is kept.
- **Work authorization**: rejected when it requires U.S. citizenship, permanent
  residency, U.S.-person status, or a security clearance. Equal-opportunity text
  about "citizenship status" is not a requirement.
- **Companies**: prop and quant trading, ByteDance/TikTok, and aerospace/defense are
  excluded by default. Put a JSON list of names in `state/excluded_companies.json`
  to replace the default list.

`python3 tools/eval_filter.py` measures the filter against SimplifyJobs' curated
new-grad list. It fetches the live postings and reports recall and a precision
sample. In November 2026, recall on in-scope roles went from 6% to 63%. Most of
the remaining misses are contract, annotation, and non-technical roles.

## Sources and discovery

The watcher polls a registry of employer career boards (`state/ats_boards.json`)
through the public Greenhouse, Lever, Ashby, SmartRecruiters, Recruitee, and
Workable APIs, plus the endpoints that Workday, Oracle Recruiting, BambooHR,
Rippling, JazzHR, and Jobvite careers pages load. Those six cover about two thirds
of curated new-grad roles. Workday, Oracle, BambooHR, Rippling, and Jobvite list
cheaply and fetch the full posting only for job IDs they haven't seen. When a
provider gives only a posting date, a job seen for the first time within two days
of that date is stamped with the time it was detected.

The registry grows on its own. Once a day the watcher reads SimplifyJobs' public
New-Grad-Positions and Summer2027-Internships `listings.json`. It turns every
Software or AI/ML/Data apply link from the past year into a board, and adds the
boards it doesn't already have, using Simplify's company name. Companies on your
excluded list are skipped. The new-grad list is also read every 15 minutes, with a
conditional GET, as a backstop source for roles on systems the watcher can't read
(iCIMS, custom career sites). A role found both directly and through Simplify is
mailed once.

Each run checks up to 200 due boards. Boards with a current match run every 30 minutes,
boards with three or more relevant roles every hour, boards with some relevant
roles every three hours, and quiet boards every 12 hours. Boards that answer with
an HTTP error retry after 5 and then 10 minutes, and after a third failure wait a
week, keeping dead boards from consuming the active polling budget.

Network errors are not counted against a board. launchd starts a missed run as
soon as the Mac wakes, often before DNS is back, so each run first waits up to 60
seconds for DNS. If network errors still outnumber successful fetches, the run is
treated as an outage: the affected boards stay due and aggregators and feeds are
skipped so they keep their polling slot. A lone timeout or reset counts as a check
that found nothing.

Companies often keep their slug when they switch ATS. When a board returns 404 or
410, the same slug is probed on the public Greenhouse, Ashby, and Lever APIs. If
it has listed jobs there, `state/ats_boards.json` is rewritten to point at the new
provider, with a `migrated_from` field recording the old key. The move is logged
as `Board moved:` in `state/watcher.log`. Because a slug can belong to a different
company with the same name, review those lines occasionally.

A board's first successful poll creates a silent
baseline; only later matching postings are mailed. Jobs must have a detectable
posted timestamp within the last 24 hours to be eligible.

Approved public RSS, Atom, JSON, and JazzHR customer XML feeds can be configured
in `state/feeds.json`. Feed polling runs no more often than every 15 minutes. A feed must be explicitly
marked `approved`, and `us_only` may only be set when the publisher documents the
feed as US-only:

```json
[
  {
    "name": "publisher-name",
    "url": "https://publisher.example/jobs.json",
    "approved": true,
    "us_only": true,
    "interval_seconds": 900
  }
]
```

Alert emails are grouped by ML, AI, data science, data, SDE, robotics, and other,
then sorted by posting time. A daily health email reports sources checked, jobs
inspected, matches, notifications, failures, and exact filter rejection totals.
Run `./run.sh --health-report` to request it immediately.

See `SOURCE_POLICY.md` for the source allowlist and rules prohibiting restricted
scraping, browser automation, cookies, private endpoints, and access-control bypass.

## Activate

1. Create a free account at <https://resend.com> using the destination email.
2. Create an API key at <https://resend.com/api-keys>.
3. Run `bash setup.sh` and enter the destination and key locally.

The installer writes a local LaunchAgent plist to `~/Library/LaunchAgents` and
starts it immediately. The first run records existing jobs without emailing them.
Later runs send only new matches. Logs are under `state/`.

## Local Scheduling

`setup.sh` installs a LaunchAgent that runs every five minutes. That is now the
source of truth for polling, so the watcher does not depend on GitHub Actions.

Optional approved aggregator secrets can still be added locally through `.env`:

- `JOB_WATCHER_ADZUNA_APP_ID` and `JOB_WATCHER_ADZUNA_APP_KEY`
- `JOB_WATCHER_JOOBLE_API_KEY`

Register through <https://developer.adzuna.com/> and <https://jooble.org/api/about>.
Never commit or paste those keys into issues, source files, or chat.

The credential-free Jobicy API is enabled by default at a six-hour cadence, as
requested by its fair-use policy. Its public listings are delayed by six hours
but remain eligible for the watcher's 24-hour freshness window.

## Coverage Boundaries

LinkedIn, Jobright, Indeed, and similar authenticated boards are not crawled.
Their native alerts can be used alongside this watcher, but ingesting them cannot
reliably enforce the 24-hour and US-only rules unless the provider supplies a
documented API or structured feed. iCIMS, Taleo, Eightfold, and SuccessFactors are
not read directly; roles there reach the watcher only through the Simplify backstop.
