# New-Grad Job Watcher

Checks approved employer ATS boards every five minutes and emails new US and
US-remote entry-level technical roles that do not include an explicit citizenship
requirement. SQLite state prevents duplicate mail and records a heartbeat so
missed runs are visible in the logs.

The watcher polls a reviewed registry of employer career boards through public
Greenhouse, Lever, Ashby, SmartRecruiters, Recruitee, and Workable APIs. Each run
checks up to 200 due boards. Boards with a current match run every 30 minutes,
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
documented API or structured feed. Workday, iCIMS, Taleo, Eightfold, and BambooHR
are added only when a documented public job-listing interface is
available and validated; private career-site endpoints are not used.
