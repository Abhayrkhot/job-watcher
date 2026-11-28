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
  ("Software Development Engineer 1", "Forward Deployed Engineer"). Analyst and
  associate titles must be directly data- or analytics-qualified ("Data Analyst",
  "Analyst, Data Systems", "Data Associate"), so "Facility Analyst" and "Business
  Analyst, Finance" are dropped. Lab, pharma, and humanities scientist titles need
  an ML or computational word. Hardware, controls, lithography, R&D product
  development, wireless, and manufacturing titles are dropped unless they also say
  software, as are AVP, post-doc, administrator, and coordinator titles.
- **Experience**: the strictest hard requirement counts. "3+ years building ..."
  is a requirement even without the word "experience". A posting is rejected for
  3+ years, or for 2+ years without a new-grad title; "2-3 years" counts as a
  junior band. In tiered wording ("BS + 6 years or MS + 5 years or PhD") the
  Master's path is the one that counts, and in leveled postings ("Level 1 - none.
  Level 2 - Two years ...") the higher levels don't count. "Degree or 4 years", "or a minimum of 5
  years" after a degree, "may be substituted for a degree", "including
  internships", and anything under a "Preferred", "Nice to have", or
  "Considerations for top candidates" header don't count. A "preferred", "a plus",
  or "ideally" counts only next to its own figure: in "4+ years in a customer-facing
  role, ideally at a startup" the 4 years still count, and "Minimum 5+ years ...
  preferred" counts. HTML bullets stay separate sentences, so a "nice to have" in one
  bullet never cancels the years in the next. "Three (3) years" reads as 3; "less
  than 3 years", "within the last five years", "a 4 year university", "18 years of
  age", and application-form questions are not requirements. "Mid-level", "several
  years", "proven track record", and "deep expertise in ..." reject, unless negated
  ("you don't need to be an experienced engineer").
- **Location**: US only. The posting needs a US city, state, or "United States"
  in its location. A bare "Remote" or blank location is kept only when the
  description itself says US (for example "authorized to work in the U.S." or US
  time zones). A role open in both the US and Canada is kept; country codes such
  as "IN" or "IND" next to an Indian city are not read as US states.
- **Work authorization**: rejected when it requires U.S. citizenship, permanent
  residency, U.S.-person status (ITAR, "only open to U.S. citizens"), or a
  clearance (TS/SCI, polygraph, "Active DoD Secret", Public Trust). Equal-opportunity
  text about "citizenship status" is not a requirement, and "may include meeting the
  definition of a U.S. person" is a maybe, not a rule.
- **Sponsorship**: rejected when the posting says it will not sponsor a work visa
  ("Visa sponsorship is not available", "must be authorized to work without the need
  for employer sponsorship", "will not sponsor applicants"), unless it also says it
  does sponsor ("We do sponsor visas!", "support H-1B transfers"). Application-form
  questions such as "Will you require sponsorship?" are ignored. Set
  `JOB_WATCHER_KEEP_NO_SPONSORSHIP=1` in `.env` to keep these postings.
- **Companies**: prop and quant trading, ByteDance/TikTok, aerospace/defense, and
  intelligence contractors whose roles need a clearance are excluded by default. Put a JSON list of names in `state/excluded_companies.json`
  to replace the default list.

`python3 tools/eval_filter.py` measures the filter against SimplifyJobs' curated
new-grad list. It fetches the live postings and reports recall and a precision
sample. In November 2026, recall on in-scope roles went from 6% to 63%. "Eligible
recall" also leaves out roles that say no sponsorship or need citizenship, since
those are rejected on purpose. Most of
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

Five big employers run their own career sites, which the Simplify harvest never
finds, so they have their own readers and one board each: Amazon (the
`amazon.jobs` search JSON, US software, ML and data science roles), Google (the
early-career search page, whose job data is embedded in the HTML), Apple (the
newest US postings from the search page, then the job page for new IDs whose title
passes), Meta (the public job sitemap for new IDs, then each new job page's
schema.org `JobPosting`), and Qualcomm (the Eightfold search API its careers site
calls). Microsoft's careers API answers 429 to an honest client, so it is not read.
Their first poll is a baseline like any other board's.

iCIMS and UKG (UltiPro) boards are read too, and are harvested from Simplify and
found by Jobright discovery like the others. iCIMS has no JSON, so the portal's search
page gives job IDs and titles and the job page (its schema.org `JobPosting`) the
details for new IDs; quiet iCIMS boards are read every 20 minutes. A UKG job board's
own JSON search gives the newest postings, and its job page embeds the details.
Avature is not read: its lists are alphabetical a few jobs at a time.

Jobright's hourly public GitHub lists (new-grad software and data roles) name many
employers Simplify never lists. The watcher reads them once an hour, but never
jobright.ai itself. For a US employer it doesn't poll, it looks for the employer's
own board: links on the homepage Jobright names (or the first careers page it links
to), then the employer's name on Greenhouse, Lever, and Ashby, accepted only when that
board lists the same role, so a namesake isn't added. Up to 8 employers an hour are
looked up, each once (or again after a week if nothing was found). The new board is
polled from then on, and the posting that led to it is alerted from the employer's
own board if it is recent and passes the filter.

The registry grows on its own. Once a day the watcher reads SimplifyJobs' public
New-Grad-Positions and Summer2027-Internships `listings.json`. It turns every
Software or AI/ML/Data apply link from the past year into a board, and adds the
boards it doesn't already have, using Simplify's company name. Companies on your
excluded list are skipped. The new-grad list is also read every 15 minutes, with a
conditional GET, as a backstop source for roles on systems the watcher can't read
(SuccessFactors, Avature, custom career sites). A role found both directly and through Simplify is
mailed once. Simplify listings carry no description, so a Simplify copy of a job on a
board the watcher already reads in full (baselined more than a day before the
listing, and reading cleanly) is dropped; otherwise a posting the direct reader
rejected for experience, sponsorship, or a clearance would be mailed anyway. For the
backstop jobs that remain (SuccessFactors, Avature, custom career sites), the watcher
reads the posting page once before mailing: the schema.org `JobPosting` most career
pages embed, or else the visible text up to the application form. At most 12 pages
per run; a page that can't be read leaves the job as it was. See `SOURCE_POLICY.md`.

## Speed

The point is to see a role minutes after it's posted, so polling has to be cheap
enough to run constantly:

- Boards with roles in our area are polled on every run (two minutes apart on the
  box), and all other boards every 10 minutes.
- Every list request sends gzip and `If-None-Match`. Greenhouse, Lever, Ashby, and
  Workable answer 304 when nothing changed, so an unchanged board costs one tiny
  response. A board's ETag is saved only after all of its new jobs are processed.
- Greenhouse is read without descriptions (21 KB gzipped instead of about 1.4 MB
  for a large board). The full posting is fetched only for new job IDs, and the
  posting time comes from `first_published`, so an edit doesn't make an old job
  look new.
- With `JOB_WATCHER_NTFY_TOPIC` set, each new match is pushed to your phone through
  ntfy before the email goes out. The alert opens the apply link. Pick an
  unguessable topic name, since ntfy.sh topics are public to anyone who knows them.
  `./run.sh --test-push` sends a test alert.

Each run checks up to 2000 due boards. Boards that answer with an HTTP error retry
after 5, 10, 20 minutes and so on, doubling up to a day; only a board that has
failed 10 times in a row (about 42 hours) waits a week. Dead boards stop consuming
the polling budget, while a provider outage of an hour or two can't bench boards for
a week. Throttling (429) and gateway errors (502, 503, 504) are not counted against
a board, and neither is a Workday maintenance page (a 200 that is not JSON). Such a
board was not read, so it stays due and the next run tries it again; a new posting
whose details were throttled keeps its board on the every-run schedule until it is
read.

Workday tenants share a few hosts (wd1, wd3, wd5 and so on), and each host has its
own lane that reads its due boards oldest first. Most hosts get 3 requests at a time,
0.35 s apart; hosts that throttle after a few requests (wd3, wd103) get one at a
time, 5 s apart. After a 429 the watcher sends nothing more to that host until the
next run, and a lane starts no new board after 120 seconds. Boards left over stay
due, lead the next run's lane, and are summarized in one line per host in
`watcher.log`.

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

A board's first successful poll creates a baseline: what is already open is
recorded, not mailed, so a new board doesn't flood the inbox. The exception is a
matching posting the source itself dates within the last 24 hours (a timestamp, or
today's date), which is as new as anything a later poll would find, so it is mailed;
readers that list cheaply fetch details at the first poll only for such postings. A
job known only by when the watcher first saw it is never mailed from a first poll.
Jobs must have a detectable posted timestamp within the last 24 hours to be eligible.

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
then sorted by posting time. The Posted column shows when the company posted the
job ("2h ago · Nov 19, 10:04 PM ET"), or just the date when the source only
publishes a date (Workday, Oracle, BambooHR). Jobvite publishes no date, so those
rows say "Not published" with the time the watcher first saw the job. A daily health email reports sources checked, jobs
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

## Scheduling

Run the watcher on a machine that never sleeps. A laptop that sleeps misses the
first hours of every posting it sleeps through.

**Linux (systemd, recommended):** the units in `deploy/systemd` run one poll at a
time, two minutes after the previous poll finishes:

```bash
mkdir -p ~/.config/systemd/user
sed "s|__PROJECT_DIR__|$PWD|g" deploy/systemd/job-watcher.service.in > ~/.config/systemd/user/job-watcher.service
cp deploy/systemd/job-watcher.timer ~/.config/systemd/user/
systemctl --user daemon-reload && systemctl --user enable --now job-watcher.timer
loginctl enable-linger "$USER"   # keep running when you are logged out
```

**macOS:** `setup.sh` installs a LaunchAgent that runs every five minutes.

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
documented API or structured feed. Taleo, SuccessFactors, and Avature are not read
directly (nor Eightfold beyond Qualcomm); roles there reach the watcher only through
the Simplify backstop.
