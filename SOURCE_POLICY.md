# Source Policy

This personal job watcher uses documented public APIs, provider-approved API keys,
user-configured native alerts, and the unauthenticated endpoints that companies'
public careers pages load (see "Career-site endpoints" below). It does not scrape
sites that require login, use authenticated browser sessions or cookies, evade
access controls or rate limits, rotate proxies, or solve CAPTCHAs.

This is an engineering policy, not legal advice. Provider terms can change and must
be reviewed before adding a source.

## Enabled Sources

| Source | Access basis | Constraints |
| --- | --- | --- |
| Greenhouse | Public Job Board API GET endpoints | Published jobs only |
| Lever | Public Postings API GET endpoints | Published jobs only |
| Ashby | Public Job Postings API | Published jobs only |
| SmartRecruiters | Public Posting API | Maximum 8 concurrent and 10 requests/second |
| Recruitee | Public unauthenticated Careers Site API | Published offers only |
| Workable | Documented public account jobs endpoint | Published jobs; reviewed accounts only |
| RSS/Atom/JSON feeds | Publisher-provided public feed | Explicit review and allowlist required |

## Career-site endpoints

Enabled on 2026-11-20 by the owner's decision, so the watcher can see the roughly
two thirds of new-grad roles that are posted on these systems. Each endpoint is the
one the company's own public careers page calls, without login or cookies, and is
read at the cadence of the watcher's normal polling.

| Source | Endpoint | Constraints |
| --- | --- | --- |
| Workday | `POST https://<tenant>.wdN.myworkdayjobs.com/wday/cxs/<tenant>/<site>/jobs` and the matching job detail GET | 20 newest postings per page, at most 5 pages; details only for new IDs |
| Oracle Recruiting Cloud | `GET /hcmRestApi/resources/latest/recruitingCEJobRequisitions` and `...JobRequisitionDetails` | 50 newest per poll; details only for new IDs |
| BambooHR | `GET https://<company>.bamboohr.com/careers/list` and `/careers/<id>/detail` | Details only for new IDs |
| Rippling | `GET https://api.rippling.com/platform/api/ats/v1/board/<slug>/jobs` | Details only for new IDs |
| JazzHR | `GET https://app.jazz.co/feeds/export/jobs/<company>` (syndication XML) | One request per poll |
| Jobvite | `GET https://jobs.jobvite.com/<company>/jobs` and the job page (HTML) | Details only for new IDs |
| SimplifyJobs lists | `listings.json` from the public New-Grad-Positions and Summer2027-Internships GitHub repos | Conditional GET with gzip; board discovery at most daily; new-grad list at most every 15 minutes |

All readers share the global limits: 8 concurrent requests, at most 25 detail
fetches per board per poll, and the first poll of a board records existing IDs
without fetching any details. iCIMS, Taleo, Eightfold, and SuccessFactors boards are
not polled.

## Posting pages for Simplify backstop jobs

Enabled on 2026-11-25 by the owner's decision. A SimplifyJobs listing has no job
description, so postings on systems the watcher does not poll (iCIMS, SuccessFactors,
custom career sites) were mailed without the experience, clearance, or sponsorship
checks. For those jobs the watcher now reads the posting page itself.

| Source | Endpoint | Constraints |
| --- | --- | --- |
| Posting page | `GET` of the apply URL that the Simplify listing links to | Only new jobs that already pass every other filter; one GET per job, never repeated once mailed; at most 12 pages per run and 4 at a time; 10 s timeout and 3 MB cap; no login, cookies, or JavaScript; honest User-Agent; a page that cannot be read leaves the job as it was |
| Detail endpoint instead of the page | For Workday, Oracle Recruiting, BambooHR, and Workable apply URLs, whose pages render only with JavaScript: the same public job-detail endpoint listed above (Workable: `GET https://apply.workable.com/api/v2/accounts/<account>/jobs/<code>`, which its careers page calls) | Same limits as the page read; Workday requests go through the shared per-host pacing |

The schema.org `JobPosting` JSON-LD that most career pages embed for search engines is
read first; otherwise the page's visible text, cut off where the application form
starts. Pages are never crawled for other jobs, and their text is not stored.

## Optional Approved Sources

- Jobicy: its documented public API is queried with `geo=usa` every six hours.
  Job links retain Jobicy attribution, and listings are never republished.
- Adzuna: API key required. Personal research is permitted; default limit is 250
  calls/day. The connector makes at most 96 calls/day and includes attribution.
- Jooble: API key approval required. Only the REST API is called at a conservative
  15-minute cadence; its website is never crawled or automated.
- Recruitee: its documented unauthenticated Careers Site API may be added when a
  relevant active board is available for validation.
- LinkedIn and Jobright: native alerts only. No crawling, browser automation, cookie
  reuse, or private API calls.

## References

- https://developers.greenhouse.io/job-board
- https://github.com/lever/postings-api
- https://developers.ashbyhq.com/docs/public-job-posting-api
- https://developers.smartrecruiters.com/docs/endpoints
- https://developers.smartrecruiters.com/docs/rate-limiting
- https://docs.recruitee.com/reference/intro-to-careers-site-api
- https://workable.readme.io/reference/jobs-1
- https://jobicy.com/jobs-rss-feed
- https://developer.adzuna.com/docs/terms_of_service
- https://jooble.org/api/about
- https://jooble.org/info/terms
- https://www.linkedin.com/help/linkedin/answer/a1341387
