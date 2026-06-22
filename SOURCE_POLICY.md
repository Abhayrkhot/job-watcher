# Source Policy

This personal job watcher uses only documented public APIs, provider-approved API
keys, and user-configured native alerts. It does not scrape restricted websites,
use authenticated browser sessions, evade access controls, rotate proxies, solve
CAPTCHAs, or call private application endpoints.

This is an engineering policy, not legal advice. Provider terms can change and must
be reviewed before adding a source.

## Enabled Sources

| Source | Access basis | Constraints |
| --- | --- | --- |
| Greenhouse | Public Job Board API GET endpoints | Published jobs only |
| Lever | Public Postings API GET endpoints | Published jobs only |
| Ashby | Public Job Postings API | Published jobs only |
| SmartRecruiters | Public Posting API | Maximum 8 concurrent and 10 requests/second |

## Optional Approved Sources

- Adzuna: API key required. Personal research is permitted; default limit is 250
  calls/day. Attribution requirements apply.
- Jooble: API key approval required. Only the REST API may be used; its website must
  not be crawled or automated.
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
- https://developer.adzuna.com/docs/terms_of_service
- https://jooble.org/api/about
- https://jooble.org/info/terms
- https://www.linkedin.com/help/linkedin/answer/a1341387
