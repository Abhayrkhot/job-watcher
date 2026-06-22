# New-Grad Job Watcher

Checks approved employer ATS boards every five minutes and emails new US and
US-remote entry-level technical roles that explicitly offer sponsorship. SQLite
state prevents duplicate mail and records a heartbeat so missed runs are visible
in the logs.

The watcher polls a reviewed registry of employer career boards through public
Greenhouse, Lever, Ashby, and SmartRecruiters APIs. Each run checks up to 200 due
boards. Boards with several relevant roles or a sponsorship match run every five
minutes, boards with some relevant roles run every 30 minutes, and quiet boards run
every two hours. A board's first successful poll creates a silent baseline; only
later matching postings are mailed. Roles must contain explicit positive
visa-sponsorship language.

See `SOURCE_POLICY.md` for the source allowlist and rules prohibiting restricted
scraping, browser automation, cookies, private endpoints, and access-control bypass.

## Activate

1. Create a free account at <https://resend.com> using the destination email.
2. Create an API key at <https://resend.com/api-keys>.
3. Run `bash setup.sh` and enter the destination and key locally.

The first run records existing jobs without emailing them. Later runs send only new
matches. Logs are under `state/`.

## GitHub Actions

The workflow in `.github/workflows/job-watcher.yml` runs every five minutes. It
stores the current SQLite database and board cursor as a single squashed commit on
the `job-watcher-state` branch, keeping secrets out of persisted state.

Use a public repository so standard GitHub-hosted runner usage remains free. Add
these repository Actions secrets:

- `JOB_WATCHER_TO`: destination email address
- `JOB_WATCHER_RESEND_API_KEY`: Resend API key

Optional approved aggregator secrets:

- `JOB_WATCHER_ADZUNA_APP_ID` and `JOB_WATCHER_ADZUNA_APP_KEY`
- `JOB_WATCHER_JOOBLE_API_KEY`

Register through <https://developer.adzuna.com/> and <https://jooble.org/api/about>.
The watcher checks configured aggregators every 15 minutes. Never commit or paste
their keys into issues, source files, or chat.

Scheduled Actions can be delayed or dropped during GitHub load. The workflow is a
free best-effort scheduler, not a guaranteed five-minute service.
