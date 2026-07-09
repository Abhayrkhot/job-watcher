import sqlite3
import unittest
import json
import time
from unittest.mock import patch
from datetime import datetime, timezone

RECENT_TEST_DATE = datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z')

from job_watcher import (
    canonical_url,
    heartbeat_message,
    fetch_adzuna_jobs,
    description_mentions_citizenship_requirement,
    fetch_direct_board,
    fetch_feed_jobs,
    fetch_jobicy_jobs,
    fetch_jooble_jobs,
    health_report,
    experience_summary,
    failure_retry_seconds,
    matches,
    relative_posted_time,
    rejection_reason,
    record_metrics,
    role_category,
    select_due_boards,
    source_candidates,
)


class MatchTests(unittest.TestCase):
    def job(self, title, active=True, locations=None, description=None, posted_at=None):
        return {"title": title, "active": active, "is_visible": True,
                "locations": locations or ["New York, NY"],
                "description": description or
                "This entry-level role offers visa sponsorship and immigration support.",
                "posted_at": posted_at if posted_at is not None else int(time.time()) - 3600}

    def test_target_roles(self):
        self.assertTrue(matches(self.job("Software Engineer - New Grad")))
        self.assertTrue(matches(self.job("Machine Learning Engineer I")))
        self.assertTrue(matches(self.job("Data Scientist - Entry Level")))

    def test_exclusions(self):
        self.assertFalse(matches(self.job("Senior Machine Learning Engineer")))
        self.assertFalse(matches(self.job("Software Engineer Intern")))
        self.assertFalse(matches(self.job("Software Engineer", active=False)))

    def test_requires_us_location(self):
        self.assertTrue(matches(self.job("Software Engineer - New Grad", locations=["Remote in USA"])))
        self.assertFalse(matches(self.job("Software Engineer - New Grad", locations=["Toronto, ON, Canada"])))
        self.assertFalse(matches(self.job("Software Engineer - New Grad", locations=["Remote in Canada"])))
        self.assertFalse(matches(self.job("Software Engineer - New Grad", locations=["New York, NY", "Toronto, ON, Canada"])))

    @patch("job_watcher.time.time", return_value=100000)
    def test_requires_recent_posting(self, _time_mock):
        self.assertTrue(matches(self.job(
            "Software Engineer - New Grad",
            description="General responsibilities.",
            posted_at=100000 - (23 * 60 * 60),
        )))
        self.assertFalse(matches(self.job(
            "Software Engineer - New Grad",
            description="General responsibilities.",
            posted_at=100000 - (25 * 60 * 60),
        )))
        self.assertFalse(matches(self.job(
            "Software Engineer - New Grad",
            description="General responsibilities.",
            posted_at=100000 - (48 * 60 * 60),
        )))
        self.assertTrue(matches(self.job(
            "Software Engineer - New Grad",
            description="General responsibilities.",
            posted_at=100000 + 60,
        )))
        self.assertFalse(matches({
            "title": "Software Engineer - New Grad",
            "active": True,
            "is_visible": True,
            "locations": ["New York, NY"],
            "description": "General responsibilities.",
        }))

    def test_does_not_require_explicit_sponsorship_language(self):
        self.assertTrue(matches(self.job(
            "Software Engineer - New Grad", description="No sponsorship information provided."
        )))
        self.assertTrue(matches(self.job(
            "Software Engineer - New Grad", description="Standard benefits and growth."
        )))

    def test_direct_ats_role(self):
        job = self.job("Machine Learning Engineer - New Grad")
        job.update({
            "source": "direct-ats",
            "description": "Standard benefits and career growth.",
        })
        self.assertTrue(matches(job))

    def test_direct_ats_requires_entry_level_evidence(self):
        job = self.job("Machine Learning Engineer")
        job.update({
            "source": "direct-ats",
            "description": "Five years of experience required.",
        })
        self.assertFalse(matches(job))

    def test_citizenship_requirement_wins(self):
        description = (
            "Applicants must be US citizens and able to obtain a security clearance."
        )
        self.assertFalse(description_mentions_citizenship_requirement(description))
        self.assertFalse(matches(self.job(
            "Software Engineer - New Grad", description=description
        )))

    def test_rejection_reasons_are_specific(self):
        self.assertEqual(
            rejection_reason(self.job("Senior Software Engineer")),
            "excluded_seniority_or_internship",
        )
        self.assertEqual(
            rejection_reason(self.job("Software Engineer", description="Five years required.")),
            "missing_entry_level_evidence",
        )
        self.assertEqual(
            rejection_reason(self.job(
                "Software Engineer - New Grad", locations=["Toronto, Canada"]
            )),
            "outside_us",
        )

    def test_expanded_role_families(self):
        titles = [
            "Data Engineer - New Grad",
            "MLOps Engineer - Entry Level",
            "Compiler Engineer I",
            "Site Reliability Engineer - University Grad",
            "Security Engineer 1",
            "Forward Deployed Engineer - Early Career",
            "Business Intelligence Engineer I",
        ]
        for title in titles:
            with self.subTest(title=title):
                self.assertTrue(matches(self.job(title)))

    def test_canonicalizes_application_url(self):
        job = {"url": "https://jobs.ashbyhq.com/Acme/123/application?utm_source=test"}
        self.assertEqual(canonical_url(job), "https://jobs.ashbyhq.com/Acme/123")

    def test_experience_summary(self):
        self.assertEqual(experience_summary(self.job("Software Engineer - New Grad")), "New grad / entry level")
        self.assertEqual(experience_summary(self.job(
            "Software Engineer", description="0-2 years of experience required."
        )), "0-2 years")
        self.assertEqual(experience_summary(self.job(
            "Software Engineer", description="General responsibilities."
        )), "Entry level")

    def test_role_category(self):
        self.assertEqual(role_category(self.job("Machine Learning Engineer")), "ml")
        self.assertEqual(role_category(self.job("Software Engineer")), "sde")
        self.assertEqual(role_category(self.job("AI Engineer")), "ai")
        self.assertEqual(role_category(self.job("Data Scientist")), "data_science")
        self.assertEqual(role_category(self.job("Data Engineer")), "data")
        self.assertEqual(role_category(self.job("Robotics Software Engineer")), "robotics")

    @patch("job_watcher.time.time", return_value=100000)
    def test_relative_posted_time(self, _time_mock):
        self.assertEqual(relative_posted_time({"posted_at": 100000 - 30}), "Just now")
        self.assertEqual(relative_posted_time({"posted_at": 100000 - (15 * 60)}), "15m ago")
        self.assertEqual(relative_posted_time({"posted_at": 100000 - (3 * 60 * 60)}), "3h ago")
        self.assertEqual(relative_posted_time({}), "Unknown")

    @patch("job_watcher.fetch_json")
    def test_smartrecruiters_normalization(self, fetch_json):
        fetch_json.side_effect = [
            {
                "totalFound": 1,
                "content": [{
                    "id": "42",
                    "name": "Software Engineer - New Grad",
                    "location": {"fullLocation": "Austin, TX", "country": "us"},
                    "company": {"name": "Acme"},
                    "createdOn": RECENT_TEST_DATE,
                }],
            },
            {
                "id": "42",
                "name": "Software Engineer - New Grad",
                "location": {"fullLocation": "Austin, TX", "country": "us"},
                "company": {"name": "Acme"},
                "applyUrl": "https://jobs.smartrecruiters.com/Acme/42",
                "createdOn": RECENT_TEST_DATE,
                "jobAd": {"sections": {"qualifications": {
                    "text": "Applicants must be authorized to work in the U.S."
                }}},
            },
        ]
        _, jobs = fetch_direct_board({"provider": "smartrecruiters", "slug": "Acme"})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertTrue(matches(jobs[0]))

    @patch("job_watcher.fetch_json")
    def test_recruitee_normalization(self, fetch_json):
        fetch_json.return_value = {"offers": [{
            "id": 42,
            "title": "Machine Learning Engineer - New Grad",
            "location": {"name": "New York, NY"},
            "careers_url": "https://acme.recruitee.com/o/ml-engineer",
            "description": "Entry-level machine learning role.",
            "updated_at": int(time.time()) - 600,
        }]}
        _, jobs = fetch_direct_board({"provider": "recruitee", "slug": "acme"})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertTrue(matches(jobs[0]))

    @patch("job_watcher.fetch_json")
    def test_workable_normalization(self, fetch_json):
        fetch_json.return_value = {
            "name": "Acme",
            "jobs": [{
                "shortcode": "ABC123",
                "title": "Software Engineer - New Grad",
                "url": "https://apply.workable.com/j/ABC123",
                "published_on": "2026-06-22",
                "country": "United States",
                "city": "Austin",
                "state": "Texas",
                "telecommuting": True,
                "experience": "Entry level",
                "description": "<p>Build production software.</p>",
                "locations": [{
                    "city": "Austin", "region": "Texas", "country": "United States"
                }],
            }],
        }
        _, jobs = fetch_direct_board({"provider": "workable", "slug": "acme"})
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertEqual(jobs[0]["posted_at"], "2026-06-22")
        self.assertIn("Remote in USA", jobs[0]["locations"])

    def test_workable_only_treats_post_baseline_ids_as_new(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute(
            "CREATE TABLE observed_source_jobs (source TEXT, job_id TEXT, "
            "first_seen TEXT DEFAULT CURRENT_TIMESTAMP, PRIMARY KEY(source, job_id))"
        )
        connection.execute(
            "CREATE TABLE initialized_boards (board_key TEXT PRIMARY KEY, "
            "initialized_at TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.execute(
            "CREATE TABLE seen_jobs (id TEXT PRIMARY KEY, first_seen TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.execute(
            "CREATE TABLE seen_urls (url TEXT PRIMARY KEY, first_seen TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        connection.execute(
            "CREATE TABLE seen_fingerprints (fingerprint TEXT PRIMARY KEY, "
            "first_seen TEXT DEFAULT CURRENT_TIMESTAMP)"
        )
        old_job = self.job("Software Engineer - New Grad", posted_at=1)
        old_job.update({
            "id": "workable:acme:old", "board_key": "workable:acme",
            "company_name": "Acme", "url": "https://apply.workable.com/j/old",
        })
        candidates, initialized = source_candidates(
            connection, "workable:acme", [old_job]
        )
        self.assertTrue(initialized)
        self.assertEqual(candidates, [])

        new_job = dict(old_job, id="workable:acme:new", url="https://apply.workable.com/j/new")
        candidates, initialized = source_candidates(
            connection, "workable:acme", [old_job, new_job]
        )
        self.assertFalse(initialized)
        self.assertEqual([job["id"] for job in candidates], ["workable:acme:new"])
        self.assertEqual(candidates[0]["posted_display"], "Newly detected")

    @patch("job_watcher.fetch_text")
    def test_approved_feed_normalization_does_not_assume_us(self, fetch_text):
        fetch_text.return_value = json.dumps({"items": [{
            "id": "1",
            "title": "Software Engineer - New Grad",
            "company": "Acme",
            "location": "Toronto, Canada",
            "url": "https://example.com/1",
            "content_text": "Entry-level role.",
            "date_published": RECENT_TEST_DATE,
        }]})
        jobs = fetch_feed_jobs({
            "name": "example", "url": "https://example.com/feed.json",
            "approved": True, "us_only": False,
        })
        self.assertEqual(jobs[0]["locations"], ["Toronto, Canada"])
        self.assertFalse(matches(jobs[0]))

    @patch("job_watcher.fetch_text")
    def test_jazzhr_customer_xml_feed_normalization(self, fetch_text):
        fetch_text.return_value = f"""<jobs><job>
            <title>Data Scientist - New Grad</title><company>Acme</company>
            <city>Boston</city><state>MA</state><country>United States</country>
            <url>https://acme.applytojob.com/apply/ABC/data-scientist</url>
            <description>Entry-level data science role.</description>
            <date>{RECENT_TEST_DATE}</date>
        </job></jobs>"""
        jobs = fetch_feed_jobs({
            "name": "acme-jazzhr",
            "url": "https://example.com/acme.xml",
            "approved": True,
        })
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertEqual(jobs[0]["locations"], ["Boston, MA, United States"])
        self.assertTrue(matches(jobs[0]))

    def test_adaptive_board_schedule(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute(
            "CREATE TABLE board_stats (board_key TEXT PRIMARY KEY, last_checked INTEGER, "
            "relevant_count INTEGER, match_count INTEGER, failure_count INTEGER)"
        )
        boards = [
            {"provider": "greenhouse", "slug": "new"},
            {"provider": "greenhouse", "slug": "high"},
            {"provider": "greenhouse", "slug": "medium"},
            {"provider": "greenhouse", "slug": "quiet"},
        ]
        connection.executemany(
            "INSERT INTO board_stats VALUES (?, ?, ?, ?, 0)",
            [
                ("greenhouse:high", 700, 3, 0),
                ("greenhouse:medium", 0, 1, 0),
                ("greenhouse:quiet", 900, 0, 0),
            ],
        )
        selected = select_due_boards(connection, boards, now=1000)
        self.assertEqual(
            {board["slug"] for board in selected}, {"new"}
        )

    def test_board_batch_is_capped_at_200(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute(
            "CREATE TABLE board_stats (board_key TEXT PRIMARY KEY, last_checked INTEGER, "
            "relevant_count INTEGER, match_count INTEGER, failure_count INTEGER)"
        )
        boards = [
            {"provider": "greenhouse", "slug": f"company-{index}"}
            for index in range(250)
        ]
        self.assertEqual(len(select_due_boards(connection, boards, now=1000)), 200)

    def test_failed_boards_retry_with_bounded_backoff(self):
        self.assertEqual(failure_retry_seconds(1), 5 * 60)
        self.assertEqual(failure_retry_seconds(2), 10 * 60)
        self.assertEqual(failure_retry_seconds(20), 7 * 24 * 60 * 60)

    def test_heartbeat_initializes_on_first_run(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute(
            "CREATE TABLE heartbeat (id INTEGER PRIMARY KEY CHECK (id = 1), last_run INTEGER NOT NULL)"
        )
        message, last_run = heartbeat_message(connection, now=1000)
        self.assertEqual(message, "Heartbeat initialized")
        self.assertIsNone(last_run)

    def test_heartbeat_reports_missed_runs(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute(
            "CREATE TABLE heartbeat (id INTEGER PRIMARY KEY CHECK (id = 1), last_run INTEGER NOT NULL)"
        )
        connection.execute("INSERT INTO heartbeat VALUES (1, 0)")
        message, last_run = heartbeat_message(connection, now=1200)
        self.assertIn("Heartbeat gap detected", message)
        self.assertEqual(last_run, 0)

    @patch("job_watcher.fetch_json")
    def test_adzuna_normalization(self, fetch_json):
        fetch_json.return_value = {"results": [{
            "id": "a1",
            "title": "Data Engineer - New Grad",
            "company": {"display_name": "Acme"},
            "location": {"display_name": "Austin, TX"},
            "redirect_url": "https://example.com/a1",
            "description": "Entry-level role with no citizenship requirement.",
            "created": RECENT_TEST_DATE,
        }]}
        jobs = fetch_adzuna_jobs("app", "key")
        self.assertEqual(jobs[0]["source"], "adzuna")
        self.assertTrue(matches(jobs[0]))

    @patch("job_watcher.fetch_json")
    def test_jooble_normalization(self, fetch_json):
        fetch_json.return_value = {"jobs": [{
            "id": "j1",
            "title": "Software Engineer I",
            "company": "Acme",
            "location": "Remote",
            "link": "https://example.com/j1",
            "snippet": "Entry-level role with no citizenship requirement.",
            "created": RECENT_TEST_DATE,
        }]}
        jobs = fetch_jooble_jobs("key")
        self.assertEqual(jobs[0]["source"], "jooble")
        self.assertTrue(matches(jobs[0]))

    @patch("job_watcher.fetch_json")
    def test_jobicy_normalization(self, fetch_json):
        fetch_json.return_value = {"jobs": [{
            "id": "j2",
            "jobTitle": "AI Engineer - Entry Level",
            "companyName": "Acme",
            "jobGeo": "USA",
            "url": "https://jobicy.com/jobs/j2",
            "jobDescription": "Entry-level role.",
            "pubDate": RECENT_TEST_DATE,
        }]}
        jobs = fetch_jobicy_jobs()
        self.assertEqual(jobs[0]["source"], "jobicy")
        self.assertTrue(matches(jobs[0]))


    def test_rejects_roles_requiring_too_much_experience(self):
        base_job = {
            "title": "Software Engineer I",
            "company_name": "Acme",
            "locations": ["United States"],
            "url": "https://example.com/job",
            "posted_at": RECENT_TEST_DATE,
            "active": True,
            "is_visible": True,
        }

        bad_descriptions = [
            "Entry-level title but requires 2+ years of professional experience.",
            "Must have at least 3 years of software engineering experience.",
            "Requires 4 years experience building backend systems.",
            "Looking for 3-5 years of experience with distributed systems.",
            "Seeking an experienced engineer with proven experience in production systems.",
        ]

        for description in bad_descriptions:
            job = dict(base_job, description=description)
            self.assertFalse(matches(job), description)
            self.assertEqual(rejection_reason(job), "too_much_experience")

        good_job = dict(
            base_job,
            description="New grad role for candidates with 0-2 years of experience. Applicants must be authorized to work in the U.S.",
        )
        self.assertTrue(matches(good_job))

    def test_health_report_summarizes_filter_outcomes(self):
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE run_metrics (run_at INTEGER, source TEXT, fetched INTEGER, "
            "matched INTEGER, new_jobs INTEGER, failures INTEGER, rejections TEXT)"
        )
        now = int(time.time())
        record_metrics(connection, now, "ats:test", [
            self.job("Software Engineer - New Grad", posted_at=now - 60),
            self.job("Senior Software Engineer", posted_at=now - 60),
        ])
        record_metrics(connection, now, "notifications", [], new_jobs=1)
        subject, text, _ = health_report(connection, now)
        self.assertIn("1 alerts", subject)
        self.assertIn("Jobs inspected: 2", text)
        self.assertIn("excluded_seniority_or_internship: 1", text)
        connection.close()


if __name__ == "__main__":
    unittest.main()
