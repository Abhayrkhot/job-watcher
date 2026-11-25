import sqlite3
import socket
import tempfile
import unittest
import urllib.error
import json
import time
from pathlib import Path
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
import job_watcher


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
        # A role open in both countries is still a US role.
        self.assertTrue(matches(self.job("Software Engineer - New Grad", locations=["New York, NY", "Toronto, ON, Canada"])))

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
            "too_much_experience",
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

    def test_parses_recruitee_utc_suffix(self):
        self.assertEqual(
            job_watcher.parse_job_datetime("2026-11-19 16:00:31 UTC"),
            int(datetime(2026, 11, 19, 16, 0, 31, tzinfo=timezone.utc).timestamp()),
        )

    def test_posted_label_shows_when_the_company_posted(self):
        now = int(datetime(2026, 11, 20, 6, 0, tzinfo=timezone.utc).timestamp())
        posted = int(datetime(2026, 11, 20, 3, 4, tzinfo=timezone.utc).timestamp())
        self.assertEqual(job_watcher.posted_label({"posted_at": posted}, now), "2h ago · Nov 19, 10:04 PM ET")
        self.assertEqual(job_watcher.posted_label(
            {"posted_at": "2026-11-19", "posted_precision": "date", "detected_at": now}, now), "Nov 19")
        self.assertEqual(job_watcher.posted_label(
            {"posted_at": None, "posted_precision": "unknown", "detected_at": now - 120}, now),
            "Not published (seen 2m ago)")
        self.assertTrue(job_watcher.posted_label(
            {"posted_at": posted, "posted_precision": "listed"}, now).startswith("Listed 2h ago on Simplify"))

    def test_date_only_posting_stays_fresh_by_detection_time(self):
        now = int(time.time())
        yesterday = time.strftime("%Y-%m-%d", time.gmtime(now - 86400 - 7200))
        job = job_watcher.with_posting(self.job("Software Engineer - New Grad"), yesterday, now)
        self.assertEqual(job["posted_at"], yesterday)
        self.assertEqual(rejection_reason(job), "matched")
        stale = job_watcher.with_posting(self.job("Software Engineer - New Grad"), "2020-01-01", now)
        self.assertEqual(rejection_reason(stale), "outside_24_hours")

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
        # The real posting time is kept for display; first-seen time drives freshness.
        self.assertEqual(candidates[0]["posted_at"], 1)
        self.assertIn("detected_at", candidates[0])

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
        # Boards with relevant roles are polled every 2 minutes, quiet ones every 10.
        selected = select_due_boards(connection, boards, now=1000)
        self.assertEqual(
            {board["slug"] for board in selected}, {"new", "high", "medium"}
        )

    def test_board_batch_is_capped(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute(
            "CREATE TABLE board_stats (board_key TEXT PRIMARY KEY, last_checked INTEGER, "
            "relevant_count INTEGER, match_count INTEGER, failure_count INTEGER)"
        )
        boards = [
            {"provider": "greenhouse", "slug": f"company-{index}"}
            for index in range(2500)
        ]
        self.assertEqual(len(select_due_boards(connection, boards, now=1000)), 2000)

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


class FilterRecallTests(unittest.TestCase):
    """Cases from the 2026-11 audit against a curated new-grad list."""

    def job(self, title, description="General responsibilities.", locations=None, company="Acme"):
        return {"title": title, "description": description, "company_name": company,
                "locations": ["New York, NY"] if locations is None else locations,
                "active": True, "is_visible": True, "posted_at": int(time.time()) - 600}

    def test_titles_humans_label_new_grad(self):
        for title in [
            "Software Development Engineer 1 - Security Platform",
            "Analyst \u2013 Early Career - Full Stack",
            "Robotics Systems & Simulation Engineer",
            "Associate Data Ops Engineer",
            "Associate AI Engineer",
            "Forward Deployed Engineer",
            "Trading Technology Software Developer",
            "Member of Technical Staff - ML Systems",
            "Software Engineer 1 New Grad - Internal Tools",
            "Software Engineer, Agent (New Grad 2027)",
            "Product Engineer",
            "Site Reliability Engineer",
            "Quality Assurance Engineer",
            "Junior Developer",
        ]:
            with self.subTest(title=title):
                self.assertEqual(rejection_reason(self.job(title)), "matched")

    def test_titles_still_rejected(self):
        for title, reason in [
            ("Senior Software Engineer", "excluded_seniority_or_internship"),
            ("Sr. Data Scientist", "excluded_seniority_or_internship"),
            ("Staff Machine Learning Engineer", "excluded_seniority_or_internship"),
            ("Software Engineer III", "excluded_seniority_or_internship"),
            ("Machine Learning Intern", "excluded_seniority_or_internship"),
            ("Engineering Manager, Platform", "excluded_seniority_or_internship"),
            ("Tech Lead, Payments", "excluded_seniority_or_internship"),
            ("Mechanical Engineer", "outside_role_families"),
            ("Hardware Test Engineer", "outside_role_families"),
            ("Field Application Engineer", "outside_role_families"),
            ("Financial Analyst", "outside_role_families"),
            ("Account Executive", "excluded_seniority_or_internship"),
            ("Product Designer", "outside_role_families"),
            ("MBA Summer Associate, Strategy & Analytics - Summer 2027", "excluded_seniority_or_internship"),
        ]:
            with self.subTest(title=title):
                self.assertEqual(rejection_reason(self.job(title)), reason)

    def test_experience_rules(self):
        accepted = [
            "Our customers run an industry that is 20-40 years behind on software.",
            "With over 10 years of experience serving customers, we build tools.",
            "Bachelor's degree or 4 years of relevant work experience.",
            "2+ years of experience, or a Master's degree in Computer Science.",
            "2+ years of experience, including internships or research.",
            "Strong experience building ML systems in Python (coursework welcome).",
            "You will work alongside experienced engineers.",
            "3+ years of Go experience is a plus.",
        ]
        for description in accepted:
            with self.subTest(description=description):
                self.assertEqual(rejection_reason(self.job("Software Engineer", description)), "matched")
        rejected = [
            "Bachelor's degree in Computer Science or related field and 3+ years of experience.",
            "Minimum of 5 years of professional software experience.",
            "2+ years of professional experience building APIs.",
        ]
        for description in rejected:
            with self.subTest(description=description):
                self.assertEqual(
                    rejection_reason(self.job("Software Engineer", description)), "too_much_experience"
                )
        self.assertEqual(rejection_reason(self.job(
            "Full-Stack Software Engineer (New Grad)", "2+ years of experience with React."
        )), "matched")

    def test_location_rules(self):
        us_remote = "Remote role; you must be authorized to work in the United States."
        self.assertEqual(rejection_reason(self.job("Software Engineer", us_remote, locations=["Remote"])), "matched")
        self.assertEqual(rejection_reason(self.job("Software Engineer", us_remote, locations=[])), "matched")
        # Bare "Remote" with nothing saying US is not enough.
        self.assertEqual(rejection_reason(self.job("Software Engineer", locations=["Remote"])), "outside_us")
        self.assertEqual(rejection_reason(self.job("Software Engineer", locations=["LA"])), "matched")
        self.assertEqual(rejection_reason(self.job(
            "Software Engineer", locations=["Remote - US", "Remote - Canada"]
        )), "matched")
        for locations in (["London, UK"], ["Remote in Canada"], ["Bengaluru, India"],
                          ["Noida, Uttar Pradesh, India", "IN"], ["IND - Pune"], ["Gurugram, Haryana"]):
            with self.subTest(locations=locations):
                self.assertEqual(
                    rejection_reason(self.job("Software Engineer", locations=locations)), "outside_us"
                )

    def test_review_of_emailed_jobs_2026_11_20(self):
        rejected = {
            "Department Administrator, Software Engineering": "excluded_seniority_or_internship",
            "Software Engineer AVP": "excluded_seniority_or_internship",
            "Associate Scientist, Post Doc Fellow- Computational Pathology": "excluded_seniority_or_internship",
            "Drug Development Data Scientist": "outside_role_families",
            "Research Scientist - Arts, Humanities, and Ethics": "outside_role_families",
            "Research Scientist - Bioanalytical LCMS Research and Development": "outside_role_families",
            "Facility Analyst - Strategy Data & Analytics": "outside_role_families",
            "Business Analyst, Finance AI & Digital Solutions": "outside_role_families",
            "Strategic Technology Analyst, Vendor Solutions": "outside_role_families",
            "Product Development Engineer - R&D": "outside_role_families",
            "Lithography System Performance Engineer - Defectivity": "outside_role_families",
            "Product Change Control Engineer": "outside_role_families",
            "Controls/Automation Engineer": "outside_role_families",
            "Solutions Engineer, Wireless - NJ, Remote": "outside_role_families",
        }
        for title, reason in rejected.items():
            with self.subTest(title=title):
                self.assertEqual(rejection_reason(self.job(title)), reason)
        for title in [
            "Associate Applied ML Scientist - 1087",
            "Machine Learning Research Scientist, Open-Endedness",
            "AI Scientist, Computational Protein Design",
            "Analyst, Data Systems and Automation (2027 New College Graduate)",
            "Pricing Analytics Analyst",
            "2027 Advance Program Data Associate (DC)",
            "Systems / Software Engineer I Graduate",
            "Embedded Software Engineering Specialist",
        ]:
            with self.subTest(title=title):
                self.assertEqual(rejection_reason(self.job(title)), "matched")

    def test_stricter_experience_rules(self):
        too_much = [
            # the strictest requirement counts, not the smallest
            "At least one year using AI coding assistants. 6+ years of professional .NET development experience.",
            # "N+ years <doing something>" is a requirement without the word "experience"
            "3+ years building and operating distributed systems.",
            # tiered requirements: the Master's path applies
            "Bachelor's Degree and 6 years of experience OR Master's Degree and 5 years of experience OR PhD and 0 years of experience.",
            "BS in Electrical Engineering with 10+ years of relevant industry experience OR MS with 8+ years.",
            # sections run together without periods
            "Basic Qualifications: Bachelor's Degree At least 2 years of experience with integrations Preferred Qualifications: Master's degree",
            "We are seeking a mid-level Data Platform Engineer.",
            "You have several years of experience shipping consumer software.",
        ]
        for description in too_much:
            with self.subTest(description=description):
                self.assertEqual(rejection_reason(self.job("Software Engineer", description)), "too_much_experience")
        fine = [
            "Bachelor's degree with 4+ years of relevant experience, or Master's degree with 1+ years.",
            "2-3 years of professional software experience.",
            "With our global scale and deep expertise, you'll grow your career.",
            "Considerations For Top Candidates: Extensive experience designing embedded software in C++.",
            "Preferred Qualifications: 5+ years of Java experience.",
        ]
        for description in fine:
            with self.subTest(description=description):
                self.assertEqual(rejection_reason(self.job("Software Engineer", description)), "matched")
        self.assertEqual(rejection_reason(self.job(
            "Full-Stack Software Engineer (New Grad)", "Our founders bring 12+ years in construction tech."
        )), "matched")

    def test_citizenship_rules(self):
        eeo = ("We consider all applicants without regard to race, religion, national origin, "
               "or U.S. citizenship status.")
        self.assertEqual(rejection_reason(self.job("Software Engineer", eeo)), "matched")
        for description in (
            "U.S. citizenship or permanent residency required.",
            "Must be a U.S. person as defined by ITAR.",
            "This role requires an active Secret clearance.",
            "Ability to obtain a security clearance.",
        ):
            with self.subTest(description=description):
                self.assertEqual(
                    rejection_reason(self.job("Software Engineer", description)),
                    "citizenship_requirement",
                )

    def test_excluded_companies(self):
        for company in ("Jane Street", "SpaceX", "TikTok", "Hudson River Trading"):
            with self.subTest(company=company):
                self.assertEqual(
                    rejection_reason(self.job("Software Engineer", company=company)),
                    "excluded_company",
                )
        job = self.job("Software Engineer", company="Andurilindustries")
        job["board_key"] = "greenhouse:andurilindustries"
        self.assertEqual(rejection_reason(job), "excluded_company")


class AlertAuditTests(unittest.TestCase):
    """Cases from the 2026-11-25 review of three days of emailed alerts."""

    def job(self, description, title="Software Engineer I", locations=None):
        return {"title": title, "description": description, "company_name": "Acme",
                "locations": ["Austin, TX"] if locations is None else locations,
                "active": True, "is_visible": True, "posted_at": int(time.time()) - 600}

    def reason(self, description, title="Software Engineer I"):
        return rejection_reason(self.job(description, title))

    def test_rejects_postings_that_will_not_sponsor(self):
        for description in [
            "Visa sponsorship is not available for this position.",
            "Toyota does not offer support or sponsorship of job applicants for employment-based "
            "visas. You should not apply for this role if you will require Toyota to assist with "
            "immigration support or sponsorship now or in the future.",
            "Must be legally authorized to work in the United States without the need for employer "
            "sponsorship, now or at any time in the future.",
            "Must be presently authorized to work in the U.S. without a requirement for work "
            "authorization sponsorship by our company.",
            "At this time, we are unable to offer employment for individuals located outside of the "
            "United States and/or those who require sponsorship for employment visa status (e.g., H-1B).",
            "**No sponsorship offered for this position**",
            "The Company will not sponsor applicants for work visas for this position.",
            "Unable to sponsor or transfer H-1B visas currently.",
            "Veeva does not provide sponsorship for employment visa status (e.g., H-1B, OPT, or TN status).",
            "Employer is unable to provide visa sponsorship for employment, or support STEM OPT.",
            "U.S. work authorization required (no visa sponsorship provided)",
            "We are not able to sponsor visas at this time.",
        ]:
            with self.subTest(description=description[:50]):
                self.assertEqual(self.reason(description), "no_sponsorship")

    def test_keeps_postings_that_sponsor_or_say_nothing(self):
        for description in [
            "Visa sponsorship: We do sponsor visas! However, we aren't able to successfully "
            "sponsor visas for every role and every candidate.",
            "Visa sponsorship is available, including O-1A, H-1B transfer, and TN support.",
            "We help with relocation and support H-1B transfers.",
            "We are pleased to offer visa sponsorship for eligible and qualified individuals.",
            "Will you, now or in the future, require sponsorship for employment in the U.S.? Yes No",
            "No sponsorship information provided.",
            "Employees are covered under Smartsheet-sponsored life insurance.",
            "Company sponsored events and game nights. Legally eligible to work in the U.S.",
        ]:
            with self.subTest(description=description[:50]):
                self.assertEqual(self.reason(description), "matched")

    def test_sponsorship_filter_can_be_turned_off(self):
        with patch("job_watcher.FILTER_NO_SPONSORSHIP", False):
            self.assertEqual(self.reason("Visa sponsorship is not available."), "matched")

    def test_rejects_clearance_and_us_person_requirements(self):
        for description in [
            "Required Qualifications: TS/SCI with Full Scope Polygraph",
            "Clearance: Active DoD Secret. Employment Type: Full-time",
            "Clearance Requirements TS Clearance with Customer Appropriate Polygraph",
            "Due to federal contract requirements, United States Citizenship and "
            "position-appropriate security clearance are required.",
            "Due to ITAR regulations, this role is only open to U.S. Citizens, lawful permanent "
            "residents (green card holders) or foreign nationals granted refugee or asylee status.",
            "We are required to ensure that such access is provided only to individuals who are "
            "classified as 'U.S. Persons.'",
            "Must be able to obtain and maintain a Public Trust clearance.",
            "Must be able to obtain and maintain the required customer clearance for access to systems.",
        ]:
            with self.subTest(description=description[:50]):
                self.assertEqual(self.reason(description), "citizenship_requirement")

    def test_keeps_clearance_lookalikes(self):
        for description in [
            "Understand the customer's security objectives, current secret stores, and deployment model.",
            "Candidates must be able to comply with U.S. export control regulations, which may include "
            "meeting the definition of a U.S. Person where required.",
            "Do you currently hold an active Secret or Top Secret clearance? Yes No",
            "Our mission is to earn the public trust in AI.",
        ]:
            with self.subTest(description=description[:50]):
                self.assertEqual(self.reason(description), "matched")

    def test_years_in_one_bullet_are_not_cancelled_by_another(self):
        for description in [
            "<p>Requirements</p><ul><li>Experience: 4+ years in a technical, customer-facing "
            "engineering role, ideally at a fast-growing company.</li></ul>",
            "<ul><li>CISSP, CCSP, or GIAC security certification preferred</li>"
            "<li>A minimum of 3 years of security experience</li></ul>",
            "<ul><li>3+ years of experience in data analytics, ideally including revenue reporting."
            "</li><li>Tableau is a plus</li></ul>",
            "What You Need Minimum 5+ years of experience shipping architectures for critical "
            "systems preferred",
            "A bachelor's degree in a technical discipline and three (3) years of experience.",
            "No bachelor's degree with eleven (11) years of qualifying experience.",
        ]:
            with self.subTest(description=description[:50]):
                text = job_watcher.plain_text(description)
                self.assertEqual(self.reason(text), "too_much_experience")

    def test_years_that_are_not_a_requirement(self):
        for description in [
            "Bachelor's degree with zero (0) to two (2) years of professional experience.",
            "One to three years of experience building and shipping production software, ideally "
            "at an early-stage startup.",
            "Bachelor's in Computer Science, Data Science, or relevant STEM certification; or 4 years "
            "of equivalent experience.",
            "Bachelor's degree or a minimum of 5 years' experience developing software applications.",
            "4 years of experience may be substituted for a bachelor's degree.",
            "Bachelor's degree from an accredited 4 year university with a 3.0 GPA. Must have taken C.S. classes.",
            "The team has tripled in size within the last five (5) years.",
            "Experience: Less than 3 years prior related experience.",
            "You don't need to be an experienced software engineer.",
            "How many years of work experience do you have? * Please Select 0 - 1 Years 1 - 2 Years "
            "2 - 3 Years 3 - 4 Years 4 - 7 Years 7 - 15 Years",
            "Basic Qualifications - Are 18 years of age or older - Experience in HTML, CSS, or JavaScript",
        ]:
            with self.subTest(description=description[:50]):
                self.assertEqual(self.reason(description), "matched")

    def test_each_figure_keeps_its_own_cue(self):
        # "2 years required" counts even though "4 years preferred" follows it.
        self.assertEqual(job_watcher.required_years("Experience: 2 years required/ 4 years preferred"), 2)
        self.assertIsNone(job_watcher.required_years("3+ years of Go experience is a plus."))

    def test_plain_text_keeps_bullets_on_their_own_lines(self):
        self.assertEqual(
            job_watcher.plain_text("<ul><li>Python</li><li>3+ years&nbsp;of Go</li></ul><p>Apply</p>"),
            "Python\n3+ years of Go\nApply",
        )
        self.assertEqual(job_watcher.plain_text("Software <b>Engineer</b>"), "Software Engineer")

    @patch("job_watcher.fetch_json")
    def test_lever_reads_requirement_lists(self, fetch_json):
        fetch_json.return_value = [{
            "id": "abc", "text": "IT Infrastructure Engineer II", "hostedUrl": "https://jobs.lever.co/acme/abc",
            "createdAt": int(time.time() * 1000) - 60_000,
            "categories": {"location": "Broomfield, CO"}, "country": "US",
            "descriptionPlain": "Join our team.",
            "lists": [{"text": "YOU MUST HAVE:", "content": "<li>Bachelor's degree</li>"
                       "<li>3–5 years’ experience across systems</li>"}],
            "additionalPlain": "Benefits.",
        }]
        _, jobs = fetch_direct_board({"provider": "lever", "slug": "acme"})
        self.assertIn("3–5 years", jobs[0]["description"])
        self.assertEqual(rejection_reason(jobs[0]), "too_much_experience")

    def test_simplify_does_not_offer_sponsorship_label(self):
        now = int(time.time())
        jobs = job_watcher.simplify_jobs([{
            "id": "a", "category": "Software", "date_posted": now - 600,
            "title": "Software Engineer, New Grad", "company_name": "Acme", "locations": ["Austin, TX"],
            "url": "https://careers.acme.com/1", "active": True, "sponsorship": "Does Not Offer Sponsorship",
        }], now)
        self.assertEqual(rejection_reason(jobs[0]), "no_sponsorship")

    def test_simplify_copies_of_directly_read_boards_are_dropped(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE initialized_boards (board_key TEXT PRIMARY KEY, initialized_at TEXT)")
        connection.execute("CREATE TABLE board_stats (board_key TEXT PRIMARY KEY, last_checked INTEGER, "
                           "relevant_count INTEGER, match_count INTEGER, failure_count INTEGER)")
        now = int(time.time())
        old = datetime.fromtimestamp(now - 10 * 86400, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        new = datetime.fromtimestamp(now - 3600, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        connection.executemany("INSERT INTO initialized_boards VALUES (?, ?)", [
            ("smartrecruiters:AbbVie", old), ("greenhouse:fresh", new), ("lever:broken", old),
        ])
        connection.execute("INSERT INTO board_stats VALUES ('lever:broken', 0, 0, 0, 3)")
        jobs = [{"url": url, "posted_at": now - 600} for url in (
            "https://jobs.smartrecruiters.com/AbbVie/3743990017034256",  # read directly: dropped
            "https://job-boards.greenhouse.io/fresh/jobs/1",  # board baselined after posting
            "https://jobs.lever.co/broken/abc",  # board failing, so Simplify is the only path
            "https://jobs.lever.co/unknown/abc",  # board not in the registry yet
            "https://careers.example.com/jobs/1",  # an ATS the watcher cannot read
        )]
        kept = job_watcher.without_direct_duplicates(connection, jobs)
        self.assertEqual([job["url"] for job in kept], [job["url"] for job in jobs[1:]])

    def test_higher_levels_of_a_leveled_posting_do_not_count(self):
        description = (
            "Level 1 - No experience required beyond the minimum education requirement.\n"
            "Level 2 - Two years of directly related experience is required.\n"
            "Level 3 - Four years of directly related experience is required."
        )
        self.assertEqual(self.reason(description, "Developer - Analytics Engineer"), "matched")


class PostingPageTests(unittest.TestCase):
    """Simplify backstop jobs have their posting page read (owner's decision 2026-11-25)."""

    LD_PAGE = """<html><head><script type="application/ld+json">
    {"@context": "https://schema.org", "@graph": [{"@type": "Organization", "name": "Acme"},
     {"@type": "JobPosting", "title": "Software Engineer",
      "description": "&lt;p&gt;Build things.&lt;/p&gt;&lt;ul&gt;&lt;li&gt;Python&lt;/li&gt;&lt;/ul&gt;",
      "qualifications": "Must be legally authorized to work in the United States without the need for employer sponsorship.",
      "experienceRequirements": {"@type": "OccupationalExperienceRequirements", "monthsOfExperience": 60}}]}
    </script></head><body><nav>Careers home</nav></body></html>"""

    def test_reads_the_job_posting_json_ld(self):
        text = job_watcher.posting_page_text(self.LD_PAGE)
        self.assertIn("Build things.\nPython", text)
        self.assertIn("without the need for employer sponsorship", text)
        self.assertIn("Minimum of 5 years of experience required.", text)
        self.assertNotIn("Careers home", text)

    def test_falls_back_to_visible_text_before_the_application_form(self):
        page = ("<html><head><style>p{}</style></head><body><script>var x = '10+ years';</script>"
                f"<h1>Software Engineer</h1><p>{'About the role. ' * 40}</p>"
                "<p>Visa sponsorship is not available.</p><h2>Apply for this job</h2>"
                "<label>How many years of experience do you have?</label><p>7 - 15 Years</p></body></html>")
        text = job_watcher.posting_page_text(page)
        self.assertIn("Visa sponsorship is not available.", text)
        self.assertNotIn("10+ years", text)
        self.assertNotIn("7 - 15 Years", text)

    def connection(self):
        directory = Path(tempfile.mkdtemp())
        with patch("job_watcher.DB_PATH", directory / "jobs.sqlite3"), \
                patch("job_watcher.STATE_DIR", directory):
            connection = job_watcher.connect_db()
        self.addCleanup(connection.close)
        return connection

    def backstop_job(self, url, title="Software Engineer, New Grad", description=""):
        return {"source": "simplify", "id": f"simplify:{url}", "title": title, "company_name": "Acme",
                "locations": ["Austin, TX"], "url": url, "description": description,
                "posted_at": int(time.time()) - 600, "posted_precision": "listed",
                "active": True, "is_visible": True}

    def test_only_new_matching_jobs_without_a_description_are_read(self):
        connection = self.connection()
        jobs = [
            self.backstop_job("https://careers.acme.com/1"),  # read: no sponsorship
            self.backstop_job("https://careers.acme.com/2"),  # read: clean, still mailed
            self.backstop_job("https://careers.acme.com/3"),  # page fails: left as it was
            self.backstop_job("https://careers.acme.com/4", title="Senior Software Engineer"),
            self.backstop_job("https://careers.acme.com/5", description="U.S. citizenship is required."),
            self.backstop_job("https://careers.acme.com/6", title="Data Engineer, New Grad"),  # mailed
        ]
        job_watcher.record_jobs(connection, [jobs[5]])
        pages = {
            "https://careers.acme.com/1": "Visa sponsorship is not available for this position.",
            "https://careers.acme.com/2": "Entry level. 0-2 years of experience.",
        }
        read = []

        def fetch(url):
            read.append(url)
            if url not in pages:
                raise http_error(403)
            return pages[url]

        with patch("job_watcher.fetch_posting_page", side_effect=fetch):
            job_watcher.with_page_descriptions(connection, jobs)
        self.assertEqual(sorted(read), [f"https://careers.acme.com/{n}" for n in (1, 2, 3)])
        self.assertEqual([rejection_reason(job) for job in jobs[:3]], ["no_sponsorship", "matched", "matched"])

    def test_javascript_only_pages_use_the_readers_detail_endpoints(self):
        calls = []

        def fetch(url, data=None, board=None):
            calls.append(url)
            if "bamboohr" in url:
                return {"result": {"jobOpening": {"minimumExperience": "Mid-level", "description": "<p>Build</p>"}}}
            return {"items": [{"ExternalQualificationsStr": "<li>We are unable to sponsor visas.</li>"}]}

        with patch("job_watcher.fetch_json", side_effect=fetch), \
                patch("job_watcher.workday_fetch", return_value={"jobPostingInfo": {"jobDescription": "<p>Ship</p>"}}) as workday:
            self.assertEqual(job_watcher.fetch_posting_page("https://acme.bamboohr.com/careers/215"), "Mid-level\nBuild")
            self.assertIn("unable to sponsor", job_watcher.fetch_posting_page(
                "https://efds.fa.em5.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/74499"))
            self.assertEqual(job_watcher.fetch_posting_page(
                "https://acme.wd1.myworkdayjobs.com/en-US/Careers/job/Austin-TX/Engineer_R1"), "Ship")
        self.assertEqual(calls, [
            "https://acme.bamboohr.com/careers/215/detail",
            "https://efds.fa.em5.oraclecloud.com/hcmRestApi/resources/latest/recruitingCEJobRequisitionDetails"
            "?expand=all&onlyData=true&finder=ById;Id=%2274499%22,siteNumber=CX_1",
        ])
        workday.assert_called_once_with(
            "https://acme.wd1.myworkdayjobs.com/wday/cxs/acme/Careers/job/Austin-TX/Engineer_R1")

    def test_page_reads_are_capped_per_run(self):
        connection = self.connection()
        jobs = [self.backstop_job(f"https://careers.acme.com/{n}") for n in range(20)]
        with patch("job_watcher.fetch_posting_page", return_value="0-2 years of experience.") as fetch:
            job_watcher.with_page_descriptions(connection, jobs)
        self.assertEqual(fetch.call_count, job_watcher.SIMPLIFY_PAGE_READS_PER_RUN)


def http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError("https://example.com", code, "error", None, None)


DNS_ERROR = urllib.error.URLError(
    socket.gaierror(8, "nodename nor servname provided, or not known")
)


class BoardHealthTests(unittest.TestCase):
    def test_network_errors_are_not_board_errors(self):
        for exc in (
            DNS_ERROR,
            socket.timeout("The read operation timed out"),
            ConnectionResetError(54, "Connection reset by peer"),
        ):
            self.assertTrue(job_watcher.is_network_error(exc), exc)
            self.assertFalse(job_watcher.is_missing_board_error(exc), exc)
        self.assertFalse(job_watcher.is_network_error(http_error(404)))
        self.assertTrue(job_watcher.is_missing_board_error(http_error(404)))
        self.assertTrue(job_watcher.is_missing_board_error(http_error(410)))
        self.assertFalse(job_watcher.is_missing_board_error(http_error(500)))
        self.assertFalse(job_watcher.is_network_error(ValueError("bad json")))

    def test_network_outage_rule(self):
        self.assertFalse(job_watcher.network_outage(0, 0))
        self.assertTrue(job_watcher.network_outage(0, 1))
        self.assertTrue(job_watcher.network_outage(2, 3))
        self.assertFalse(job_watcher.network_outage(20, 2))

    def test_wait_for_network_retries_then_gives_up(self):
        sleeps = []
        with patch("job_watcher.network_available", side_effect=[False, False, True]):
            self.assertTrue(job_watcher.wait_for_network(sleep=sleeps.append))
        self.assertEqual(sleeps, [5, 5])

        ticks = iter(range(0, 1000, 5))
        with patch("job_watcher.network_available", return_value=False):
            self.assertFalse(job_watcher.wait_for_network(
                timeout=20, sleep=lambda _: None, clock=lambda: next(ticks),
            ))

    def test_unreachable_board_keeps_failure_count(self):
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute(
            "CREATE TABLE board_stats (board_key TEXT PRIMARY KEY, last_checked INTEGER NOT NULL, "
            "relevant_count INTEGER NOT NULL DEFAULT 0, match_count INTEGER NOT NULL DEFAULT 0, "
            "failure_count INTEGER NOT NULL DEFAULT 0)"
        )
        connection.execute("INSERT INTO board_stats VALUES ('lever:flaky', 100, 0, 0, 2)")
        job_watcher.record_board_unreachable(
            connection, {"provider": "lever", "slug": "flaky"}, 500
        )
        job_watcher.record_board_unreachable(
            connection, {"provider": "ashby", "slug": "fresh"}, 500
        )
        self.assertEqual(
            sorted(connection.execute("SELECT * FROM board_stats")),
            [("ashby:fresh", 500, 0, 0, 0), ("lever:flaky", 500, 0, 0, 2)],
        )

    def test_find_migrated_board_probes_other_providers(self):
        calls = []

        def fetch(url):
            calls.append(url)
            if "ashbyhq" in url:
                return {"jobs": [{"title": "Engineer", "isListed": True}]}
            raise http_error(404)

        moved = job_watcher.find_migrated_board(
            {"provider": "greenhouse", "slug": "benchling"}, fetch=fetch
        )
        self.assertEqual(moved, {
            "provider": "ashby", "slug": "benchling",
            "migrated_from": "greenhouse:benchling",
        })
        self.assertFalse(any("greenhouse" in url for url in calls))

    def test_find_migrated_board_ignores_empty_and_unlisted_boards(self):
        def fetch(url):
            if "ashbyhq" in url:
                return {"jobs": [{"title": "Engineer", "isListed": False}]}
            if "lever" in url:
                return []
            raise http_error(404)

        self.assertIsNone(job_watcher.find_migrated_board(
            {"provider": "lever-eu", "slug": "gone"}, fetch=fetch
        ))

    def test_apply_board_migration_rewrites_registry_and_keeps_baseline(self):
        directory = Path(tempfile.mkdtemp())
        registry = directory / "ats_boards.json"
        registry.write_text(json.dumps([
            {"provider": "ashby", "slug": "first"},
            {"provider": "greenhouse", "slug": "benchling"},
            {"provider": "lever", "slug": "last"},
        ]))
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE initialized_boards (board_key TEXT PRIMARY KEY)")
        connection.execute(
            "CREATE TABLE board_stats (board_key TEXT PRIMARY KEY, last_checked INTEGER, "
            "relevant_count INTEGER, match_count INTEGER, failure_count INTEGER)"
        )
        connection.execute("INSERT INTO initialized_boards VALUES ('greenhouse:benchling')")
        connection.execute("INSERT INTO board_stats VALUES ('greenhouse:benchling', 1, 0, 0, 3)")

        job_watcher.apply_board_migration(
            connection,
            {"provider": "greenhouse", "slug": "benchling"},
            {"provider": "ashby", "slug": "benchling", "migrated_from": "greenhouse:benchling"},
            registry,
        )

        self.assertEqual(json.loads(registry.read_text()), [
            {"provider": "ashby", "slug": "first"},
            {"provider": "ashby", "slug": "benchling", "migrated_from": "greenhouse:benchling"},
            {"provider": "lever", "slug": "last"},
        ])
        self.assertEqual(list(directory.glob(".*.tmp")), [])
        self.assertIsNotNone(connection.execute(
            "SELECT 1 FROM initialized_boards WHERE board_key = 'ashby:benchling'"
        ).fetchone())
        self.assertEqual(connection.execute("SELECT COUNT(*) FROM board_stats").fetchone(), (0,))


class MainRunTests(unittest.TestCase):
    def run_main(self, boards, fetch_board, fetch_json=None):
        directory = Path(tempfile.mkdtemp())
        registry = directory / "ats_boards.json"
        registry.write_text(json.dumps(boards))
        patches = [
            patch("job_watcher.STATE_DIR", directory),
            patch("job_watcher.DB_PATH", directory / "jobs.sqlite3"),
            patch("job_watcher.BOARDS_PATH", registry),
            patch("job_watcher.load_feeds", return_value=[]),
            patch("job_watcher.wait_for_network", return_value=True),
            patch("job_watcher.fetch_direct_board", side_effect=fetch_board),
            patch("job_watcher.send_email"),
            patch("job_watcher.send_push"),
            patch("job_watcher.send_health_report_if_due"),
            patch("job_watcher.fetch_simplify", return_value=[]),
            patch("job_watcher.sys.argv", ["job_watcher.py"]),
        ]
        if fetch_json is not None:
            patches.append(patch("job_watcher.fetch_json", side_effect=fetch_json))
        aggregators = patch("job_watcher.fetch_due_aggregators", return_value=[])
        for item in patches:
            item.start()
            self.addCleanup(item.stop)
        self.aggregators = aggregators.start()
        self.addCleanup(aggregators.stop)
        self.assertEqual(job_watcher.main(), 0)
        connection = sqlite3.connect(directory / "jobs.sqlite3")
        self.addCleanup(connection.close)
        stats = {
            row[0]: row[1:]
            for row in connection.execute(
                "SELECT board_key, last_checked, failure_count FROM board_stats"
            )
        }
        return stats, json.loads(registry.read_text())

    def test_unchanged_board_is_marked_checked_without_jobs(self):
        def fetch_board(board):
            board["_not_modified"] = True
            return board, []

        stats, _ = self.run_main([{"provider": "lever", "slug": "acme"}], fetch_board)
        self.assertEqual(stats["lever:acme"][1], 0)

    def test_workday_baseline_then_new_job_is_emailed(self):
        board = {"provider": "workday", "slug": "acme.wd5.myworkdayjobs.com/acme/Careers", "company": "Acme"}
        postings = [{"title": "Software Engineer", "externalPath": "/job/NYC/SE_R-1", "postedOn": "Posted Today"}]
        detail_calls = []

        def fetch_json(url, data=None):
            if url.endswith("/jobs"):
                return {"total": len(postings), "jobPostings": list(postings)}
            detail_calls.append(url)
            return {"jobPostingInfo": {"title": "Software Engineer, New Grad", "jobDescription": "Build.",
                                       "location": "New York, NY", "startDate": TODAY}}

        with patch("job_watcher.fetch_json", side_effect=fetch_json), \
                patch("job_watcher.select_due_boards", side_effect=lambda c, b, n: b):
            directory = Path(tempfile.mkdtemp())
            registry = directory / "ats_boards.json"
            registry.write_text(json.dumps([board]))
            with patch("job_watcher.STATE_DIR", directory), \
                    patch("job_watcher.DB_PATH", directory / "jobs.sqlite3"), \
                    patch("job_watcher.BOARDS_PATH", registry), \
                    patch("job_watcher.load_feeds", return_value=[]), \
                    patch("job_watcher.wait_for_network", return_value=True), \
                    patch("job_watcher.fetch_due_aggregators", return_value=[]), \
                    patch("job_watcher.fetch_simplify", return_value=[]), \
                    patch("job_watcher.send_health_report_if_due"), \
                    patch("job_watcher.sys.argv", ["job_watcher.py"]), \
                    patch("job_watcher.send_push"), \
                    patch("job_watcher.send_email") as send_email:
                job_watcher.main()                      # first poll: baseline
                self.assertEqual(detail_calls, [])
                send_email.assert_not_called()
                postings.insert(0, {"title": "Software Engineer, New Grad",
                                    "externalPath": "/job/NYC/SE-NG_R-2", "postedOn": "Posted Today"})
                job_watcher.main()                      # second poll: one new id
                self.assertEqual(len(detail_calls), 1)
                self.assertEqual([job["title"] for job in send_email.call_args[0][0]],
                                 ["Software Engineer, New Grad"])

    def test_offline_batch_leaves_boards_due_and_skips_aggregators(self):
        def fetch_board(board):
            raise DNS_ERROR

        stats, _ = self.run_main(
            [{"provider": "greenhouse", "slug": f"company-{n}"} for n in range(5)],
            fetch_board,
        )
        self.assertEqual(stats, {})
        self.aggregators.assert_not_called()

    def test_mixed_batch_moves_404_boards_and_spares_flaky_ones(self):
        def fetch_board(board):
            if board["slug"] == "benchling":
                raise http_error(404)
            if board["slug"] == "dead":
                raise http_error(404)
            if board["slug"] == "slow":
                raise socket.timeout("The read operation timed out")
            if board["slug"] == "broken":
                raise http_error(500)
            if board["slug"] == "throttled":
                raise http_error(429)
            return board, []

        def fetch_json(url):
            if "ashbyhq" in url and "/benchling" in url:
                return {"jobs": [{"title": "Engineer"}]}
            raise http_error(404)

        boards = [
            {"provider": "greenhouse", "slug": "benchling"},
            {"provider": "greenhouse", "slug": "dead"},
            {"provider": "greenhouse", "slug": "slow"},
            {"provider": "greenhouse", "slug": "broken"},
            {"provider": "workday", "slug": "throttled"},
            *({"provider": "lever", "slug": f"ok-{n}"} for n in range(4)),
        ]
        stats, registry = self.run_main(boards, fetch_board, fetch_json)

        self.assertEqual(stats["greenhouse:dead"][1], 1)
        self.assertEqual(stats["greenhouse:broken"][1], 1)
        self.assertEqual(stats["greenhouse:slow"][1], 0)
        self.assertEqual(stats["workday:throttled"][1], 0)  # 429 is not the board's fault
        self.assertNotIn("greenhouse:benchling", stats)
        self.assertIn(
            {"provider": "ashby", "slug": "benchling", "migrated_from": "greenhouse:benchling"},
            registry,
        )
        self.assertNotIn({"provider": "greenhouse", "slug": "benchling"}, registry)
        self.aggregators.assert_called_once()



TODAY = time.strftime("%Y-%m-%d", time.gmtime())


class CareerSiteTests(unittest.TestCase):
    def test_workday_lists_then_fetches_only_new_recent_titles(self):
        board = {"provider": "workday", "slug": "acme.wd5.myworkdayjobs.com/acme/Careers",
                 "company": "Acme", "_known_ids": frozenset({"Data-Engineer_R-1"})}
        listing = {"total": 3, "jobPostings": [
            {"title": "Software Engineer, New Grad", "externalPath": "/job/NYC/Software-Engineer_R-3", "postedOn": "Posted Today", "locationsText": "New York, NY"},
            {"title": "Senior Accountant", "externalPath": "/job/NYC/Senior-Accountant_R-2", "postedOn": "Posted Today"},
            {"title": "Data Engineer", "externalPath": "/job/NYC/Data-Engineer_R-1", "postedOn": "Posted Yesterday"},
        ]}
        detail = {"jobPostingInfo": {"title": "Software Engineer, New Grad", "jobDescription": "<p>Build things.</p>",
                  "location": "New York, NY", "country": {"descriptor": "United States of America"},
                  "startDate": TODAY, "externalUrl": "https://acme.wd5.myworkdayjobs.com/Careers/job/NYC/Software-Engineer_R-3"}}
        calls = []

        def fetch(url, data=None):
            calls.append(url)
            return listing if url.endswith("/jobs") else detail

        with patch("job_watcher.fetch_json", side_effect=fetch):
            jobs = job_watcher.fetch_workday_board(board, now=int(time.time()))
        self.assertEqual([job["title"] for job in jobs], ["Software Engineer, New Grad"])
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertTrue(matches(jobs[0]))
        self.assertEqual(sorted(board["_listed_ids"]),
                         ["Data-Engineer_R-1", "Senior-Accountant_R-2", "Software-Engineer_R-3"])
        self.assertEqual(len(calls), 2)  # one list call, one detail call

    def test_workday_pacing_widens_after_a_429(self):
        job_watcher._workday_throttled.clear()
        job_watcher._workday_next_slot.clear()
        with patch("job_watcher.fetch_json", side_effect=http_error(429)), \
                patch("job_watcher.time.sleep"):
            with self.assertRaises(urllib.error.HTTPError):
                job_watcher.workday_fetch("https://acme.wd3.myworkdayjobs.com/wday/cxs/acme/C/jobs")
        self.assertIn("wd3.myworkdayjobs.com", job_watcher._workday_throttled)
        self.assertTrue(job_watcher.is_transient_http_error(http_error(429)))

    def test_first_poll_is_a_baseline_without_detail_calls(self):
        board = {"provider": "bamboohr", "slug": "acme", "_baseline": True}
        with patch("job_watcher.fetch_json", return_value={"result": [
            {"id": "7", "jobOpeningName": "Software Engineer"},
        ]}) as fetch:
            self.assertEqual(job_watcher.fetch_bamboohr_board(board, now=int(time.time())), [])
        self.assertEqual(fetch.call_count, 1)
        self.assertEqual(board["_listed_ids"], ["7"])

    def test_bamboohr_detail(self):
        board = {"provider": "bamboohr", "slug": "acme"}

        def fetch(url, data=None):
            if url.endswith("/list"):
                return {"result": [{"id": "7", "jobOpeningName": "Junior Data Analyst", "isRemote": True}]}
            return {"result": {"jobOpening": {
                "jobOpeningName": "Junior Data Analyst", "description": "<p>SQL</p>",
                "location": {"city": "Austin", "state": "Texas", "addressCountry": "United States"},
                "datePosted": TODAY, "jobOpeningShareUrl": "https://acme.bamboohr.com/careers/7"}}}

        with patch("job_watcher.fetch_json", side_effect=fetch):
            jobs = job_watcher.fetch_bamboohr_board(board, now=int(time.time()))
        self.assertEqual(jobs[0]["locations"], ["Austin, Texas, United States", "Remote"])
        self.assertTrue(matches(jobs[0]))

    def test_rippling_detail_uses_company_name_and_role_text(self):
        board = {"provider": "rippling", "slug": "acme"}

        def fetch(url, data=None):
            if url.endswith("/jobs"):
                return [{"uuid": "u1", "name": "Software Engineer I", "workLocation": {"label": "Remote (US)"}}]
            return {"name": "Software Engineer I", "companyName": "Acme Inc",
                    "description": {"company": "<p>About</p>", "role": "<p>0-2 years</p>"},
                    "workLocations": ["Remote (US)"], "createdOn": "2099-01-01T00:00:00-08:00",
                    "url": "https://ats.rippling.com/acme/jobs/u1"}

        with patch("job_watcher.fetch_json", side_effect=fetch):
            jobs = job_watcher.fetch_rippling_board(board, now=int(time.time()))
        self.assertEqual(jobs[0]["company_name"], "Acme Inc")
        self.assertIn("0-2 years", jobs[0]["description"])

    def test_oracle_detail(self):
        board = {"provider": "oracle", "slug": "abcd.fa.us2.oraclecloud.com/CX_1"}

        def fetch(url, data=None):
            if "recruitingCEJobRequisitions?" in url:
                return {"items": [{"requisitionList": [
                    {"Id": "42", "Title": "Associate Software Engineer", "PostedDate": TODAY,
                     "PrimaryLocation": "Dallas, TX, United States", "PrimaryLocationCountry": "US",
                     "secondaryLocations": []},
                    {"Id": "41", "Title": "Software Engineer", "PostedDate": "2020-01-01"},
                ]}]}
            self.assertIn("Id=%2242%22", url)
            return {"items": [{"ExternalDescriptionStr": "<p>New grads welcome.</p>"}]}

        with patch("job_watcher.fetch_json", side_effect=fetch):
            jobs = job_watcher.fetch_oracle_board(board, now=int(time.time()))
        self.assertEqual([job["id"] for job in jobs], ["oracle:abcd.fa.us2.oraclecloud.com/CX_1:42"])
        self.assertEqual(
            jobs[0]["url"], "https://abcd.fa.us2.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/42"
        )

    def test_jobvite_html(self):
        board = {"provider": "jobvite", "slug": "acme"}
        listing = ('<tr><td class="jv-job-list-name"> <a href="/acme/job/oAbC123">Junior Application Developer</a> '
                   '</td> <td class="jv-job-list-location"> Washington, District Of Columbia </td></tr>')
        detail = ('<div class="jv-job-detail-description"><p>Entry level role.</p></div>'
                  '<div class="jv-job-detail-bottom-actions"></div>')
        with patch("job_watcher.fetch_text", side_effect=lambda url: listing if url.endswith("/jobs") else detail):
            jobs = job_watcher.fetch_jobvite_board(board, now=int(time.time()))
        self.assertEqual(jobs[0]["url"], "https://jobs.jobvite.com/acme/job/oAbC123")
        self.assertEqual(jobs[0]["description"], "Entry level role.")

    def test_jazzhr_feed_uses_timestamp_in_id(self):
        stamp = time.strftime("%Y%m%d%H%M%S", time.gmtime(time.time() - 3600))
        feed = f"""<jobs><company>Acme</company>
            <job><id>job_{stamp}_ABC</id><status>Open</status><title>Software Engineer - Entry Level</title>
            <url>https://acme.applytojob.com/apply/x/SE</url><city>Boston</city><state>MA</state>
            <country>United States</country><description>&lt;p&gt;Hi&lt;/p&gt;</description></job>
            <job><id>job_20200101000000_OLD</id><status>Open</status><title>Software Engineer</title></job>
        </jobs>"""
        with patch("job_watcher.fetch_text", return_value=feed):
            jobs = job_watcher.fetch_jazzhr_board({"provider": "jazzhr", "slug": "acme"}, now=int(time.time()))
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertTrue(matches(jobs[0]))


class ConditionalFetchTests(unittest.TestCase):
    class Response:
        def __init__(self, body, headers):
            self.body, self.headers = body, headers
        def read(self): return self.body
        def __enter__(self): return self
        def __exit__(self, *a): return False

    def test_fetch_json_gzip_etag_and_304(self):
        import gzip
        board = {"provider": "greenhouse", "slug": "acme"}
        response = self.Response(gzip.compress(b'{"jobs": []}'), {"Content-Encoding": "gzip", "ETag": '"e1"'})
        with patch("job_watcher.urllib.request.urlopen", return_value=response):
            self.assertEqual(job_watcher.fetch_json("https://x/jobs", board=board), {"jobs": []})
        self.assertEqual(board["_new_etag"], '"e1"')
        board["_etag"] = '"e1"'
        sent = []

        def not_modified(request, timeout):
            sent.append(request.get_header("If-none-match"))
            raise urllib.error.HTTPError("https://x/jobs", 304, "Not Modified", None, None)

        with patch("job_watcher.urllib.request.urlopen", side_effect=not_modified):
            board_out, jobs = job_watcher.fetch_direct_board(board)
        self.assertEqual((jobs, board_out["_not_modified"], sent), ([], True, ['"e1"']))

    def test_greenhouse_lists_then_fetches_new_recent_jobs(self):
        now = int(time.time())
        recent = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(now - 1800))
        old = "2020-01-01T00:00:00Z"
        board = {"provider": "greenhouse", "slug": "acme", "_known_ids": frozenset({"3"})}
        listing = {"jobs": [
            {"id": 1, "title": "Software Engineer, New Grad", "first_published": recent,
             "updated_at": recent, "company_name": "Acme Robotics", "location": {"name": "Boston, MA"}},
            {"id": 2, "title": "Software Engineer", "first_published": old, "updated_at": recent},
            {"id": 3, "title": "Data Engineer", "first_published": recent},
        ]}

        def fetch(url, data=None, board=None):
            if url.endswith("/jobs"):
                return listing
            self.assertTrue(url.endswith("/jobs/1"))
            return {"title": "Software Engineer, New Grad", "content": "&lt;p&gt;Build&lt;/p&gt;",
                    "absolute_url": "https://acme.com/careers?gh_jid=1", "location": {"name": "Boston, MA"}}

        with patch("job_watcher.fetch_json", side_effect=fetch):
            jobs = job_watcher.fetch_greenhouse_board(board, now)
        self.assertEqual([job["company_name"] for job in jobs], ["Acme Robotics"])
        self.assertEqual(jobs[0]["description"], "Build")
        self.assertEqual(job_watcher.job_posted_timestamp(jobs[0]), now - 1800)
        self.assertEqual(sorted(board["_listed_ids"]), ["1", "2", "3"])

    def test_etag_is_dropped_while_new_ids_are_unprocessed(self):
        board = {"provider": "bamboohr", "slug": "acme", "_new_etag": '"e2"'}
        plan = job_watcher.DetailPlan(board)
        for index in range(job_watcher.MAX_DETAIL_FETCHES_PER_BOARD + 3):
            plan.consider(index, "Software Engineer")
        plan.details(lambda job_id, payload: None)
        self.assertNotIn("_new_etag", board)
        self.assertEqual(len(board["_listed_ids"]), job_watcher.MAX_DETAIL_FETCHES_PER_BOARD)


class PushTests(unittest.TestCase):
    def test_push_sends_newest_first_and_summarizes_overflow(self):
        now = int(time.time())
        jobs = [{"title": f"Software Engineer {n}", "company_name": "Acme", "locations": ["Austin, TX"],
                 "url": f"https://x/{n}", "posted_at": now - n * 60} for n in range(7)]
        sent = []

        def capture(request, timeout):
            sent.append(json.loads(request.data))
            return ConditionalFetchTests.Response(b"", {})

        with patch.dict("os.environ", {"JOB_WATCHER_NTFY_TOPIC": "secret-topic"}), \
                patch("job_watcher.urllib.request.urlopen", side_effect=capture):
            job_watcher.send_push(jobs)
        self.assertEqual(len(sent), 6)
        self.assertEqual(sent[0]["click"], "https://x/0")
        self.assertEqual(sent[0]["topic"], "secret-topic")
        self.assertEqual(sent[-1]["title"], "+2 more new roles")

    def test_push_is_off_without_a_topic(self):
        with patch.dict("os.environ", {}, clear=True), \
                patch("job_watcher.urllib.request.urlopen") as urlopen:
            job_watcher.send_push([{"title": "x"}])
        urlopen.assert_not_called()


class DiscoveryTests(unittest.TestCase):
    def test_board_from_url(self):
        cases = {
            "https://job-boards.greenhouse.io/Acme/jobs/123": ("greenhouse", "acme"),
            "https://boards.greenhouse.io/embed/job_app?for=acme&token=1": ("greenhouse", "acme"),
            "https://jobs.lever.co/acme/uuid?lever-source=x": ("lever", "acme"),
            "https://jobs.ashbyhq.com/Acme/uuid": ("ashby", "Acme"),
            "https://pipersandler.wd501.myworkdayjobs.com/en-US/Piper_Sandler_Careers/job/X_R-1":
                ("workday", "pipersandler.wd501.myworkdayjobs.com/pipersandler/Piper_Sandler_Careers"),
            "https://wd5.myworkdaysite.com/en-US/recruiting/acme/External/job/X":
                ("workday", "wd5.myworkdaysite.com/acme/External"),
            "https://efds.fa.em5.oraclecloud.com/hcmUI/CandidateExperience/en/sites/CX_1/job/74623":
                ("oracle", "efds.fa.em5.oraclecloud.com/CX_1"),
            "https://amii.bamboohr.com/careers/254/": ("bamboohr", "amii"),
            "https://ataengineering.applytojob.com/apply/ivX/SE": ("jazzhr", "ataengineering"),
            "https://jobs.jobvite.com/MetaPhase/job/oaR7Afws?nl=1": ("jobvite", "metaphase"),
            "https://ats.rippling.com/en-GB/uplinq/jobs/db54": ("rippling", "uplinq"),
            "https://jobs.smartrecruiters.com/AECOM2/123": ("smartrecruiters", "AECOM2"),
            "https://apply.workable.com/accora/j/ABC/": ("workable", "accora"),
        }
        for url, (provider, slug) in cases.items():
            with self.subTest(url=url):
                board = job_watcher.board_from_url(url, "Acme")
                self.assertEqual((board["provider"], board["slug"]), (provider, slug))
        self.assertIsNone(job_watcher.board_from_url("https://careers.example.com/jobs/1"))

    def test_harvest_adds_new_boards_once_and_skips_excluded(self):
        now = int(time.time())
        listings = [
            {"category": "Software", "date_posted": now - 86400, "company_name": "Acme",
             "url": "https://jobs.lever.co/acme/1"},
            {"category": "AI/ML/Data", "date_posted": now - 86400, "company_name": "Acme",
             "url": "https://jobs.lever.co/acme/2"},
            {"category": "Hardware", "date_posted": now, "company_name": "Chips",
             "url": "https://jobs.lever.co/chips/1"},
            {"category": "Software", "date_posted": now - 400 * 86400, "company_name": "Old",
             "url": "https://jobs.lever.co/old/1"},
            {"category": "Software", "date_posted": now, "company_name": "Jane Street",
             "url": "https://jobs.lever.co/janestreet/1"},
            {"category": "Software", "date_posted": now, "company_name": "Known",
             "url": "https://jobs.ashbyhq.com/known/1"},
        ]
        found = job_watcher.harvest_boards(listings, {"ashby:known"}, now)
        self.assertEqual(found, [{"provider": "lever", "slug": "acme", "company": "Acme", "source": "simplify"}])

    def test_harvest_skips_denied_and_already_moved_boards(self):
        now = int(time.time())
        listings = [
            {"category": "Software", "date_posted": now, "company_name": "Alma",
             "url": "https://job-boards.greenhouse.io/alma/jobs/1"},
            {"category": "Software", "date_posted": now, "company_name": "Benchling",
             "url": "https://job-boards.greenhouse.io/benchling/jobs/2"},
        ]
        boards = [{"provider": "ashby", "slug": "benchling", "migrated_from": "greenhouse:benchling"}]
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE source_checks (source TEXT PRIMARY KEY, last_checked INTEGER)")
        connection.execute("CREATE TABLE http_cache (url TEXT PRIMARY KEY, etag TEXT)")
        registry = Path(tempfile.mkdtemp()) / "ats_boards.json"
        registry.write_text(json.dumps(boards))
        with patch("job_watcher.fetch_conditional", return_value=json.dumps(listings).encode()):
            job_watcher.fetch_simplify(connection, boards, now, registry)
        self.assertEqual(json.loads(registry.read_text()), boards)
        self.assertIsNone(job_watcher.find_migrated_board(
            {"provider": "greenhouse", "slug": "alma"}, fetch=lambda url: {"jobs": [{"id": 1}]}
        ))

    def test_simplify_listing_citizenship_is_enforced(self):
        now = int(time.time())
        jobs = job_watcher.simplify_jobs([
            {"id": "a", "category": "Software", "date_posted": now - 600, "title": "Software Engineer, New Grad",
             "company_name": "Acme", "locations": ["Austin, TX"], "url": "https://x/1", "active": True,
             "sponsorship": "U.S. Citizenship is Required"},
            {"id": "b", "category": "Software", "date_posted": now - 600, "title": "Software Engineer, New Grad",
             "company_name": "Beta", "locations": ["Austin, TX"], "url": "https://x/2", "active": True,
             "sponsorship": "Offers Sponsorship"},
        ], now)
        self.assertEqual([rejection_reason(job) for job in jobs], ["citizenship_requirement", "matched"])

    def test_fetch_conditional_uses_etag_and_gzip(self):
        import gzip, io
        connection = sqlite3.connect(":memory:")
        self.addCleanup(connection.close)
        connection.execute("CREATE TABLE http_cache (url TEXT PRIMARY KEY, etag TEXT)")

        class Response(io.BytesIO):
            headers = {"Content-Encoding": "gzip", "ETag": '"v1"'}
            def __enter__(self): return self
            def __exit__(self, *a): return False

        with patch("job_watcher.urllib.request.urlopen", return_value=Response(gzip.compress(b"[1]"))):
            self.assertEqual(job_watcher.fetch_conditional(connection, "https://x/l.json"), b"[1]")
        seen = []

        def not_modified(request, timeout):
            seen.append(request.get_header("If-none-match"))
            raise urllib.error.HTTPError("https://x/l.json", 304, "Not Modified", None, None)

        with patch("job_watcher.urllib.request.urlopen", side_effect=not_modified):
            self.assertIsNone(job_watcher.fetch_conditional(connection, "https://x/l.json"))
        self.assertEqual(seen, ['"v1"'])


if __name__ == "__main__":
    unittest.main()
