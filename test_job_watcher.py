import sqlite3
import unittest
from unittest.mock import patch

from job_watcher import (
    canonical_url,
    heartbeat_message,
    fetch_adzuna_jobs,
    description_mentions_citizenship_requirement,
    fetch_direct_board,
    fetch_jooble_jobs,
    experience_summary,
    matches,
    role_category,
    select_due_boards,
)


class MatchTests(unittest.TestCase):
    def job(self, title, active=True, locations=None, description=None):
        return {"title": title, "active": active, "is_visible": True,
                "locations": locations or ["New York, NY"],
                "description": description or
                "This entry-level role offers visa sponsorship and immigration support."}

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
                }],
            },
            {
                "id": "42",
                "name": "Software Engineer - New Grad",
                "location": {"fullLocation": "Austin, TX", "country": "us"},
                "company": {"name": "Acme"},
                "applyUrl": "https://jobs.smartrecruiters.com/Acme/42",
                "jobAd": {"sections": {"qualifications": {
                    "text": "Applicants must be authorized to work in the U.S."
                }}},
            },
        ]
        _, jobs = fetch_direct_board({"provider": "smartrecruiters", "slug": "Acme"})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertTrue(matches(jobs[0]))

    def test_adaptive_board_schedule(self):
        connection = sqlite3.connect(":memory:")
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
            {board["slug"] for board in selected}, {"new", "high"}
        )

    def test_board_batch_is_capped_at_200(self):
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE board_stats (board_key TEXT PRIMARY KEY, last_checked INTEGER, "
            "relevant_count INTEGER, match_count INTEGER, failure_count INTEGER)"
        )
        boards = [
            {"provider": "greenhouse", "slug": f"company-{index}"}
            for index in range(250)
        ]
        self.assertEqual(len(select_due_boards(connection, boards, now=1000)), 200)

    def test_heartbeat_initializes_on_first_run(self):
        connection = sqlite3.connect(":memory:")
        connection.execute(
            "CREATE TABLE heartbeat (id INTEGER PRIMARY KEY CHECK (id = 1), last_run INTEGER NOT NULL)"
        )
        message, last_run = heartbeat_message(connection, now=1000)
        self.assertEqual(message, "Heartbeat initialized")
        self.assertIsNone(last_run)

    def test_heartbeat_reports_missed_runs(self):
        connection = sqlite3.connect(":memory:")
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
        }]}
        jobs = fetch_jooble_jobs("key")
        self.assertEqual(jobs[0]["source"], "jooble")
        self.assertTrue(matches(jobs[0]))


if __name__ == "__main__":
    unittest.main()
