import unittest
from unittest.mock import patch

from job_watcher import (
    canonical_url,
    description_offers_sponsorship,
    fetch_direct_board,
    matches,
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
        self.assertFalse(matches(self.job("Senior Machine Learning Engineer", "AI/ML/Data")))
        self.assertFalse(matches(self.job("Software Engineer Intern")))
        self.assertFalse(matches(self.job("Software Engineer", active=False)))

    def test_requires_us_location(self):
        self.assertTrue(matches(self.job("Software Engineer - New Grad", locations=["Remote in USA"])))
        self.assertFalse(matches(self.job("Software Engineer - New Grad", locations=["Toronto, ON, Canada"])))

    def test_requires_explicit_sponsorship(self):
        self.assertFalse(matches(self.job(
            "Software Engineer - New Grad", description="No sponsorship information provided."
        )))
        self.assertFalse(matches(self.job(
            "Software Engineer - New Grad", description="We do not offer visa sponsorship."
        )))

    def test_direct_ats_role(self):
        job = self.job("Machine Learning Engineer - New Grad")
        job.update({
            "source": "direct-ats",
            "description": "We offer visa sponsorship and immigration support for this role.",
        })
        self.assertTrue(matches(job))

    def test_direct_ats_requires_entry_level_evidence(self):
        job = self.job("Machine Learning Engineer")
        job.update({
            "source": "direct-ats",
            "description": "Visa sponsorship is available. Five years of experience required.",
        })
        self.assertFalse(matches(job))

    def test_negative_sponsorship_language_wins(self):
        description = (
            "We support employees with immigration questions, but visa sponsorship "
            "is not available for this position."
        )
        self.assertFalse(description_offers_sponsorship(description))

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
                    "text": "Visa sponsorship is available for this role."
                }}},
            },
        ]
        _, jobs = fetch_direct_board({"provider": "smartrecruiters", "slug": "Acme"})
        self.assertEqual(len(jobs), 1)
        self.assertEqual(jobs[0]["company_name"], "Acme")
        self.assertTrue(matches(jobs[0]))


if __name__ == "__main__":
    unittest.main()
