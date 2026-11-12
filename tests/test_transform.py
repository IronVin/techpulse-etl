import pandera.errors
import pytest

from techpulse.transform import clean_html_text, extract_skills, transform_jobs


def test_clean_html_text_removes_tags_and_normalizes_spaces():
    raw = "<p>Cerchiamo   uno <strong>sviluppatore Python</strong> &amp; SQL.</p>"
    assert clean_html_text(raw) == "Cerchiamo uno sviluppatore Python & SQL."
    assert clean_html_text(None) == ""


def test_extract_skills_detects_technologies_case_insensitive():
    text = "Experience with python, FastAPI, PostgreSQL and docker is required."
    skills = extract_skills(text)
    assert "Python" in skills
    assert "FastAPI" in skills
    assert "SQL" in skills
    assert "Docker" in skills
    assert "AWS" not in skills


def test_transform_jobs_valid_and_deduplicates():
    raw_jobs = [
        {
            "slug": "python-dev-123",
            "title": "Senior Python Backend Engineer",
            "company_name": "TechCorp",
            "location": "Remote",
            "remote": True,
            "description": "<p>Stack: Python, Docker, Git</p>",
            "url": "https://example.com/jobs/123",
            "created_at": 1700000000,
        },
        # Duplicato intenzionale con lo stesso slug
        {
            "slug": "python-dev-123",
            "title": "Senior Python Backend Engineer",
            "company_name": "TechCorp",
            "location": "Remote",
            "remote": True,
            "description": "<p>Stack: Python, Docker, Git</p>",
            "url": "https://example.com/jobs/123",
            "created_at": 1700000000,
        },
    ]

    df = transform_jobs(raw_jobs)

    assert len(df) == 1
    assert df.loc[0, "job_id"] == "python-dev-123"
    assert bool(df.loc[0, "is_python_role"]) is True
    assert "Python" in df.loc[0, "matched_skills"]
    assert df.loc[0, "published_date"] == "2023-11-14"


def test_transform_jobs_fails_pandera_on_invalid_url():
    invalid_jobs = [
        {
            "slug": "bad-job-999",
            "title": "Data Analyst",
            "company_name": "DataInc",
            "location": "Milan",
            "remote": False,
            "description": "SQL and Pandas",
            "url": "not-a-valid-url",  # Viola la regola str_startswith="http"
            "created_at": 1700000000,
        }
    ]

    with pytest.raises(pandera.errors.SchemaError):
        transform_jobs(invalid_jobs)
