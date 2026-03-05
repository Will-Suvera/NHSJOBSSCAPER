"""NHS Jobs scraper — search pages + detail pages."""

import re
import time
import logging
import urllib.parse
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

BASE_URL = "https://www.jobs.nhs.uk"
SEARCH_URL = f"{BASE_URL}/candidate/search/results"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-GB,en;q=0.9",
}

REQUEST_DELAY = 1.5  # seconds between requests
MAX_RETRIES = 3
BACKOFF_BASE = 2  # exponential backoff: 2s, 4s, 8s


def _clean(text):
    """Collapse whitespace and strip."""
    return re.sub(r"\s+", " ", text).strip()


def _fetch(url, session):
    """GET with retry + exponential backoff. Returns Response or None."""
    for attempt in range(MAX_RETRIES):
        try:
            resp = session.get(url, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.RequestException as e:
            wait = BACKOFF_BASE ** (attempt + 1)
            logger.warning(f"Request failed (attempt {attempt + 1}/{MAX_RETRIES}): {e}")
            if attempt < MAX_RETRIES - 1:
                logger.info(f"Retrying in {wait}s...")
                time.sleep(wait)
            else:
                logger.error(f"Failed to fetch {url} after {MAX_RETRIES} attempts")
    return None


def _has_next_page(soup):
    """Check if there is a next page link in pagination."""
    # Real selector: nav.nhsuk-pagination > ul > li > a.nhsuk-pagination__link--next
    link = soup.find("a", class_="nhsuk-pagination__link--next")
    return link is not None


def _extract_job_from_result(element):
    """Extract job data from a single search result <li>. Returns dict or None."""
    try:
        job = {}

        # Title + URL — a[data-test="search-result-job-title"]
        link = element.find("a", {"data-test": "search-result-job-title"})
        if not link:
            return None

        job["title"] = link.get_text(strip=True)
        href = link.get("href", "")
        if href:
            url = href if href.startswith("http") else BASE_URL + href
            # Strip query params for a clean job URL
            job["job_url"] = re.sub(r"\?.*", "", url)
            m = re.search(r"/jobadvert/([A-Za-z0-9\-]+)", url)
            if m:
                job["job_id"] = m.group(1)

        if not job.get("job_id"):
            return None

        # Employer + Location — inside div[data-test="search-result-location"]
        loc_div = element.find(attrs={"data-test": "search-result-location"})
        if loc_div:
            h3 = loc_div.find("h3")
            if h3:
                # Employer is the direct text of h3 (before the nested div)
                loc_sub = h3.find("div", class_="location-font-size")
                if loc_sub:
                    job["location"] = _clean(loc_sub.get_text())
                    # Employer = h3 text minus the location div text
                    job["employer"] = _clean(h3.get_text().replace(loc_sub.get_text(), ""))
                else:
                    job["employer"] = h3.get_text(strip=True)

        # Salary — [data-test="search-result-salary"] strong
        sal = element.find(attrs={"data-test": "search-result-salary"})
        if sal:
            strong = sal.find("strong")
            job["salary"] = _clean(strong.get_text()) if strong else _clean(sal.get_text())

        # Closing date — [data-test="search-result-closingDate"] strong
        closing = element.find(attrs={"data-test": "search-result-closingDate"})
        if closing:
            strong = closing.find("strong")
            job["closing_date"] = strong.get_text(strip=True) if strong else closing.get_text(strip=True)

        return job

    except Exception as e:
        logger.error(f"Error extracting search result: {e}")
        return None


def _extract_detail(soup):
    """Extract detail-page fields from parsed HTML. Returns dict."""
    details = {}

    def _text(element_id):
        el = soup.find(id=element_id)
        return el.get_text(strip=True) if el else ""

    # Description — combine job_overview + job_description_large, take first 1000 chars
    overview = _text("job_overview")
    description = _text("job_description_large")
    full_text = _clean(overview + " " + description)
    if full_text:
        details["description_summary"] = full_text[:1000]

    # Date posted
    val = _text("date_posted")
    if val:
        details["date_posted"] = val

    # Contract type
    val = _text("contract_type")
    if val:
        details["contract_type"] = val

    # Working pattern — the <p> after the working_pattern_heading
    wp_heading = soup.find(id="working_pattern_heading")
    if wp_heading:
        p = wp_heading.find_next_sibling("p")
        if p:
            # Clean up whitespace from nested spans/divs
            details["working_pattern"] = re.sub(r"\s*,\s*", ", ", p.get_text(" ", strip=True))

    # Pay band — #payscheme-type (e.g. "Band 7", "Other")
    val = _text("payscheme-type")
    if val and val.lower() != "other":
        details["pay_band"] = val

    # Hours per week — not a standard ID; look for it in the sidebar text
    # Some listings have it, many don't. Skip if not present.

    # Contact details — all have stable IDs
    val = _text("contact_details_name")
    if val:
        details["contact_name"] = val

    email_el = soup.find(id="contact_details_email")
    if email_el:
        a = email_el.find("a")
        details["contact_email"] = a.get_text(strip=True) if a else email_el.get_text(strip=True)

    val = _text("contact_details_number")
    if val:
        details["contact_phone"] = val

    return details


MAX_PAGES = 50  # cap per keyword to keep run times sane

# Keywords and optional title filters. If title_filter is set, only keep jobs
# where the filter string appears in the title (case-insensitive). This handles
# OR-logic keywords like "Practice Nurse" that return thousands of unrelated results.
SEARCH_KEYWORDS = [
    {"keyword": "ARRS", "title_filter": None},
    {"keyword": "PCN", "title_filter": None},
    {"keyword": "Practice Nurse", "title_filter": "practice nurse"},
]

# Only keep jobs matching these roles (checked against title, case-insensitive)
ALLOWED_ROLES = [
    "pharmacist", "pharmacy technician", "gp", "general practitioner",
    "practice nurse", "practice manager", "business manager",
]

# Exclude hospital/trust employers
EXCLUDE_EMPLOYER = ["trust", "hospital"]

# Exclude agency/recruiter contact emails (filtered after detail page fetch)
EXCLUDE_EMAILS = [
    "recruitment@thepharmacistnetwork.co.uk",
    "primarycarefcp.talent@nhs.net",
    "enquiries@eoeprimarycarecareers.nhs.uk",
    "hr@pcmsolutions.co.uk",
    "recruitment@practiceindex.co.uk",
    "apply@virtualpharmacist.co.uk",
]

# Exclude emails containing these keywords (catches e.g. wales.nhs.uk)
EXCLUDE_EMAIL_KEYWORDS = ["wales"]


def _is_excluded(job):
    """Return True if job should be filtered out."""
    title = job.get("title", "").lower()
    employer = job.get("employer", "").lower()

    # Must match at least one allowed role
    if not any(role in title for role in ALLOWED_ROLES):
        return True

    # No hospital/trust employers
    for term in EXCLUDE_EMPLOYER:
        if term in employer:
            return True

    return False


def _scrape_keyword(keyword, session, title_filter=None):
    """Scrape up to MAX_PAGES for a single keyword. Returns list of job dicts."""
    jobs = []
    page = 1

    while page <= MAX_PAGES:
        params = {"keyword": keyword, "language": "en"}
        if page > 1:
            params["page"] = page

        url = SEARCH_URL + "?" + urllib.parse.urlencode(params)
        logger.info(f"[{keyword}] Scraping page {page}")

        resp = _fetch(url, session)
        if not resp:
            logger.error(f"[{keyword}] Failed to fetch page {page}, stopping")
            break

        soup = BeautifulSoup(resp.content, "html.parser")
        elements = soup.find_all("li", {"data-test": "search-result"})

        for el in elements:
            job = _extract_job_from_result(el)
            if job:
                if title_filter and title_filter not in job.get("title", "").lower():
                    continue
                if _is_excluded(job):
                    continue
                jobs.append(job)

        logger.info(f"[{keyword}] Page {page}: {len(elements)} results, {len(jobs)} kept")

        if not _has_next_page(soup):
            break

        page += 1
        time.sleep(REQUEST_DELAY)

    if page > MAX_PAGES:
        logger.info(f"[{keyword}] Hit {MAX_PAGES}-page cap")

    return jobs


def scrape_all_jobs(known_ids=None):
    """Scrape all keywords, deduplicate by job_id, fetch detail pages.

    known_ids: set of job IDs already in the sheet. Detail pages are only
    fetched for NEW jobs (not in known_ids), which cuts runtime dramatically
    on repeat runs.
    """
    if known_ids is None:
        known_ids = set()

    session = requests.Session()
    session.headers.update(HEADERS)

    # --- Search results across all keywords ---
    seen_ids = set()
    all_jobs = []

    for entry in SEARCH_KEYWORDS:
        kw = entry["keyword"]
        tf = entry.get("title_filter")
        keyword_jobs = _scrape_keyword(kw, session, title_filter=tf)
        new = 0
        for job in keyword_jobs:
            jid = job.get("job_id")
            if jid and jid not in seen_ids:
                seen_ids.add(jid)
                all_jobs.append(job)
                new += 1
        logger.info(f"[{kw}] {len(keyword_jobs)} results, {new} new unique jobs")
        time.sleep(REQUEST_DELAY)

    logger.info(f"Total unique jobs from search results: {len(all_jobs)}")

    # --- Detail pages (only for new jobs) ---
    new_jobs = [j for j in all_jobs if j.get("job_id") not in known_ids]
    skipped = len(all_jobs) - len(new_jobs)
    if skipped:
        logger.info(f"Skipping detail pages for {skipped} jobs already in sheet")

    for idx, job in enumerate(new_jobs, 1):
        job_url = job.get("job_url")
        if not job_url:
            continue

        logger.info(f"Detail page {idx}/{len(new_jobs)}: {job.get('title', '?')}")

        if idx > 1:
            time.sleep(REQUEST_DELAY)

        resp = _fetch(job_url, session)
        if not resp:
            logger.warning(f"Skipping detail page for {job.get('job_id')}")
            continue

        detail_soup = BeautifulSoup(resp.content, "html.parser")
        details = _extract_detail(detail_soup)
        job.update(details)

    # Remove agency listings (contact email only available after detail fetch)
    def _email_excluded(email):
        email = email.lower()
        if email in EXCLUDE_EMAILS:
            return True
        return any(kw in email for kw in EXCLUDE_EMAIL_KEYWORDS)

    before = len(new_jobs)
    new_jobs = [j for j in new_jobs if not _email_excluded(j.get("contact_email", ""))]
    if before - len(new_jobs):
        logger.info(f"Filtered out {before - len(new_jobs)} agency/excluded listings by email")

    # Add first_seen timestamp
    now = datetime.now(timezone.utc).isoformat()
    for job in new_jobs:
        job["first_seen"] = now

    # Rebuild all_jobs: known jobs + filtered new jobs
    known_jobs = [j for j in all_jobs if j.get("job_id") in known_ids]
    all_jobs = known_jobs + new_jobs

    logger.info(f"Scraping complete: {len(all_jobs)} total, {len(new_jobs)} new")
    return all_jobs
