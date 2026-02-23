"""Slack webhook notifications."""

import logging
import time
import requests
from collections import Counter
from datetime import datetime

logger = logging.getLogger(__name__)

ROLE_CATEGORIES = [
    ("gp", "GP"),
    ("general practitioner", "GP"),
    ("practice nurse", "Practice Nurse"),
    ("nurse", "Practice Nurse"),
    ("clinical pharmacist", "Clinical Pharmacist"),
    ("pharmacist", "Clinical Pharmacist"),
    ("pharmacy technician", "Pharmacy Technician"),
    ("physiotherapist", "Physiotherapist"),
    ("physio", "Physiotherapist"),
    ("social prescri", "Social Prescriber"),
    ("practice manager", "Practice Manager"),
    ("business manager", "Business Manager"),
]


def _categorise(title):
    t = title.lower()
    for keyword, category in ROLE_CATEGORIES:
        if keyword in t:
            return category
    return "Other"


def _post(webhook_url, payload):
    """POST JSON to Slack webhook. Returns True on success."""
    try:
        resp = requests.post(
            webhook_url,
            json=payload,
            headers={"Content-Type": "application/json"},
            timeout=10,
        )
        resp.raise_for_status()
        return True
    except requests.RequestException as e:
        logger.error(f"Slack send failed: {e}")
        return False


def _format_job(job):
    """Format a single job as a compact Slack message."""
    title = job.get("title", "Unknown")
    employer = job.get("employer", "Unknown")
    location = job.get("location", "")
    contract = job.get("contract_type", "")
    date_posted = job.get("date_posted", "")
    closing = job.get("closing_date", "")
    contact_name = job.get("contact_name", "")
    contact_email = job.get("contact_email", "")
    contact_phone = str(job.get("contact_phone", ""))
    if contact_phone and contact_phone.isdigit() and not contact_phone.startswith("0"):
        contact_phone = "0" + contact_phone
    url = job.get("job_url", "")

    working = job.get("working_pattern", "")
    header = f"*{title}* — {employer}"
    bracket_parts = [p for p in [contract, working] if p]
    if bracket_parts:
        header += f" ({', '.join(bracket_parts)})"
    lines = [header]

    if location:
        lines.append(location)

    date_parts = []
    if date_posted:
        date_parts.append(f"Posted {date_posted}")
    if closing:
        date_parts.append(f"Closes {closing}")
    if date_parts:
        lines.append(" | ".join(date_parts))

    contact_parts = []
    if contact_name:
        contact_parts.append(contact_name)
    if contact_email:
        contact_parts.append(contact_email)
    contact_parts.append(contact_phone if contact_phone else "No number")
    if contact_name or contact_email:
        lines.append("Contact: " + " | ".join(contact_parts))

    if url:
        lines.append(f"<{url}|View listing>")

    return "\n".join(lines)


def send_update(webhook_url, new_jobs, total_active, sheet_url):
    """Send summary message, then one message per new job."""
    today = datetime.now().strftime("%-d %b %Y")
    count = len(new_jobs)

    if not new_jobs:
        text = (
            f":hospital: *NHS Jobs Daily Update — {today}*\n\n"
            f"No new listings today. {total_active} active roles tracked.\n"
            f"<{sheet_url}|View full tracker>"
        )
        return _post(webhook_url, {"text": text})

    # --- Summary message ---
    cats = Counter(_categorise(j.get("title", "")) for j in new_jobs)
    breakdown_lines = []
    for cat, n in cats.most_common():
        dots = "." * max(1, 24 - len(cat) - len(str(n)))
        breakdown_lines.append(f"  {cat} {dots} {n}")
    breakdown = "\n".join(breakdown_lines)

    summary = (
        f":hospital: *NHS Jobs Daily Update — {today}*\n\n"
        f"*{count} new role{'s' if count != 1 else ''}* found today:\n"
        f"```{breakdown}```\n"
        f"_{total_active} active listings tracked_\n"
        f"<{sheet_url}|:bar_chart: View full tracker>"
    )
    _post(webhook_url, {"text": summary})

    # --- Individual job messages ---
    for i, job in enumerate(new_jobs):
        if i > 0:
            time.sleep(1)  # respect Slack rate limits
        text = _format_job(job)
        _post(webhook_url, {"text": text})
        logger.info(f"Sent Slack message {i + 1}/{count}")

    return True


def send_error(webhook_url, error_message):
    """Send error alert."""
    today = datetime.now().strftime("%-d %b %Y")
    text = (
        f":rotating_light: *NHS Jobs Scraper Failed — {today}*\n"
        f"```{error_message}```"
    )
    return _post(webhook_url, {"text": text})
