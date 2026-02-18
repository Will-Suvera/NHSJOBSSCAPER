"""Slack webhook notifications."""

import logging
import requests
from datetime import datetime

logger = logging.getLogger(__name__)


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


def send_update(webhook_url, new_jobs, total_active, sheet_url):
    """Send daily update — new jobs list or 'no new jobs' message."""
    today = datetime.now().strftime("%Y-%m-%d")

    if new_jobs:
        lines = [f"\U0001f3e5 NHS ARRS Jobs Update \u2014 {today}\n"]
        lines.append(f"{len(new_jobs)} new role{'s' if len(new_jobs) != 1 else ''} found:\n")

        for i, job in enumerate(new_jobs[:15], 1):
            title = job.get("title", "Unknown")
            employer = job.get("employer", "Unknown")
            location = job.get("location", "Unknown")
            salary = job.get("salary", "Not specified")
            closing = job.get("closing_date", "Not specified")
            url = job.get("job_url", "")
            lines.append(
                f"{i}. *{title}* \u2014 {employer}\n"
                f"   \U0001f4cd {location} | \U0001f4b0 {salary} | \u23f0 Closes {closing}\n"
                f"   {url}\n"
            )

        if len(new_jobs) > 15:
            lines.append(f"\n_...and {len(new_jobs) - 15} more_\n")

        lines.append(f"\nTotal active listings: {total_active}")
        lines.append(f"<{sheet_url}|View full tracker>")

        text = "\n".join(lines)
    else:
        text = (
            f"\U0001f3e5 NHS ARRS Jobs Update \u2014 {today}\n"
            f"No new listings today. {total_active} active roles tracked."
        )

    return _post(webhook_url, {"text": text})


def send_error(webhook_url, error_message):
    """Send error alert."""
    today = datetime.now().strftime("%Y-%m-%d")
    text = f"\U0001f6a8 NHS ARRS Scraper Failed \u2014 {today}\n{error_message}"
    return _post(webhook_url, {"text": text})
