#!/usr/bin/env python3
"""NHS ARRS Jobs Scraper — daily pipeline."""

import os
import sys
import logging

from scraper import scrape_all_jobs
from sheets import get_existing_job_ids, append_jobs
from notifier import send_update, send_error

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger(__name__)


def main():
    # --- Load config from env ---
    credentials_json = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    spreadsheet_id = os.getenv("SPREADSHEET_ID", "")
    slack_url = os.getenv("SLACK_WEBHOOK_URL", "")

    if not credentials_json:
        logger.error("GOOGLE_CREDENTIALS_JSON not set")
        sys.exit(1)
    if not spreadsheet_id:
        logger.error("SPREADSHEET_ID not set")
        sys.exit(1)

    try:
        # 1. Get existing job IDs from Google Sheet (do this first to skip known detail pages)
        logger.info("Step 1: Checking sheet for existing jobs...")
        existing_ids, sheet_url = get_existing_job_ids(credentials_json, spreadsheet_id)

        # 2. Scrape all listings, skipping detail pages for known jobs
        logger.info("Step 2: Scraping NHS Jobs...")
        jobs = scrape_all_jobs(known_ids=existing_ids)
        logger.info(f"Scraped {len(jobs)} unique jobs")

        if not jobs:
            logger.warning("No jobs found — possible site issue")
            if slack_url:
                send_error(slack_url, "Scraper returned 0 jobs — this may indicate a problem")
            return

        # 3. Filter to only new jobs
        new_jobs = [j for j in jobs if j.get("job_id") and j["job_id"] not in existing_ids]
        logger.info(f"Step 3: {len(new_jobs)} new jobs, {len(jobs) - len(new_jobs)} already in sheet")

        # 4. Append new jobs to Google Sheet
        if new_jobs:
            logger.info("Step 4: Appending new jobs to sheet...")
            sheet_url = append_jobs(credentials_json, spreadsheet_id, new_jobs)
        else:
            logger.info("Step 4: Nothing to append")

        # 5. Send Slack notification
        if slack_url:
            logger.info("Step 5: Sending Slack notification...")
            send_update(slack_url, new_jobs, len(jobs), sheet_url)
        else:
            logger.info("Step 5: Slack not configured, skipping")
            logger.info("Set SLACK_WEBHOOK_URL secret to enable notifications")

        logger.info("Done.")

    except Exception as e:
        logger.exception("Pipeline failed")
        if slack_url:
            send_error(slack_url, str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
