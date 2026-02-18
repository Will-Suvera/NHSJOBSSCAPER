"""Google Sheets integration — auth, dedup, batch write."""

import json
import logging
import os
import gspread
from google.oauth2.service_account import Credentials

logger = logging.getLogger(__name__)

COLUMNS = [
    "job_id",
    "title",
    "employer",
    "location",
    "salary",
    "date_posted",
    "closing_date",
    "contract_type",
    "working_pattern",
    "hours_per_week",
    "pay_band",
    "contact_name",
    "contact_email",
    "contact_phone",
    "description_summary",
    "job_url",
    "first_seen",
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]


def _authenticate(credentials_json):
    """Authenticate with Google Sheets API. Returns gspread client.

    credentials_json can be:
      - a file path to a JSON credentials file
      - a JSON string (for GitHub Actions secrets)
    """
    if os.path.isfile(credentials_json):
        creds = Credentials.from_service_account_file(credentials_json, scopes=SCOPES)
    else:
        info = json.loads(credentials_json)
        creds = Credentials.from_service_account_info(info, scopes=SCOPES)
    return gspread.authorize(creds)


def _get_worksheet(client, spreadsheet_id):
    """Open spreadsheet by ID and return the first worksheet, creating headers if needed."""
    spreadsheet = client.open_by_key(spreadsheet_id)
    worksheet = spreadsheet.sheet1

    # If the sheet is empty, write headers
    existing = worksheet.row_values(1)
    if not existing:
        worksheet.append_row(COLUMNS)
        logger.info("Wrote header row")

    return worksheet, spreadsheet.url


def get_existing_job_ids(credentials_json, spreadsheet_id):
    """Return set of job_ids already in the sheet."""
    client = _authenticate(credentials_json)
    worksheet, url = _get_worksheet(client, spreadsheet_id)
    ids = worksheet.col_values(1)[1:]  # skip header
    logger.info(f"Found {len(ids)} existing job IDs in sheet")
    return set(ids), url


def append_jobs(credentials_json, spreadsheet_id, jobs):
    """Append jobs to the sheet in a single batch call. Returns sheet URL."""
    client = _authenticate(credentials_json)
    worksheet, url = _get_worksheet(client, spreadsheet_id)

    rows = []
    for job in jobs:
        rows.append([job.get(col, "") for col in COLUMNS])

    worksheet.append_rows(rows)
    logger.info(f"Appended {len(rows)} new jobs to sheet")
    return url
