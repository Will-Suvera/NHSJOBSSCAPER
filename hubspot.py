"""HubSpot integration — upsert contacts and add to outreach list."""

import logging
import re
import time
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.hubapi.com"
LIST_ID = "2158"  # "NHS Jobs Scraper | Auto Outreach"


def _headers(api_key):
    return {
        "Authorization": f"Bearer {api_key}",
        "Content-Type": "application/json",
    }


def _split_name(full_name):
    """Split 'Dr Sarah Williams' into (first, last). Drops titles."""
    if not full_name:
        return "", ""
    parts = full_name.strip().split()
    # Drop common titles
    titles = {"dr", "mr", "mrs", "ms", "miss", "prof", "professor"}
    while parts and parts[0].lower().rstrip(".") in titles:
        parts.pop(0)
    if not parts:
        return full_name.strip(), ""
    if len(parts) == 1:
        return parts[0], ""
    return parts[0], " ".join(parts[1:])


def _upsert_contact(api_key, email, firstname, lastname, jobtitle):
    """Create or update a contact by email. Returns contact ID or None."""
    props = {
        "email": email,
        "firstname": firstname,
        "lastname": lastname,
    }
    if jobtitle:
        props["jobtitle"] = jobtitle

    # Try create first
    resp = requests.post(
        f"{BASE_URL}/crm/v3/objects/contacts",
        headers=_headers(api_key),
        json={"properties": props},
        timeout=15,
    )

    if resp.status_code == 201:
        contact_id = resp.json().get("id")
        logger.info(f"Created contact: {email} (ID: {contact_id})")
        return contact_id

    # 409 = contact already exists — update instead
    if resp.status_code == 409:
        existing_id = resp.json().get("message", "")
        # Extract ID from error message: "Contact already exists. Existing ID: 123"
        m = re.search(r"Existing ID:\s*(\d+)", existing_id)
        if m:
            contact_id = m.group(1)
            # Update the existing contact
            update_resp = requests.patch(
                f"{BASE_URL}/crm/v3/objects/contacts/{contact_id}",
                headers=_headers(api_key),
                json={"properties": props},
                timeout=15,
            )
            if update_resp.ok:
                logger.info(f"Updated contact: {email} (ID: {contact_id})")
                return contact_id
            else:
                logger.warning(f"Failed to update {email}: {update_resp.status_code}")
                return contact_id  # still return ID for list add
        else:
            logger.warning(f"Contact exists but couldn't parse ID for {email}")
            return None

    logger.warning(f"Failed to upsert {email}: {resp.status_code} {resp.text[:200]}")
    return None


def _add_to_list(api_key, contact_ids):
    """Add contacts to the outreach static list. Returns count added."""
    if not contact_ids:
        return 0

    # API accepts max 250 per call
    added = 0
    for i in range(0, len(contact_ids), 250):
        batch = [str(cid) for cid in contact_ids[i:i + 250]]
        resp = requests.put(
            f"{BASE_URL}/crm/v3/lists/{LIST_ID}/memberships/add",
            headers=_headers(api_key),
            json=batch,
            timeout=15,
        )
        if resp.ok:
            added += len(resp.json().get("recordsIdsAdded", []))
        else:
            logger.warning(f"List add failed: {resp.status_code} {resp.text[:200]}")

    return added


def push_contacts(api_key, jobs):
    """Upsert contacts from job listings and add to outreach list.

    Returns set of job_ids that were successfully pushed to HubSpot.
    """
    pushed_ids = set()
    contact_ids = []

    for job in jobs:
        email = job.get("contact_email", "").strip()
        if not email:
            continue

        firstname, lastname = _split_name(job.get("contact_name", ""))
        jobtitle = job.get("title", "")

        contact_id = _upsert_contact(api_key, email, firstname, lastname, jobtitle)
        if contact_id:
            contact_ids.append(contact_id)
            pushed_ids.add(job.get("job_id"))

        time.sleep(0.2)  # respect HubSpot rate limits

    if contact_ids:
        added = _add_to_list(api_key, contact_ids)
        logger.info(f"HubSpot: {len(contact_ids)} contacts upserted, {added} added to list")

    return pushed_ids
