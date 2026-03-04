"""
Clay Contact Refresh Service

FastAPI service that Clay calls via HTTP API enrichment when a returning
visitor is detected on Normalization. Looks up the matching Contact Profiles
record(s) by domain and triggers Intent Lookup re-enrichment, which cascades
through scores → Lead Summary → HubSpot push.

Deployed on Railway. Clay HTTP API enrichment POSTs here with the domain.
"""

import json
import os
import time
import urllib.request
import urllib.parse
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="Clay Contact Refresh", version="1.0.5")

# Clay config
CLAY_BASE = "https://api.clay.com/v3"
CONTACT_PROFILES_TABLE = "t_0taqh4mZJ8nEsoCZMpJ"
INTENT_LOOKUP_FIELD = "f_0taztueiZDv5zyyKZxK"
DOMAIN_FIELD = "f_0taqh54EVWvpb65SiJq"
NEEDS_REFRESH_FIELD = "f_0tbbpnjz4Mv4KyyXCB9"

# Default view for Contact Profiles (All rows)
CONTACT_PROFILES_VIEW = "gv_0taqh4mJYsojqrp34TE"


def get_clay_cookie():
    cookie = os.environ.get("CLAY_SESSION_COOKIE", "")
    if not cookie:
        raise HTTPException(status_code=500, detail="CLAY_SESSION_COOKIE not configured")
    return cookie


def clay_request(method: str, path: str, body=None):
    """Make a request to the Clay v3 API."""
    url = f"{CLAY_BASE}{path}"
    data = json.dumps(body).encode() if body else None
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Cookie", f"claysession={get_clay_cookie()}")
    req.add_header("Content-Type", "application/json")
    req.add_header("Accept", "application/json")
    req.add_header("Origin", "https://app.clay.com")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            text = resp.read().decode()
            return json.loads(text) if text else {}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode() if hasattr(e, "read") else ""
        raise HTTPException(
            status_code=502,
            detail=f"Clay API error {e.code}: {error_body[:300]}"
        )


def find_contact_records_by_domain(domain: str) -> list[dict]:
    """Search Contact Profiles for records matching a domain.

    Uses Clay search API which returns {fieldId, recordId} pairs.
    We collect unique recordIds where the matching field is the Domain field.
    If no domain-specific matches, we use all returned recordIds (search is fuzzy).
    """
    results = clay_request(
        "POST",
        f"/tables/{CONTACT_PROFILES_TABLE}/views/{CONTACT_PROFILES_VIEW}/search",
        {"searchTerm": domain}
    )

    hits = results.get("results", [])

    # Collect unique record IDs — prefer domain field matches
    domain_matches = set()
    all_record_ids = set()
    for hit in hits:
        rid = hit.get("recordId", "")
        fid = hit.get("fieldId", "")
        if rid:
            all_record_ids.add(rid)
            if fid == DOMAIN_FIELD:
                domain_matches.add(rid)

    # Use domain-specific matches if any, otherwise all search hits
    record_ids = domain_matches if domain_matches else all_record_ids

    return [{"id": rid, "domain": domain} for rid in record_ids]


def trigger_intent_refresh(record_ids: list[str]) -> dict:
    """Run Intent Lookup enrichment on specified Contact Profiles records."""
    if not record_ids:
        return {"recordCount": 0}

    payload = {
        "callerName": "clay-refresh-service",
        "fieldIds": [INTENT_LOOKUP_FIELD],
        "forceRun": True,
        "runRecords": {"recordIds": record_ids},
    }
    return clay_request(
        "PATCH",
        f"/tables/{CONTACT_PROFILES_TABLE}/run",
        payload
    )


# --- Request/Response Models ---

class RefreshRequest(BaseModel):
    domain: str


class RefreshResponse(BaseModel):
    refreshed: bool
    domain: str
    records_found: int
    records_refreshed: int
    record_ids: list[str]


class HealthResponse(BaseModel):
    status: str
    service: str
    version: str


# --- Endpoints ---

@app.get("/", response_model=HealthResponse)
def health():
    return HealthResponse(
        status="ok",
        service="clay-contact-refresh",
        version="1.0.5",
    )


@app.get("/debug-list")
def debug_list():
    """Temporary debug endpoint — raw Clay API response."""
    qs = urllib.parse.urlencode({"offset": 0, "limit": 2})
    result = clay_request(
        "GET",
        f"/tables/{CONTACT_PROFILES_TABLE}/views/{CONTACT_PROFILES_VIEW}/records?{qs}"
    )
    # Return raw response truncated
    raw = json.dumps(result)[:3000]
    return {"raw": raw, "version": "1.0.5"}


@app.post("/refresh-contact", response_model=RefreshResponse)
def refresh_contact(req: RefreshRequest):
    """
    Called by Clay HTTP API enrichment when a returning visitor is detected.

    Finds Contact Profiles records matching the domain and triggers
    Intent Lookup re-enrichment. The cascade handles the rest:
    Intent Lookup → scores → Lead Summary → HubSpot push.
    """
    domain = req.domain.strip().lower()
    if not domain:
        raise HTTPException(status_code=400, detail="domain is required")

    # Find matching contacts
    matches = find_contact_records_by_domain(domain)

    if not matches:
        return RefreshResponse(
            refreshed=False,
            domain=domain,
            records_found=0,
            records_refreshed=0,
            record_ids=[],
        )

    # Trigger refresh on all matching records
    record_ids = [m["id"] for m in matches]
    result = trigger_intent_refresh(record_ids)

    return RefreshResponse(
        refreshed=True,
        domain=domain,
        records_found=len(matches),
        records_refreshed=result.get("recordCount", len(record_ids)),
        record_ids=record_ids,
    )


@app.post("/refresh-stale")
def refresh_all_stale():
    """
    Batch endpoint: finds ALL Contact Profiles with Needs Refresh = "true"
    and triggers Intent Lookup. Use for periodic cron runs.
    """
    # List all records with pagination
    all_records = []
    offset = 0
    limit = 100
    while True:
        qs = urllib.parse.urlencode({
            "offset": offset,
            "limit": limit,
        })
        result = clay_request(
            "GET",
            f"/tables/{CONTACT_PROFILES_TABLE}/views/{CONTACT_PROFILES_VIEW}/records?{qs}"
        )
        records = result.get("results", result.get("records", []))
        all_records.extend(records)
        if len(records) < limit:
            break
        offset += limit

    # Filter to stale records
    stale_ids = []
    for r in all_records:
        cells = r.get("cells", {})
        needs_refresh = cells.get(NEEDS_REFRESH_FIELD, {}).get("value", "")
        if needs_refresh == "true":
            stale_ids.append(r["id"])

    if not stale_ids:
        return {"refreshed": 0, "total_contacts": len(all_records)}

    # Batch refresh in groups of 20
    total_refreshed = 0
    for i in range(0, len(stale_ids), 20):
        batch = stale_ids[i:i + 20]
        result = trigger_intent_refresh(batch)
        total_refreshed += result.get("recordCount", len(batch))
        if i + 20 < len(stale_ids):
            time.sleep(1)  # Rate limit between batches

    return {
        "refreshed": total_refreshed,
        "total_contacts": len(all_records),
        "stale_found": len(stale_ids),
    }
