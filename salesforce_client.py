"""
Salesforce Client — Multi-Mode Authentication
==============================================
Automatically detects the runtime environment and uses the appropriate
authentication method:

  1. TOKEN MODE — Uses a pre-existing access token
     (requires SF_ACCESS_TOKEN, SF_INSTANCE_URL)

  2. REPLIT MODE — Uses Replit's built-in Salesforce connector API
     (requires REPLIT_CONNECTORS_HOSTNAME + identity token)

  3. REFRESH TOKEN MODE [RECOMMENDED] — Uses OAuth refresh token
     (requires SF_CLIENT_ID, SF_CLIENT_SECRET, SF_REFRESH_TOKEN)
     Best for SSO-enabled orgs and Claude Code. Never expires.

  4. DIRECT MODE — Uses Salesforce OAuth 2.0 Username-Password flow
     (requires SF_CLIENT_ID, SF_CLIENT_SECRET, SF_USERNAME, SF_PASSWORD)

The calling code doesn't need to know which mode is active:
    access_token, instance_url = get_salesforce_credentials()

Usage:
    from salesforce_client import get_salesforce_credentials, sf_api_get, sf_query, sf_query_all
"""

import os
import logging
import threading
import requests
from datetime import datetime

log = logging.getLogger("sf_client")

_cached_token = None
_cached_instance = None
_token_fetched_at = None
_token_lock = threading.Lock()

SF_API_VERSION = "v59.0"
SF_REQUEST_TIMEOUT = int(os.environ.get("SF_REQUEST_TIMEOUT", 30))
SF_TOKEN_CACHE_TTL = int(os.environ.get("SF_TOKEN_CACHE_TTL", 1800))


# ===========================================================================
# Auth Mode Implementations
# ===========================================================================

def _refresh_via_connector():
    """Mode 2: Fetch credentials via Replit's internal connector API."""
    hostname = os.environ.get("REPLIT_CONNECTORS_HOSTNAME")
    repl_identity = os.environ.get("REPL_IDENTITY")
    web_repl = os.environ.get("WEB_REPL_RENEWAL")

    if repl_identity:
        token = "repl " + repl_identity
    elif web_repl:
        token = "depl " + web_repl
    else:
        return None, None

    if not hostname:
        return None, None

    resp = requests.get(
        f"https://{hostname}/api/v2/connection?include_secrets=true&connector_names=salesforce",
        headers={"Accept": "application/json", "X-Replit-Token": token},
        timeout=SF_REQUEST_TIMEOUT,
    )
    resp.raise_for_status()
    data = resp.json()
    item = (data.get("items") or [None])[0]
    if not item:
        return None, None

    settings = item.get("settings", {})
    access_token = settings.get("access_token") or (
        settings.get("oauth", {}).get("credentials", {}).get("access_token")
    )
    instance_url = settings.get("instance_url")
    return access_token, instance_url


def _refresh_via_oauth():
    """Mode 3: Authenticate using a refresh token (recommended)."""
    client_id = os.environ.get("SF_CLIENT_ID")
    client_secret = os.environ.get("SF_CLIENT_SECRET")
    refresh_token = os.environ.get("SF_REFRESH_TOKEN")
    login_url = os.environ.get("SF_LOGIN_URL", "https://surestaff.my.salesforce.com").strip()

    if not all([client_id, client_secret, refresh_token]):
        raise RuntimeError(
            "Refresh Token mode requires: SF_CLIENT_ID, SF_CLIENT_SECRET, SF_REFRESH_TOKEN"
        )

    resp = requests.post(f"{login_url}/services/oauth2/token", data={
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
        "client_id": client_id,
        "client_secret": client_secret,
    }, timeout=SF_REQUEST_TIMEOUT)

    if resp.status_code != 200:
        error_detail = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        raise RuntimeError(f"Salesforce refresh token auth failed ({resp.status_code}): {error_detail}")

    data = resp.json()
    return data["access_token"], data["instance_url"]


def _refresh_via_password():
    """Mode 4: Authenticate via OAuth 2.0 Username-Password flow."""
    client_id = os.environ.get("SF_CLIENT_ID")
    client_secret = os.environ.get("SF_CLIENT_SECRET")
    username = os.environ.get("SF_USERNAME")
    password = os.environ.get("SF_PASSWORD", "")
    security_token = os.environ.get("SF_SECURITY_TOKEN", "")
    login_url = os.environ.get("SF_LOGIN_URL", "https://login.salesforce.com").strip()

    if not all([client_id, client_secret, username]):
        raise RuntimeError(
            "Direct OAuth requires: SF_CLIENT_ID, SF_CLIENT_SECRET, SF_USERNAME, "
            "SF_PASSWORD, SF_SECURITY_TOKEN"
        )

    resp = requests.post(f"{login_url}/services/oauth2/token", data={
        "grant_type": "password",
        "client_id": client_id,
        "client_secret": client_secret,
        "username": username,
        "password": password + security_token,
    }, timeout=SF_REQUEST_TIMEOUT)

    if resp.status_code != 200:
        error_detail = resp.json() if resp.headers.get("content-type", "").startswith("application/json") else resp.text
        raise RuntimeError(f"Salesforce OAuth failed ({resp.status_code}): {error_detail}")

    data = resp.json()
    return data["access_token"], data["instance_url"]


# ===========================================================================
# Main credential function (with token caching)
# ===========================================================================

def get_salesforce_credentials():
    """Returns (access_token, instance_url) using whichever auth method is available.

    Priority:
      1. Token mode (SF_ACCESS_TOKEN set) — fastest, no auth call
      2. Replit connector (REPLIT_CONNECTORS_HOSTNAME set) — inside Replit
      3. Refresh token (SF_REFRESH_TOKEN + SF_CLIENT_ID) — recommended
      4. Username-Password (SF_CLIENT_ID + SF_USERNAME) — legacy
    """
    global _cached_token, _cached_instance, _token_fetched_at

    with _token_lock:
        if _cached_token and _token_fetched_at:
            age = (datetime.now() - _token_fetched_at).total_seconds()
            if age < SF_TOKEN_CACHE_TTL:
                return _cached_token, _cached_instance

        # --- Mode 1: Pre-existing token ---
        if os.environ.get("SF_ACCESS_TOKEN") and os.environ.get("SF_INSTANCE_URL"):
            log.debug("Using Token mode (SF_ACCESS_TOKEN)")
            _cached_token = os.environ["SF_ACCESS_TOKEN"]
            _cached_instance = os.environ["SF_INSTANCE_URL"]
            _token_fetched_at = datetime.now()
            return _cached_token, _cached_instance

        # --- Mode 2: Replit connector ---
        try:
            token, instance = _refresh_via_connector()
            if token and instance:
                test_resp = requests.get(
                    f"{instance}/services/data/{SF_API_VERSION}/limits",
                    headers={"Authorization": f"Bearer {token}"},
                    timeout=SF_REQUEST_TIMEOUT,
                )
                if test_resp.status_code == 200:
                    _cached_token = token
                    _cached_instance = instance
                    _token_fetched_at = datetime.now()
                    log.info(f"Authenticated via connector: {instance}")
                    return token, instance
                else:
                    log.warning("Connector token invalid, falling back to OAuth")
        except Exception as e:
            log.warning(f"Connector auth failed ({e}), falling back to OAuth")

        # --- Mode 3: Refresh token (recommended) ---
        if os.environ.get("SF_REFRESH_TOKEN") and os.environ.get("SF_CLIENT_ID"):
            token, instance = _refresh_via_oauth()
            _cached_token = token
            _cached_instance = instance
            _token_fetched_at = datetime.now()
            log.info(f"Authenticated via refresh token: {instance}")
            return token, instance

        # --- Mode 4: Username-Password ---
        if os.environ.get("SF_CLIENT_ID") and os.environ.get("SF_USERNAME"):
            token, instance = _refresh_via_password()
            _cached_token = token
            _cached_instance = instance
            _token_fetched_at = datetime.now()
            log.info(f"Authenticated via username-password: {instance}")
            return token, instance

        raise RuntimeError(
            "No Salesforce credentials configured. Set one of:\n"
            "  A) SF_ACCESS_TOKEN + SF_INSTANCE_URL (token mode)\n"
            "  B) REPLIT_CONNECTORS_HOSTNAME (Replit mode)\n"
            "  C) SF_CLIENT_ID + SF_CLIENT_SECRET + SF_REFRESH_TOKEN (refresh token — recommended)\n"
            "  D) SF_CLIENT_ID + SF_CLIENT_SECRET + SF_USERNAME + SF_PASSWORD + SF_SECURITY_TOKEN (direct OAuth)"
        )


def _invalidate_token_cache():
    global _cached_token, _token_fetched_at
    with _token_lock:
        _cached_token = None
        _token_fetched_at = None


# ===========================================================================
# Convenience API helpers (with 401 auto-retry)
# ===========================================================================

def sf_api_get(path):
    """GET request to Salesforce REST API with 401 auto-retry."""
    access_token, instance_url = get_salesforce_credentials()
    url = f"{instance_url}/services/data/{SF_API_VERSION}{path}"
    resp = requests.get(url, headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }, timeout=SF_REQUEST_TIMEOUT)
    if resp.status_code == 401:
        _invalidate_token_cache()
        access_token, instance_url = get_salesforce_credentials()
        url = f"{instance_url}/services/data/{SF_API_VERSION}{path}"
        resp = requests.get(url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }, timeout=SF_REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def sf_api_post(path, body):
    """POST request to Salesforce REST API with 401 auto-retry."""
    access_token, instance_url = get_salesforce_credentials()
    url = f"{instance_url}/services/data/{SF_API_VERSION}{path}"
    resp = requests.post(url, headers={
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }, json=body, timeout=SF_REQUEST_TIMEOUT)
    if resp.status_code == 401:
        _invalidate_token_cache()
        access_token, instance_url = get_salesforce_credentials()
        url = f"{instance_url}/services/data/{SF_API_VERSION}{path}"
        resp = requests.post(url, headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json",
        }, json=body, timeout=SF_REQUEST_TIMEOUT)
    resp.raise_for_status()
    return resp.json()


def sf_query(soql):
    """Execute a SOQL query and return the raw response."""
    from urllib.parse import quote
    return sf_api_get(f"/query?q={quote(soql)}")


def sf_query_all(soql):
    """Execute a SOQL query and return all records (handles pagination)."""
    result = sf_query(soql)
    records = result.get("records", [])
    while not result.get("done", True) and result.get("nextRecordsUrl"):
        next_url = result["nextRecordsUrl"]
        if next_url.startswith("/services/data/"):
            next_url = next_url[len(f"/services/data/{SF_API_VERSION}"):]
        result = sf_api_get(next_url)
        records.extend(result.get("records", []))
    return records


def find_sobjects(keyword):
    """Search for Salesforce objects by keyword in name/label."""
    sobjects_data = sf_api_get("/sobjects")
    all_objects = sobjects_data.get("sobjects", [])
    keyword_lower = keyword.lower()
    matches = [
        {
            "name": obj["name"],
            "label": obj.get("label", ""),
            "labelPlural": obj.get("labelPlural", ""),
            "keyPrefix": obj.get("keyPrefix", ""),
            "custom": obj.get("custom", False),
            "queryable": obj.get("queryable", False),
            "urls": obj.get("urls", {}),
        }
        for obj in all_objects
        if keyword_lower in obj.get("name", "").lower()
        or keyword_lower in obj.get("label", "").lower()
        or keyword_lower in obj.get("labelPlural", "").lower()
    ]
    return matches


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    try:
        token, url = get_salesforce_credentials()
        print(f"[OK] Authenticated to: {url}")
        print(f"   Token obtained: {'Yes' if token else 'No'} ({len(token)} chars)")
    except Exception as e:
        print(f"[ERROR] {e}")
