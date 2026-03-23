from fastapi import FastAPI, HTTPException, Query, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse, RedirectResponse, JSONResponse
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from fastapi.requests import Request as _Request
from starlette.middleware.base import BaseHTTPMiddleware
from typing import Optional
import httpx
import os
import csv
import io
import asyncio
import secrets
import time as _time
import base64
import urllib.parse
from datetime import datetime, timedelta, date
from dotenv import load_dotenv
import json
from collections import defaultdict
import aiosmtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders as _enc
from itsdangerous import URLSafeTimedSerializer, BadSignature, SignatureExpired
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
import uuid as _uuid

load_dotenv()

app = FastAPI()

# Return unhandled exceptions as JSON so errors are readable in the browser/curl
from fastapi.responses import JSONResponse
from fastapi.requests import Request
import traceback

@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception):
    tb = traceback.format_exc()
    print(f"[ERROR] {request.url}\n{tb}")
    return JSONResponse(status_code=500, content={"detail": str(exc), "type": type(exc).__name__})

# Allow Azure Static Web Apps + SharePoint + localhost for dev.
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:8000",
        "http://127.0.0.1:8000",
    ],
    allow_origin_regex=r"https://.*\.(azurestaticapps\.net|sharepoint\.com|sharepoint\.us)$",
    allow_credentials=True,
    allow_methods=["GET", "POST"],
    allow_headers=["*"],
)

# ── Microsoft OAuth (Azure AD) ────────────────────────────────────────────────
# Set these on Render. AZURE_CLIENT_ID is required to enable auth;
# if missing the app runs open (local dev mode).
_AZ_CLIENT_ID  = os.getenv("AZURE_CLIENT_ID",     "")
_AZ_CLIENT_SEC = os.getenv("AZURE_CLIENT_SECRET",  "")
_AZ_TENANT_ID  = os.getenv("AZURE_TENANT_ID",      "common")
_SESSION_SECRET = os.getenv("SESSION_SECRET",      "dev-secret-please-set-me")
_ALLOWED_DOMAIN = os.getenv("ALLOWED_EMAIL_DOMAIN", "microf.com")
_COOKIE_MAX_AGE = 8 * 3600   # 8 hours

_signer = URLSafeTimedSerializer(_SESSION_SECRET)

# Separate token for internal/automated endpoints (no special chars needed).
# Set SYNC_TOKEN on Render and in GitHub Secrets.
_SYNC_TOKEN = os.getenv("SYNC_TOKEN", "")

# ── Scheduled email reports ───────────────────────────────────────────────
# Set these env vars on Render to enable report delivery.
# SMTP_USER + REPORT_RECIPIENTS are required; everything else has defaults.
_ANTHROPIC_KEY = os.getenv("ANTHROPIC_API_KEY", "")

_SMTP_HOST  = os.getenv("SMTP_HOST",      "smtp.gmail.com")
_SMTP_PORT  = int(os.getenv("SMTP_PORT",  "587"))
_SMTP_USER  = os.getenv("SMTP_USER",      "")
_SMTP_PASS  = os.getenv("SMTP_PASS",      "")
_SMTP_FROM  = os.getenv("SMTP_FROM_NAME", "Microf Reports")
_RECIPIENTS = [r.strip() for r in os.getenv("REPORT_RECIPIENTS", "").split(",") if r.strip()]

# ── Admin / Scheduler ─────────────────────────────────────────────────────
_ADMIN_EMAILS   = {e.strip().lower() for e in os.getenv("ADMIN_EMAIL", "jsykes@microf.com,bsanders@microf.com").split(",") if e.strip()}
_SCHEDULES_FILE = os.getenv("SCHEDULES_FILE", os.path.join(os.path.dirname(__file__), "schedules.json"))
_scheduler      = AsyncIOScheduler()
_schedules: dict = {}   # job_id → schedule dict


def _load_schedules_from_disk():
    if not os.path.exists(_SCHEDULES_FILE):
        return
    try:
        with open(_SCHEDULES_FILE) as f:
            saved = json.load(f)
        for s in saved:
            _register_schedule(s, persist=False)
    except Exception as e:
        print(f"[scheduler] Failed to load schedules: {e}")


def _save_schedules_to_disk():
    try:
        with open(_SCHEDULES_FILE, "w") as f:
            json.dump(list(_schedules.values()), f, indent=2)
    except Exception as e:
        print(f"[scheduler] Failed to save schedules: {e}")


def _register_schedule(s: dict, persist: bool = True):
    job_id = s["id"]
    freq   = s["frequency"]       # daily | weekly | monthly
    hour   = int(s.get("hour", 9))
    minute = int(s.get("minute", 0))

    if freq == "daily":
        trigger = CronTrigger(hour=hour, minute=minute)
    elif freq == "weekly":
        trigger = CronTrigger(day_of_week=s.get("day_of_week", "mon"), hour=hour, minute=minute)
    else:  # monthly
        trigger = CronTrigger(day=int(s.get("day_of_month", 1)), hour=hour, minute=minute)

    report_type = s["report_type"]
    recipients  = s["recipients"]
    period      = s.get("period") or None

    async def _run():
        job = _REPORT_JOBS.get(report_type)
        if not job:
            print(f"[scheduler] Unknown report type: {report_type}")
            return
        try:
            kwargs = {"recipients": recipients}
            if period:
                kwargs["preset"] = period
            await job(**kwargs)
            print(f"[scheduler] Sent '{report_type}' → {recipients}")
        except Exception as exc:
            print(f"[scheduler] Job '{report_type}' failed: {exc}")

    _scheduler.add_job(_run, trigger=trigger, id=job_id, replace_existing=True)
    _schedules[job_id] = s
    if persist:
        _save_schedules_to_disk()


def _require_admin(request: _Request):
    email = _get_session_email(request)
    if not _AZ_CLIENT_ID:          # no Azure → local dev, allow all
        return "local"
    if not email or email.lower() not in _ADMIN_EMAILS:
        raise HTTPException(status_code=403, detail="Admin only")
    return email


def _redirect_uri() -> str:
    base = os.getenv("RENDER_EXTERNAL_URL", "http://localhost:8000").rstrip("/")
    return f"{base}/auth/callback"


def _get_session_email(request: _Request) -> Optional[str]:
    """Return the authenticated email from the session cookie, or None."""
    token = request.cookies.get("session")
    if not token:
        return None
    try:
        return _signer.loads(token, max_age=_COOKIE_MAX_AGE)
    except (BadSignature, SignatureExpired):
        return None


def require_auth(request: _Request):
    """Dependency: returns the current user email. Middleware enforces blocking."""
    if not _AZ_CLIENT_ID:          # no Azure configured → open (local dev)
        return "dev@microf.com"
    return _get_session_email(request) or "unknown"


class _MSAuthMiddleware(BaseHTTPMiddleware):
    """Block unauthenticated requests. Redirects pages → /login, 401s for APIs."""
    _PUBLIC = {"/login", "/auth/start", "/auth/callback", "/logout", "/health",
               "/api/dealer-index/status"}

    async def dispatch(self, request: _Request, call_next):
        path = request.url.path
        # Always allow public paths and static assets
        if path in self._PUBLIC or path.startswith("/static"):
            return await call_next(request)
        # Dev mode — no Azure client ID configured
        if not _AZ_CLIENT_ID:
            return await call_next(request)
        # Automated endpoints: accept SYNC_TOKEN header/query instead of cookie
        bearer = request.headers.get("Authorization", "")
        sync_q = request.query_params.get("token", "")
        if _SYNC_TOKEN and (bearer == f"Bearer {_SYNC_TOKEN}" or sync_q == _SYNC_TOKEN):
            return await call_next(request)
        # Check session cookie
        email = _get_session_email(request)
        if not email:
            if path.startswith("/api/"):
                return JSONResponse(status_code=401, content={"detail": "Not authenticated"})
            return RedirectResponse(url="/login", status_code=302)
        return await call_next(request)

# Register auth middleware after the class is defined, after CORS so CORS
# headers are still applied before the 401/redirect response is returned.
app.add_middleware(_MSAuthMiddleware)

app.mount("/static", StaticFiles(directory="static"), name="static")


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.get("/login")
async def login_page():
    return FileResponse("static/login.html")


@app.get("/auth/start")
async def auth_start():
    """Redirect browser to Microsoft login."""
    params = {
        "client_id":     _AZ_CLIENT_ID,
        "response_type": "code",
        "redirect_uri":  _redirect_uri(),
        "scope":         "openid email profile",
        "response_mode": "query",
    }
    ms_url = (
        f"https://login.microsoftonline.com/{_AZ_TENANT_ID}"
        f"/oauth2/v2.0/authorize?{urllib.parse.urlencode(params)}"
    )
    return RedirectResponse(url=ms_url)


@app.get("/auth/callback")
async def auth_callback(
    code:  Optional[str] = Query(None),
    error: Optional[str] = Query(None),
):
    """Exchange code for token, validate @microf.com, set session cookie."""
    if error or not code:
        return RedirectResponse(url="/login?error=cancelled")

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(
            f"https://login.microsoftonline.com/{_AZ_TENANT_ID}/oauth2/v2.0/token",
            data={
                "client_id":     _AZ_CLIENT_ID,
                "client_secret": _AZ_CLIENT_SEC,
                "code":          code,
                "redirect_uri":  _redirect_uri(),
                "grant_type":    "authorization_code",
                "scope":         "openid email profile",
            },
        )
    token_data = resp.json()

    # Decode the id_token JWT (middle segment) — no signature verification needed
    # since we got it directly from Microsoft over TLS.
    id_token = token_data.get("id_token", "")
    try:
        seg = id_token.split(".")[1]
        seg += "=" * (-len(seg) % 4)           # re-pad base64
        payload = json.loads(base64.urlsafe_b64decode(seg))
    except Exception:
        return RedirectResponse(url="/login?error=token")

    email = (payload.get("email") or payload.get("preferred_username", "")).lower()

    if not email.endswith(f"@{_ALLOWED_DOMAIN}"):
        return RedirectResponse(url=f"/login?error=domain&email={urllib.parse.quote(email)}")

    session_token = _signer.dumps(email)
    response = RedirectResponse(url="/search", status_code=302)
    response.set_cookie(
        "session", session_token,
        max_age=_COOKIE_MAX_AGE, httponly=True, samesite="lax", secure=True,
    )
    return response


@app.get("/logout")
async def logout():
    response = RedirectResponse(url="/login", status_code=302)
    response.delete_cookie("session")
    return response


@app.get("/search")
async def search_page(_: None = Depends(require_auth)):
    return FileResponse("static/search.html")

@app.on_event("startup")
async def _startup():
    """Kick off the dealer ID index build in the background so it doesn't block startup."""
    asyncio.create_task(_build_dealer_id_index())
    asyncio.create_task(_keep_alive())
    _load_schedules_from_disk()
    _scheduler.start()
    print(f"[scheduler] Started with {len(_schedules)} job(s)")

async def _keep_alive() -> None:
    """Ping this app's own health endpoint every 10 minutes to prevent Render from
    spinning down the instance due to inactivity."""
    import os as _os
    self_url = _os.getenv("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not self_url:
        print("[keep-alive] RENDER_EXTERNAL_URL not set — skipping keep-alive pings")
        return
    await asyncio.sleep(120)   # wait 2 min after boot before first ping
    while True:
        try:
            async with httpx.AsyncClient(timeout=10) as _hc:
                await _hc.get(f"{self_url}/api/dealer-index/status")
            print("[keep-alive] pinged OK")
        except Exception as _e:
            print(f"[keep-alive] ping failed: {_e}")
        await asyncio.sleep(600)   # 10 minutes

@app.get("/api/dealer-index/refresh")
async def dealer_index_refresh(_: None = Depends(require_auth)):
    """Manually trigger a full rebuild of the dealer_id ↔ account index."""
    asyncio.create_task(_build_dealer_id_index())
    return {"status": "rebuild started", "accounts_indexed": len(_dealer_id_index)}

@app.get("/api/dealer-index/status")
async def dealer_index_status():
    """Return current index size and age."""
    age = int(_time.time() - _dealer_index_ts) if _dealer_index_ts else None
    return {
        "indexed_dealers":   len(_dealer_id_index),
        "indexed_accounts":  len(_account_to_dealer),
        "indexed_platforms": len(_account_to_platform),
        "indexed_bdrs":      len(_account_to_bdr),
        "age_seconds":       age,
        "last_error":        _dealer_index_error or None,
    }

@app.get("/api/dealer-index/diagnose")
async def dealer_index_diagnose(_: None = Depends(require_auth)):
    """Fetch first page of accountCustomFieldData and first SLP record raw — debug only."""
    cf_page  = await ac_get("accountCustomFieldData", {"limit": 5, "offset": 0})
    slp_page = await ac_get(f"customObjects/records/{SLP_SCHEMA_ID}", {"limit": 1})
    slp_rec  = slp_page.get("records", [{}])[0] if slp_page.get("records") else {}
    return {
        "cf_total":    cf_page.get("meta", {}).get("total"),
        "cf_sample":   cf_page.get("accountCustomFieldData", [])[:3],
        "slp_sample":  slp_rec,
        "index_sizes": {
            "dealers":   len(_account_to_dealer),
            "platforms": len(_account_to_platform),
            "bdrs":      len(_account_to_bdr),
        },
    }

AC_BASE_URL = (os.getenv("AC_BASE_URL") or os.getenv("PROD_URL", "")).rstrip("/")
AC_API_KEY  = os.getenv("AC_API_KEY") or os.getenv("PROD_KEY", "")
HEADERS     = {"Api-Token": AC_API_KEY, "Content-Type": "application/json"}

import re as _re
import time as _time
_subdomain  = _re.match(r"https?://([^.]+)", AC_BASE_URL)
AC_UI_BASE  = f"https://{_subdomain.group(1)}.activehosted.com" if _subdomain else ""

def ac_account_url(account_id) -> str:
    return f"{AC_UI_BASE}/app/accounts/{account_id}" if account_id else ""

def ac_contact_url(contact_id) -> str:
    return f"{AC_UI_BASE}/app/contacts/{contact_id}" if contact_id else ""

# ── Account custom field metadata (cached) ──────────────────────────────────
_cf_meta_cache: dict = {}   # str(field_id) -> label
_cf_meta_ts: float   = 0.0

# ── Dealer ID ↔ Account index (built at startup, refreshed hourly) ───────────
_dealer_id_index:  dict  = {}   # dealer_id (str) → {"id": account_id, "name": account_name}
_account_to_dealer: dict = {}   # account_id (str) → dealer_id (str)
_account_to_platform: dict = {} # account_id (str) → platform/Dealer Program (customfield 29)
_account_to_bdr: dict = {}      # account_id (str) → Assigned BDR (customfield 119)
_account_to_name: dict = {}     # account_id (str) → account name
_account_to_owner: dict = {}    # account_id (str) → owner user_id (str)
_program_to_accounts: dict = {} # lowercase(dealer_program) → set of account_ids
_dealer_index_ts:  float = 0.0
_dealer_index_error: str = ""   # last build error message, for /api/dealer-index/status

async def _get_account_cf_meta() -> dict:
    """Return {str(field_id): label} cached for 1 hour."""
    global _cf_meta_ts
    if _cf_meta_cache and (_time.time() - _cf_meta_ts) < 3600:
        return _cf_meta_cache
    data = await ac_get("accountCustomFieldMeta", {"limit": 200})
    _cf_meta_cache.clear()
    for f in data.get("accountCustomFieldMeta", []):
        fid = str(f.get("id", ""))
        _cf_meta_cache[fid] = f.get("fieldLabel") or f.get("perstag") or f"field_{fid}"
    _cf_meta_ts = _time.time()
    return _cf_meta_cache

def _normalize_platform(val: str) -> str:
    """Normalize platform/program display names for reporting.
    Microf (LTO Only), LTO → Microf
    Optimus 2.0, OPTIMUS 3.0, etc. → OPTIMUS
    """
    v = val.strip().lower()
    if "optimus" in v:
        return "OPTIMUS"
    if "microf" in v or v == "lto":
        return "Microf"
    return val.strip()


def _extract_cf_value(cf: dict) -> str:
    """Read the first non-empty value across all custom field value types."""
    for key in ("custom_field_text_value", "custom_field_date_value",
                "custom_field_datetime_value", "custom_field_number_value",
                "custom_field_currency_value"):
        v = cf.get(key)
        if v and v not in ("0000-00-00", "0000-00-00 00:00:00"):
            return str(v).strip()
    return ""

async def _build_dealer_id_index() -> None:
    """Build dealer_id ↔ account index using the bulk accountCustomFieldData endpoint.
    Phase 1: page through all CF data concurrently (20 pages at a time) to extract
             customFieldId-18 (dealer ID) records — ~10-15 s for ~190k records.
    Phase 2: paginate accounts to get names.
    Runs on server startup; re-triggered via /api/dealer-index/refresh."""
    global _dealer_index_ts, _dealer_index_error
    DEALER_CF_ID   = 18    # customFieldId for "Parent Dealer ID"
    PLATFORM_CF_ID = 29    # customFieldId for "Dealer Program"
    BDR_CF_ID      = 119   # customFieldId for "Assigned BDR"
    CF_PAGE        = 1000  # 1000 records/page → ~190 pages instead of ~1900
    CONCURRENCY    = 8     # 8 concurrent requests → index builds in ~10s instead of ~5min

    try:
        print("[dealer-index] Starting build…")

        # ── Phase 1: bulk accountCustomFieldData ──────────────────────────
        first_page = await ac_get("accountCustomFieldData", {"limit": CF_PAGE, "offset": 0})
        total_cf   = int(first_page.get("meta", {}).get("total", 0))
        print(f"[dealer-index] {total_cf} CF records total, scanning…")

        acct_to_dealer:   dict = {}
        acct_to_platform: dict = {}
        acct_to_bdr:      dict = {}

        def _ingest(items: list) -> None:
            for item in items:
                cf_id   = int(item.get("customFieldId", 0))
                aid     = str(item.get("accountId", ""))
                raw     = item.get("fieldValue")
                if isinstance(raw, list):
                    raw = ", ".join(str(v) for v in raw if v)
                val = (str(raw) if raw is not None else "").strip()
                if not (aid and val):
                    continue
                if cf_id == DEALER_CF_ID:
                    acct_to_dealer[aid]   = val
                elif cf_id == PLATFORM_CF_ID:
                    acct_to_platform[aid] = val
                elif cf_id == BDR_CF_ID:
                    acct_to_bdr[aid]      = val

        _ingest(first_page.get("accountCustomFieldData", []))

        # Remaining pages — fetch in parallel batches
        remaining_offsets = list(range(CF_PAGE, total_cf, CF_PAGE))
        for i in range(0, len(remaining_offsets), CONCURRENCY):
            batch = remaining_offsets[i : i + CONCURRENCY]
            pages = await asyncio.gather(
                *[ac_get("accountCustomFieldData", {"limit": CF_PAGE, "offset": off})
                  for off in batch],
                return_exceptions=True,
            )
            for page in pages:
                if not isinstance(page, Exception):
                    _ingest(page.get("accountCustomFieldData", []))

        print(f"[dealer-index] {len(acct_to_dealer)} dealer IDs, "
              f"{len(acct_to_platform)} platforms, {len(acct_to_bdr)} BDRs indexed; "
              f"fetching account names…")

        # ── Phase 2: paginate accounts for names ──────────────────────────
        all_accounts = await ac_get_all("accounts", "accounts", {})
        acct_to_name  = {str(a.get("id", "")): a.get("name", "")              for a in all_accounts}
        acct_to_owner = {str(a.get("id", "")): str(a.get("owner", "") or "")  for a in all_accounts}
        print(f"[dealer-index] {len(all_accounts)} account names loaded")

        # ── Publish index from bulk scan immediately so app is usable ─────
        new_did: dict = {}
        new_atd: dict = {}
        for aid, did in acct_to_dealer.items():
            new_atd[aid] = did
            new_did[did] = {"id": aid, "name": acct_to_name.get(aid, "")}

        _dealer_id_index.clear();    _dealer_id_index.update(new_did)
        _account_to_dealer.clear();  _account_to_dealer.update(new_atd)
        _account_to_platform.clear(); _account_to_platform.update(acct_to_platform)
        _account_to_bdr.clear();     _account_to_bdr.update(acct_to_bdr)
        _account_to_name.clear();    _account_to_name.update(acct_to_name)
        _account_to_owner.clear();   _account_to_owner.update(acct_to_owner)

        # Reverse index: lowercase dealer program → set of account IDs
        new_prog: dict = {}
        for aid, prog in acct_to_platform.items():
            key = prog.lower().strip()
            if key:
                new_prog.setdefault(key, set()).add(aid)
        _program_to_accounts.clear(); _program_to_accounts.update(new_prog)

        _dealer_index_ts = _time.time()
        print(f"[dealer-index] Done. {len(new_did)} dealer IDs, "
              f"{len(new_prog)} dealer programs indexed across {len(new_atd)} accounts.")

    except Exception as _build_exc:
        import traceback
        _dealer_index_error = f"{type(_build_exc).__name__}: {_build_exc}"
        print(f"[dealer-index] BUILD FAILED: {_build_exc}")
        traceback.print_exc()


SLP_SCHEMA_ID           = "d5ccf74f-981f-40ff-8a03-23cd0309808f"
LICENSE_SCHEMA_ID       = "4bc17cb1-31be-4c15-a186-853ea85b1d40"
TRAINING_SCHEMA_ID      = "9368fee4-ccef-407b-a0d3-4b72c346b2af"
ACCT_ACTIVITY_SCHEMA_ID = "3a11374e-4b3d-47b8-b423-17ebcb7b1f4b"
ALT_CONTACT_SCHEMA_ID   = "b8259d61-10ba-4b15-8b2e-d1c8045712e0"

# Known account custom field IDs (from field_id_mapping.csv)
ACCT_FIELD = {
    "dealer_id":             "18",
    "account_status":        "19",
    "dba_name":              "15",
    "doing_business_in":     "22",
    "sales_region":          "23",
    "partner_activation":    "26",
    "dealer_program":        "29",
    "platforms":             "34",
    "original_owner":        "35",
    "assigned_bdr":          "119",
    "oracle_producer_id":    "118",
}

# ═══════════════════════════════════════════════════════════════════════════
# CACHING
# ═══════════════════════════════════════════════════════════════════════════

CACHE: dict = {
    "account_custom_fields": {},
    "contact_custom_fields": {},
    "deal_custom_fields":    {},
    "field_metadata":        {},
    "schemas":               {},
}
CACHE_TIMESTAMPS: dict = {
    "account_custom_fields": {},
    "contact_custom_fields": {},
    "deal_custom_fields":    {},
    "field_metadata":        {},
    "schemas":               {},
}
CACHE_TTL = 300  # seconds

def get_cached(cache_type: str, key: str):
    if key in CACHE[cache_type]:
        ts = CACHE_TIMESTAMPS[cache_type].get(key, 0)
        if datetime.now().timestamp() - ts < CACHE_TTL:
            return CACHE[cache_type][key]
    return None

def set_cached(cache_type: str, key: str, value):
    CACHE[cache_type][key] = value
    CACHE_TIMESTAMPS[cache_type][key] = datetime.now().timestamp()

MAX_CONCURRENT_REQUESTS = 20
semaphore = asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)


# ═══════════════════════════════════════════════════════════════════════════
# AC API HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def ac_url(path: str) -> str:
    return f"{AC_BASE_URL}/api/3/{path.lstrip('/')}"

async def ac_get(path: str, params: dict = None):
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(ac_url(path), headers=HEADERS, params=params or {})
        r.raise_for_status()
        return r.json()

async def ac_post(path: str, body: dict):
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.post(ac_url(path), headers=HEADERS, json=body)
        if not r.is_success:
            raise Exception(f"HTTP {r.status_code} {r.text[:300]}")
        return r.json()

async def ac_put(path: str, body: dict):
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.put(ac_url(path), headers=HEADERS, json=body)
        if not r.is_success:
            raise Exception(f"HTTP {r.status_code} {r.text[:300]}")
        return r.json()

async def ac_get_all(path: str, key: str, params: dict = None) -> list:
    """Paginate through all records, deduplicating by id.

    AC's custom-objects endpoint has non-deterministic pagination: pages overlap
    heavily and meta.total is inflated. For custom-object endpoints we run two
    SEQUENTIAL passes and union results to catch records that slip through due
    to AC's page reordering. Standard endpoints (accounts, contacts, etc.) only
    need one pass.
    """
    is_custom_obj = "customObjects" in path
    num_passes    = 3 if is_custom_obj else 1

    seen  = {}
    p     = params or {}
    limit = 100

    for pass_num in range(num_passes):
        offset = 0
        while True:
            data = await ac_get(path, {**p, "limit": limit, "offset": offset})
            page = data.get(key, [])
            if not page:
                break
            for item in page:
                item_id = item.get("id")
                if item_id is not None:
                    seen[item_id] = item
                else:
                    seen[len(seen)] = item
            offset += limit
        if is_custom_obj:
            print(f"[ac_get_all] {path} pass {pass_num+1} done, total unique={len(seen)}")

    return list(seen.values())


# ═══════════════════════════════════════════════════════════════════════════
# FIELD DISCOVERY
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/objects")
async def list_objects():
    return {
        "objects": [
            {"id": "slp",             "name": "Strategic Lending Partners",   "icon": "📊"},
            {"id": "accounts",        "name": "Accounts",                     "icon": "🏢"},
            {"id": "contacts",        "name": "Contacts",                     "icon": "👤"},
            {"id": "deals",           "name": "Deals",                        "icon": "💰"},
            {"id": "trainings",       "name": "Trainings",                    "icon": "🎓"},
            {"id": "license_details", "name": "Contractor License Details",   "icon": "📜"},
            {"id": "notes",           "name": "Notes",                        "icon": "📝"},
        ]
    }


async def _schema_fields(schema_id: str) -> tuple:
    """Return (fields_list, field_types_dict) for a custom object schema."""
    data   = await ac_get(f"customObjects/schemas/{schema_id}")
    schema = data.get("schema", {})
    fields, ftypes = [], {}
    for f in schema.get("fields", []):
        fid   = f.get("id", f.get("slug"))
        label = f.get("labels", {}).get("singular", f.get("slug", ""))
        ftype = f.get("type", "text")
        fields.append({"id": fid, "label": label, "type": "primary", "dataType": ftype})
        ftypes[fid] = {"type": ftype, "options": f.get("options", [])}
    return fields, ftypes


async def _account_custom_field_defs() -> list:
    """Return account custom field meta as list of field dicts."""
    try:
        data = await ac_get("accountCustomFieldMeta")
        return data.get("accountCustomFieldMeta", [])
    except Exception:
        return []


async def _contact_custom_field_defs() -> list:
    try:
        data = await ac_get("fields")
        return data.get("fields", [])
    except Exception:
        return []


@app.get("/api/fields/{object_type}")
async def get_fields(object_type: str):
    fields: list = []
    ftypes: dict = {}

    if object_type == "slp":
        f, ft = await _schema_fields(SLP_SCHEMA_ID)
        fields.extend(f); ftypes.update(ft)
        fields.extend(await _related_account_fields("Account"))
        fields.extend(_related_contact_summary_fields("Contacts"))
        fields.extend(_related_training_summary_fields("Trainings"))
        fields.extend(_related_deal_summary_fields("Deals"))

    elif object_type == "license_details":
        f, ft = await _schema_fields(LICENSE_SCHEMA_ID)
        fields.extend(f); ftypes.update(ft)
        fields.extend(await _related_account_fields("Account"))

    elif object_type == "trainings":
        f, ft = await _schema_fields(TRAINING_SCHEMA_ID)
        fields.extend(f); ftypes.update(ft)
        fields.extend(await _related_account_fields("Account"))

    elif object_type == "accounts":
        # Built-in account fields
        sample = await _sample("accounts", "accounts")
        for key, val in sample.items():
            if key not in ("links", "fieldValues") and not isinstance(val, dict):
                fields.append({"id": key, "label": key, "type": "primary", "dataType": "text"})
        # Account custom fields
        for cf in await _account_custom_field_defs():
            fid   = f"customfield_{cf['id']}"
            label = cf.get("fieldLabel", cf.get("fieldName", str(cf["id"])))
            ftype = cf.get("fieldType", "text")
            obj   = {"id": fid, "label": label, "type": "primary", "dataType": ftype}
            if ftype in ("dropdown", "listbox", "radio"):
                opts = cf.get("fieldOptions", "")
                if isinstance(opts, str) and opts:
                    obj["options"] = [o.strip() for o in opts.replace("\n", ",").split(",") if o.strip()]
                elif isinstance(opts, list):
                    obj["options"] = [str(o.get("value", o)) if isinstance(o, dict) else str(o) for o in opts if o]
            fields.append(obj)
            ftypes[fid] = obj
        # Cross-object fields
        fields.extend(await _related_slp_fields("SLP"))
        fields.extend(_related_contact_summary_fields("Contacts"))
        fields.extend(_related_deal_summary_fields("Deals"))
        fields.extend(_related_training_summary_fields("Trainings"))
        fields.extend(_related_notes_summary_fields("Notes"))

    elif object_type == "contacts":
        sample = await _sample("contacts", "contacts")
        for key, val in sample.items():
            if key not in ("links", "fieldValues") and not isinstance(val, dict):
                fields.append({"id": key, "label": key, "type": "primary", "dataType": "text"})
        for cf in await _contact_custom_field_defs():
            fid   = f"customfield_{cf['id']}"
            label = cf.get("title", str(cf["id"]))
            fields.append({"id": fid, "label": label, "type": "primary", "dataType": "text"})
        fields.extend(await _related_account_fields("Account"))
        fields.extend(await _related_slp_fields("SLP"))
        fields.extend(_related_deal_summary_fields("Deals"))
        fields.extend(_related_notes_summary_fields("Notes"))

    elif object_type == "notes":
        fields = [
            {"id": "id",                "label": "Note ID",            "type": "primary", "dataType": "text"},
            {"id": "note",              "label": "Note Content",       "type": "primary", "dataType": "text"},
            {"id": "cdate",             "label": "Created Date",       "type": "primary", "dataType": "date"},
            {"id": "mdate",             "label": "Modified Date",      "type": "primary", "dataType": "date"},
            {"id": "userid",            "label": "Author User ID",     "type": "primary", "dataType": "text"},
            {"id": "reltype",           "label": "Related To Type",    "type": "primary", "dataType": "text"},
            {"id": "rel_id",            "label": "Related Object ID",  "type": "primary", "dataType": "text"},
            {"id": "pinned",            "label": "Pinned",             "type": "primary", "dataType": "text"},
            {"id": "contact.firstName", "label": "Contact: First Name","type": "related", "dataType": "text"},
            {"id": "contact.lastName",  "label": "Contact: Last Name", "type": "related", "dataType": "text"},
            {"id": "contact.email",     "label": "Contact: Email",     "type": "related", "dataType": "text"},
        ]
        fields.extend(await _related_account_fields("Account"))

    elif object_type == "deals":
        sample = await _sample("deals", "deals")
        for key, val in sample.items():
            if key not in ("links",) and not isinstance(val, dict):
                fields.append({"id": key, "label": key, "type": "primary", "dataType": "text"})
        fields.extend(await _related_account_fields("Account"))
        fields.extend([
            {"id": "contact.email",     "label": "Contact: Email",      "type": "related", "dataType": "text"},
            {"id": "contact.firstName", "label": "Contact: First Name", "type": "related", "dataType": "text"},
            {"id": "contact.lastName",  "label": "Contact: Last Name",  "type": "related", "dataType": "text"},
            {"id": "contact.phone",     "label": "Contact: Phone",      "type": "related", "dataType": "text"},
        ])

    return {"fields": fields, "fieldTypes": ftypes}


async def _sample(path: str, key: str) -> dict:
    try:
        data = await ac_get(path, {"limit": 1})
        return data.get(key, [{}])[0]
    except Exception:
        return {}


async def _related_account_fields(prefix: str) -> list:
    fields = []
    sample = await _sample("accounts", "accounts")
    for key, val in sample.items():
        if key not in ("links", "fieldValues") and not isinstance(val, dict):
            fields.append({"id": f"account.{key}", "label": f"{prefix}: {key}", "type": "related", "dataType": "text"})
    for cf in await _account_custom_field_defs():
        fid   = f"account.customfield_{cf['id']}"
        label = cf.get("fieldLabel", cf.get("fieldName", str(cf["id"])))
        fields.append({"id": fid, "label": f"{prefix}: {label}", "type": "related", "dataType": "text"})
    return fields


async def _related_slp_fields(prefix: str) -> list:
    fields = []
    try:
        f, _ = await _schema_fields(SLP_SCHEMA_ID)
        for field in f:
            fields.append({"id": f"slp.{field['id']}", "label": f"{prefix}: {field['label']}", "type": "related", "dataType": field.get("dataType", "text")})
        fields.append({"id": "slp._count", "label": f"{prefix}: Record Count", "type": "related", "dataType": "number"})
    except Exception:
        pass
    return fields


def _related_contact_summary_fields(prefix: str) -> list:
    return [
        {"id": "primary_contact.email",     "label": f"{prefix}: Primary Email",      "type": "related", "dataType": "text"},
        {"id": "primary_contact.firstName", "label": f"{prefix}: Primary First Name", "type": "related", "dataType": "text"},
        {"id": "primary_contact.lastName",  "label": f"{prefix}: Primary Last Name",  "type": "related", "dataType": "text"},
        {"id": "primary_contact.phone",     "label": f"{prefix}: Primary Phone",      "type": "related", "dataType": "text"},
        {"id": "contact_count",             "label": f"{prefix}: Count",              "type": "related", "dataType": "number"},
    ]


def _related_deal_summary_fields(prefix: str) -> list:
    return [
        {"id": "deal.title",  "label": f"{prefix}: Latest Title",  "type": "related", "dataType": "text"},
        {"id": "deal.stage",  "label": f"{prefix}: Latest Stage",  "type": "related", "dataType": "text"},
        {"id": "deal.status", "label": f"{prefix}: Latest Status", "type": "related", "dataType": "text"},
        {"id": "deal.cdate",  "label": f"{prefix}: Latest Date",   "type": "related", "dataType": "date"},
        {"id": "deal_count",  "label": f"{prefix}: Count",         "type": "related", "dataType": "number"},
    ]


def _related_training_summary_fields(prefix: str) -> list:
    return [
        {"id": "training.training-type",   "label": f"{prefix}: Latest Type",    "type": "related", "dataType": "text"},
        {"id": "training.training-agenda", "label": f"{prefix}: Latest Agenda",  "type": "related", "dataType": "text"},
        {"id": "training.date-of-training","label": f"{prefix}: Latest Date",    "type": "related", "dataType": "date"},
        {"id": "training.trained-by",      "label": f"{prefix}: Trained By",     "type": "related", "dataType": "text"},
        {"id": "training_count",           "label": f"{prefix}: Count",          "type": "related", "dataType": "number"},
    ]


def _related_notes_summary_fields(prefix: str) -> list:
    return [
        {"id": "note_count",       "label": f"{prefix}: Count",       "type": "related", "dataType": "number"},
        {"id": "latest_note_date", "label": f"{prefix}: Latest Date", "type": "related", "dataType": "date"},
        {"id": "latest_note",      "label": f"{prefix}: Latest Text", "type": "related", "dataType": "text"},
    ]


@app.get("/api/field-values/{object_type}/{field_id}")
async def get_field_values(object_type: str, field_id: str):
    """Return unique values for a field (for dropdown filters in UI)."""
    values: set = set()
    try:
        schema_map = {"slp": SLP_SCHEMA_ID, "trainings": TRAINING_SCHEMA_ID, "license_details": LICENSE_SCHEMA_ID}
        if object_type in schema_map:
            records = await ac_get_all(f"customObjects/records/{schema_map[object_type]}", "records", {})
            for r in records[:2000]:
                for fo in r.get("fields", []):
                    if fo.get("id") == field_id and fo.get("value"):
                        values.add(str(fo["value"]))
        elif object_type == "accounts":
            records = await ac_get_all("accounts", "accounts", {})
            for r in records[:2000]:
                val = r.get(field_id)
                if val:
                    values.add(str(val))
    except Exception:
        pass
    return {"values": sorted(values)}


# ═══════════════════════════════════════════════════════════════════════════
# FILTER EVALUATION
# ═══════════════════════════════════════════════════════════════════════════

def evaluate_filter(record: dict, f: dict) -> bool:
    from datetime import timezone
    field      = f.get("field")
    ftype      = f.get("type", "text")
    operator   = f.get("operator", "equals")
    value      = f.get("value")
    values     = f.get("values", [])
    date_range = f.get("dateRange")

    if not field:
        return True
    if ftype != "date" and not value and not values:
        return True

    rv = record.get(field)

    if ftype == "text":
        if rv is None:
            return False
        rs = str(rv).lower()
        if values:
            return any(str(v).lower() in rs for v in values)
        vs = str(value).lower()
        if operator == "equals":      return rs == vs
        if operator == "contains":    return vs in rs
        if operator == "starts_with": return rs.startswith(vs)
        if operator == "not_equals":  return rs != vs

    elif ftype == "dropdown":
        if rv is None:
            return False
        if values: return str(rv) in values
        if value:  return str(rv) == str(value)

    elif ftype == "date":
        if not rv or rv == "null":
            return False
        try:
            if isinstance(rv, str):
                if not rv.strip():
                    return False
                rd = datetime.fromisoformat(rv.replace("Z", "+00:00")) if "T" in rv else datetime.strptime(rv[:10], "%Y-%m-%d")
            else:
                rd = rv
            if rd.tzinfo is None:
                rd = rd.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)

            if date_range == "today":         return rd.date() == now.date()
            if date_range == "yesterday":     return rd.date() == (now - timedelta(days=1)).date()
            if date_range == "this_week":
                start = (now - timedelta(days=now.weekday())).replace(hour=0, minute=0, second=0, microsecond=0)
                return rd >= start
            if date_range == "last_7_days":   return rd >= now - timedelta(days=7)
            if date_range == "last_30_days":  return rd >= now - timedelta(days=30)
            if date_range == "last_90_days":  return rd >= now - timedelta(days=90)
            if date_range == "next_30_days":  return now <= rd <= now + timedelta(days=30)
            if date_range == "next_90_days":  return now <= rd <= now + timedelta(days=90)
            if date_range == "this_month":
                start = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                end   = (start.replace(month=start.month % 12 + 1, day=1) if start.month < 12
                         else start.replace(year=start.year + 1, month=1, day=1))
                return start <= rd < end
            if date_range == "last_month":
                end   = now.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
                start = (end.replace(month=end.month - 1) if end.month > 1
                         else end.replace(year=end.year - 1, month=12))
                return start <= rd < end
            if date_range == "this_quarter":
                q     = (now.month - 1) // 3
                start = now.replace(month=q * 3 + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
                return rd >= start
            if date_range == "last_quarter":
                q = (now.month - 1) // 3
                if q == 0:
                    start = now.replace(year=now.year - 1, month=10, day=1, hour=0, minute=0, second=0, microsecond=0)
                    end   = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                else:
                    start = now.replace(month=(q - 1) * 3 + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
                    end   = now.replace(month=q * 3 + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
                return start <= rd < end
            if date_range == "ytd":
                start = now.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
                return rd >= start
            if date_range == "this_year":     return rd.year == now.year
            if date_range == "last_year":     return rd.year == now.year - 1
            if date_range == "custom":
                fd = f.get("fromDate")
                td = f.get("toDate")
                if fd and rd < datetime.strptime(fd, "%Y-%m-%d").replace(tzinfo=timezone.utc):
                    return False
                if td and rd > datetime.strptime(td, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc):
                    return False
                return True
        except Exception:
            return False

    return True


# ═══════════════════════════════════════════════════════════════════════════
# PRIMARY RECORD FETCHERS
# ═══════════════════════════════════════════════════════════════════════════

def _flatten_custom_object(r: dict) -> dict:
    flat = {"id": r.get("id"), "_relationships": r.get("relationships", {})}
    for fo in r.get("fields", []):
        flat[fo.get("id")] = fo.get("value")
    return flat

async def fetch_slp_records() -> list:
    raw = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    return [_flatten_custom_object(r) for r in raw]

async def fetch_license_records() -> list:
    raw = await ac_get_all(f"customObjects/records/{LICENSE_SCHEMA_ID}", "records", {})
    return [_flatten_custom_object(r) for r in raw]

async def fetch_training_records() -> list:
    raw = await ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})
    return [_flatten_custom_object(r) for r in raw]

async def fetch_account_records() -> list:
    accounts_data = await ac_get_all("accounts", "accounts", {})
    records = []
    for acc in accounts_data:
        flat = {"id": acc.get("id")}
        for key, val in acc.items():
            if key != "links" and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    print(f"Fetching custom fields for {len(records)} accounts...")
    for i in range(0, len(records), 500):
        batch = records[i:i+500]
        print(f"  {i}/{len(records)}", end="\r")
        await asyncio.gather(*[_fetch_account_cf(rec) for rec in batch], return_exceptions=True)
    print(f"  ✓ {len(records)} accounts done")
    return records

async def fetch_account_records_basic() -> list:
    accounts_data = await ac_get_all("accounts", "accounts", {})
    records = []
    for acc in accounts_data:
        flat = {"id": acc.get("id")}
        for key, val in acc.items():
            if key != "links" and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    print(f"Fetched {len(records)} accounts (basic)")
    return records

async def _fetch_account_cf(record: dict):
    aid    = record["id"]
    cached = get_cached("account_custom_fields", aid)
    if cached:
        record.update(cached)
        return
    async with semaphore:
        try:
            data = await ac_get(f"accounts/{aid}/accountCustomFieldData")
            cfs  = {}
            for cf in data.get("customerAccountCustomFieldData", []):
                fid = cf.get("custom_field_id")
                val = (cf.get("custom_field_text_value") or cf.get("custom_field_date_value") or
                       cf.get("custom_field_number_value") or cf.get("custom_field_currency_value"))
                if fid and val is not None:
                    key = f"customfield_{fid}"
                    record[key] = val
                    cfs[key]    = val
            set_cached("account_custom_fields", aid, cfs)
        except Exception:
            pass

async def fetch_contact_records() -> list:
    contacts_data = await ac_get_all("contacts", "contacts", {})
    records = []
    for c in contacts_data:
        flat = {"id": c.get("id"), "_account_id": c.get("account")}
        for key, val in c.items():
            if key not in ("links", "fieldValues") and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    print(f"Fetching custom fields for {len(records)} contacts...")
    for i in range(0, len(records), 500):
        await asyncio.gather(*[_fetch_contact_cf(rec) for rec in records[i:i+500]], return_exceptions=True)
    print(f"  ✓ {len(records)} contacts done")
    return records

async def fetch_contact_records_basic() -> list:
    contacts_data = await ac_get_all("contacts", "contacts", {})
    records = []
    for c in contacts_data:
        flat = {"id": c.get("id"), "_account_id": c.get("account")}
        for key, val in c.items():
            if key not in ("links", "fieldValues") and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    print(f"Fetched {len(records)} contacts (basic)")
    return records

async def _fetch_contact_cf(record: dict):
    cid    = record["id"]
    cached = get_cached("contact_custom_fields", cid)
    if cached:
        record.update(cached)
        return
    async with semaphore:
        try:
            data = await ac_get(f"contacts/{cid}/fieldValues")
            cfs  = {}
            for fv in data.get("fieldValues", []):
                fid = fv.get("field")
                val = fv.get("value")
                if fid and val is not None:
                    key = f"customfield_{fid}"
                    record[key] = val
                    cfs[key]    = val
            set_cached("contact_custom_fields", cid, cfs)
        except Exception:
            pass

async def fetch_deal_records() -> list:
    deals_data = await ac_get_all("deals", "deals", {})
    records = []
    for d in deals_data:
        flat = {"id": d.get("id"), "_contact_id": d.get("contact"), "_account_id": d.get("account")}
        for key, val in d.items():
            if key != "links" and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    print(f"Fetching custom fields for {len(records)} deals...")
    for i in range(0, len(records), 500):
        await asyncio.gather(*[_fetch_deal_cf(rec) for rec in records[i:i+500]], return_exceptions=True)
    print(f"  ✓ {len(records)} deals done")
    return records

async def fetch_deal_records_basic() -> list:
    deals_data = await ac_get_all("deals", "deals", {})
    records = []
    for d in deals_data:
        flat = {"id": d.get("id"), "_contact_id": d.get("contact"), "_account_id": d.get("account")}
        for key, val in d.items():
            if key != "links" and not isinstance(val, dict):
                flat[key] = val
        records.append(flat)
    print(f"Fetched {len(records)} deals (basic)")
    return records

async def _fetch_deal_cf(record: dict):
    did    = record["id"]
    cached = get_cached("deal_custom_fields", did)
    if cached:
        record.update(cached)
        return
    async with semaphore:
        try:
            data = await ac_get(f"deals/{did}/dealCustomFieldData")
            cfs  = {}
            for cf in data.get("dealCustomFieldData", []):
                fid = cf.get("customFieldId")
                val = cf.get("fieldValue")
                if fid and val is not None:
                    key = f"customfield_{fid}"
                    record[key] = val
                    cfs[key]    = val
            set_cached("deal_custom_fields", did, cfs)
        except Exception:
            pass


async def fetch_note_records() -> list:
    """Fetch all notes from AC, enriched with contact name/email and account ID."""
    print("Fetching notes...")
    raw = await ac_get_all("notes", "notes", {})

    records      = []
    contact_ids  = set()
    deal_ids     = set()

    for n in raw:
        reltype = (n.get("reltype") or "").lower()
        rec = {
            "id":      n.get("id"),
            "note":    n.get("note", ""),
            "cdate":   n.get("cdate", ""),
            "mdate":   n.get("mdate", ""),
            "userid":  n.get("userid", ""),
            "reltype": n.get("reltype", ""),
            "rel_id":  str(n.get("rel_id", "")),
            "pinned":  str(n.get("pinned", "0")),
            "_contact_id": str(n.get("rel_id", "")) if reltype == "contact" else None,
            "_deal_id":    str(n.get("rel_id", "")) if reltype == "deal"    else None,
            "_account_id": None,
        }
        if reltype == "contact":
            contact_ids.add(str(n.get("rel_id", "")))
        elif reltype == "deal":
            deal_ids.add(str(n.get("rel_id", "")))
        records.append(rec)

    print(f"  {len(records)} notes ({len(contact_ids)} contacts, {len(deal_ids)} deals)")

    # Batch-fetch contacts to get name + their account ID
    contact_map: dict = {}
    async def _fc(cid):
        try:
            async with semaphore:
                d = await ac_get(f"contacts/{cid}")
                c = d.get("contact", {})
                contact_map[cid] = {
                    "firstName":   c.get("firstName", ""),
                    "lastName":    c.get("lastName", ""),
                    "email":       c.get("email", ""),
                    "_account_id": str(c.get("account", "")) if c.get("account") else "",
                }
        except Exception:
            contact_map[cid] = {}

    if contact_ids:
        await asyncio.gather(*[_fc(cid) for cid in contact_ids], return_exceptions=True)

    # Batch-fetch deals to get their account ID
    deal_map: dict = {}
    async def _fd(did):
        try:
            async with semaphore:
                d = await ac_get(f"deals/{did}")
                dl = d.get("deal", {})
                deal_map[did] = {"_account_id": str(dl.get("account", "")) if dl.get("account") else ""}
        except Exception:
            deal_map[did] = {}

    if deal_ids:
        await asyncio.gather(*[_fd(did) for did in deal_ids], return_exceptions=True)

    # Enrich records with contact/account info
    for rec in records:
        cid = rec.pop("_contact_id", None)
        did = rec.pop("_deal_id",    None)
        if cid and cid in contact_map:
            cm = contact_map[cid]
            rec["contact.firstName"] = cm.get("firstName", "")
            rec["contact.lastName"]  = cm.get("lastName", "")
            rec["contact.email"]     = cm.get("email", "")
            rec["_account_id"]       = cm.get("_account_id", "")
        if did and did in deal_map:
            rec["_account_id"] = deal_map[did].get("_account_id", "")

    print(f"  ✓ Notes enriched")
    return records


# ═══════════════════════════════════════════════════════════════════════════
# ENRICHMENT HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _account_id_for(rec: dict, source_type: str) -> str | None:
    if source_type in ("slp", "license_details", "trainings"):
        rel = rec.get("_relationships", {}).get("account", [])
        return str(rel[0]) if isinstance(rel, list) and rel else None
    if source_type in ("contacts", "deals", "notes"):
        v = rec.get("_account_id")
        return str(v) if v else None
    if source_type == "accounts":
        return str(rec.get("id", ""))
    return None


async def enrich_with_accounts(records: list, source_type: str, field_list: list = []) -> list:
    """Merge account fields into records."""
    account_ids = {aid for rec in records if (aid := _account_id_for(rec, source_type))}
    if not account_ids:
        return records

    need_cf = any("account.customfield_" in f for f in field_list)
    print(f"Enriching with {len(account_ids)} accounts (custom_fields={need_cf})...")

    accounts_map: dict = {}
    cf_map: dict       = {}

    # Fetch in parallel with semaphore
    async def _fetch_one(aid):
        try:
            d = await ac_get(f"accounts/{aid}")
            accounts_map[aid] = d.get("account", {})
            if need_cf:
                d2 = await ac_get(f"accounts/{aid}/accountCustomFieldData")
                cf_map[aid] = {}
                for cf in d2.get("customerAccountCustomFieldData", []):
                    fid = cf.get("custom_field_id")
                    val = (cf.get("custom_field_text_value") or cf.get("custom_field_date_value") or
                           cf.get("custom_field_number_value") or cf.get("custom_field_currency_value"))
                    if fid and val is not None:
                        cf_map[aid][fid] = val
        except Exception:
            accounts_map[aid] = {}

    await asyncio.gather(*[_fetch_one(aid) for aid in account_ids], return_exceptions=True)

    for rec in records:
        aid = _account_id_for(rec, source_type)
        if aid and aid in accounts_map:
            for k, v in accounts_map[aid].items():
                if k != "links" and not isinstance(v, dict):
                    rec[f"account.{k}"] = v
            if need_cf and aid in cf_map:
                for fid, val in cf_map[aid].items():
                    rec[f"account.customfield_{fid}"] = val

    for rec in records:
        rec.pop("_relationships", None)
        rec.pop("_account_id",   None)

    return records


async def enrich_with_contacts(records: list, source_type: str) -> list:
    """Add 1:1 contact fields to deal records."""
    contact_ids = set()
    for rec in records:
        if source_type == "deals":
            cid = rec.get("_contact_id")
            if cid:
                contact_ids.add(str(cid))
    if not contact_ids:
        return records

    contacts_map: dict = {}
    async def _fetch(cid):
        try:
            d = await ac_get(f"contacts/{cid}")
            contacts_map[cid] = d.get("contact", {})
        except Exception:
            contacts_map[cid] = {}

    await asyncio.gather(*[_fetch(cid) for cid in contact_ids], return_exceptions=True)

    for rec in records:
        cid = rec.get("_contact_id")
        if cid and str(cid) in contacts_map:
            for k, v in contacts_map[str(cid)].items():
                if k not in ("links", "fieldValues") and not isinstance(v, dict):
                    rec[f"contact.{k}"] = v

    for rec in records:
        rec.pop("_contact_id", None)

    return records


async def enrich_with_contacts_list(records: list, source_type: str) -> list:
    """Add primary contact + contact_count to account/SLP/training records."""
    account_ids = {aid for rec in records if (aid := _account_id_for(rec, source_type))}
    if not account_ids:
        return records

    print(f"Fetching contacts for {len(account_ids)} accounts...")
    all_contacts = await ac_get_all("contacts", "contacts", {})

    by_account: dict = defaultdict(list)
    for c in all_contacts:
        aid = str(c.get("account", ""))
        if aid in account_ids:
            by_account[aid].append(c)

    for rec in records:
        aid      = _account_id_for(rec, source_type)
        contacts = by_account.get(aid, [])
        rec["contact_count"] = len(contacts)
        if contacts:
            p = contacts[0]
            rec["primary_contact.email"]     = p.get("email", "")
            rec["primary_contact.firstName"] = p.get("firstName", "")
            rec["primary_contact.lastName"]  = p.get("lastName", "")
            rec["primary_contact.phone"]     = p.get("phone", "")
        else:
            rec["primary_contact.email"]     = ""
            rec["primary_contact.firstName"] = ""
            rec["primary_contact.lastName"]  = ""
            rec["primary_contact.phone"]     = ""

    return records


async def enrich_with_slp(records: list, source_type: str) -> list:
    """Add SLP fields + count to account/contact records."""
    account_ids = {aid for rec in records if (aid := _account_id_for(rec, source_type))}
    if not account_ids:
        return records

    print(f"Fetching SLP records for {len(account_ids)} accounts...")
    all_slp = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})

    by_account: dict = defaultdict(list)
    for r in all_slp:
        for aid in r.get("relationships", {}).get("account", []):
            aid = str(aid)
            if aid in account_ids:
                by_account[aid].append({fo["id"]: fo.get("value") for fo in r.get("fields", [])})

    for rec in records:
        aid  = _account_id_for(rec, source_type)
        slps = by_account.get(aid, [])
        rec["slp._count"] = len(slps)
        if slps:
            for fid, val in slps[0].items():
                rec[f"slp.{fid}"] = val

    return records


async def enrich_with_deals_summary(records: list, source_type: str) -> list:
    """Add deal count + latest deal info to account/SLP/contact records."""
    account_ids = set()
    contact_ids = set()
    for rec in records:
        if source_type == "contacts":
            cid = rec.get("id")
            if cid:
                contact_ids.add(str(cid))
        else:
            aid = _account_id_for(rec, source_type)
            if aid:
                account_ids.add(aid)

    if not account_ids and not contact_ids:
        return records

    print("Fetching deals for enrichment...")
    all_deals = await ac_get_all("deals", "deals", {})

    by_account: dict = defaultdict(list)
    by_contact: dict = defaultdict(list)
    for d in all_deals:
        aid = str(d.get("account", ""))
        cid = str(d.get("contact", ""))
        if aid in account_ids:
            by_account[aid].append(d)
        if cid in contact_ids:
            by_contact[cid].append(d)

    for rec in records:
        if source_type == "contacts":
            deals = by_contact.get(str(rec.get("id", "")), [])
        else:
            deals = by_account.get(_account_id_for(rec, source_type) or "", [])

        rec["deal_count"] = len(deals)
        if deals:
            latest = max(deals, key=lambda d: d.get("cdate", ""))
            rec["deal.title"]  = latest.get("title", "")
            rec["deal.status"] = latest.get("status", "")
            rec["deal.stage"]  = latest.get("stage", "")
            rec["deal.cdate"]  = latest.get("cdate", "")

    return records


async def enrich_with_trainings_summary(records: list, source_type: str) -> list:
    """Add training count + latest training info to account/SLP records."""
    account_ids = {aid for rec in records if (aid := _account_id_for(rec, source_type))}
    if not account_ids:
        return records

    print("Fetching trainings for enrichment...")
    all_trainings = await ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})

    by_account: dict = defaultdict(list)
    for r in all_trainings:
        for aid in r.get("relationships", {}).get("account", []):
            aid = str(aid)
            if aid in account_ids:
                by_account[aid].append({fo["id"]: fo.get("value") for fo in r.get("fields", [])})

    for rec in records:
        aid       = _account_id_for(rec, source_type)
        trainings = by_account.get(aid, [])
        rec["training_count"] = len(trainings)
        if trainings:
            latest = max(trainings, key=lambda t: t.get("date-of-training", ""))
            rec["training.training-type"]    = latest.get("training-type", "")
            rec["training.training-agenda"]  = latest.get("training-agenda", "")
            rec["training.date-of-training"] = latest.get("date-of-training", "")
            rec["training.trained-by"]       = latest.get("trained-by", "")

    return records


async def enrich_with_notes_summary(records: list, source_type: str) -> list:
    """Add note_count, latest_note_date, latest_note to contact/account/SLP/etc. records."""
    print("Fetching notes for enrichment...")
    all_notes = await ac_get_all("notes", "notes", {})

    if source_type == "contacts":
        # Match notes directly to contact IDs
        entity_ids = {str(rec.get("id", "")) for rec in records if rec.get("id")}
        by_entity: dict = defaultdict(list)
        for n in all_notes:
            if (n.get("reltype") or "").lower() == "contact":
                cid = str(n.get("rel_id", ""))
                if cid in entity_ids:
                    by_entity[cid].append(n)

        for rec in records:
            notes = sorted(by_entity.get(str(rec.get("id", "")), []),
                           key=lambda n: n.get("cdate", ""), reverse=True)
            rec["note_count"]       = len(notes)
            rec["latest_note_date"] = notes[0].get("cdate", "") if notes else ""
            rec["latest_note"]      = (notes[0].get("note", "") or "")[:300] if notes else ""

    else:
        # For accounts / SLP / trainings / license_details:
        # notes belong to contacts → need contact→account map
        account_ids = {aid for rec in records if (aid := _account_id_for(rec, source_type))}
        if not account_ids:
            return records

        all_contacts = await ac_get_all("contacts", "contacts", {})
        contact_to_account: dict = {}
        for c in all_contacts:
            aid = str(c.get("account", ""))
            if aid in account_ids:
                contact_to_account[str(c.get("id", ""))] = aid

        by_account: dict = defaultdict(list)
        for n in all_notes:
            if (n.get("reltype") or "").lower() == "contact":
                cid = str(n.get("rel_id", ""))
                if cid in contact_to_account:
                    by_account[contact_to_account[cid]].append(n)

        for rec in records:
            aid   = _account_id_for(rec, source_type) or ""
            notes = sorted(by_account.get(aid, []),
                           key=lambda n: n.get("cdate", ""), reverse=True)
            rec["note_count"]       = len(notes)
            rec["latest_note_date"] = notes[0].get("cdate", "") if notes else ""
            rec["latest_note"]      = (notes[0].get("note", "") or "")[:300] if notes else ""

    return records


def deduplicate_records(records: list, dedup_field: str) -> list:
    seen: dict  = {}
    deduped = []
    for rec in records:
        key = rec.get(dedup_field)
        if not key:
            deduped.append(rec)
            continue
        if key not in seen:
            seen[key] = len(deduped)
            deduped.append(rec)
        else:
            # Keep the record with the most recent activation date
            existing_idx  = seen[key]
            existing_date = deduped[existing_idx].get("contractor-activated-date", "")
            current_date  = rec.get("contractor-activated-date", "")
            if current_date > existing_date:
                deduped[existing_idx] = rec
    return deduped


# ═══════════════════════════════════════════════════════════════════════════
# MAIN REPORT ENDPOINT
# ═══════════════════════════════════════════════════════════════════════════

RELATED_PREFIXES = ("account.", "contact.", "deal.", "slp.", "training.", "primary_contact.")

@app.get("/api/report")
async def generate_report(
    object_type: str          = Query(...),
    fields:      str          = Query(...),
    filters:     Optional[str]= Query(None),
    dedup_field: Optional[str]= Query(None),
):
    field_list  = [f for f in fields.split(",") if f] if fields else []
    filter_list = json.loads(filters) if filters else []

    print(f"\n{'='*60}\nREPORT: {object_type} | fields={len(field_list)} filters={len(filter_list)}\n{'='*60}")

    # ── Fetch primary records ──
    needs_cf    = any(f.startswith("customfield_") for f in field_list)
    needs_cf_f  = any(f.get("field", "").startswith("customfield_") for f in filter_list)

    if object_type == "slp":
        records = await fetch_slp_records()
    elif object_type == "license_details":
        records = await fetch_license_records()
    elif object_type == "trainings":
        records = await fetch_training_records()
    elif object_type == "accounts":
        records = await fetch_account_records() if (needs_cf or needs_cf_f) else await fetch_account_records_basic()
    elif object_type == "contacts":
        records = await fetch_contact_records() if (needs_cf or needs_cf_f) else await fetch_contact_records_basic()
    elif object_type == "deals":
        records = await fetch_deal_records() if (needs_cf or needs_cf_f) else await fetch_deal_records_basic()
    elif object_type == "notes":
        records = await fetch_note_records()
    else:
        raise HTTPException(status_code=400, detail=f"Unknown object type: {object_type}")

    print(f"Fetched {len(records)} primary records")

    # ── Primary filters ──
    primary_filters = [f for f in filter_list if not any(f.get("field","").startswith(p) for p in RELATED_PREFIXES)]
    related_filters = [f for f in filter_list if     any(f.get("field","").startswith(p) for p in RELATED_PREFIXES)]

    if primary_filters:
        records = [r for r in records if all(evaluate_filter(r, f) for f in primary_filters)]
        print(f"After primary filters: {len(records)}")

    # ── Cross-object enrichment (only fetch what's needed) ──
    fl = set(field_list)
    rf = set(f.get("field", "") for f in related_filters)
    all_fields = fl | rf

    if any(f.startswith("account.") for f in all_fields):
        records = await enrich_with_accounts(records, object_type, field_list)

    if any(f.startswith("contact.") for f in all_fields) and object_type == "deals":
        records = await enrich_with_contacts(records, object_type)

    if any(f.startswith("primary_contact.") or f == "contact_count" for f in all_fields):
        records = await enrich_with_contacts_list(records, object_type)

    if any(f.startswith("slp.") or f == "slp._count" for f in all_fields):
        records = await enrich_with_slp(records, object_type)

    if any(f.startswith("deal.") or f == "deal_count" for f in all_fields):
        records = await enrich_with_deals_summary(records, object_type)

    if any(f.startswith("training.") or f == "training_count" for f in all_fields):
        records = await enrich_with_trainings_summary(records, object_type)

    if any(f in ("note_count", "latest_note", "latest_note_date") for f in all_fields):
        records = await enrich_with_notes_summary(records, object_type)

    # ── Related filters ──
    if related_filters:
        records = [r for r in records if all(evaluate_filter(r, f) for f in related_filters)]
        print(f"After related filters: {len(records)}")

    # ── Dedup ──
    if dedup_field:
        records = deduplicate_records(records, dedup_field)
        print(f"After dedup: {len(records)}")

    # ── Project fields ──
    final = [{fid: r.get(fid, "") for fid in field_list} for r in records]
    print(f"Returning {len(final)} records\n")
    return {"count": len(final), "records": final}


# ═══════════════════════════════════════════════════════════════════════════
# CSV EXPORT
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/csv")
async def export_csv(
    object_type: str          = Query(...),
    fields:      str          = Query(...),
    filters:     Optional[str]= Query(None),
    dedup_field: Optional[str]= Query(None),
):
    result  = await generate_report(object_type, fields, filters, dedup_field)
    records = result["records"]
    if not records:
        raise HTTPException(status_code=404, detail="No records to export")

    filter_list = json.loads(filters) if filters else []
    fields_data = await get_fields(object_type)
    field_labels = {f["id"]: f["label"] for f in fields_data["fields"]}

    output = io.StringIO()
    output.write(f"AC Reporter Export\n")
    output.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
    output.write(f"Object: {object_type.upper()} | Records: {len(records)}\n")

    if filter_list:
        output.write("\nFilters:\n")
        for f in filter_list:
            lbl     = field_labels.get(f.get("field"), f.get("field"))
            dr      = f.get("dateRange")
            display = dr or f"{f.get('operator', '=')} {f.get('value', '')}"
            output.write(f"  - {lbl}: {display}\n")

    if dedup_field:
        output.write(f"\nDedup by: {field_labels.get(dedup_field, dedup_field)}\n")

    output.write("\n" + "=" * 80 + "\n\n")

    if records:
        fieldnames = list(records[0].keys())
        headers    = [field_labels.get(fid, fid) for fid in fieldnames]
        writer = csv.writer(output)
        writer.writerow(headers)
        for rec in records:
            writer.writerow([rec.get(k, "") for k in fieldnames])

    filename = f"report_{object_type}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ═══════════════════════════════════════════════════════════════════════════
# PRE-BUILT REPORT: PARTNER ACTIVATIONS
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/activations")
async def activations_report(
    from_date:        Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date:          Optional[str] = Query(None, description="YYYY-MM-DD"),
    platform:         Optional[str] = Query(None),
    bdr:              Optional[str] = Query(None),
    state:            Optional[str] = Query(None, description="2-letter state abbreviation"),
    exclude_platforms:Optional[str] = Query(None, description="Comma-separated"),
    format:           str           = Query("json"),
):
    """Partner activations: SLP records with Contractor Activated status, joined to accounts."""
    from datetime import timezone
    print("\nActivations report...")
    slp_records = await ac_get_all(
        f"customObjects/records/{SLP_SCHEMA_ID}", "records",
        {"filters[fields.slp-status-detail]": "Contractor Activated"},
    )
    exclude_set = {p.strip() for p in exclude_platforms.split(",")} if exclude_platforms else set()

    account_ids: set = set()
    candidates  = []

    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}

        plat      = str(fields.get("platform", "")).strip()
        plat_norm = _normalize_platform(plat)
        if platform and plat_norm != _normalize_platform(platform):
            continue
        if plat_norm in exclude_set or plat in exclude_set:
            continue

        # Resolve acc_id early so we can fall back to account-level BDR
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None

        slp_bdr = str(fields.get("assigned-bdr", "")).strip()
        eff_bdr = slp_bdr or _account_to_bdr.get(acc_id or "", "")
        if bdr and eff_bdr != bdr:
            continue

        if state:
            states_val = str(fields.get("doing-business-in-states", "") or "").upper()
            if state.upper() not in [s.strip() for s in states_val.split(",")]:
                continue

        act_str = str(fields.get("contractor-activated-date", "")).strip()
        if not act_str:
            continue
        try:
            act_dt = (datetime.fromisoformat(act_str.replace("Z", "+00:00")) if "T" in act_str
                      else datetime.strptime(act_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc))
        except Exception:
            continue

        if from_date and act_dt < datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc):
            continue
        if to_date and act_dt > datetime.strptime(to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc):
            continue

        if acc_id:
            account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id, "slp_id": r.get("id"), "eff_bdr": eff_bdr})

    print(f"  {len(candidates)} candidates")

    # Fetch account data
    acct_cache: dict = {}
    for aid in account_ids:
        try:
            ad  = await ac_get(f"accounts/{aid}")
            acd = await ac_get(f"accounts/{aid}/accountCustomFieldData")
            cfs = {str(cf["custom_field_id"]): cf.get("custom_field_text_value") or ""
                   for cf in acd.get("customerAccountCustomFieldData", [])}
            acct_cache[aid] = {"name": ad.get("account", {}).get("name", ""), "cfs": cfs}
        except Exception:
            acct_cache[aid] = {"name": "", "cfs": {}}

    results = []
    for c in candidates:
        f   = c["fields"]
        acc = acct_cache.get(c["account_id"], {"name": "", "cfs": {}}) if c["account_id"] else {"name": "", "cfs": {}}
        cfs = acc["cfs"]
        results.append({
            "slp_id":                    c["slp_id"],
            "account_id":                c["account_id"],
            "account_name":              acc["name"],
            "dba_name":                  cfs.get(ACCT_FIELD["dba_name"], ""),
            "dealer_id":                 f.get("dealer-id", ""),
            "platform":                  _normalize_platform(f.get("platform", "")),
            "platforms":                 f.get("platforms", ""),
            "slp_status":                f.get("slp-status-detail", ""),
            "contractor_activated_date": f.get("contractor-activated-date", ""),
            "original_owner":            f.get("original-owner", ""),
            "assigned_bdr":              c.get("eff_bdr") or f.get("assigned-bdr", ""),
            "sales_region":              cfs.get(ACCT_FIELD["sales_region"], ""),
            "dealer_program":            _normalize_platform(cfs.get(ACCT_FIELD["dealer_program"], "")),
            "oracle_producer_id":        cfs.get(ACCT_FIELD["oracle_producer_id"], ""),
            "doing_business_in_states":  cfs.get(ACCT_FIELD["doing_business_in"], "") or f.get("doing-business-in-states", ""),
            "ein":                       f.get("ein", ""),
            "contractor_reactivation":   f.get("contractor-reactivation", ""),
        })

    results.sort(key=lambda x: x.get("contractor_activated_date", ""), reverse=True)

    if format == "csv":
        out = io.StringIO()
        if results:
            w = csv.DictWriter(out, fieldnames=results[0].keys())
            w.writeheader(); w.writerows(results)
        fn = f"activations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})
    return {"count": len(results), "records": results}


# ═══════════════════════════════════════════════════════════════════════════
# PRE-BUILT REPORT: LICENSE EXPIRATION
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/license-expiration")
async def license_expiration_report(
    days_ahead:      int            = Query(90),
    include_expired: bool           = Query(True),
    from_date:       Optional[date] = Query(None, description="Filter by expiration date ≥ this date"),
    to_date:         Optional[date] = Query(None, description="Filter by expiration date ≤ this date"),
    preset:          Optional[str]  = Query(None, description="Date preset: this_month | last_month | last_week | etc."),
    format:          str            = Query("json"),
):
    """License records filtered by expiration date. Supports days_ahead (future window) or
    explicit from_date/to_date or a named preset (this_month, last_month, etc.)."""
    from datetime import timezone
    print("\nLicense expiration report...")
    lic_records = await ac_get_all(f"customObjects/records/{LICENSE_SCHEMA_ID}", "records", {})
    now = datetime.now(timezone.utc)

    # Resolve date-range mode vs days-ahead mode
    use_range = bool(preset or from_date or to_date)
    range_start: Optional[date] = None
    range_end:   Optional[date] = None
    if use_range:
        range_start, range_end = _resolve_date_range(from_date, to_date, preset)
    else:
        cutoff = now + timedelta(days=days_ahead)

    account_ids: set = set()
    candidates  = []

    for r in lic_records:
        fields  = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        exp_str = (fields.get("expiration-date") or fields.get("license-expiration-date")
                   or fields.get("expires") or "")
        if not exp_str:
            continue
        try:
            exp_dt = (datetime.fromisoformat(str(exp_str).replace("Z", "+00:00")) if "T" in str(exp_str)
                      else datetime.strptime(str(exp_str)[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc))
        except Exception:
            continue

        is_expired = exp_dt < now
        if use_range:
            # Filter by whether the expiration date falls in the range
            exp_date_only = exp_dt.date()
            if range_start and exp_date_only < range_start:
                continue
            if range_end and exp_date_only > range_end:
                continue
        else:
            if is_expired and not include_expired:
                continue
            if not is_expired and exp_dt > cutoff:
                continue

        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if acc_id:
            account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id, "record_id": r.get("id"),
                           "expiration_date": exp_str, "is_expired": is_expired,
                           "days_until": (exp_dt - now).days})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    results = []
    for c in candidates:
        row = {"record_id": c["record_id"], "account_id": c["account_id"],
               "account_name": acct_cache.get(c["account_id"], ""),
               "expiration_date": c["expiration_date"],
               "days_until_expiration": c["days_until"],
               "status": "EXPIRED" if c["is_expired"] else "EXPIRING"}
        for k, v in c["fields"].items():
            row[f"license.{k}"] = v
        results.append(row)

    results.sort(key=lambda x: x.get("days_until_expiration", 9999))

    if format == "csv":
        out = io.StringIO()
        if results:
            w = csv.DictWriter(out, fieldnames=results[0].keys())
            w.writeheader(); w.writerows(results)
        fn = f"license_expiration_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})
    return {"count": len(results), "records": results}


# ═══════════════════════════════════════════════════════════════════════════
# PRE-BUILT REPORT: BDR SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/bdr-summary")
async def bdr_summary_report(
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date:   Optional[str] = Query(None, description="YYYY-MM-DD"),
    platform:  Optional[str] = Query(None),
    format:    str           = Query("json"),
):
    """Activations, account counts, and platform breakdown per BDR."""
    from datetime import timezone
    print("\nBDR summary report...")
    try:
        slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    except BaseException as _e:
        print(f"[bdr-summary] CAUGHT {type(_e).__name__}: {_e}")
        return JSONResponse(status_code=500, content={"detail": str(_e), "type": type(_e).__name__})

    bdr_data: dict = defaultdict(lambda: {"total_slps": 0, "activated": 0,
                                           "platforms": defaultdict(int), "accounts": set()})
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        plat   = str(fields.get("platform", "")).strip()
        if platform and plat != platform:
            continue
        bdr    = str(fields.get("assigned-bdr", "")).strip() or "Unassigned"
        bdr_data[bdr]["total_slps"] += 1

        if fields.get("slp-status-detail") == "Contractor Activated":
            act_str  = str(fields.get("contractor-activated-date", "")).strip()
            in_range = True
            if act_str and (from_date or to_date):
                try:
                    act_dt = (datetime.fromisoformat(act_str.replace("Z", "+00:00")) if "T" in act_str
                              else datetime.strptime(act_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc))
                    if from_date:
                        in_range = in_range and act_dt >= datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if to_date:
                        in_range = in_range and act_dt <= datetime.strptime(to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                except Exception:
                    in_range = False
            if in_range:
                bdr_data[bdr]["activated"] += 1

        plat = str(fields.get("platform", "")).strip()
        if plat:
            bdr_data[bdr]["platforms"][plat] += 1

        rel = r.get("relationships", {}).get("account", [])
        if rel:
            bdr_data[bdr]["accounts"].add(str(rel[0]))

    results = [
        {"bdr": bdr, "total_slps": d["total_slps"], "activated": d["activated"],
         "account_count": len(d["accounts"]),
         "platforms": ", ".join(f"{k}:{v}" for k, v in sorted(d["platforms"].items()))}
        for bdr, d in sorted(bdr_data.items())
    ]
    results.sort(key=lambda x: x["activated"], reverse=True)

    if format == "csv":
        out = io.StringIO()
        if results:
            w = csv.DictWriter(out, fieldnames=results[0].keys())
            w.writeheader(); w.writerows(results)
        fn = f"bdr_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})
    return {"count": len(results), "records": results}


# ═══════════════════════════════════════════════════════════════════════════
# PRE-BUILT REPORT: TRAINING SUMMARY
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/training-summary")
async def training_summary_report(
    from_date:     Optional[str] = Query(None),
    to_date:       Optional[str] = Query(None),
    trainer:       Optional[str] = Query(None),
    training_type: Optional[str] = Query(None),
    format:        str           = Query("json"),
):
    """Training records joined with account data."""
    from datetime import timezone
    print("\nTraining summary report...")
    training_records = await ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})

    account_ids: set = set()
    candidates  = []

    for r in training_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}

        if trainer       and str(fields.get("trained-by", "")).strip() != trainer:
            continue
        if training_type and str(fields.get("training-type", "")).strip() != training_type:
            continue

        date_str = str(fields.get("date-of-training", "")).strip()
        if date_str and (from_date or to_date):
            try:
                td = (datetime.fromisoformat(date_str.replace("Z", "+00:00")) if "T" in date_str
                      else datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc))
                if from_date and td < datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc):
                    continue
                if to_date and td > datetime.strptime(to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=timezone.utc):
                    continue
            except Exception:
                pass

        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if acc_id:
            account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id, "record_id": r.get("id")})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            ad  = await ac_get(f"accounts/{aid}")
            acd = await ac_get(f"accounts/{aid}/accountCustomFieldData")
            cfs = {str(cf["custom_field_id"]): cf.get("custom_field_text_value") or ""
                   for cf in acd.get("customerAccountCustomFieldData", [])}
            acct_cache[aid] = {"name": ad.get("account", {}).get("name", ""), "cfs": cfs}
        except Exception:
            acct_cache[aid] = {"name": "", "cfs": {}}

    results = []
    for c in candidates:
        f   = c["fields"]
        acc = acct_cache.get(c["account_id"], {"name": "", "cfs": {}}) if c["account_id"] else {"name": "", "cfs": {}}
        results.append({
            "record_id":        c["record_id"],
            "account_id":       c["account_id"],
            "account_name":     acc["name"],
            "dealer_id":        acc["cfs"].get(ACCT_FIELD["dealer_id"], ""),
            "dealer_program":   acc["cfs"].get(ACCT_FIELD["dealer_program"], ""),
            "training_type":    f.get("training-type", ""),
            "training_agenda":  f.get("training-agenda", ""),
            "date_of_training": f.get("date-of-training", ""),
            "trained_by":       f.get("trained-by", ""),
            "training_notes":   f.get("training-notes", ""),
        })

    results.sort(key=lambda x: x.get("date_of_training", ""), reverse=True)

    if format == "csv":
        out = io.StringIO()
        if results:
            w = csv.DictWriter(out, fieldnames=results[0].keys())
            w.writeheader(); w.writerows(results)
        fn = f"training_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})
    return {"count": len(results), "records": results}


# ═══════════════════════════════════════════════════════════════════════════
# PRE-BUILT REPORT: DEALER 360 PROFILE
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/dealer-profile")
async def dealer_profile(
    account_id: Optional[str] = Query(None),
    dealer_id:  Optional[str] = Query(None),
):
    """360° view of one dealer: account + SLPs + contacts + deals + trainings."""
    if not account_id and not dealer_id:
        raise HTTPException(status_code=400, detail="Provide account_id or dealer_id")

    # Resolve dealer_id → account_id
    if not account_id and dealer_id:
        all_accounts = await ac_get_all("accounts", "accounts", {})
        for acc in all_accounts:
            try:
                acd = await ac_get(f"accounts/{acc['id']}/accountCustomFieldData")
                for cf in acd.get("customerAccountCustomFieldData", []):
                    if (str(cf.get("custom_field_id")) == ACCT_FIELD["dealer_id"] and
                            str(cf.get("custom_field_text_value", "")).strip() == str(dealer_id).strip()):
                        account_id = str(acc["id"])
                        break
            except Exception:
                pass
            if account_id:
                break

    if not account_id:
        raise HTTPException(status_code=404, detail="Account not found")

    # Fetch account + contacts in parallel
    acc_data, acc_cf_data, acc_contacts = await asyncio.gather(
        ac_get(f"accounts/{account_id}"),
        ac_get(f"accounts/{account_id}/accountCustomFieldData"),
        ac_get(f"accounts/{account_id}/contacts"),
        return_exceptions=True,
    )

    account   = acc_data.get("account", {})  if isinstance(acc_data, dict)     else {}
    acct_cfs  = {str(cf["custom_field_id"]): cf.get("custom_field_text_value") or ""
                 for cf in (acc_cf_data.get("customerAccountCustomFieldData", []) if isinstance(acc_cf_data, dict) else [])}
    contact_ids = [ac.get("contact") for ac in (acc_contacts.get("accountContacts", []) if isinstance(acc_contacts, dict) else [])]

    # Fetch SLPs, trainings, deals, contacts in parallel
    slp_task      = ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    training_task = ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})
    deal_task     = ac_get_all("deals", "deals", {})

    all_slps, all_trainings, all_deals = await asyncio.gather(slp_task, training_task, deal_task)

    slps      = [{fo["id"]: fo.get("value") for fo in r.get("fields", [])} for r in all_slps
                 if str(account_id) in [str(x) for x in r.get("relationships", {}).get("account", [])]]
    trainings = [{fo["id"]: fo.get("value") for fo in r.get("fields", [])} for r in all_trainings
                 if str(account_id) in [str(x) for x in r.get("relationships", {}).get("account", [])]]
    deals     = [{"id": d.get("id"), "title": d.get("title"), "stage": d.get("stage"), "status": d.get("status")}
                 for d in all_deals if str(d.get("account", "")) == str(account_id)]

    contacts = []
    for cid in contact_ids:
        try:
            cd = await ac_get(f"contacts/{cid}")
            c  = cd.get("contact", {})
            contacts.append({"id": c.get("id"), "email": c.get("email"),
                             "firstName": c.get("firstName"), "lastName": c.get("lastName")})
        except Exception:
            pass

    return {
        "account":  {"id": account_id, "name": account.get("name", ""), "custom_fields": acct_cfs},
        "slps":     slps,
        "contacts": contacts,
        "deals":    deals,
        "trainings":trainings,
        "summary":  {"slp_count": len(slps), "contact_count": len(contacts),
                     "deal_count": len(deals), "training_count": len(trainings)},
    }


# ═══════════════════════════════════════════════════════════════════════════
# PRE-BUILT REPORT: ACCOUNT ACTIVITY
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/account-activity")
async def account_activity_report(
    from_date:     Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date:       Optional[str] = Query(None, description="YYYY-MM-DD"),
    activity_type: Optional[str] = Query(None, description="Filter by activity type"),
    performed_by:  Optional[str] = Query(None, description="Filter by performed-by value"),
    format:        str           = Query("json"),
):
    """Account Activity custom object — counts grouped by type and performed-by."""
    print("\nAccount activity report (custom object)...")

    all_records = await ac_get_all(
        f"customObjects/records/{ACCT_ACTIVITY_SCHEMA_ID}", "records", {}
    )

    from_d = datetime.strptime(from_date, "%Y-%m-%d").date() if from_date else None
    to_d   = datetime.strptime(to_date,   "%Y-%m-%d").date() if to_date   else None

    counts_by_type:   dict = defaultdict(int)
    accounts_by_type: dict = defaultdict(set)
    counts_by_person: dict = defaultdict(int)
    type_by_person:   dict = defaultdict(lambda: defaultdict(int))
    total = 0

    for r in all_records:
        fmap         = {f["id"]: f.get("value") for f in r.get("fields", [])}
        act_type     = (fmap.get("activity-type") or "Unknown").strip()
        act_date     = (fmap.get("activity-date") or "")[:10]
        performed    = (fmap.get("performed-by")  or "").strip()
        account_id   = next(iter(r.get("relationships", {}).get("account", [])), "")

        # Date filter
        if act_date and (from_d or to_d):
            try:
                ad = datetime.strptime(act_date, "%Y-%m-%d").date()
                if from_d and ad < from_d: continue
                if to_d   and ad > to_d:   continue
            except Exception:
                pass

        # Activity type filter
        if activity_type and act_type.lower() != activity_type.lower():
            continue

        # Performed-by filter
        if performed_by and performed_by.lower() not in performed.lower():
            continue

        counts_by_type[act_type] += 1
        accounts_by_type[act_type].add(account_id)
        if performed:
            counts_by_person[performed] += 1
            type_by_person[performed][act_type] += 1
        total += 1

    by_type = sorted(
        [{"activity_type": t, "count": c, "unique_accounts": len(accounts_by_type[t])}
         for t, c in counts_by_type.items()],
        key=lambda x: -x["count"]
    )
    by_person = sorted(
        [{"performed_by": p, "count": c,
          "breakdown": dict(sorted(type_by_person[p].items(), key=lambda x: -x[1]))}
         for p, c in counts_by_person.items()],
        key=lambda x: -x["count"]
    )

    if format == "csv":
        rows = []
        for row in by_person:
            for atype, cnt in row["breakdown"].items():
                rows.append({"performed_by": row["performed_by"],
                             "activity_type": atype, "count": cnt})
        if not rows:
            rows = [{"performed_by": "", "activity_type": t, "count": c}
                    for t, c in counts_by_type.items()]
        out = io.StringIO()
        if rows:
            w = csv.DictWriter(out, fieldnames=rows[0].keys())
            w.writeheader(); w.writerows(rows)
        fn = f"account_activity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})

    return {"total": total, "by_type": by_type, "by_person": by_person}


# ═══════════════════════════════════════════════════════════════════════════
# PRE-BUILT REPORT: TEAM ACTIVITY / PERFORMANCE
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/report/team-activity")
async def team_activity_report(
    from_date: Optional[str] = Query(None, description="YYYY-MM-DD"),
    to_date:   Optional[str] = Query(None, description="YYYY-MM-DD"),
    format:    str           = Query("json"),
):
    """Per-user activity summary combining Notes (reliable author) + Account Activity (performed-by)."""
    from datetime import timezone
    print("\nTeam activity report...")

    users_data, all_notes_raw, all_contacts, all_activity = await asyncio.gather(
        ac_get("users"),
        ac_get_all("notes", "notes", {}),
        ac_get_all("contacts", "contacts", {}),
        ac_get_all(f"customObjects/records/{ACCT_ACTIVITY_SCHEMA_ID}", "records", {}),
    )

    # Build user map: userid → display name
    users: dict = {}
    for u in (users_data.get("users", []) if isinstance(users_data, dict) else []):
        uid  = str(u.get("id", ""))
        name = f"{u.get('firstName','').strip()} {u.get('lastName','').strip()}".strip()
        users[uid] = name or u.get("email", f"User {uid}")

    # Try to match a free-text performed-by value to a known user name
    def match_user(val: str) -> Optional[str]:
        if not val: return None
        v = val.strip().lower()
        for uid, name in users.items():
            if name.lower() == v: return uid           # exact match
        if len(v) == 2 and v.isalpha():                # initials e.g. "TB"
            for uid, name in users.items():
                parts = name.split()
                if (len(parts) >= 2
                        and parts[0][:1].lower() == v[0]
                        and parts[-1][:1].lower() == v[1]):
                    return uid
        for uid, name in users.items():                # first-name or contains
            parts = name.split()
            if parts and parts[0].lower() == v: return uid
            if v in name.lower(): return uid
        return None

    contact_to_account: dict = {}
    for c in all_contacts:
        cid = str(c.get("id", ""))
        aid = str(c.get("account", "") or "")
        if aid and aid != "0":
            contact_to_account[cid] = aid

    # account_id → owner user_id — use pre-built startup index (no extra API call)
    account_owner = _account_to_owner

    from_dt = (datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
               if from_date else None)
    to_dt   = (datetime.strptime(to_date, "%Y-%m-%d").replace(
                   hour=23, minute=59, second=59, tzinfo=timezone.utc)
               if to_date else None)
    from_d  = from_dt.date() if from_dt else None
    to_d    = to_dt.date()   if to_dt   else None

    user_stats: dict = defaultdict(lambda: {
        "notes": 0, "activities": 0, "accounts": set(), "latest_date": ""
    })

    # ── Notes (reliable author via userid) ───────────────────────────────
    for n in all_notes_raw:
        reltype = (n.get("reltype") or "").lower()
        if reltype not in ("contact", "customeraccount", "deal"):
            continue
        raw_date = n.get("cdate", "")
        if from_dt or to_dt:
            try:
                nd = (datetime.fromisoformat(raw_date.replace("Z", "+00:00")) if "T" in raw_date
                      else datetime.strptime(raw_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc))
                if from_dt and nd < from_dt: continue
                if to_dt   and nd > to_dt:   continue
            except Exception:
                continue
        uid = str(n.get("userid", "") or "")
        if not uid: continue
        cid = str(n.get("rel_id", "") or "")
        aid = contact_to_account.get(cid, "")
        s   = user_stats[uid]
        s["notes"] += 1
        if aid: s["accounts"].add(aid)
        if raw_date > s["latest_date"]: s["latest_date"] = raw_date

    # ── Account Activity (performed-by, fuzzy-matched to users) ──────────
    unmatched_activity: dict = defaultdict(int)   # raw performed-by → count
    for r in all_activity:
        fmap      = {f["id"]: f.get("value") for f in r.get("fields", [])}
        act_date  = (fmap.get("activity-date") or "")[:10]
        performed = (fmap.get("performed-by")  or "").strip()
        account_id = next(iter(r.get("relationships", {}).get("account", [])), "")

        if act_date and (from_d or to_d):
            try:
                ad = datetime.strptime(act_date, "%Y-%m-%d").date()
                if from_d and ad < from_d: continue
                if to_d   and ad > to_d:   continue
            except Exception:
                pass

        uid = match_user(performed)
        if not uid and account_id:
            # Fall back to the account's owner
            uid = account_owner.get(account_id)
        if uid:
            s = user_stats[uid]
            s["activities"] += 1
            if account_id: s["accounts"].add(account_id)
            if act_date and act_date > s["latest_date"][:10]:
                s["latest_date"] = act_date
        elif performed:
            unmatched_activity[performed] += 1

    # Build result rows
    user_rows = []
    all_uids  = set(user_stats.keys()) | set(users.keys())
    for uid in all_uids:
        s = user_stats.get(uid, {"notes": 0, "activities": 0, "accounts": set(), "latest_date": ""})
        total = s["notes"] + s["activities"]
        if total == 0: continue
        user_rows.append({
            "user_name":            users.get(uid, f"User {uid}"),
            "notes_written":        s["notes"],
            "activities_logged":    s["activities"],
            "total_actions":        total,
            "accounts_touched":     len(s["accounts"]),
            "latest_activity_date": s["latest_date"][:10] if s["latest_date"] else "",
        })
    user_rows.sort(key=lambda x: -x["total_actions"])

    # Unmatched performed-by values (couldn't tie to a user)
    unmatched = sorted(
        [{"performed_by": k, "activity_count": v} for k, v in unmatched_activity.items()],
        key=lambda x: -x["activity_count"]
    )

    if format == "csv":
        out = io.StringIO()
        if user_rows:
            w = csv.DictWriter(out, fieldnames=user_rows[0].keys())
            w.writeheader(); w.writerows(user_rows)
        fn = f"team_activity_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})

    return {"count": len(user_rows), "records": user_rows, "unmatched_activity": unmatched}


# ═══════════════════════════════════════════════════════════════════════════
# ACCOUNT BROWSER
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/accounts/search")
async def accounts_search(q: str = Query(""), limit: int = Query(20)):
    """List or search accounts. Empty q returns alphabetical accounts.
    Numeric queries also check the dealer-id index for direct dealer ID lookups."""
    q = q.strip()
    is_numeric = q.isdigit()

    # ── Numeric query: check dealer-id index first ─────────────────────────
    if is_numeric and q in _dealer_id_index:
        entry = _dealer_id_index[q]
        return {
            "accounts": [{"id": entry["id"], "name": entry["name"], "dealer_id": q}],
            "total": 1,
        }

    # ── Normal AC name search ──────────────────────────────────────────────
    params = {"limit": limit, "orders[name]": "ASC"}
    if q:
        params["search"] = q
    data = await ac_get("accounts", params)
    raw  = data.get("accounts", [])

    # Resolve dealer IDs: check in-memory index before making API calls
    dealer_id_field = ACCT_FIELD["dealer_id"]   # "18"

    async def get_dealer_id(account_id: str) -> str:
        aid = str(account_id)
        if aid in _account_to_dealer:
            return _account_to_dealer[aid]
        try:
            cf_data = await ac_get(f"accounts/{aid}/accountCustomFieldData")
            for cf in cf_data.get("customerAccountCustomFieldData", []):
                if str(cf.get("custom_field_id")) == dealer_id_field:
                    return cf.get("custom_field_text_value") or ""
        except Exception:
            pass
        return ""

    dealer_ids = await asyncio.gather(*[get_dealer_id(a.get("id", "")) for a in raw])

    accounts = [
        {"id": a.get("id"), "name": a.get("name", ""), "dealer_id": dealer_ids[i]}
        for i, a in enumerate(raw)
    ]
    return {"accounts": accounts, "total": len(accounts)}


@app.get("/api/accounts/{account_id}/detail")
async def account_detail(account_id: str):
    """Fast 360° account view — fetches data scoped to this account only."""

    # Stage 1: account core data + custom fields + contacts + notes (parallel)
    acc_data, acc_cf_data, acc_contacts, acc_notes_data, cf_meta = await asyncio.gather(
        ac_get(f"accounts/{account_id}"),
        ac_get(f"accounts/{account_id}/accountCustomFieldData"),
        ac_get(f"accounts/{account_id}/contacts"),
        ac_get("notes", {"reltype": "account", "rel_id": account_id, "limit": 25}),
        _get_account_cf_meta(),
        return_exceptions=True,
    )

    account = acc_data.get("account", {}) if isinstance(acc_data, dict) else {}

    # Build named custom field map — use AC labels, read all value types
    named_cfs = {}
    if isinstance(acc_cf_data, dict) and isinstance(cf_meta, dict):
        for cf in acc_cf_data.get("customerAccountCustomFieldData", []):
            fid = str(cf.get("custom_field_id", ""))
            val = _extract_cf_value(cf)
            if val:
                label = cf_meta.get(fid, f"field_{fid}")
                named_cfs[label] = val

    # Dealer ID drives SLP lookup (field 18 = "Parent Dealer ID")
    dealer_id = named_cfs.get("Parent Dealer ID", "")

    contact_ids = []
    if isinstance(acc_contacts, dict):
        contact_ids = [ac.get("contact") for ac in acc_contacts.get("accountContacts", [])]

    notes = []
    if isinstance(acc_notes_data, dict):
        for n in acc_notes_data.get("notes", []):
            notes.append({
                "id":    n.get("id"),
                "note":  n.get("note", "")[:300],
                "cdate": n.get("cdate", ""),
            })

    # Stage 2: SLPs (filtered by dealer ID), contacts (by ID), alternate contacts
    slp_params     = {"filters[fields.dealer-id]": dealer_id, "limit": 100} if dealer_id else {"limit": 0}
    slp_task       = ac_get(f"customObjects/records/{SLP_SCHEMA_ID}", slp_params)
    deal_task      = ac_get("deals", {"filters[account]": account_id, "limit": 50})
    alt_con_task   = ac_get(f"customObjects/records/{ALT_CONTACT_SCHEMA_ID}",
                            {"filters[relationships.account]": account_id, "limit": 50})

    slp_r, deal_r, alt_con_r = await asyncio.gather(slp_task, deal_task, alt_con_task, return_exceptions=True)

    def flatten_co(records):
        seen_ids, result = set(), []
        for r in records:
            rid = r.get("id")
            if rid in seen_ids:
                continue
            seen_ids.add(rid)
            row = {"record_id": rid}
            for f in r.get("fields", []):
                row[f.get("id", "")] = f.get("value", "")
            result.append(row)
        return result

    slps  = flatten_co((slp_r.get("records", []) if isinstance(slp_r, dict) else []))

    alt_contacts = []
    if isinstance(alt_con_r, dict):
        for r in alt_con_r.get("records", []):
            fmap = {f.get("id"): f.get("value") for f in r.get("fields", [])}
            alt_contacts.append({
                "id":            r.get("id"),
                "firstName":     fmap.get("name", ""),
                "lastName":      fmap.get("last-name", ""),
                "phone":         fmap.get("phone-number", ""),
                "contact_status": fmap.get("contact-status", ""),
            })
    deals = []
    if isinstance(deal_r, dict):
        for d in deal_r.get("deals", []):
            deals.append({
                "id":     d.get("id"),
                "title":  d.get("title", ""),
                "value":  d.get("value", "0"),
                "status": d.get("status", ""),
                "stage":  d.get("stage", ""),
                "cdate":  d.get("cdate", ""),
            })

    # Fetch contacts
    contacts = []
    if contact_ids:
        contact_tasks = [ac_get(f"contacts/{cid}") for cid in contact_ids[:15]]
        contact_results = await asyncio.gather(*contact_tasks, return_exceptions=True)
        for cr in contact_results:
            if isinstance(cr, dict):
                c = cr.get("contact", {})
                contacts.append({
                    "id":        c.get("id"),
                    "firstName": c.get("firstName", ""),
                    "lastName":  c.get("lastName", ""),
                    "email":     c.get("email", ""),
                    "phone":     c.get("phone", ""),
                })

    return {
        "account": {
            "id":      account_id,
            "name":    account.get("name", ""),
            "url":     account.get("accountUrl", ""),
            "owner":   account.get("owner", ""),
            "created": account.get("created_utc_timestamp", ""),
            "updated": account.get("updated_utc_timestamp", ""),
            "fields":  named_cfs,
            "ac_url":  ac_account_url(account_id),
        },
        "slps":          slps,
        "contacts":      contacts,
        "alt_contacts":  alt_contacts,
        "deals":         deals,
        "summary": {
            "slp_count":     len(slps),
            "contact_count": len(contacts),
            "deal_count":    len(deals),
        },
    }


# ═══════════════════════════════════════════════════════════════════════════
# ACCOUNT NOTES  (Account Activity custom object)
# ═══════════════════════════════════════════════════════════════════════════

class _NoteIn(BaseModel := __import__('pydantic').BaseModel):
    subject:       str
    body:          str
    activity_type: str = "Internal Note"   # Internal Note | Call | Email | Text

@app.post("/api/accounts/{account_id}/notes")
async def create_account_note(account_id: str, note: _NoteIn, request: _Request,
                               user=Depends(require_auth)):
    """Create an Account Activity (note) record linked to an account."""
    from datetime import timezone
    performed_by = _get_session_email(request) or user or "Microf Reports"
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    payload = {
        "record": {
            "fields": [
                {"id": "activity-type",  "value": note.activity_type},
                {"id": "subject",        "value": note.subject},
                {"id": "body",           "value": note.body},
                {"id": "activity-date",  "value": now_iso},
                {"id": "performed-by",   "value": performed_by},
                {"id": "source",         "value": "Microf Reports"},
            ],
            "relationships": {"account": [int(account_id)]},
        }
    }
    data = await ac_post(
        f"customObjects/records/{ACCT_ACTIVITY_SCHEMA_ID}", payload
    )
    return {"ok": True, "record": data.get("record", {})}


@app.get("/api/accounts/{account_id}/notes")
async def get_account_notes(account_id: str, user=Depends(require_auth)):
    """Fetch Account Activity records linked to an account."""
    all_records = await ac_get_all(
        f"customObjects/records/{ACCT_ACTIVITY_SCHEMA_ID}", "records",
        {}
    )
    results = []
    for r in all_records:
        rels = r.get("relationships", {}).get("account", [])
        if str(account_id) not in [str(x) for x in rels]:
            continue
        fields = {f["id"]: f.get("value", "") for f in r.get("fields", [])}
        results.append({
            "id":            r.get("id"),
            "activity_type": fields.get("activity-type", ""),
            "subject":       fields.get("subject", ""),
            "body":          fields.get("body", ""),
            "activity_date": fields.get("activity-date", ""),
            "performed_by":  fields.get("performed-by", ""),
        })
    results.sort(key=lambda x: x.get("activity_date", ""), reverse=True)
    return {"notes": results}


# ═══════════════════════════════════════════════════════════════════════════
# GLOBAL SEARCH
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/global-search")
async def global_search(q: str = Query(..., min_length=1),
                        program: Optional[str] = Query(None)):
    """Search accounts (by name), contacts (by email only, text queries), and SLPs (by dealer ID or name)."""
    q = q.strip()

    # Normalize phone-like queries: strip dashes, spaces, dots, parens so
    # "225-681-1638" → "2256811638" matches how AC stores phone numbers.
    import re as _re
    q_digits = _re.sub(r"[\s\-().+]", "", q)
    is_phone_like = (not q.isdigit()) and q_digits.isdigit() and len(q_digits) >= 7
    if is_phone_like:
        q = q_digits   # search AC with the stripped version

    # ── In-memory intersection search (program filter or multi-term query) ────
    # When a program param is supplied, or when the query embeds a known program
    # name (e.g. "ARS 360 Finance"), use _program_to_accounts + _account_to_name
    # for name matching instead of the AC API so we don't hit the limit-50 cap
    # or miss accounts whose names don't come back from the AC search.
    words = q.split()

    # Determine which program key to use
    _use_program_key: str | None = None
    if program and _program_to_accounts:
        _use_program_key = program.lower().strip()
        if _use_program_key not in _program_to_accounts:
            _use_program_key = None   # unknown program — fall through to AC search
    elif len(words) >= 2 and _program_to_accounts:
        q_lower = q.lower()
        _use_program_key = max(
            (pk for pk in _program_to_accounts if pk in q_lower),
            key=len, default=None
        )
        if _use_program_key is None:
            for w in words:
                if w.lower() in _program_to_accounts:
                    _use_program_key = w.lower()
                    break

    if _use_program_key and _account_to_name:
        # Name terms = query minus the program token (if it was embedded in query)
        q_lower = q.lower()
        if _use_program_key in q_lower:
            name_part  = q_lower.replace(_use_program_key, "").strip()
        else:
            name_part  = q_lower   # program came from ?program= param, full query is name
        name_terms = name_part.split() if name_part else []

        prog_ids: set = set(_program_to_accounts[_use_program_key])
        if name_terms:
            final_ids = {
                aid for aid in prog_ids
                if all(nt in _account_to_name.get(aid, "").lower() for nt in name_terms)
            }
        else:
            final_ids = prog_ids

        accounts_out = []
        for aid in sorted(final_ids):
            accounts_out.append({
                "id":             aid,
                "name":           _account_to_name.get(aid, ""),
                "dealer_id":      _account_to_dealer.get(aid, ""),
                "dealer_program": _account_to_platform.get(aid, ""),
                "account_url":    ac_account_url(aid),
                "matched_on":     "dealer program + name",
            })
        accounts_out.sort(key=lambda x: x["name"].lower())
        return {"accounts": accounts_out, "slps": [], "contacts": [], "query": q, "total": len(accounts_out)}

    is_numeric = q.isdigit()

    # For numeric queries:
    #   - Short  (< 7 digits): likely a dealer ID — skip contacts to avoid phone-digit noise
    #   - Longer (≥ 7 digits): likely a phone number — include contacts so the search works
    search_contacts = (not is_numeric) or (is_numeric and len(q) >= 7)

    # SLP filter: exact dealer-id for numbers, name contains for text
    if is_numeric:
        slp_params = {"filters[fields.dealer-id]": q, "limit": 50}
    else:
        slp_params = {"filters[fields.name][contains]": q, "limit": 50}

    tasks = [
        ac_get("accounts", {"search": q, "limit": 50}),
        ac_get(f"customObjects/records/{SLP_SCHEMA_ID}", slp_params),
    ]
    if search_contacts:
        tasks.append(ac_get("contacts", {"search": q, "limit": 50}))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    acc_data = results[0]
    slp_data = results[1]
    con_data = results[2] if search_contacts else {}

    # ── SLP records ───────────────────────────────────────────────────────
    matched_slps = []
    for r in ([] if isinstance(slp_data, Exception) else slp_data.get("records", [])):
        fmap       = {f.get("id"): f.get("value") for f in r.get("fields", [])}
        account_id = next(iter(r.get("relationships", {}).get("account", [])), "")
        matched_slps.append({
            "record_id":      r.get("id"),
            "dealer_id":      fmap.get("dealer-id", ""),
            "platform":       fmap.get("platform", ""),
            "account_id":     account_id,
            "account_url":    ac_account_url(account_id),
            "slp_status":     fmap.get("slp-status-detail", ""),
            "activated_date": str(fmap.get("contractor-activated-date", ""))[:10],
            "oracle_ids":     fmap.get("oracle-producer-ids", ""),
            "assigned_bdr":   fmap.get("assigned-bdr", ""),
        })

    # ── Contacts (text queries only) ───────────────────────────────────────
    matched_contacts = []
    if not isinstance(con_data, Exception):
        for c in con_data.get("contacts", []):
            aid = c.get("account", "")
            cid = c.get("id", "")
            matched_contacts.append({
                "id":           cid,
                "name":         f"{c.get('firstName','')} {c.get('lastName','')}".strip(),
                "email":        c.get("email", ""),
                "account_id":   aid,
                "account_url":  ac_account_url(aid),
                "contact_url":  ac_contact_url(cid),
            })

    # ── Accounts: name search results + index lookup + accounts linked to matched SLPs ───
    seen_account_ids: set = set()
    matched_accounts      = []

    # ① Dealer ID index lookup for numeric queries (fastest path — hits cache, no API call)
    if is_numeric and q in _dealer_id_index:
        entry = _dealer_id_index[q]
        aid   = str(entry["id"])
        seen_account_ids.add(aid)
        matched_accounts.append({
            "id":          aid,
            "name":        entry["name"],
            "dealer_id":   q,
            "matched_on":  "dealer id",
            "account_url": ac_account_url(aid),
        })

    # Name-search results — fetch dealer ID for each in parallel
    raw_accounts = [] if isinstance(acc_data, Exception) else acc_data.get("accounts", [])
    dealer_id_field = ACCT_FIELD["dealer_id"]

    async def fetch_dealer_id(account_id: str) -> str:
        # Check in-memory index first (fast, no API call)
        if account_id in _account_to_dealer:
            return _account_to_dealer[account_id]
        try:
            cf = await ac_get(f"accounts/{account_id}/accountCustomFieldData")
            for f in cf.get("customerAccountCustomFieldData", []):
                if str(f.get("custom_field_id")) == dealer_id_field:
                    return f.get("custom_field_text_value") or ""
        except Exception:
            pass
        return ""

    dealer_ids = await asyncio.gather(*[fetch_dealer_id(str(a.get("id",""))) for a in raw_accounts])

    for a, did in zip(raw_accounts, dealer_ids):
        aid = str(a.get("id", ""))
        seen_account_ids.add(aid)
        matched_accounts.append({
            "id":          aid,
            "name":        a.get("name", ""),
            "dealer_id":   did,
            "matched_on":  "name",
            "account_url": ac_account_url(aid),
        })

    # Accounts linked to matched SLPs (we already know their dealer_id from the SLP)
    for slp in matched_slps:
        aid = str(slp["account_id"])
        if not aid or aid in seen_account_ids:
            continue
        seen_account_ids.add(aid)
        matched_accounts.append({
            "id":          aid,
            "name":        "",           # filled in below
            "dealer_id":   slp["dealer_id"],
            "matched_on":  "dealer id",
            "account_url": slp["account_url"],
            "_needs_name": True,
        })

    # Accounts linked to matched contacts
    for con in matched_contacts:
        aid = str(con["account_id"])
        if not aid or aid in seen_account_ids:
            continue
        seen_account_ids.add(aid)
        matched_accounts.append({
            "id":          aid,
            "name":        "",
            "dealer_id":   "",
            "matched_on":  "contact",
            "account_url": con["account_url"],
            "_needs_name": True,
        })

    # Fetch names for SLP/contact-linked accounts we don't have yet
    needs_name = [a for a in matched_accounts if a.get("_needs_name")]
    if needs_name:
        name_results = await asyncio.gather(
            *[ac_get(f"accounts/{a['id']}") for a in needs_name],
            return_exceptions=True,
        )
        for acct, res in zip(needs_name, name_results):
            acct.pop("_needs_name", None)
            if not isinstance(res, Exception):
                acct["name"] = res.get("account", {}).get("name", "")
    for a in matched_accounts:
        a.pop("_needs_name", None)

    # If a numeric query returned nothing and the index hasn't finished building yet,
    # flag it so the UI can show a helpful "still loading" message instead of "no results".
    index_loading = False
    if is_numeric and not matched_accounts and _dealer_index_ts == 0:
        index_loading = True

    # ── Program filter: keep only accounts whose SLP platform matches ─────────
    if program:
        prog_lower = program.lower()
        # Build set of account IDs that have a matching SLP platform
        prog_account_ids: set = set()
        for slp in matched_slps:
            if (slp.get("platform") or "").lower() == prog_lower:
                prog_account_ids.add(str(slp["account_id"]))
        # Also check in-memory index for accounts not yet in matched_slps
        for aid, plat in _account_to_platform.items():
            if plat.lower() == prog_lower:
                prog_account_ids.add(str(aid))
        matched_accounts = [a for a in matched_accounts if str(a["id"]) in prog_account_ids]
        matched_slps     = [s for s in matched_slps     if (s.get("platform") or "").lower() == prog_lower]
        matched_contacts = [c for c in matched_contacts if str(c.get("account_id","")) in prog_account_ids]

    total = len(matched_accounts) + len(matched_contacts) + len(matched_slps)
    return {
        "query":         q,
        "total":         total,
        "accounts":      matched_accounts,
        "contacts":      matched_contacts,
        "slps":          matched_slps,
        "index_loading": index_loading,
    }


@app.get("/api/global-search/export")
async def global_search_export(q: str = Query(default=" "),
                               program: Optional[str] = Query(None)):
    """Export global search results as CSV using the same in-memory data as the sidebar."""
    effective_q = q.strip() or " "
    search_data = await global_search(q=effective_q, program=program)

    matched_accounts = {str(a["id"]): a for a in search_data.get("accounts", [])}
    if not matched_accounts:
        fname = f"search_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return _csv_response([], fname)

    # Group already-filtered SLPs by account_id (same data the sidebar shows)
    slps_by_account: dict = defaultdict(list)
    for s in search_data.get("slps", []):
        slps_by_account[str(s.get("account_id", ""))].append(s)

    rows = []
    for aid, acct in matched_accounts.items():
        acct_name = acct.get("name") or _account_to_name.get(aid, "")
        slp_list  = slps_by_account.get(aid, [])

        if slp_list:
            for s in slp_list:
                rows.append({
                    "account_name":   acct_name,
                    "account_id":     aid,
                    "dealer_id":      s.get("dealer_id", ""),
                    "dealer_program": s.get("platform", ""),
                    "slp_status":     s.get("slp_status", ""),
                    "activated_date": str(s.get("activated_date", ""))[:10],
                    "oracle_ids":     s.get("oracle_ids", ""),
                    "assigned_bdr":   s.get("assigned_bdr", ""),
                })
        else:
            # Account matched (by name or SLP) but no SLP data in index — still include it
            rows.append({
                "account_name":   acct_name,
                "account_id":     aid,
                "dealer_id":      acct.get("dealer_id", ""),
                "dealer_program": _account_to_platform.get(aid, ""),
                "slp_status":     "",
                "activated_date": "",
                "oracle_ids":     "",
                "assigned_bdr":   "",
            })

    rows.sort(key=lambda x: x["account_name"].lower())
    fname = f"search_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return _csv_response(rows, fname)


@app.get("/api/global-search/export-contacts")
async def global_search_export_contacts(q: str = Query(default=" "),
                                        program: Optional[str] = Query(None)):
    """Export all contacts for the matched accounts as CSV."""
    effective_q = q.strip() or " "
    search_data = await global_search(q=effective_q, program=program)

    matched_accounts = {str(a["id"]): a for a in search_data.get("accounts", [])}
    if not matched_accounts:
        fname = f"contacts_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return _csv_response([], fname)

    # Fetch contacts for each matched account in parallel
    con_tasks = [
        ac_get("contacts", {"filters[account]": aid, "limit": 100})
        for aid in matched_accounts
    ]
    con_results = await asyncio.gather(*con_tasks, return_exceptions=True)

    rows = []
    for aid, con_resp in zip(matched_accounts.keys(), con_results):
        acct = matched_accounts[aid]
        acct_name = acct.get("name") or _account_to_name.get(aid, "")
        if isinstance(con_resp, dict):
            for c in con_resp.get("contacts", []):
                rows.append({
                    "account_name": acct_name,
                    "account_id":   aid,
                    "first_name":   c.get("firstName", ""),
                    "last_name":    c.get("lastName", ""),
                    "email":        c.get("email", ""),
                    "phone":        c.get("phone", ""),
                })

    rows.sort(key=lambda x: (x["account_name"].lower(), x["last_name"].lower()))
    fname = f"contacts_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    return _csv_response(rows, fname)


@app.post("/api/global-search/email")
async def global_search_email(
    recipients:  str          = Query(..., description="Comma-separated email addresses"),
    q:           str          = Query(default=" "),
    program:     Optional[str]= Query(None),
    report_type: str          = Query(default="accounts", description="accounts or contacts"),
):
    """Generate a search export CSV and email it via Gmail SMTP."""
    if not _SMTP_USER or not _SMTP_PASS:
        raise HTTPException(status_code=503, detail="Email not configured (SMTP_USER / SMTP_PASS missing)")

    to_list = [r.strip() for r in recipients.split(",") if r.strip()]
    if not to_list:
        raise HTTPException(status_code=400, detail="No valid recipients provided")

    effective_q = q.strip() or " "

    # Generate CSV using existing export logic
    if report_type == "contacts":
        resp        = await global_search_export_contacts(q=effective_q, program=program)
        fname       = f"contacts_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        subject_tag = "Contacts"
    else:
        resp        = await global_search_export(q=effective_q, program=program)
        fname       = f"accounts_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        subject_tag = "Accounts"

    csv_bytes = b"".join([chunk async for chunk in resp.body_iterator])

    label   = f'"{effective_q.strip()}"' + (f" · {program}" if program else "")
    subject = f"Microf Reports Export — {subject_tag} {label}"

    msg = MIMEMultipart()
    msg["From"]    = f"Microf Reports <{_SMTP_USER}>"
    msg["To"]      = ", ".join(to_list)
    msg["Subject"] = subject
    msg.attach(MIMEText(
        f"<p>Please find the attached {subject_tag.lower()} export for search: "
        f"<strong>{label}</strong>.</p>"
        f"<p style='color:#6b7280;font-size:0.85em;'>Sent from microf-search</p>",
        "html"
    ))

    part = MIMEBase("application", "octet-stream")
    part.set_payload(csv_bytes)
    _enc.encode_base64(part)
    part.add_header("Content-Disposition", f'attachment; filename="{fname}"')
    msg.attach(part)

    await aiosmtplib.send(
        msg,
        hostname="smtp.gmail.com",
        port=587,
        start_tls=True,
        username=_SMTP_USER,
        password=_SMTP_PASS,
    )

    return {"ok": True, "to": to_list, "filename": fname}


# ═══════════════════════════════════════════════════════════════════════════
# ADMIN / SCHEDULER
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/me")
async def get_me(request: _Request):
    email = _get_session_email(request)
    is_admin = (not _AZ_CLIENT_ID) or (email and email.lower() in _ADMIN_EMAILS)
    return {"email": email or "anonymous", "is_admin": bool(is_admin)}


@app.get("/api/admin/schedules")
async def list_schedules(admin=Depends(_require_admin)):
    return {"schedules": list(_schedules.values())}


@app.post("/api/admin/schedules")
async def create_schedule(
    report_type:  str          = Query(...),
    frequency:    str          = Query(..., description="daily | weekly | monthly"),
    hour:         int          = Query(9),
    minute:       int          = Query(0),
    day_of_week:  Optional[str]= Query(None, description="mon-sun for weekly"),
    day_of_month: Optional[int]= Query(None, description="1-28 for monthly"),
    recipients:   str          = Query(..., description="Comma-separated emails"),
    label:        Optional[str]= Query(None),
    period:       Optional[str]= Query(None, description="Date preset applied at run time: yesterday | last_week | this_month | last_month | last_quarter | ytd"),
    admin=Depends(_require_admin),
):
    if report_type not in _REPORT_JOBS:
        raise HTTPException(400, f"Unknown report type. Valid: {list(_REPORT_JOBS)}")
    if frequency not in ("daily", "weekly", "monthly"):
        raise HTTPException(400, "frequency must be daily, weekly, or monthly")

    job_id = str(_uuid.uuid4())[:8]
    s = {
        "id":           job_id,
        "report_type":  report_type,
        "frequency":    frequency,
        "hour":         hour,
        "minute":       minute,
        "day_of_week":  day_of_week or "mon",
        "day_of_month": day_of_month or 1,
        "recipients":   [r.strip() for r in recipients.split(",") if r.strip()],
        "label":        label or report_type,
        "period":       period or "",
        "created_at":   datetime.now().isoformat(),
    }
    _register_schedule(s)
    return {"ok": True, "schedule": s}


@app.delete("/api/admin/schedules/{job_id}")
async def delete_schedule(job_id: str, admin=Depends(_require_admin)):
    if job_id not in _schedules:
        raise HTTPException(404, "Schedule not found")
    try:
        _scheduler.remove_job(job_id)
    except Exception:
        pass
    del _schedules[job_id]
    _save_schedules_to_disk()
    return {"ok": True}


# ═══════════════════════════════════════════════════════════════════════════
# GROUP-BY / SUMMARY ANALYTICS
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/api/summary")
async def group_by_summary(
    object_type:  str           = Query(...),
    group_by:     str           = Query(...),
    filters:      Optional[str] = Query(None),
    count_field:  Optional[str] = Query(None, description="Count distinct values of this field"),
):
    """Count records grouped by any field. Supports filters."""
    filter_list = json.loads(filters) if filters else []

    fetchers = {
        "slp":             fetch_slp_records,
        "trainings":       fetch_training_records,
        "license_details": fetch_license_records,
        "accounts":        fetch_account_records_basic,
        "contacts":        fetch_contact_records_basic,
        "deals":           fetch_deal_records_basic,
        "notes":           fetch_note_records,
    }
    if object_type not in fetchers:
        raise HTTPException(status_code=400, detail=f"Unknown object type: {object_type}")

    records = await fetchers[object_type]()
    if filter_list:
        records = [r for r in records if all(evaluate_filter(r, f) for f in filter_list)]

    groups: dict = defaultdict(lambda: {"count": 0, "distinct": set()})
    for r in records:
        key = str(r.get(group_by) or "(empty)")
        groups[key]["count"] += 1
        if count_field and (v := r.get(count_field)):
            groups[key]["distinct"].add(str(v))

    results = []
    for grp, data in sorted(groups.items(), key=lambda x: x[1]["count"], reverse=True):
        row = {"group": grp, "count": data["count"]}
        if count_field:
            row["distinct_count"] = len(data["distinct"])
        results.append(row)

    return {"group_by": group_by, "total_groups": len(results), "total_records": len(records), "records": results}


# ═══════════════════════════════════════════════════════════════════════════
# CONTACT LOOKUP
# ═══════════════════════════════════════════════════════════════════════════

ALLOWED_ACTIVITY_TYPES = {"send", "open", "click", "bounce", "forward", "unsubscribe", "note", "task"}
ACTIVITY_LABELS = {"send": "Email Sent", "open": "Email Opened", "click": "Email Clicked",
                   "bounce": "Email Bounced", "forward": "Email Forwarded",
                   "unsubscribe": "Unsubscribed", "note": "Note", "task": "Task / Call"}
ACTIVITY_ICONS  = {"send": "📧", "open": "📬", "click": "🔗", "bounce": "⚠️",
                   "forward": "↩️", "unsubscribe": "🚫", "note": "📝", "task": "📞"}


@app.get("/api/contact-search")
async def contact_search(q: str = Query(..., min_length=2)):
    results, seen_ids = [], set()
    async with httpx.AsyncClient(timeout=30) as client:
        try:
            r = await client.get(ac_url("contacts"), headers=HEADERS, params={"search": q, "limit": 20})
            r.raise_for_status()
            for c in r.json().get("contacts", []):
                if c["id"] not in seen_ids:
                    seen_ids.add(c["id"])
                    results.append({"id": c["id"], "firstName": c.get("firstName", ""),
                                    "lastName": c.get("lastName", ""), "email": c.get("email", ""),
                                    "phone": c.get("phone", ""), "orgName": c.get("orgname", "")})
        except Exception:
            pass
    return {"contacts": results[:20]}


@app.get("/api/contact-profile/{contact_id}")
async def contact_profile(contact_id: str):
    contact_data, activity_data, notes_data = await asyncio.gather(
        ac_get(f"contacts/{contact_id}"),
        ac_get(f"contacts/{contact_id}/activityLogs", {"limit": 100}),
        ac_get(f"contacts/{contact_id}/notes", {"limit": 50}),
        return_exceptions=True,
    )

    contact = {}
    if isinstance(contact_data, dict):
        c = contact_data.get("contact", {})
        contact = {"id": c.get("id"), "firstName": c.get("firstName", ""),
                   "lastName": c.get("lastName", ""), "email": c.get("email", ""),
                   "phone": c.get("phone", ""), "orgName": c.get("orgname", ""),
                   "created": c.get("cdate", ""), "updated": c.get("udate", "")}

    activity = []
    if isinstance(activity_data, dict):
        for log in activity_data.get("contactActivities", []):
            a_type = log.get("type", "").lower()
            if a_type in ALLOWED_ACTIVITY_TYPES:
                desc = log.get("subject") or (log.get("campaign", {}).get("name", "")
                       if isinstance(log.get("campaign"), dict) else "")
                activity.append({"type": a_type, "label": ACTIVITY_LABELS.get(a_type, a_type),
                                  "icon": ACTIVITY_ICONS.get(a_type, "•"), "description": desc,
                                  "timestamp": log.get("tstamp", log.get("cdate", ""))})

    if isinstance(notes_data, dict):
        for note in notes_data.get("notes", []):
            activity.append({"type": "note", "label": "Note", "icon": "📝",
                              "description": note.get("note", ""), "timestamp": note.get("cdate", "")})

    def _ts(item):
        try:
            return datetime.fromisoformat(item.get("timestamp", "").replace("Z", "+00:00"))
        except Exception:
            return datetime.min.replace(tzinfo=None)

    activity.sort(key=_ts, reverse=True)
    return {"contact": contact, "activity": activity}


# ═══════════════════════════════════════════════════════════════════════════
# FRONTEND
# ═══════════════════════════════════════════════════════════════════════════

@app.get("/mover")
async def serve_mover():
    return FileResponse("static/mover.html")

@app.get("/contacts")
async def serve_contact_lookup():
    return FileResponse("static/contact_lookup.html")

@app.get("/")
async def serve_ui(_: None = Depends(require_auth)):
    return FileResponse("static/index.html")


# ═══════════════════════════════════════════════════════════════════════════
# BROWSER-VIEW REPORTS  (JSON + CSV download, no email)
# ═══════════════════════════════════════════════════════════════════════════

def _csv_response(records: list, filename: str):
    out = io.StringIO()
    if records:
        w = csv.DictWriter(out, fieldnames=list(records[0].keys()))
        w.writeheader(); w.writerows(records)
    return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                             headers={"Content-Disposition": f"attachment; filename={filename}"})


@app.get("/api/report/training-activity")
async def report_training_activity(
    from_date: Optional[str] = Query(None),
    to_date:   Optional[str] = Query(None),
    format:    str           = Query("json"),
):
    from datetime import timezone
    tz = timezone.utc
    from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=tz) if from_date else None
    to_dt   = datetime.strptime(to_date,   "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=tz) if to_date else None

    tr_records  = await ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})
    account_ids: set = set()
    candidates = []
    for r in tr_records:
        fields   = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        date_str = str(fields.get("date-of-training", "")).strip()
        if date_str and (from_dt or to_dt):
            try:
                td = (datetime.fromisoformat(date_str.replace("Z", "+00:00")) if "T" in date_str
                      else datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=tz))
                if from_dt and td < from_dt: continue
                if to_dt   and td > to_dt:   continue
            except Exception:
                continue
        elif not date_str and (from_dt or to_dt):
            continue
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if acc_id: account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    results = []
    for c in candidates:
        f   = c["fields"]
        aid = c["account_id"] or ""
        results.append({
            "account":       acct_cache.get(aid, ""),
            "dealer_id":     _account_to_dealer.get(aid, ""),
            "trained_by":    f.get("trained-by", ""),
            "training_type": f.get("training-type", ""),
            "agenda":        f.get("training-agenda", ""),
            "date":          str(f.get("date-of-training", ""))[:10],
            "notes":         (f.get("training-notes", "") or "")[:200],
        })
    results.sort(key=lambda x: x["date"], reverse=True)
    if format == "csv":
        return _csv_response(results, f"training_activity_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


@app.get("/api/report/stale-untrained")
async def report_stale_untrained(
    from_date:  Optional[str] = Query(None, description="Filter by activation date from"),
    to_date:    Optional[str] = Query(None, description="Filter by activation date to"),
    stale_days: int           = Query(90),
    platform:   Optional[str] = Query(None),
    bdr:        Optional[str] = Query(None),
    format:     str           = Query("json"),
):
    today        = date.today()
    stale_cutoff = str(today - timedelta(days=stale_days))

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    tr_records  = await ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})

    training_by_acct: dict = defaultdict(list)
    for r in tr_records:
        fields   = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        date_str = str(fields.get("date-of-training", "")).strip()
        if not date_str: continue
        for aid in r.get("relationships", {}).get("account", []):
            training_by_acct[str(aid)].append(date_str[:10])

    account_ids: set = set()
    candidates = []
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        if fields.get("slp-status-detail") != "Contractor Activated": continue
        slp_plat = str(fields.get("platform", "")).strip()
        slp_bdr  = str(fields.get("assigned-bdr", "")).strip()
        rel      = r.get("relationships", {}).get("account", [])
        acc_id   = str(rel[0]) if rel else None
        eff_plat = slp_plat or _account_to_platform.get(acc_id or "", "")
        eff_bdr  = slp_bdr  or _account_to_bdr.get(acc_id or "", "")
        if platform and eff_plat != platform: continue
        if bdr      and eff_bdr  != bdr:      continue
        act_str  = str(fields.get("contractor-activated-date", "")).strip()
        act_date = act_str[:10] if act_str else ""
        if from_date and act_date and act_date < from_date: continue
        if to_date   and act_date and act_date > to_date:   continue
        if acc_id: account_ids.add(acc_id)
        trainings  = training_by_acct.get(acc_id or "", [])
        last_train = max(trainings) if trainings else None
        if last_train and last_train >= stale_cutoff: continue
        days_stale = (today - date.fromisoformat(last_train)).days if last_train else None
        candidates.append({"fields": fields, "account_id": acc_id, "act_date": act_date,
                           "training_count": len(trainings), "last_training": last_train or "",
                           "days_stale": days_stale})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    results = []
    for c in sorted(candidates, key=lambda x: x["days_stale"] or 99999, reverse=True):
        f   = c["fields"]
        aid = c["account_id"] or ""
        results.append({
            "account":         acct_cache.get(aid, ""),
            "dealer_id":       f.get("dealer-id")    or _account_to_dealer.get(aid, ""),
            "platform":        f.get("platform")     or _account_to_platform.get(aid, ""),
            "bdr":             f.get("assigned-bdr") or _account_to_bdr.get(aid, ""),
            "activation_date": c["act_date"],
            "training_count":  c["training_count"],
            "last_training":   c["last_training"] or "Never",
            "days_stale":      c["days_stale"] if c["days_stale"] is not None else "Never trained",
        })
    if format == "csv":
        return _csv_response(results, f"stale_untrained_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


@app.get("/api/report/account-status")
async def report_account_status(format: str = Query("json")):
    all_accounts = await ac_get_all("accounts", "accounts", {})
    cf_map       = await _fetch_acct_cf_map({"19", "23"})

    results = []
    for a in all_accounts:
        aid  = str(a.get("id", ""))
        cfs  = cf_map.get(aid, {})
        results.append({
            "account":      a.get("name", ""),
            "dealer_id":    _account_to_dealer.get(aid, ""),
            "platform":     _account_to_platform.get(aid, ""),
            "bdr":          _account_to_bdr.get(aid, ""),
            "status":       cfs.get("19", ""),
            "sales_region": cfs.get("23", ""),
        })
    results.sort(key=lambda x: (x["status"], x["sales_region"], x["account"]))
    if format == "csv":
        return _csv_response(results, f"account_status_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


@app.get("/api/report/platform-breakdown")
async def report_platform_breakdown(
    from_date: Optional[str] = Query(None),
    to_date:   Optional[str] = Query(None),
    format:    str           = Query("json"),
):
    from datetime import timezone
    tz = timezone.utc
    from_dt = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=tz) if from_date else None
    to_dt   = datetime.strptime(to_date,   "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=tz) if to_date else None

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    plat_data: dict = defaultdict(lambda: {"new_activations": 0, "active_slps": 0,
                                           "total_slps": 0, "bdrs": defaultdict(int)})
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        plat   = (str(fields.get("platform", "")).strip()
                  or _account_to_platform.get(acc_id or "", "") or "Unknown")
        bdr    = (str(fields.get("assigned-bdr", "")).strip()
                  or _account_to_bdr.get(acc_id or "", "") or "Unassigned")
        plat_data[plat]["total_slps"] += 1
        if fields.get("slp-status-detail") == "Contractor Activated":
            plat_data[plat]["active_slps"] += 1
            act_str = str(fields.get("contractor-activated-date", "")).strip()
            if act_str and (from_dt or to_dt):
                try:
                    act_dt = (datetime.fromisoformat(act_str.replace("Z", "+00:00")) if "T" in act_str
                              else datetime.strptime(act_str[:10], "%Y-%m-%d").replace(tzinfo=tz))
                    if from_dt and act_dt < from_dt: pass
                    elif to_dt and act_dt > to_dt:   pass
                    else:
                        plat_data[plat]["new_activations"] += 1
                        plat_data[plat]["bdrs"][bdr] += 1
                except Exception:
                    pass
            elif not from_dt and not to_dt:
                plat_data[plat]["new_activations"] += 1
                plat_data[plat]["bdrs"][bdr] += 1

    results = []
    for plat, d in sorted(plat_data.items()):
        top_bdr = max(d["bdrs"], key=d["bdrs"].get) if d["bdrs"] else ""
        results.append({
            "platform":        plat,
            "new_activations": d["new_activations"],
            "active_slps":     d["active_slps"],
            "total_slps":      d["total_slps"],
            "top_bdr":         top_bdr,
        })
    results.sort(key=lambda x: x["new_activations"], reverse=True)
    if format == "csv":
        return _csv_response(results, f"platform_breakdown_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


@app.get("/api/report/partner-activation")
async def report_partner_activation(
    from_date: Optional[str] = Query(None),
    to_date:   Optional[str] = Query(None),
    format:    str           = Query("json"),
):
    cf_map       = await _fetch_acct_cf_map({"26"})
    all_accounts = await ac_get_all("accounts", "accounts", {})
    acct_by_id   = {str(a.get("id", "")): a for a in all_accounts}

    results = []
    for aid, cfs in cf_map.items():
        pa_val = cfs.get("26", "")
        if not pa_val: continue
        pa_str = str(pa_val)[:10]
        try:
            if from_date and pa_str < from_date: continue
            if to_date   and pa_str > to_date:   continue
        except Exception:
            pass
        a = acct_by_id.get(aid, {})
        results.append({
            "account":            a.get("name", ""),
            "dealer_id":          _account_to_dealer.get(aid, ""),
            "platform":           _account_to_platform.get(aid, ""),
            "bdr":                _account_to_bdr.get(aid, ""),
            "partner_activation": pa_str,
        })
    results.sort(key=lambda x: x["partner_activation"], reverse=True)
    if format == "csv":
        return _csv_response(results, f"partner_activation_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


@app.get("/api/report/oracle-missing")
async def report_oracle_missing(
    from_date: Optional[str] = Query(None, description="Filter by activation date from"),
    to_date:   Optional[str] = Query(None, description="Filter by activation date to"),
    platform:  Optional[str] = Query(None),
    bdr:       Optional[str] = Query(None),
    format:    str           = Query("json"),
):
    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    cf_map      = await _fetch_acct_cf_map({"118"})

    account_ids: set = set()
    candidates = []
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        if fields.get("slp-status-detail") != "Contractor Activated": continue
        rel      = r.get("relationships", {}).get("account", [])
        acc_id   = str(rel[0]) if rel else None
        if cf_map.get(acc_id or "", {}).get("118"): continue
        slp_plat = str(fields.get("platform", "")).strip()
        slp_bdr  = str(fields.get("assigned-bdr", "")).strip()
        eff_plat = slp_plat or _account_to_platform.get(acc_id or "", "")
        eff_bdr  = slp_bdr  or _account_to_bdr.get(acc_id or "", "")
        if platform and eff_plat != platform: continue
        if bdr      and eff_bdr  != bdr:      continue
        act_str  = str(fields.get("contractor-activated-date", "")).strip()
        act_date = act_str[:10] if act_str else ""
        if from_date and act_date and act_date < from_date: continue
        if to_date   and act_date and act_date > to_date:   continue
        if acc_id: account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id, "act_date": act_date})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    results = []
    for c in candidates:
        f   = c["fields"]
        aid = c["account_id"] or ""
        results.append({
            "account":         acct_cache.get(aid, ""),
            "dealer_id":       f.get("dealer-id")    or _account_to_dealer.get(aid, ""),
            "platform":        f.get("platform")     or _account_to_platform.get(aid, ""),
            "bdr":             f.get("assigned-bdr") or _account_to_bdr.get(aid, ""),
            "activation_date": c["act_date"],
        })
    results.sort(key=lambda x: (x["platform"], x["bdr"], x["account"]))
    if format == "csv":
        return _csv_response(results, f"oracle_missing_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


@app.get("/api/report/account-program-search")
async def report_account_program_search(
    account_name: str           = Query("", description="Fuzzy account name filter"),
    program:      Optional[str] = Query(None, description="SLP platform/dealer program"),
    format:       str           = Query("json"),
):
    """Search accounts by name (fuzzy) and dealer program (from SLP platform)."""
    import re as _re

    name_q   = account_name.strip().lower()
    # Split into words for multi-term matching (e.g. "ARS optimus" → ["ars", "optimus"])
    name_terms = [t for t in _re.split(r'\s+', name_q) if t] if name_q else []

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})

    # Build account_id → list of SLP summaries
    slp_by_account: dict = {}
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        slp_plat = str(fields.get("platform", "")).strip()
        if program and slp_plat.lower() != program.lower():
            continue
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if not acc_id:
            continue
        if acc_id not in slp_by_account:
            slp_by_account[acc_id] = []
        slp_by_account[acc_id].append({
            "dealer_id":        fields.get("dealer-id", ""),
            "platform":         slp_plat,
            "slp_status":       fields.get("slp-status-detail", ""),
            "activated_date":   str(fields.get("contractor-activated-date", ""))[:10],
            "program_name":     fields.get("program-name-1", ""),
            "oracle_ids":       fields.get("oracle-producer-ids", ""),
            "assigned_bdr":     fields.get("assigned-bdr", ""),
        })

    if not slp_by_account:
        if format == "csv":
            return _csv_response([], "account_program_search.csv")
        return {"count": 0, "records": []}

    # Fetch account names — use in-memory index where possible
    results = []
    for acc_id, slps in slp_by_account.items():
        acct_name = _account_to_name.get(acc_id, "")
        if not acct_name:
            try:
                d = await ac_get(f"accounts/{acc_id}")
                acct_name = d.get("account", {}).get("name", "")
            except Exception:
                pass

        # Fuzzy name filter — all terms must appear in the name
        if name_terms:
            name_lower = acct_name.lower()
            if not all(t in name_lower for t in name_terms):
                continue

        for slp in slps:
            results.append({
                "account_name":    acct_name,
                "account_id":      acc_id,
                "dealer_id":       slp["dealer_id"],
                "dealer_program":  slp["platform"],
                "slp_status":      slp["slp_status"],
                "activated_date":  slp["activated_date"],
                "program_name":    slp["program_name"],
                "oracle_ids":      slp["oracle_ids"],
                "assigned_bdr":    slp["assigned_bdr"],
            })

    results.sort(key=lambda x: (x["account_name"].lower(), x["platform"]))
    if format == "csv":
        return _csv_response(results, f"account_program_search_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


# ═══════════════════════════════════════════════════════════════════════════
# SCHEDULED EMAIL REPORTS
# Triggered by GitHub Actions cron → /api/send-report/{type}
# Can also be triggered manually via the same endpoint (Basic Auth required).
# ═══════════════════════════════════════════════════════════════════════════

_HTML_WRAPPER = """\
<!DOCTYPE html><html>
<head><style>
  body{{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif;
        color:#1a1a2e;background:#f5f7fc;margin:0;padding:20px}}
  .card{{background:white;border-radius:12px;padding:24px 28px;max-width:960px;
         margin:0 auto;box-shadow:0 2px 12px rgba(0,0,0,.07)}}
  h2{{color:#1e44b8;margin:0 0 4px;font-size:1.3rem}}
  .sub{{color:#666;font-size:.85rem;margin-bottom:20px}}
  table{{width:100%;border-collapse:collapse;font-size:.82rem}}
  th{{background:#f0f3fb;color:#1e44b8;font-weight:600;text-align:left;
      padding:8px 10px;border-bottom:2px solid #d0d7f0}}
  td{{padding:7px 10px;border-bottom:1px solid #eef0f8;white-space:nowrap}}
  tr:nth-child(even) td{{background:#fafbff}}
  .footer{{color:#aaa;font-size:.75rem;margin-top:16px;text-align:center}}
</style></head><body><div class="card">
<h2>{title}</h2><div class="sub">{subtitle}</div>
{table}
<div class="footer">Generated by Moogle &middot; {timestamp}</div>
</div></body></html>"""


def _html_table(records: list, cols: list) -> str:
    """Build an HTML table. cols = [(header_label, dict_key), ...]"""
    if not records:
        return "<p style='color:#888;padding:12px 0'>No records found.</p>"
    headers = "".join(f"<th>{h}</th>" for h, _ in cols)
    rows = "".join(
        "<tr>" + "".join(f"<td>{str(r.get(k, '') or '')}</td>" for _, k in cols) + "</tr>"
        for r in records
    )
    return f"<table><thead><tr>{headers}</tr></thead><tbody>{rows}</tbody></table>"


def _csv_bytes(records: list) -> bytes:
    if not records:
        return b""
    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=list(records[0].keys()))
    w.writeheader()
    w.writerows(records)
    return buf.getvalue().encode()


async def _send_email(subject: str, html: str,
                      csv_data: bytes = None, csv_name: str = None,
                      recipients: list = None):
    """Send an HTML email with an optional CSV attachment via SMTP STARTTLS.
    Pass recipients to override the default REPORT_RECIPIENTS env list."""
    to = recipients or _RECIPIENTS
    if not _SMTP_USER or not to:
        print(f"[reports] Email not configured — skipping: {subject}")
        return
    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"]    = f"{_SMTP_FROM} <{_SMTP_USER}>"
    msg["To"]      = ", ".join(to)
    msg.attach(MIMEText(html, "html"))
    if csv_data:
        part = MIMEBase("application", "octet-stream")
        part.set_payload(csv_data)
        _enc.encode_base64(part)
        part.add_header("Content-Disposition", f'attachment; filename="{csv_name}"')
        msg.attach(part)
    try:
        await aiosmtplib.send(
            msg,
            hostname=_SMTP_HOST,
            port=_SMTP_PORT,
            username=_SMTP_USER,
            password=_SMTP_PASS,
            start_tls=True,
        )
        print(f"[reports] Sent '{subject}' → {to}")
    except Exception as exc:
        print(f"[reports] Email failed: {exc}")


# ── Date-range helpers ────────────────────────────────────────────────────

def _resolve_date_range(
    start: Optional[date],
    end:   Optional[date],
    preset: Optional[str],
    *,
    default_start: Optional[date] = None,
    default_end:   Optional[date] = None,
) -> tuple:
    """Resolve start/end from explicit params, a named preset, or defaults.
    Presets: yesterday | last_week | last_7_days | last_30_days | last_90_days |
             this_week | this_month | last_month
    """
    today = date.today()
    if preset:
        p = preset.lower().replace("-", "_")
        if p == "yesterday":
            d = today - timedelta(days=1); return d, d
        if p == "last_week":
            dow = today.weekday()                      # Mon=0 … Sun=6
            last_sun = today - timedelta(days=dow + 1)
            return last_sun - timedelta(days=6), last_sun
        if p in ("last_7_days", "last_7"):
            return today - timedelta(days=7), today - timedelta(days=1)
        if p in ("last_30_days", "last_30"):
            return today - timedelta(days=30), today - timedelta(days=1)
        if p in ("last_90_days", "last_90"):
            return today - timedelta(days=90), today - timedelta(days=1)
        if p == "this_week":
            return today - timedelta(days=today.weekday()), today
        if p in ("this_month", "current_month"):
            return today.replace(day=1), today
        if p == "last_month":
            first_this = today.replace(day=1)
            last_prev  = first_this - timedelta(days=1)
            return last_prev.replace(day=1), last_prev
        if p == "this_quarter":
            q = (today.month - 1) // 3
            return today.replace(month=q*3+1, day=1), today
        if p == "last_quarter":
            q = (today.month - 1) // 3
            if q == 0:
                qs, qy = 3, today.year - 1
            else:
                qs, qy = q - 1, today.year
            from calendar import monthrange as _mr
            qe_month = qs * 3 + 3
            qe_day   = _mr(qy, qe_month)[1]
            return date(qy, qs*3+1, 1), date(qy, qe_month, qe_day)
        if p in ("this_year", "ytd"):
            return today.replace(month=1, day=1), today
        if p == "last_year":
            y = today.year - 1
            return date(y, 1, 1), date(y, 12, 31)
        if p in ("last_18_months", "last_18mo", "last_18"):
            return today - timedelta(days=548), today
        if p in ("all", "all_time"):
            return date(2000, 1, 1), today
    return (start or default_start), (end or default_end)


async def _fetch_acct_cf_map(field_ids: set) -> dict:
    """Bulk-fetch account custom fields. Returns {account_id: {field_id_str: value}}."""
    result: dict   = defaultdict(dict)
    field_ids_int  = {int(f) for f in field_ids}
    offset, PAGE   = 0, 100
    while True:
        page  = await ac_get("accountCustomFieldData", {"limit": PAGE, "offset": offset})
        items = page.get("accountCustomFieldData", [])
        if not items:
            break
        for item in items:
            fid = int(item.get("customFieldId", 0))
            if fid not in field_ids_int:
                continue
            aid = str(item.get("accountId", ""))
            val = (item.get("fieldValue") or "").strip()
            if aid and val:
                result[aid][str(fid)] = val
        offset += PAGE
        if len(items) < PAGE:
            break
    return dict(result)


# ── Activations (daily Mon–Fri) ──────────────────────────────────────────

async def _job_activations(start_date: Optional[date] = None, end_date: Optional[date] = None,
                           preset: Optional[str] = None, recipients: list = None):
    """Email 'Contractor Activated' SLP records for a date range (defaults to yesterday)."""
    from datetime import timezone
    tz_utc = timezone.utc
    today  = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_start=today - timedelta(days=1))
    if _start is None: _start = today - timedelta(days=1)
    if _end   is None: _end   = _start
    from_dt    = datetime(_start.year, _start.month, _start.day, tzinfo=tz_utc)
    to_dt      = datetime(_end.year,   _end.month,   _end.day,   23, 59, 59, tzinfo=tz_utc)
    yesterday  = str(_start)
    date_label = str(_start) if _start == _end else f"{_start} to {_end}"
    print(f"[reports] Activations for {date_label}")

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    account_ids: set = set()
    candidates = []
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        if fields.get("slp-status-detail") != "Contractor Activated":
            continue
        act_str = str(fields.get("contractor-activated-date", "")).strip()
        if not act_str:
            continue
        try:
            act_dt = (datetime.fromisoformat(act_str.replace("Z", "+00:00")) if "T" in act_str
                      else datetime.strptime(act_str[:10], "%Y-%m-%d").replace(tzinfo=tz_utc))
        except Exception:
            continue
        if not (from_dt <= act_dt <= to_dt):
            continue
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if acc_id:
            account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id})

    async def _fetch_acct_act(aid: str) -> tuple:
        try:
            name_r, cf_r = await asyncio.gather(
                ac_get(f"accounts/{aid}"),
                ac_get(f"accounts/{aid}/accountCustomFieldData"),
                return_exceptions=True,
            )
            name = name_r.get("account", {}).get("name", "") if isinstance(name_r, dict) else ""
            cfs: dict = {}
            if isinstance(cf_r, dict):
                for item in cf_r.get("accountCustomFieldData", []):
                    cfs[str(item.get("customFieldId", ""))] = (item.get("fieldValue") or "").strip()
            return aid, {"name": name, "platform": cfs.get("29", ""), "bdr": cfs.get("119", ""),
                         "dealer_id": _account_to_dealer.get(aid, "")}
        except Exception:
            return aid, {"name": "", "platform": "", "bdr": "", "dealer_id": _account_to_dealer.get(aid, "")}

    acct_cache: dict = dict(await asyncio.gather(*[_fetch_acct_act(aid) for aid in account_ids]))

    records = []
    for c in candidates:
        f    = c["fields"]
        acct = acct_cache.get(c["account_id"]) or {}
        records.append({
            "Account":   acct.get("name") or f.get("name", ""),
            "Dealer ID": f.get("dealer-id") or acct.get("dealer_id", ""),
            "Platform":  f.get("platform") or acct.get("platform", ""),
            "BDR":       f.get("assigned-bdr") or acct.get("bdr", ""),
            "Activated": str(f.get("contractor-activated-date", "") or "")[:10],
        })
    records.sort(key=lambda x: x["Activated"], reverse=True)

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"),
            ("Platform","Platform"), ("BDR","BDR"), ("Activated","Activated")]
    html = _HTML_WRAPPER.format(
        title=f"Activations — {date_label}",
        subtitle=f"{len(records)} new activation{'s' if len(records) != 1 else ''}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    csv_label = yesterday if _start == _end else f"{_start}_{_end}"
    await _send_email(
        subject=f"Activations Report — {date_label} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"activations_{csv_label}.csv",
        recipients=recipients,
    )


# ── License Expiration (weekly Monday) ───────────────────────────────────

async def _job_license_expiration(start_date: Optional[date] = None, end_date: Optional[date] = None,
                                   preset: Optional[str] = None, recipients: list = None):
    """Email licenses expiring in a date window (defaults to already-expired through 90 days out)."""
    from datetime import timezone
    tz_utc = timezone.utc
    now    = datetime.now(tz_utc)
    today  = date.today()
    # For license expiration, start/end bound the expiration date itself
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_end=today + timedelta(days=90))
    cutoff_dt = datetime(_end.year, _end.month, _end.day, 23, 59, 59, tzinfo=tz_utc) if _end else now + timedelta(days=90)
    floor_dt  = datetime(_start.year, _start.month, _start.day, tzinfo=tz_utc) if _start else None
    today_str = str(today)
    print("[reports] License expiration report")

    lic_records = await ac_get_all(f"customObjects/records/{LICENSE_SCHEMA_ID}", "records", {})
    account_ids: set = set()
    candidates = []
    for r in lic_records:
        fields  = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        exp_str = (fields.get("expiration-date") or fields.get("license-expiration-date")
                   or fields.get("expires") or "")
        if not exp_str:
            continue
        try:
            exp_dt = (datetime.fromisoformat(str(exp_str).replace("Z", "+00:00")) if "T" in str(exp_str)
                      else datetime.strptime(str(exp_str)[:10], "%Y-%m-%d").replace(tzinfo=tz_utc))
        except Exception:
            continue
        if exp_dt > cutoff_dt:
            continue
        if floor_dt and exp_dt < floor_dt:
            continue
        is_expired = exp_dt < now
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if acc_id:
            account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id,
                           "exp_str": str(exp_str)[:10],
                           "days_until": (exp_dt - now).days,
                           "is_expired": is_expired})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    records = []
    for c in sorted(candidates, key=lambda x: x["days_until"]):
        status = "EXPIRED" if c["is_expired"] else f"In {c['days_until']}d"
        f = c["fields"]
        records.append({
            "Account":    acct_cache.get(c["account_id"], ""),
            "Expiration": c["exp_str"],
            "Status":     status,
            "License #":  f.get("license-number", f.get("license_number", "")),
            "State":      f.get("state", f.get("license-state", "")),
        })

    cols = [("Account","Account"), ("Expiration","Expiration"),
            ("Status","Status"), ("License #","License #"), ("State","State")]
    end_label   = str(_end)   if _end   else str(today + timedelta(days=90))
    start_label = str(_start) if _start else "past"
    range_label = f"{start_label} – {end_label}"
    html = _HTML_WRAPPER.format(
        title="License Expiration Report",
        subtitle=f"{len(records)} license{'s' if len(records) != 1 else ''} — expiration {range_label}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"License Expiration Report — {today_str} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"license_expiration_{today_str}.csv",
        recipients=recipients,
    )


# ── BDR Summary (weekly Monday) ──────────────────────────────────────────

async def _job_bdr_summary(start_date: Optional[date] = None, end_date: Optional[date] = None,
                           preset: Optional[str] = None, recipients: list = None):
    """Email activations grouped by BDR for a date range (defaults to past 7 days)."""
    from datetime import timezone
    tz_utc = timezone.utc
    _today = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_start=_today - timedelta(days=7),
                                       default_end=_today - timedelta(days=1))
    if _start is None: _start = _today - timedelta(days=7)
    if _end   is None: _end   = _today - timedelta(days=1)
    week_start = _start.strftime("%Y-%m-%d")
    week_end   = _end.strftime("%Y-%m-%d")
    from_dt    = datetime(_start.year, _start.month, _start.day, tzinfo=tz_utc)
    to_dt      = datetime(_end.year,   _end.month,   _end.day,   23, 59, 59, tzinfo=tz_utc)
    print(f"[reports] BDR summary {week_start} → {week_end}")

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})

    # Pass 1 – collect raw data; find accounts where BDR or platform is missing
    raw_slps = []
    acct_ids_needed: set = set()
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        bdr    = str(fields.get("assigned-bdr", "")).strip()
        plat   = str(fields.get("platform", "")).strip()
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if (not bdr or not plat) and acc_id:
            acct_ids_needed.add(acc_id)
        raw_slps.append({"fields": fields, "bdr": bdr, "plat": plat, "acc_id": acc_id})

    # Fetch account CFs for accounts where BDR/platform is missing
    acct_cf_cache: dict = {}
    if acct_ids_needed:
        async def _fetch_cf_bdr(aid: str) -> tuple:
            try:
                cf_r = await ac_get(f"accounts/{aid}/accountCustomFieldData")
                cfs  = {str(i.get("customFieldId", "")): (i.get("fieldValue") or "").strip()
                        for i in cf_r.get("accountCustomFieldData", [])}
                return aid, {"bdr": cfs.get("119", ""), "platform": cfs.get("29", "")}
            except Exception:
                return aid, {"bdr": "", "platform": ""}
        acct_cf_cache = dict(await asyncio.gather(*[_fetch_cf_bdr(aid) for aid in acct_ids_needed]))

    # Pass 2 – process with fallbacks
    bdr_data: dict = defaultdict(lambda: {"activated_week": 0, "total_slps": 0,
                                           "platforms": defaultdict(int), "accounts": set()})
    for rd in raw_slps:
        fields   = rd["fields"]
        acc_id   = rd["acc_id"]
        fallback = acct_cf_cache.get(acc_id, {}) if acc_id else {}
        bdr      = rd["bdr"] or fallback.get("bdr", "") or "Unassigned"
        bdr_data[bdr]["total_slps"] += 1
        if fields.get("slp-status-detail") == "Contractor Activated":
            act_str = str(fields.get("contractor-activated-date", "")).strip()
            if act_str:
                try:
                    act_dt = (datetime.fromisoformat(act_str.replace("Z", "+00:00")) if "T" in act_str
                              else datetime.strptime(act_str[:10], "%Y-%m-%d").replace(tzinfo=tz_utc))
                    if from_dt <= act_dt <= to_dt:
                        bdr_data[bdr]["activated_week"] += 1
                except Exception:
                    pass
        plat = rd["plat"] or fallback.get("platform", "")
        if plat:
            bdr_data[bdr]["platforms"][plat] += 1
        if acc_id:
            bdr_data[bdr]["accounts"].add(acc_id)

    records = [
        {"BDR": bdr,
         "Activations (week)": d["activated_week"],
         "Total SLPs": d["total_slps"],
         "Accounts": len(d["accounts"]),
         "Platforms": ", ".join(f"{k}:{v}" for k, v in sorted(d["platforms"].items()))}
        for bdr, d in sorted(bdr_data.items())
    ]
    records.sort(key=lambda x: x["Activations (week)"], reverse=True)

    cols = [("BDR","BDR"), ("Activations (week)","Activations (week)"),
            ("Total SLPs","Total SLPs"), ("Accounts","Accounts"), ("Platforms","Platforms")]
    html = _HTML_WRAPPER.format(
        title=f"BDR Summary — Week of {week_start}",
        subtitle=f"{week_start} through {week_end}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"BDR Summary — Week of {week_start}",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"bdr_summary_{week_start}.csv",
        recipients=recipients,
    )


# ── Training Activity (weekly Monday) ────────────────────────────────────

async def _job_training_activity(start_date: Optional[date] = None, end_date: Optional[date] = None,
                                  preset: Optional[str] = None, recipients: list = None):
    """Email training sessions conducted in the date window, grouped by trainer."""
    from datetime import timezone
    tz_utc = timezone.utc
    today  = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_start=today - timedelta(days=7),
                                       default_end=today - timedelta(days=1))
    if _start is None: _start = today - timedelta(days=7)
    if _end   is None: _end   = today - timedelta(days=1)
    from_dt    = datetime(_start.year, _start.month, _start.day, tzinfo=tz_utc)
    to_dt      = datetime(_end.year,   _end.month,   _end.day,   23, 59, 59, tzinfo=tz_utc)
    date_label = str(_start) if _start == _end else f"{_start} to {_end}"
    print(f"[reports] Training activity {date_label}")

    training_records = await ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})
    account_ids: set = set()
    candidates = []
    for r in training_records:
        fields   = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        date_str = str(fields.get("date-of-training", "")).strip()
        if not date_str:
            continue
        try:
            td = (datetime.fromisoformat(date_str.replace("Z", "+00:00")) if "T" in date_str
                  else datetime.strptime(date_str[:10], "%Y-%m-%d").replace(tzinfo=tz_utc))
        except Exception:
            continue
        if not (from_dt <= td <= to_dt):
            continue
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if acc_id:
            account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    records = []
    for c in candidates:
        f   = c["fields"]
        aid = c["account_id"] or ""
        records.append({
            "Account":       acct_cache.get(aid, ""),
            "Dealer ID":     _account_to_dealer.get(aid, ""),
            "Trained By":    f.get("trained-by", ""),
            "Training Type": f.get("training-type", ""),
            "Agenda":        f.get("training-agenda", ""),
            "Date":          str(f.get("date-of-training", ""))[:10],
            "Notes":         (f.get("training-notes", "") or "")[:120],
        })
    records.sort(key=lambda x: (x["Date"], x["Trained By"]), reverse=True)

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"), ("Trained By","Trained By"),
            ("Training Type","Training Type"), ("Agenda","Agenda"), ("Date","Date")]
    html = _HTML_WRAPPER.format(
        title=f"Training Activity — {date_label}",
        subtitle=f"{len(records)} session{'s' if len(records) != 1 else ''}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    csv_label = str(_start) if _start == _end else f"{_start}_{_end}"
    await _send_email(
        subject=f"Training Activity — {date_label} ({len(records)} sessions)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"training_activity_{csv_label}.csv",
        recipients=recipients,
    )


# ── Stale / Untrained Dealers (monthly) ──────────────────────────────────

async def _job_stale_untrained(start_date: Optional[date] = None, end_date: Optional[date] = None,
                                preset: Optional[str] = None, recipients: list = None):
    """Email activated dealers with no training or last training >90 days ago.
    start_date/end_date optionally filter by contractor-activated-date."""
    today = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset)
    stale_cutoff = today - timedelta(days=90)
    print("[reports] Stale/untrained dealers")

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    tr_records  = await ac_get_all(f"customObjects/records/{TRAINING_SCHEMA_ID}", "records", {})

    training_by_acct: dict = defaultdict(list)
    for r in tr_records:
        fields   = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        date_str = str(fields.get("date-of-training", "")).strip()
        if not date_str:
            continue
        for aid in r.get("relationships", {}).get("account", []):
            training_by_acct[str(aid)].append(date_str[:10])

    account_ids: set = set()
    candidates = []
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        if fields.get("slp-status-detail") != "Contractor Activated":
            continue
        act_str  = str(fields.get("contractor-activated-date", "")).strip()
        act_date = act_str[:10] if act_str else ""
        if _start and act_date and act_date < str(_start):
            continue
        if _end   and act_date and act_date > str(_end):
            continue
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if acc_id:
            account_ids.add(acc_id)
        trainings  = training_by_acct.get(acc_id or "", [])
        last_train = max(trainings) if trainings else None
        is_stale   = (not last_train) or (last_train < str(stale_cutoff))
        if not is_stale:
            continue
        days_stale = (today - date.fromisoformat(last_train)).days if last_train else None
        candidates.append({"fields": fields, "account_id": acc_id, "act_date": act_date,
                           "training_count": len(trainings), "last_training": last_train or "",
                           "days_stale": days_stale})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    records = []
    for c in sorted(candidates, key=lambda x: x["days_stale"] or 99999, reverse=True):
        f   = c["fields"]
        aid = c["account_id"] or ""
        records.append({
            "Account":         acct_cache.get(aid, ""),
            "Dealer ID":       f.get("dealer-id")    or _account_to_dealer.get(aid, ""),
            "Platform":        f.get("platform")     or _account_to_platform.get(aid, ""),
            "BDR":             f.get("assigned-bdr") or _account_to_bdr.get(aid, ""),
            "Activation Date": c["act_date"],
            "# Trainings":     c["training_count"],
            "Last Training":   c["last_training"] or "Never",
            "Days Stale":      c["days_stale"] if c["days_stale"] is not None else "Never trained",
        })

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"), ("Platform","Platform"),
            ("BDR","BDR"), ("Activation Date","Activation Date"),
            ("# Trainings","# Trainings"), ("Last Training","Last Training"),
            ("Days Stale","Days Stale")]
    html = _HTML_WRAPPER.format(
        title="Stale / Untrained Dealers",
        subtitle=f"{len(records)} activated dealer{'s' if len(records) != 1 else ''} with no training or last training >90 days ago",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"Stale/Untrained Dealers — {today} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"stale_untrained_{today}.csv",
        recipients=recipients,
    )


# ── Account Status Summary (weekly Monday) ───────────────────────────────

async def _job_account_status(start_date: Optional[date] = None, end_date: Optional[date] = None,
                               preset: Optional[str] = None, recipients: list = None):
    """Email all accounts with their status and sales region (snapshot, date params unused)."""
    today = date.today()
    print("[reports] Account status summary")

    all_accounts = await ac_get_all("accounts", "accounts", {})
    cf_map       = await _fetch_acct_cf_map({"19", "23"})  # account_status, sales_region

    records = []
    for a in all_accounts:
        aid  = str(a.get("id", ""))
        cfs  = cf_map.get(aid, {})
        records.append({
            "Account":      a.get("name", ""),
            "Dealer ID":    _account_to_dealer.get(aid, ""),
            "Platform":     _account_to_platform.get(aid, ""),
            "BDR":          _account_to_bdr.get(aid, ""),
            "Status":       cfs.get("19", ""),
            "Sales Region": cfs.get("23", ""),
        })
    records.sort(key=lambda x: (x["Status"], x["Sales Region"], x["Account"]))

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"), ("Platform","Platform"),
            ("BDR","BDR"), ("Status","Status"), ("Sales Region","Sales Region")]
    html = _HTML_WRAPPER.format(
        title="Account Status Summary",
        subtitle=f"{len(records)} account{'s' if len(records) != 1 else ''} as of {today}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"Account Status Summary — {today} ({len(records)} accounts)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"account_status_{today}.csv",
        recipients=recipients,
    )


# ── Platform / Dealer Program Breakdown (weekly Monday) ──────────────────

async def _job_platform_breakdown(start_date: Optional[date] = None, end_date: Optional[date] = None,
                                   preset: Optional[str] = None, recipients: list = None):
    """Email new activations and total SLP counts grouped by platform."""
    from datetime import timezone
    tz_utc = timezone.utc
    today  = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_start=today - timedelta(days=7),
                                       default_end=today - timedelta(days=1))
    if _start is None: _start = today - timedelta(days=7)
    if _end   is None: _end   = today - timedelta(days=1)
    from_dt    = datetime(_start.year, _start.month, _start.day, tzinfo=tz_utc)
    to_dt      = datetime(_end.year,   _end.month,   _end.day,   23, 59, 59, tzinfo=tz_utc)
    date_label = str(_start) if _start == _end else f"{_start} to {_end}"
    print(f"[reports] Platform breakdown {date_label}")

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    plat_data: dict = defaultdict(lambda: {"new_activations": 0, "active_slps": 0,
                                           "total_slps": 0, "bdrs": defaultdict(int)})
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        plat   = (str(fields.get("platform", "")).strip()
                  or _account_to_platform.get(acc_id or "", "") or "Unknown")
        bdr    = (str(fields.get("assigned-bdr", "")).strip()
                  or _account_to_bdr.get(acc_id or "", "") or "Unassigned")
        plat_data[plat]["total_slps"] += 1
        if fields.get("slp-status-detail") == "Contractor Activated":
            plat_data[plat]["active_slps"] += 1
            act_str = str(fields.get("contractor-activated-date", "")).strip()
            if act_str:
                try:
                    act_dt = (datetime.fromisoformat(act_str.replace("Z", "+00:00")) if "T" in act_str
                              else datetime.strptime(act_str[:10], "%Y-%m-%d").replace(tzinfo=tz_utc))
                    if from_dt <= act_dt <= to_dt:
                        plat_data[plat]["new_activations"] += 1
                        plat_data[plat]["bdrs"][bdr] += 1
                except Exception:
                    pass

    records = []
    for plat, d in sorted(plat_data.items()):
        top_bdr = max(d["bdrs"], key=d["bdrs"].get) if d["bdrs"] else ""
        records.append({
            "Platform":        plat,
            "New Activations": d["new_activations"],
            "Active SLPs":     d["active_slps"],
            "Total SLPs":      d["total_slps"],
            "Top BDR":         top_bdr,
        })
    records.sort(key=lambda x: x["New Activations"], reverse=True)

    total_new = sum(r["New Activations"] for r in records)
    cols = [("Platform","Platform"), ("New Activations","New Activations"),
            ("Active SLPs","Active SLPs"), ("Total SLPs","Total SLPs"), ("Top BDR","Top BDR")]
    html = _HTML_WRAPPER.format(
        title=f"Platform Breakdown — {date_label}",
        subtitle=f"{total_new} new activation{'s' if total_new != 1 else ''} across {len(records)} platform{'s' if len(records) != 1 else ''}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    csv_label = str(_start) if _start == _end else f"{_start}_{_end}"
    await _send_email(
        subject=f"Platform Breakdown — {date_label} ({total_new} new activations)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"platform_breakdown_{csv_label}.csv",
        recipients=recipients,
    )


# ── Partner Activation (monthly) ─────────────────────────────────────────

async def _job_partner_activation(start_date: Optional[date] = None, end_date: Optional[date] = None,
                                   preset: Optional[str] = None, recipients: list = None):
    """Email accounts where partner_activation (CF 26) date falls in the window."""
    today = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_start=today.replace(day=1),
                                       default_end=today)
    if _start is None: _start = today.replace(day=1)
    if _end   is None: _end   = today
    date_label = str(_start) if _start == _end else f"{_start} to {_end}"
    print(f"[reports] Partner activation {date_label}")

    cf_map       = await _fetch_acct_cf_map({"26"})
    all_accounts = await ac_get_all("accounts", "accounts", {})
    acct_by_id   = {str(a.get("id", "")): a for a in all_accounts}

    records = []
    for aid, cfs in cf_map.items():
        pa_val = cfs.get("26", "")
        if not pa_val:
            continue
        pa_str = str(pa_val)[:10]
        try:
            pa_date = date.fromisoformat(pa_str)
            if pa_date < _start or pa_date > _end:
                continue
        except Exception:
            if start_date or end_date or preset:
                continue           # skip unparseable dates when a filter is active
            pa_str = str(pa_val)  # show raw value when no filter
        a = acct_by_id.get(aid, {})
        records.append({
            "Account":            a.get("name", ""),
            "Dealer ID":          _account_to_dealer.get(aid, ""),
            "Platform":           _account_to_platform.get(aid, ""),
            "BDR":                _account_to_bdr.get(aid, ""),
            "Partner Activation": pa_str,
        })
    records.sort(key=lambda x: x["Partner Activation"], reverse=True)

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"), ("Platform","Platform"),
            ("BDR","BDR"), ("Partner Activation","Partner Activation")]
    html = _HTML_WRAPPER.format(
        title=f"Partner Activations — {date_label}",
        subtitle=f"{len(records)} partner activation{'s' if len(records) != 1 else ''}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    csv_label = str(_start) if _start == _end else f"{_start}_{_end}"
    await _send_email(
        subject=f"Partner Activations — {date_label} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"partner_activation_{csv_label}.csv",
        recipients=recipients,
    )


# ── Oracle Producer ID Missing (weekly Monday) ────────────────────────────

async def _job_oracle_missing(start_date: Optional[date] = None, end_date: Optional[date] = None,
                               preset: Optional[str] = None, recipients: list = None):
    """Email activated SLPs whose account has no Oracle Producer ID (CF 118).
    start_date/end_date optionally filter by contractor-activated-date."""
    today = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset)
    print("[reports] Oracle Producer ID missing")

    slp_records = await ac_get_all(f"customObjects/records/{SLP_SCHEMA_ID}", "records", {})
    cf_map      = await _fetch_acct_cf_map({"118"})  # oracle_producer_id

    account_ids: set = set()
    candidates = []
    for r in slp_records:
        fields = {fo["id"]: fo.get("value", "") for fo in r.get("fields", [])}
        if fields.get("slp-status-detail") != "Contractor Activated":
            continue
        rel    = r.get("relationships", {}).get("account", [])
        acc_id = str(rel[0]) if rel else None
        if cf_map.get(acc_id or "", {}).get("118"):
            continue   # oracle_producer_id already set
        act_str  = str(fields.get("contractor-activated-date", "")).strip()
        act_date = act_str[:10] if act_str else ""
        if _start and act_date and act_date < str(_start):
            continue
        if _end   and act_date and act_date > str(_end):
            continue
        if acc_id:
            account_ids.add(acc_id)
        candidates.append({"fields": fields, "account_id": acc_id, "act_date": act_date})

    acct_cache: dict = {}
    for aid in account_ids:
        try:
            d = await ac_get(f"accounts/{aid}")
            acct_cache[aid] = d.get("account", {}).get("name", "")
        except Exception:
            acct_cache[aid] = ""

    records = []
    for c in candidates:
        f   = c["fields"]
        aid = c["account_id"] or ""
        records.append({
            "Account":         acct_cache.get(aid, ""),
            "Dealer ID":       f.get("dealer-id")    or _account_to_dealer.get(aid, ""),
            "Platform":        f.get("platform")     or _account_to_platform.get(aid, ""),
            "BDR":             f.get("assigned-bdr") or _account_to_bdr.get(aid, ""),
            "Activation Date": c["act_date"],
        })
    records.sort(key=lambda x: (x["Platform"], x["BDR"], x["Account"]))

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"), ("Platform","Platform"),
            ("BDR","BDR"), ("Activation Date","Activation Date")]
    html = _HTML_WRAPPER.format(
        title="Oracle Producer ID Missing",
        subtitle=f"{len(records)} activated dealer{'s' if len(records) != 1 else ''} missing Oracle Producer ID",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"Oracle Producer ID Missing — {today} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"oracle_missing_{today}.csv",
        recipients=recipients,
    )


# ── Account Activity (ad hoc / on-demand) ────────────────────────────────

async def _job_account_activity(start_date=None, end_date=None, preset=None, recipients=None):
    """Email account activity summary — per-account note/deal/contact counts."""
    from datetime import timezone
    today = str(date.today())
    _start, _end = _resolve_date_range(start_date, end_date, preset)
    date_label = f"{_start} – {_end}" if (_start or _end) else "All Time"
    from_date = str(_start) if _start else None
    to_date   = str(_end)   if _end   else None
    from_dt   = (datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                 if from_date else None)
    to_dt     = (datetime.strptime(to_date, "%Y-%m-%d")
                 .replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                 if to_date else None)

    accounts_data, all_contacts, all_notes, all_deals = await asyncio.gather(
        ac_get_all("accounts", "accounts", {}),
        ac_get_all("contacts", "contacts", {}),
        ac_get_all("notes",    "notes",    {}),
        ac_get_all("deals",    "deals",    {}),
    )

    contact_to_account: dict  = {}
    contacts_by_account: dict = defaultdict(list)
    for c in all_contacts:
        aid = str(c.get("account", ""))
        cid = str(c.get("id", ""))
        if aid:
            contact_to_account[cid] = aid
            contacts_by_account[aid].append(c)

    notes_by_account: dict = defaultdict(list)
    for n in all_notes:
        if (n.get("reltype") or "").lower() != "contact":
            continue
        cid = str(n.get("rel_id", ""))
        aid = contact_to_account.get(cid)
        if not aid:
            continue
        if from_dt or to_dt:
            raw_date = n.get("cdate", "")
            try:
                nd = (datetime.fromisoformat(raw_date.replace("Z", "+00:00")) if "T" in raw_date
                      else datetime.strptime(raw_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc))
                if from_dt and nd < from_dt: continue
                if to_dt   and nd > to_dt:   continue
            except Exception:
                continue
        notes_by_account[aid].append(n)

    deals_by_account: dict = defaultdict(list)
    for d in all_deals:
        aid = str(d.get("account", ""))
        if aid:
            deals_by_account[aid].append(d)

    records = []
    for acc in accounts_data:
        aid        = str(acc.get("id", ""))
        acct_notes = sorted(notes_by_account.get(aid, []), key=lambda n: n.get("cdate", ""), reverse=True)
        acct_deals = deals_by_account.get(aid, [])
        if not acct_notes and not acct_deals:
            continue
        last_note = acct_notes[0] if acct_notes else None
        records.append({
            "Account":        acc.get("name", ""),
            "Account ID":     aid,
            "Contacts":       len(contacts_by_account.get(aid, [])),
            "Notes":          len(acct_notes),
            "Last Note Date": last_note.get("cdate", "")[:10] if last_note else "",
            "Deals":          len(acct_deals),
        })
    records.sort(key=lambda x: x["Last Note Date"], reverse=True)

    cols = [("Account","Account"), ("Account ID","Account ID"),
            ("Contacts","Contacts"), ("Notes","Notes"),
            ("Last Note Date","Last Note Date"), ("Deals","Deals")]
    html = _HTML_WRAPPER.format(
        title=f"Account Activity — {date_label}",
        subtitle=f"{len(records)} active account{'s' if len(records) != 1 else ''}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"Account Activity — {today} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"account_activity_{today}.csv",
        recipients=recipients,
    )


# ── Team Activity (ad hoc / on-demand) ───────────────────────────────────

async def _job_team_activity(start_date=None, end_date=None, preset=None, recipients=None):
    """Email team performance summary — per-user note activity."""
    from datetime import timezone
    today = str(date.today())
    _start, _end = _resolve_date_range(start_date, end_date, preset)
    date_label = f"{_start} – {_end}" if (_start or _end) else "All Time"
    from_date = str(_start) if _start else None
    to_date   = str(_end)   if _end   else None
    from_dt   = (datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                 if from_date else None)
    to_dt     = (datetime.strptime(to_date, "%Y-%m-%d")
                 .replace(hour=23, minute=59, second=59, tzinfo=timezone.utc)
                 if to_date else None)

    users_data, all_notes_raw, all_contacts = await asyncio.gather(
        ac_get("users"),
        ac_get_all("notes", "notes", {}),
        ac_get_all("contacts", "contacts", {}),
    )

    users: dict = {}
    for u in (users_data.get("users", []) if isinstance(users_data, dict) else []):
        uid  = str(u.get("id", ""))
        name = f"{u.get('firstName','').strip()} {u.get('lastName','').strip()}".strip()
        users[uid] = name or u.get("email", f"User {uid}")

    contact_to_account: dict = {}
    for c in all_contacts:
        cid = str(c.get("id", ""))
        aid = str(c.get("account", "") or "")
        if aid and aid != "0":
            contact_to_account[cid] = aid

    user_stats: dict = defaultdict(lambda: {"note_count": 0, "accounts": set(), "latest_date": ""})
    for n in all_notes_raw:
        reltype = (n.get("reltype") or "").lower()
        if reltype not in ("contact", "deal"):
            continue
        raw_date = n.get("cdate", "")
        if from_dt or to_dt:
            try:
                nd = (datetime.fromisoformat(raw_date.replace("Z", "+00:00")) if "T" in raw_date
                      else datetime.strptime(raw_date[:10], "%Y-%m-%d").replace(tzinfo=timezone.utc))
                if from_dt and nd < from_dt: continue
                if to_dt   and nd > to_dt:   continue
            except Exception:
                continue
        uid = str(n.get("userid", "") or "")
        cid = str(n.get("rel_id", "") or "") if reltype == "contact" else ""
        aid = contact_to_account.get(cid, "")
        if uid:
            s = user_stats[uid]
            s["note_count"] += 1
            if aid:
                s["accounts"].add(aid)
            if raw_date > s["latest_date"]:
                s["latest_date"] = raw_date

    records = []
    for uid, s in sorted(user_stats.items(), key=lambda x: x[1]["note_count"], reverse=True):
        records.append({
            "Team Member": users.get(uid, f"User {uid}"),
            "Notes":       s["note_count"],
            "Accounts":    len(s["accounts"]),
            "Last Active": s["latest_date"][:10] if s["latest_date"] else "",
        })

    cols = [("Team Member","Team Member"), ("Notes","Notes"),
            ("Accounts","Accounts"), ("Last Active","Last Active")]
    html = _HTML_WRAPPER.format(
        title=f"Team Performance — {date_label}",
        subtitle=f"{len(records)} team member{'s' if len(records) != 1 else ''} with activity",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"Team Performance — {today} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"team_activity_{today}.csv",
        recipients=recipients,
    )


# ── Last App Date Report ────────────────────────────────────────────────────

@app.get("/api/report/last-app-date")
async def report_last_app_date(
    from_date: Optional[date] = Query(None),
    to_date:   Optional[date] = Query(None),
    preset:    Optional[str]  = Query(None),
    format:    str            = Query("json"),
    _: None    = Depends(require_auth),
):
    """Accounts where CF140 (Last App Date) falls within the given window (default: last 18 months)."""
    today    = date.today()
    _start, _end = _resolve_date_range(from_date, to_date, preset,
                                       default_start=today - timedelta(days=548),
                                       default_end=today)
    if _start is None: _start = today - timedelta(days=548)
    if _end   is None: _end   = today
    date_label = f"{_start} to {_end}"

    cf_map       = await _fetch_acct_cf_map({"140", "18", "23", "76"})
    all_accounts = await ac_get_all("accounts", "accounts", {})
    acct_by_id   = {str(a.get("id", "")): a for a in all_accounts}

    records = []
    for aid, cfs in cf_map.items():
        val = cfs.get("140", "")
        if not val:
            continue
        date_str = str(val)[:10]
        try:
            d = date.fromisoformat(date_str)
            if d < _start or d > _end:
                continue
        except Exception:
            continue
        a = acct_by_id.get(aid, {})
        records.append({
            "Account":       a.get("name", ""),
            "Dealer ID":     cfs.get("18", "") or _account_to_dealer.get(aid, ""),
            "Region":        cfs.get("23", ""),
            "Account Type":  cfs.get("76", ""),
            "Last App Date": date_str,
        })
    records.sort(key=lambda x: x["Last App Date"], reverse=True)

    if format == "csv":
        out = io.StringIO()
        if records:
            w = csv.DictWriter(out, fieldnames=records[0].keys())
            w.writeheader(); w.writerows(records)
        fn = f"last_app_date_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})
    return {"count": len(records), "date_range": date_label, "records": records}


async def _job_last_app_date(start_date: Optional[date] = None, end_date: Optional[date] = None,
                              preset: Optional[str] = None, recipients: list = None):
    today    = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_start=today - timedelta(days=548),
                                       default_end=today)
    if _start is None: _start = today - timedelta(days=548)
    if _end   is None: _end   = today
    date_label = f"{_start} to {_end}"
    print(f"[reports] Last App Date {date_label}")

    cf_map       = await _fetch_acct_cf_map({"140", "18", "23", "76"})
    all_accounts = await ac_get_all("accounts", "accounts", {})
    acct_by_id   = {str(a.get("id", "")): a for a in all_accounts}

    records = []
    for aid, cfs in cf_map.items():
        val = cfs.get("140", "")
        if not val:
            continue
        date_str = str(val)[:10]
        try:
            d = date.fromisoformat(date_str)
            if d < _start or d > _end:
                continue
        except Exception:
            continue
        a = acct_by_id.get(aid, {})
        records.append({
            "Account":       a.get("name", ""),
            "Dealer ID":     cfs.get("18", "") or _account_to_dealer.get(aid, ""),
            "Region":        cfs.get("23", ""),
            "Account Type":  cfs.get("76", ""),
            "Last App Date": date_str,
        })
    records.sort(key=lambda x: x["Last App Date"], reverse=True)

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"), ("Region","Region"),
            ("Account Type","Account Type"), ("Last App Date","Last App Date")]
    html = _HTML_WRAPPER.format(
        title=f"Last App Date — {date_label}",
        subtitle=f"{len(records)} account{'s' if len(records) != 1 else ''}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"Last App Date — {date_label} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"last_app_date_{_start}_{_end}.csv",
        recipients=recipients,
    )


# ── Last RPA Date Report ─────────────────────────────────────────────────────

@app.get("/api/report/last-rpa-date")
async def report_last_rpa_date(
    from_date: Optional[date] = Query(None),
    to_date:   Optional[date] = Query(None),
    preset:    Optional[str]  = Query(None),
    format:    str            = Query("json"),
    _: None    = Depends(require_auth),
):
    """Accounts where CF38 (Last RPA Date) falls within the given window (default: last 18 months)."""
    today    = date.today()
    _start, _end = _resolve_date_range(from_date, to_date, preset,
                                       default_start=today - timedelta(days=548),
                                       default_end=today)
    if _start is None: _start = today - timedelta(days=548)
    if _end   is None: _end   = today
    date_label = f"{_start} to {_end}"

    cf_map       = await _fetch_acct_cf_map({"38", "18", "23", "76"})
    all_accounts = await ac_get_all("accounts", "accounts", {})
    acct_by_id   = {str(a.get("id", "")): a for a in all_accounts}

    records = []
    for aid, cfs in cf_map.items():
        val = cfs.get("38", "")
        if not val:
            continue
        date_str = str(val)[:10]
        try:
            d = date.fromisoformat(date_str)
            if d < _start or d > _end:
                continue
        except Exception:
            continue
        a = acct_by_id.get(aid, {})
        records.append({
            "Account":       a.get("name", ""),
            "Dealer ID":     cfs.get("18", "") or _account_to_dealer.get(aid, ""),
            "Region":        cfs.get("23", ""),
            "Account Type":  cfs.get("76", ""),
            "Last RPA Date": date_str,
        })
    records.sort(key=lambda x: x["Last RPA Date"], reverse=True)

    if format == "csv":
        out = io.StringIO()
        if records:
            w = csv.DictWriter(out, fieldnames=records[0].keys())
            w.writeheader(); w.writerows(records)
        fn = f"last_rpa_date_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(iter([out.getvalue()]), media_type="text/csv",
                                 headers={"Content-Disposition": f"attachment; filename={fn}"})
    return {"count": len(records), "date_range": date_label, "records": records}


async def _job_last_rpa_date(start_date: Optional[date] = None, end_date: Optional[date] = None,
                              preset: Optional[str] = None, recipients: list = None):
    today    = date.today()
    _start, _end = _resolve_date_range(start_date, end_date, preset,
                                       default_start=today - timedelta(days=548),
                                       default_end=today)
    if _start is None: _start = today - timedelta(days=548)
    if _end   is None: _end   = today
    date_label = f"{_start} to {_end}"
    print(f"[reports] Last RPA Date {date_label}")

    cf_map       = await _fetch_acct_cf_map({"38", "18", "23", "76"})
    all_accounts = await ac_get_all("accounts", "accounts", {})
    acct_by_id   = {str(a.get("id", "")): a for a in all_accounts}

    records = []
    for aid, cfs in cf_map.items():
        val = cfs.get("38", "")
        if not val:
            continue
        date_str = str(val)[:10]
        try:
            d = date.fromisoformat(date_str)
            if d < _start or d > _end:
                continue
        except Exception:
            continue
        a = acct_by_id.get(aid, {})
        records.append({
            "Account":       a.get("name", ""),
            "Dealer ID":     cfs.get("18", "") or _account_to_dealer.get(aid, ""),
            "Region":        cfs.get("23", ""),
            "Account Type":  cfs.get("76", ""),
            "Last RPA Date": date_str,
        })
    records.sort(key=lambda x: x["Last RPA Date"], reverse=True)

    cols = [("Account","Account"), ("Dealer ID","Dealer ID"), ("Region","Region"),
            ("Account Type","Account Type"), ("Last RPA Date","Last RPA Date")]
    html = _HTML_WRAPPER.format(
        title=f"Last RPA Date — {date_label}",
        subtitle=f"{len(records)} account{'s' if len(records) != 1 else ''}",
        table=_html_table(records, cols),
        timestamp=datetime.now().strftime("%b %d %Y %H:%M"),
    )
    await _send_email(
        subject=f"Last RPA Date — {date_label} ({len(records)} records)",
        html=html,
        csv_data=_csv_bytes(records),
        csv_name=f"last_rpa_date_{_start}_{_end}.csv",
        recipients=recipients,
    )


# ── Manual / GitHub Actions trigger ──────────────────────────────────────

_REPORT_JOBS = {
    "activations":          _job_activations,
    "license-expiration":   _job_license_expiration,
    "bdr-summary":          _job_bdr_summary,
    "training-activity":    _job_training_activity,
    "stale-untrained":      _job_stale_untrained,
    "account-status":       _job_account_status,
    "platform-breakdown":   _job_platform_breakdown,
    "partner-activation":   _job_partner_activation,
    "oracle-missing":       _job_oracle_missing,
    "account-activity":     _job_account_activity,
    "team-activity":        _job_team_activity,
    "last-app-date":        _job_last_app_date,
    "last-rpa-date":        _job_last_rpa_date,
}

@app.get("/api/send-report/{report_type}")
async def trigger_report(
    report_type: str,
    start_date:  Optional[date] = Query(None, description="Start of date range (YYYY-MM-DD)"),
    end_date:    Optional[date] = Query(None, description="End of date range (YYYY-MM-DD)"),
    preset:      Optional[str]  = Query(None,
        description="Date preset: yesterday | last_week | last_7_days | last_30_days | "
                    "last_90_days | this_week | this_month | last_month"),
    to:          Optional[str]  = Query(None,
        description="Override recipients — comma-separated email addresses"),
    _: None = Depends(require_auth),
):
    """Manually trigger a report email. Also called by GitHub Actions on schedule.
    Use preset OR explicit start_date/end_date to override the default date window.
    Pass to= to override the configured REPORT_RECIPIENTS list."""
    job = _REPORT_JOBS.get(report_type)
    if not job:
        raise HTTPException(
            status_code=404,
            detail=f"Unknown report '{report_type}'. Valid: {list(_REPORT_JOBS)}"
        )
    override_recipients = [r.strip() for r in to.split(",") if r.strip()] if to else None
    final_recipients    = override_recipients or _RECIPIENTS
    if not _SMTP_USER or not _SMTP_PASS:
        raise HTTPException(status_code=503, detail="Email not configured — set SMTP_USER and SMTP_PASS in Render environment variables")
    if not final_recipients:
        raise HTTPException(status_code=400, detail="No recipients — enter an email address in the To field")
    try:
        await job(start_date=start_date, end_date=end_date, preset=preset,
                  recipients=override_recipients)
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"Email failed: {exc}")
    return {"status": "sent", "report": report_type,
            "start_date": str(start_date) if start_date else None,
            "end_date":   str(end_date)   if end_date   else None,
            "preset":     preset,
            "recipients": final_recipients}


# ── SLP field sync ────────────────────────────────────────────────────────────
# Copies missing field values from the linked account's custom fields into
# the SLP custom object record.  Fields synced:
#   dealer-id    ← _account_to_dealer index (account customfield 18)
#   platform     ← account customfield 29  (Dealer Program)
#   assigned-bdr ← account customfield 119 (Assigned BDR)

_SLP_SYNC_FIELDS = [
    # (slp_field_id, account_cf_id_str)  — None means use the dealer-id index
    ("dealer-id",    None),
    ("platform",     "29"),
    ("assigned-bdr", "119"),
]

# Holds the last sync job result/status in memory
_slp_sync_status: dict = {"status": "idle"}

async def _run_slp_sync(dry_run: bool) -> None:
    """Background worker — pages through SLP records and fills blank fields from account CFs."""
    global _slp_sync_status
    _slp_sync_status = {"status": "running", "dry_run": dry_run,
                        "scanned": 0, "updated": 0, "skipped": 0, "errors": 0,
                        "started": datetime.utcnow().isoformat()}

    PAGE_SIZE = 100

    scanned = updated = skipped = errors = 0
    preview = []
    offset  = 0

    # Ensure the in-memory account indexes are populated before scanning
    if not _account_to_dealer:
        _slp_sync_status["status_detail"] = "building account index…"
        print("[sync-slp] Account index empty — rebuilding before sync")
        await _build_dealer_id_index()
        print(f"[sync-slp] Index ready: {len(_account_to_dealer)} accounts")
    _slp_sync_status["indexed_accounts"] = len(_account_to_dealer)

    try:
        while True:
            page    = await ac_get(f"customObjects/records/{SLP_SCHEMA_ID}",
                                   {"limit": PAGE_SIZE, "offset": offset})
            records = page.get("records", [])
            if not records:
                break

            for r in records:
                scanned += 1
                rec_id = r.get("id")
                fields = {fo["id"]: fo.get("value") for fo in r.get("fields", [])}
                rel    = r.get("relationships", {}).get("account", [])
                # rel may be list of ints OR list of dicts — normalise to string id
                if rel:
                    first = rel[0]
                    acc_id = str(first.get("id", first) if isinstance(first, dict) else first)
                else:
                    acc_id = None

                # Debug first 3 records so we can verify structure
                if scanned <= 3:
                    print(f"[sync-slp][debug] rec={rec_id} acc_id={acc_id} "
                          f"fields={list(fields.keys())} "
                          f"dealer_lookup={_account_to_dealer.get(acc_id,'MISS')} "
                          f"platform_lookup={_account_to_platform.get(acc_id,'MISS')}")

                to_update = []
                for slp_fid, cf_id in _SLP_SYNC_FIELDS:
                    if fields.get(slp_fid):
                        continue
                    if cf_id is None:          # dealer-id → dealer index
                        val = _account_to_dealer.get(acc_id, "") if acc_id else ""
                    elif cf_id == "29":        # platform → platform index
                        val = _account_to_platform.get(acc_id, "") if acc_id else ""
                    elif cf_id == "119":       # BDR → BDR index
                        val = _account_to_bdr.get(acc_id, "") if acc_id else ""
                    else:
                        val = ""
                    if val:
                        to_update.append({"id": slp_fid, "value": val})

                if not to_update:
                    skipped += 1
                    # Track why first few records were skipped for diagnostics
                    if skipped <= 5:
                        reason = "no_account" if not acc_id else "no_index_match"
                        _slp_sync_status.setdefault("skip_samples", []).append(
                            {"rec": rec_id, "acc_id": acc_id, "reason": reason,
                             "field_keys": list(fields.keys())})
                    continue

                if dry_run:
                    if len(preview) < 50:
                        preview.append({"record_id": rec_id, "account_id": acc_id,
                                        "fields": to_update})
                    updated += 1
                    continue

                try:
                    await ac_post(f"customObjects/records/{SLP_SCHEMA_ID}",
                                  {"record": {"id": rec_id, "fields": to_update}})
                    updated += 1
                except Exception as e:
                    errors += 1
                    err_str = str(e)
                    print(f"[sync-slp] Error updating record {rec_id}: {err_str}")
                    _slp_sync_status["last_error"] = f"record {rec_id}: {err_str}"

            # Update live progress
            _slp_sync_status.update({"scanned": scanned, "updated": updated,
                                     "skipped": skipped, "errors": errors})
            del records
            offset += PAGE_SIZE
            if len(page.get("records", [])) < PAGE_SIZE:
                break

        _slp_sync_status.update({"status": "done", "scanned": scanned, "updated": updated,
                                  "skipped": skipped, "errors": errors,
                                  "finished": datetime.utcnow().isoformat()})
        if dry_run:
            _slp_sync_status["preview"] = preview
        print(f"[sync-slp] Done — scanned={scanned} updated={updated} skipped={skipped} errors={errors}")

    except Exception as e:
        _slp_sync_status.update({"status": "error", "detail": str(e),
                                  "scanned": scanned, "updated": updated,
                                  "skipped": skipped, "errors": errors})
        print(f"[sync-slp] Fatal error: {e}")


def _check_sync_token(token: str = Query(..., description="SYNC_TOKEN value from Render env")):
    if not _SYNC_TOKEN:
        return   # not configured → open (local dev)
    if not secrets.compare_digest(token, _SYNC_TOKEN):
        raise HTTPException(status_code=401, detail="Invalid sync token")


@app.post("/api/sync-slp-fields")
async def sync_slp_fields(
    dry_run: bool = Query(False, description="Preview changes without writing to AC"),
    _: None = Depends(_check_sync_token),
):
    """Start a background sync of missing SLP fields from account data.

    Returns immediately. Poll GET /api/sync-slp-fields/status to track progress.
    Authenticate with ?token=<SYNC_TOKEN>.
    """
    if _slp_sync_status.get("status") == "running":
        return {"status": "already_running", "progress": _slp_sync_status}
    asyncio.create_task(_run_slp_sync(dry_run))
    return {"status": "started", "dry_run": dry_run,
            "poll": "/api/sync-slp-fields/status"}


@app.get("/api/sync-slp-fields/status")
async def sync_slp_fields_status(
    _: None = Depends(_check_sync_token),
):
    """Check the status/results of the last sync-slp-fields run."""
    return _slp_sync_status


@app.get("/api/report/ars-360")
async def report_ars_360(
    format: str = Query("json"),
    _: None = Depends(require_auth),
):
    """ARS dealers on 360 Finance program with all linked contacts."""
    all_accounts = await ac_get_all("accounts", "accounts", {})

    # Filter to ARS accounts on 360 Finance
    ars_accounts = {
        str(a["id"]): a
        for a in all_accounts
        if "ARS" in a.get("name", "").upper()
        and _account_to_platform.get(str(a["id"]), "") == "360 Finance"
    }

    # Fetch contacts per account using accountContacts (more reliable than
    # filtering all contacts by the account field)
    by_account: dict = defaultdict(list)
    async def _fetch_contacts_for_account(aid: str):
        try:
            ac_resp = await ac_get(f"accounts/{aid}/accountContacts")
            contact_ids = [
                str(ac.get("contact"))
                for ac in ac_resp.get("accountContacts", [])
                if ac.get("contact")
            ]
            contacts = []
            for cid in contact_ids:
                try:
                    cr = await ac_get(f"contacts/{cid}")
                    contacts.append(cr.get("contact", {}))
                except Exception:
                    pass
            return aid, contacts
        except Exception:
            return aid, []

    contact_results = await asyncio.gather(
        *[_fetch_contacts_for_account(aid) for aid in ars_accounts]
    )
    for aid, contacts in contact_results:
        by_account[aid] = contacts

    results = []
    for aid, acct in sorted(ars_accounts.items(), key=lambda x: x[1].get("name", "")):
        contacts = by_account.get(aid, [])
        base = {
            "dealer_id":      _account_to_dealer.get(aid, ""),
            "dealer_name":    acct.get("name", ""),
            "dealer_program": _account_to_platform.get(aid, ""),
            "bdr":            _account_to_bdr.get(aid, ""),
        }
        if contacts:
            for c in contacts:
                results.append({**base,
                    "contact_first": c.get("firstName", ""),
                    "contact_last":  c.get("lastName", ""),
                    "contact_email": c.get("email", ""),
                    "contact_phone": c.get("phone", ""),
                })
        else:
            results.append({**base,
                "contact_first": "", "contact_last": "",
                "contact_email": "", "contact_phone": "",
            })

    if format == "csv":
        return _csv_response(results, f"ars_360_{datetime.now().strftime('%Y%m%d')}.csv")
    return {"count": len(results), "records": results}


# ── Smart Query — Claude-powered NL → report intent ──────────────────────────

_SMART_SYSTEM = """You are a report-routing assistant for Microf, a financing platform.
Your job: parse a natural-language query and return a JSON object describing which report to run.

Available report types (use exactly these values for "report_type"):
  activations       — new contractor/dealer activations
  training-summary  — training records and completions
  license-expiration — licenses expiring soon or already expired
  bdr-summary       — BDR (Business Development Rep) performance summary
  team-activity     — internal team activity / notes
  account-activity  — account engagement / cold accounts with no activity
  dealer-profile    — look up a specific dealer by ID

Available platforms (use exactly as shown, or null):
  "360 Finance", "OPTIMUS", "LTO", "Microf", "SpectrumAC",
  "ACIMA", "FlexShopper", "Snap", "Kornerstone", "GreenSky", "UOWn", "Wells"

Today's date: {today}

Return ONLY valid JSON, no prose. Schema:
{{
  "report_type": "<one of the above, or null if truly ambiguous>",
  "from_date": "<YYYY-MM-DD or null>",
  "to_date": "<YYYY-MM-DD or null>",
  "platform": "<platform name or null>",
  "bdr": "<BDR name/username or null>",
  "dealer_id": "<numeric dealer ID or null>",
  "cold_accounts": <true if user wants accounts with no activity, else false>,
  "days_ahead": <integer if asking about upcoming license expiration, else null>,
  "include_expired": <true if asking about already-expired licenses, else false>,
  "explanation": "<one short sentence describing what you understood, shown to the user>",
  "error": "<only if truly cannot map to a report; leave null otherwise>"
}}

Examples:
  "activations for Optimus last month"
  → {{"report_type":"activations","from_date":"<first of last month>","to_date":"<last of last month>","platform":"OPTIMUS","bdr":null,"dealer_id":null,"cold_accounts":false,"days_ahead":null,"include_expired":false,"explanation":"Activations for OPTIMUS platform last month","error":null}}

  "which BDR signed up the most 360 Finance partners this quarter"
  → {{"report_type":"bdr-summary","from_date":"<Q start>","to_date":"<today>","platform":"360 Finance","bdr":null,"dealer_id":null,"cold_accounts":false,"days_ahead":null,"include_expired":false,"explanation":"BDR summary for 360 Finance this quarter","error":null}}

  "show me licenses expiring in the next 60 days"
  → {{"report_type":"license-expiration","from_date":null,"to_date":null,"platform":null,"bdr":null,"dealer_id":null,"cold_accounts":false,"days_ahead":60,"include_expired":false,"explanation":"Licenses expiring in the next 60 days","error":null}}

  "dealers who haven't had any activity in 6 months"
  → {{"report_type":"account-activity","from_date":"<6 months ago>","to_date":"<today>","platform":null,"bdr":null,"dealer_id":null,"cold_accounts":true,"days_ahead":null,"include_expired":false,"explanation":"Cold accounts with no activity in the last 6 months","error":null}}
"""


@app.get("/api/smart-query")
async def smart_query_endpoint(q: str, user=Depends(require_auth)):
    """Parse a natural-language query with Claude and return structured report intent."""
    if not q.strip():
        raise HTTPException(400, "Query required")

    if not _ANTHROPIC_KEY:
        return {"error": "ANTHROPIC_API_KEY not configured", "fallback": True}

    today = datetime.now().strftime("%Y-%m-%d")
    system = _SMART_SYSTEM.format(today=today)

    try:
        import anthropic as _anthropic
        client = _anthropic.AsyncAnthropic(api_key=_ANTHROPIC_KEY)
        msg = await client.messages.create(
            model="claude-3-5-haiku-20241022",
            max_tokens=512,
            system=system,
            messages=[{"role": "user", "content": q}],
        )
        raw = msg.content[0].text.strip()
        # Strip markdown code fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        import json as _json
        parsed = _json.loads(raw)
        return parsed
    except Exception as e:
        print(f"[smart-query] Claude parse error: {e}")
        return {"error": str(e), "fallback": True}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)
