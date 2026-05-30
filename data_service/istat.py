"""
ISTAT SDMX REST client for Sandwich Club data service.

Hard constraints from ISTAT:
- 5 queries / minute / IP — exceeding triggers a 1-2 day block
- /rest/data endpoint has a known bug: endPeriod returns +1 year of data
- /rest/v2/data not yet implemented
- Slow endpoint (some queries take 2+ minutes)

Design:
- Aggressive on-disk caching by TTL (monthly data cached for 24h)
- Token bucket rate limiter (max 4/minute to stay safely below the cap)
- Pre-curated dataflow registry — no live discovery
- CSV format (smaller than XML, easier to parse)
- All sync fetches isolated to a single thread to prevent burst usage
"""

import os
import json
import time
import hashlib
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional
import io
import httpx
import pandas as pd

# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

ISTAT_BASE = "https://esploradati.istat.it/SDMXWS/rest"
CACHE_DIR = Path(os.getenv("ISTAT_CACHE_DIR", "/tmp/istat_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)

# Stay safely below 5/min cap
MAX_QUERIES_PER_MINUTE = 4
REQUEST_TIMEOUT = 180  # ISTAT can be slow

# ─────────────────────────────────────────────
# Curated dataflow registry
# ─────────────────────────────────────────────
# Each entry: friendly name → (dataflow_id, key_filter, ttl_hours, description)
# key_filter follows SDMX dot-notation for dimension filtering.
# Use "" if you want everything (then filter in pandas afterwards).
#
# Most ISTAT monthly indicators refresh ~once per month, so 24h cache is safe.
# Quarterly: 7-day cache. Confidence indices: 24h.

ISTAT_DATAFLOWS = {
    "industrial_production": {
        "id": "163_156",
        "description": "Indice della produzione industriale (base 2021=100)",
        "ttl_hours": 24,
        "headline_metric": "production_index_yoy",
    },
    "consumer_confidence": {
        "id": "92_499",
        "description": "Clima di fiducia dei consumatori",
        "ttl_hours": 24,
        "headline_metric": "consumer_climate_index",
    },
    "business_confidence": {
        "id": "92_500",
        "description": "Clima di fiducia delle imprese",
        "ttl_hours": 24,
        "headline_metric": "business_climate_index",
    },
    "unemployment": {
        "id": "151_914",
        "description": "Tasso di disoccupazione mensile",
        "ttl_hours": 24,
        "headline_metric": "unemployment_rate",
    },
    "gdp_quarterly": {
        "id": "163_158",
        "description": "PIL trimestrale a prezzi correnti e concatenati",
        "ttl_hours": 168,
        "headline_metric": "gdp_qoq",
    },
    "cpi_inflation": {
        "id": "151_913",
        "description": "NIC - Indice nazionale dei prezzi al consumo",
        "ttl_hours": 24,
        "headline_metric": "cpi_yoy",
    },
    "retail_trade": {
        "id": "163_159",
        "description": "Commercio al dettaglio - indici di valore e volume",
        "ttl_hours": 24,
        "headline_metric": "retail_volume_yoy",
    },
    "exports": {
        "id": "163_166",
        "description": "Esportazioni e importazioni di merci",
        "ttl_hours": 24,
        "headline_metric": "exports_yoy",
    },
    "construction": {
        "id": "163_157",
        "description": "Indice della produzione nelle costruzioni",
        "ttl_hours": 24,
        "headline_metric": "construction_yoy",
    },
    "wages": {
        "id": "151_912",
        "description": "Retribuzioni contrattuali per dipendente",
        "ttl_hours": 168,
        "headline_metric": "wages_yoy",
    },
}

# ─────────────────────────────────────────────
# Rate limiter — token bucket, thread-safe
# ─────────────────────────────────────────────

class TokenBucket:
    """Simple token bucket. Allows MAX_QUERIES_PER_MINUTE sustained, with refill."""

    def __init__(self, capacity: int = MAX_QUERIES_PER_MINUTE, refill_per_minute: int = MAX_QUERIES_PER_MINUTE):
        self.capacity = capacity
        self.tokens = float(capacity)
        self.refill_rate = refill_per_minute / 60.0  # tokens per second
        self.last_refill = time.monotonic()
        self.lock = threading.Lock()

    def acquire(self, tokens: int = 1, timeout: float = 90.0) -> bool:
        deadline = time.monotonic() + timeout
        while True:
            with self.lock:
                now = time.monotonic()
                elapsed = now - self.last_refill
                self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
                self.last_refill = now
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return True
            if time.monotonic() > deadline:
                return False
            time.sleep(0.5)


_bucket = TokenBucket()

# ─────────────────────────────────────────────
# Cache
# ─────────────────────────────────────────────

def _cache_path(key: str) -> Path:
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    return CACHE_DIR / f"{h}.json"

def _cache_get(key: str, ttl_hours: int):
    p = _cache_path(key)
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text())
        ts = datetime.fromisoformat(payload["cached_at"])
        if datetime.utcnow() - ts < timedelta(hours=ttl_hours):
            return payload["data"]
    except Exception:
        return None
    return None

def _cache_set(key: str, data):
    p = _cache_path(key)
    try:
        p.write_text(json.dumps({
            "cached_at": datetime.utcnow().isoformat(),
            "data": data
        }))
    except Exception:
        pass

# ─────────────────────────────────────────────
# Core fetch
# ─────────────────────────────────────────────

def _fetch_istat_csv_sync(dataflow_id: str, start_period: Optional[str] = None) -> Optional[str]:
    """
    Synchronous fetch with rate limiting.
    Returns CSV string or None on failure.
    """
    if not _bucket.acquire(tokens=1, timeout=120):
        return None

    url = f"{ISTAT_BASE}/data/{dataflow_id}"
    params = {}
    if start_period:
        params["startPeriod"] = start_period

    headers = {
        "Accept": "application/vnd.sdmx.data+csv;version=1.0.0",
        "User-Agent": "SandwichClub-ThinkTank/1.0"
    }

    try:
        with httpx.Client(timeout=REQUEST_TIMEOUT, follow_redirects=True) as client:
            r = client.get(url, params=params, headers=headers)
            if r.status_code == 200 and r.text:
                return r.text
            return None
    except Exception:
        return None


def _parse_istat_csv(csv_text: str) -> pd.DataFrame:
    """
    Parse ISTAT CSV response into a clean DataFrame.
    Standard columns: TIME_PERIOD, OBS_VALUE, plus dimension columns.
    """
    try:
        df = pd.read_csv(io.StringIO(csv_text))
        if "TIME_PERIOD" not in df.columns or "OBS_VALUE" not in df.columns:
            return pd.DataFrame()
        df["OBS_VALUE"] = pd.to_numeric(df["OBS_VALUE"], errors="coerce")
        df = df.dropna(subset=["OBS_VALUE"])
        # Sort by time period
        df = df.sort_values("TIME_PERIOD")
        return df
    except Exception:
        return pd.DataFrame()


def _extract_latest_with_yoy(df: pd.DataFrame) -> dict:
    """
    Generic extractor: latest observation + YoY change if periodicity permits.
    Looks for the most common dimension combination (usually national level, total)
    and returns clean latest + YoY.
    """
    if df.empty:
        return {"error": "no_data"}

    # Try to identify the most likely "headline" series:
    # filter to ITALIA / total / not seasonally adjusted by default if those dims exist
    candidate = df.copy()

    # Common filters that ISTAT applies — drop them if present
    for col, preferred in [
        ("REF_AREA", ["IT", "ITC", "ITF", "ITG", "ITH", "ITI"]),  # IT first
        ("ITTER107", ["IT", "ITALIA"]),
        ("ATECO_2007", ["TOTAL", "TOT", "B-D"]),
        ("ADJUSTMENT", ["N", "NSA"]),  # not seasonally adjusted, raw
    ]:
        if col in candidate.columns:
            for pref in preferred:
                subset = candidate[candidate[col].astype(str).str.upper().str.contains(pref, na=False)]
                if not subset.empty:
                    candidate = subset
                    break

    # If still many dimension combinations, take the one with most observations
    if len(candidate) > 50:
        # group by all non-time dimensions, find largest series
        dims = [c for c in candidate.columns if c not in ["TIME_PERIOD", "OBS_VALUE"]]
        if dims:
            grouped = candidate.groupby(dims).size().reset_index(name="n")
            top = grouped.sort_values("n", ascending=False).iloc[0]
            mask = pd.Series(True, index=candidate.index)
            for d in dims:
                mask &= candidate[d] == top[d]
            candidate = candidate[mask]

    candidate = candidate.sort_values("TIME_PERIOD")

    if candidate.empty:
        return {"error": "no_filtered_data"}

    latest = candidate.iloc[-1]
    latest_period = str(latest["TIME_PERIOD"])
    latest_value = float(latest["OBS_VALUE"])

    result = {
        "latest_period": latest_period,
        "latest_value": round(latest_value, 4),
        "n_observations": len(candidate),
    }

    # YoY calculation
    # Detect periodicity from period string format
    if "-" in latest_period:
        parts = latest_period.split("-")
        if len(parts) == 2 and len(parts[1]) == 2:
            # Monthly: YYYY-MM. Look back 12 months.
            target = f"{int(parts[0])-1}-{parts[1]}"
        elif "Q" in latest_period:
            target = latest_period.replace(parts[0], str(int(parts[0])-1), 1)
        else:
            target = None
    else:
        # Annual: YYYY
        try:
            target = str(int(latest_period) - 1)
        except ValueError:
            target = None

    if target:
        prev = candidate[candidate["TIME_PERIOD"].astype(str) == target]
        if not prev.empty:
            prev_value = float(prev.iloc[0]["OBS_VALUE"])
            if prev_value != 0:
                yoy = ((latest_value - prev_value) / prev_value) * 100
                result["yoy_pct"] = round(yoy, 2)
                result["prev_year_period"] = target
                result["prev_year_value"] = round(prev_value, 4)

    return result


# ─────────────────────────────────────────────
# Public functions
# ─────────────────────────────────────────────

async def fetch_istat_indicator(indicator: str) -> dict:
    """
    Fetch a single ISTAT indicator by friendly name.
    Returns: { source, dataflow_id, description, latest_period, latest_value, yoy_pct, ... }
    """
    cfg = ISTAT_DATAFLOWS.get(indicator)
    if not cfg:
        return {"error": f"unknown_indicator: {indicator}", "available": list(ISTAT_DATAFLOWS.keys())}

    cache_key = f"istat:{cfg['id']}"
    cached = _cache_get(cache_key, cfg["ttl_hours"])
    if cached:
        return {**cached, "from_cache": True}

    # Fetch last ~3 years for YoY context
    start = (datetime.utcnow() - timedelta(days=36*30)).strftime("%Y-%m")

    loop = asyncio.get_event_loop()
    csv_text = await loop.run_in_executor(None, _fetch_istat_csv_sync, cfg["id"], start)

    if not csv_text:
        # Fall through with cached value if available regardless of TTL
        stale = _cache_get(cache_key, ttl_hours=24*30)
        if stale:
            return {**stale, "from_cache": True, "stale": True}
        return {
            "error": "fetch_failed",
            "indicator": indicator,
            "dataflow_id": cfg["id"],
            "hint": "ISTAT API may be slow or rate-limited"
        }

    df = _parse_istat_csv(csv_text)
    extracted = _extract_latest_with_yoy(df)

    result = {
        "source": "ISTAT",
        "indicator": indicator,
        "dataflow_id": cfg["id"],
        "description": cfg["description"],
        "fetched_at": datetime.utcnow().isoformat(),
        **extracted
    }

    if "error" not in extracted:
        _cache_set(cache_key, result)

    return result


async def fetch_istat_italy_snapshot() -> dict:
    """
    Master endpoint: returns all curated ISTAT indicators in one call.
    Uses cache aggressively; first call after restart may take ~2 minutes
    (10 indicators × ~15s each at rate limit).
    """
    indicators = list(ISTAT_DATAFLOWS.keys())

    # Fetch in sequence (rate limit prevents parallelism)
    results = {}
    for ind in indicators:
        try:
            results[ind] = await fetch_istat_indicator(ind)
        except Exception as e:
            results[ind] = {"error": str(e)}

    # Build human-readable summary
    summary_lines = []
    for ind, data in results.items():
        if data.get("error"):
            continue
        line = f"  {data.get('description', ind)}: {data.get('latest_value')} ({data.get('latest_period')})"
        if "yoy_pct" in data:
            line += f" — YoY {data['yoy_pct']:+.1f}%"
        summary_lines.append(line)

    return {
        "source": "ISTAT (Italian National Institute of Statistics)",
        "timestamp": datetime.utcnow().isoformat(),
        "indicators": results,
        "summary_markdown": "### 🇮🇹 ISTAT — Italian Macro Hard Data\n" + "\n".join(summary_lines)
    }


def list_istat_dataflows() -> dict:
    """Return the curated dataflow registry."""
    return {
        "source": "ISTAT",
        "endpoint": ISTAT_BASE,
        "rate_limit": f"{MAX_QUERIES_PER_MINUTE}/minute (configured below ISTAT cap of 5)",
        "available": {
            name: {
                "dataflow_id": cfg["id"],
                "description": cfg["description"],
                "ttl_hours": cfg["ttl_hours"]
            } for name, cfg in ISTAT_DATAFLOWS.items()
        }
    }
