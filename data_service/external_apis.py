"""
External data sources for Sandwich Club fact-checking layer.

Free, authoritative, no-auth APIs:
- ECB Data Portal (rates, FX, yields)
- Eurostat SDMX (EU comparable stats)
- FRED (US benchmarks)
- World Bank (cross-country, long-term)
- IMF SDMX (IFS, fiscal)
- BIS Stats (banking, cross-border)
- OECD SDMX (cross-country)

Design:
- Each function returns a normalized dict: { source, indicator, value, date, unit, url }
- All cached for 1h by default — these are reference data, not real-time
- Async-friendly via httpx
- Graceful degradation on API failure
"""

import os
import json
import hashlib
import asyncio
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional
import httpx
import io
import pandas as pd

CACHE_DIR = Path(os.getenv("EXT_CACHE_DIR", "/tmp/ext_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
DEFAULT_TTL_MIN = 60


# ─────────────────────────────────────────────
# Cache helpers
# ─────────────────────────────────────────────

def _cache_path(key: str) -> Path:
    h = hashlib.sha256(key.encode()).hexdigest()[:16]
    return CACHE_DIR / f"{h}.json"

def _cache_get(key: str, ttl_min: int = DEFAULT_TTL_MIN):
    p = _cache_path(key)
    if not p.exists():
        return None
    try:
        payload = json.loads(p.read_text())
        ts = datetime.fromisoformat(payload["cached_at"])
        if datetime.utcnow() - ts < timedelta(minutes=ttl_min):
            return payload["data"]
    except Exception:
        return None
    return None

def _cache_set(key: str, data):
    try:
        _cache_path(key).write_text(json.dumps({
            "cached_at": datetime.utcnow().isoformat(),
            "data": data
        }))
    except Exception:
        pass


# ─────────────────────────────────────────────
# ECB Data Portal
# ─────────────────────────────────────────────
# Docs: https://data.ecb.europa.eu/help/api/data
# Format: https://data-api.ecb.europa.eu/service/data/{flowRef}/{key}?format=jsondata

ECB_INDICATORS = {
    # Policy rates
    "ecb_deposit_rate":   ("FM/B.U2.EUR.4F.KR.DFR.LEV",     "ECB Deposit Facility Rate", "%"),
    "ecb_mro_rate":       ("FM/B.U2.EUR.4F.KR.MRR_FR.LEV",  "ECB MRO Rate",              "%"),
    "ecb_marginal_rate":  ("FM/B.U2.EUR.4F.KR.MLFR.LEV",    "ECB Marginal Lending Rate", "%"),
    # FX rates (per EUR)
    "eurusd_ecb":         ("EXR/D.USD.EUR.SP00.A",          "EUR/USD",                   "USD per EUR"),
    "eurgbp_ecb":         ("EXR/D.GBP.EUR.SP00.A",          "EUR/GBP",                   "GBP per EUR"),
    "eurchf_ecb":         ("EXR/D.CHF.EUR.SP00.A",          "EUR/CHF",                   "CHF per EUR"),
    "eurjpy_ecb":         ("EXR/D.JPY.EUR.SP00.A",          "EUR/JPY",                   "JPY per EUR"),
    "eurcny_ecb":         ("EXR/D.CNY.EUR.SP00.A",          "EUR/CNY",                   "CNY per EUR"),
    # Inflation
    "hicp_ez":            ("ICP/M.U2.N.000000.4.ANR",       "HICP Eurozone YoY",         "%"),
    "hicp_italy":         ("ICP/M.IT.N.000000.4.ANR",       "HICP Italy YoY",            "%"),
    "hicp_germany":       ("ICP/M.DE.N.000000.4.ANR",       "HICP Germany YoY",          "%"),
    # Bond yields
    "btp_10y":            ("FM/M.IT.EUR.4F.BB.U_A_10Y.YLD", "Italian 10Y Yield",         "%"),
    "bund_10y":           ("FM/M.DE.EUR.4F.BB.U_A_10Y.YLD", "German 10Y Yield",          "%"),
    "oat_10y":            ("FM/M.FR.EUR.4F.BB.U_A_10Y.YLD", "French 10Y Yield",          "%"),
    # Eurozone unemployment
    "unemployment_ez":    ("LFSI/M.I9.S.UNEHRT.TOTAL0.15_74.T", "Eurozone Unemployment", "%"),
}

async def fetch_ecb(indicator: str) -> dict:
    cfg = ECB_INDICATORS.get(indicator)
    if not cfg:
        return {"error": f"unknown_ecb_indicator: {indicator}", "available": list(ECB_INDICATORS.keys())}

    series_key, label, unit = cfg
    cached = _cache_get(f"ecb:{indicator}")
    if cached:
        return {**cached, "from_cache": True}

    url = f"https://data-api.ecb.europa.eu/service/data/{series_key}?format=jsondata&lastNObservations=24"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"source": "ECB", "indicator": indicator, "error": f"http_{r.status_code}"}

            data = r.json()
            series = data.get("dataSets", [{}])[0].get("series", {})
            time_periods = (data.get("structure", {})
                                .get("dimensions", {})
                                .get("observation", [{}])[0]
                                .get("values", []))

            # Single series, extract observations
            for k, v in series.items():
                obs = v.get("observations", {})
                if not obs:
                    continue
                # Sort by index
                sorted_obs = sorted(obs.items(), key=lambda x: int(x[0]))
                latest_idx, latest_val = sorted_obs[-1]
                latest_value = float(latest_val[0]) if latest_val[0] is not None else None
                latest_date = time_periods[int(latest_idx)].get("id") if int(latest_idx) < len(time_periods) else None

                # YoY for monthly series
                yoy_pct = None
                if len(sorted_obs) >= 13:
                    prev_idx, prev_val = sorted_obs[-13]
                    if prev_val[0] is not None and latest_value is not None:
                        prev = float(prev_val[0])
                        if prev != 0:
                            yoy_pct = round((latest_value - prev) / prev * 100, 2)

                result = {
                    "source": "ECB Data Portal",
                    "indicator": indicator,
                    "label": label,
                    "value": round(latest_value, 4) if latest_value is not None else None,
                    "date": latest_date,
                    "unit": unit,
                    "yoy_pct": yoy_pct,
                    "url": f"https://data.ecb.europa.eu/data/datasets/{series_key.split('/')[0]}",
                }
                _cache_set(f"ecb:{indicator}", result)
                return result

            return {"source": "ECB", "indicator": indicator, "error": "no_data"}
    except Exception as e:
        return {"source": "ECB", "indicator": indicator, "error": str(e)}


async def fetch_btp_bund_spread() -> dict:
    """Compute BTP-Bund 10Y spread from two ECB queries."""
    cached = _cache_get("ecb:btp_bund_spread")
    if cached:
        return {**cached, "from_cache": True}

    btp, bund = await asyncio.gather(fetch_ecb("btp_10y"), fetch_ecb("bund_10y"))
    if btp.get("value") is None or bund.get("value") is None:
        return {"error": "could_not_compute_spread", "btp": btp, "bund": bund}

    spread_bps = round((btp["value"] - bund["value"]) * 100, 1)
    result = {
        "source": "ECB Data Portal (computed)",
        "label": "BTP-Bund 10Y Spread",
        "value": spread_bps,
        "unit": "bps",
        "btp_yield": btp["value"],
        "bund_yield": bund["value"],
        "date": btp.get("date") or bund.get("date"),
    }
    _cache_set("ecb:btp_bund_spread", result)
    return result


# ─────────────────────────────────────────────
# Eurostat SDMX
# ─────────────────────────────────────────────
# Endpoint: https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{dataset}/{key}

EUROSTAT_INDICATORS = {
    "gdp_growth_ez":       ("namq_10_gdp",     "Q.CLV10_MEUR.SCA.B1GQ.EA20", "Eurozone GDP (chain-linked)", "EUR M"),
    "gdp_growth_it":       ("namq_10_gdp",     "Q.CLV10_MEUR.SCA.B1GQ.IT",   "Italy GDP",                   "EUR M"),
    "unemployment_it":     ("une_rt_m",        "M.SA.PC_ACT.TOTAL.T.IT",      "Italy Unemployment Rate",     "%"),
    "unemployment_ez":     ("une_rt_m",        "M.SA.PC_ACT.TOTAL.T.EA20",    "Eurozone Unemployment Rate",  "%"),
    "unemployment_de":     ("une_rt_m",        "M.SA.PC_ACT.TOTAL.T.DE",      "Germany Unemployment",        "%"),
    "industrial_prod_ez":  ("sts_inpr_m",      "M.PROD.B-D.SCA.I21.EA20",     "Eurozone Industrial Production", "Index"),
    "industrial_prod_it":  ("sts_inpr_m",      "M.PROD.B-D.SCA.I21.IT",       "Italy Industrial Production", "Index"),
    "govt_debt_gdp_it":    ("gov_10dd_edpt1",  "A.PC_GDP.GD.IT",              "Italy Govt Debt/GDP",         "% GDP"),
    "govt_debt_gdp_de":    ("gov_10dd_edpt1",  "A.PC_GDP.GD.DE",              "Germany Govt Debt/GDP",       "% GDP"),
    "govt_deficit_it":     ("gov_10dd_edpt1",  "A.PC_GDP.B9.IT",              "Italy Govt Deficit",          "% GDP"),
}

async def fetch_eurostat(indicator: str) -> dict:
    cfg = EUROSTAT_INDICATORS.get(indicator)
    if not cfg:
        return {"error": f"unknown_eurostat_indicator: {indicator}", "available": list(EUROSTAT_INDICATORS.keys())}

    dataset, key, label, unit = cfg
    cached = _cache_get(f"eurostat:{indicator}")
    if cached:
        return {**cached, "from_cache": True}

    url = f"https://ec.europa.eu/eurostat/api/dissemination/sdmx/2.1/data/{dataset}/{key}?format=JSON&lastTimePeriod=24"

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"source": "Eurostat", "indicator": indicator, "error": f"http_{r.status_code}"}

            data = r.json()
            values = data.get("value", {})
            time_dim = next(
                (d for d in data.get("dimension", {}).get("id", []) if d == "time"),
                None
            )
            time_index = data.get("dimension", {}).get("time", {}).get("category", {}).get("index", {})

            if not values or not time_index:
                return {"source": "Eurostat", "indicator": indicator, "error": "no_data"}

            # values is keyed by flat index, time_index maps period → index
            inv_time = {v: k for k, v in time_index.items()}
            sorted_keys = sorted(values.keys(), key=lambda x: int(x))
            latest_key = sorted_keys[-1]
            latest_value = values[latest_key]
            latest_date = inv_time.get(int(latest_key))

            # YoY
            yoy_pct = None
            if len(sorted_keys) >= 13:
                prev_key = sorted_keys[-13]
                prev_value = values.get(prev_key)
                if prev_value and prev_value != 0:
                    yoy_pct = round((latest_value - prev_value) / prev_value * 100, 2)

            result = {
                "source": "Eurostat",
                "indicator": indicator,
                "label": label,
                "value": round(latest_value, 4),
                "date": latest_date,
                "unit": unit,
                "yoy_pct": yoy_pct,
                "url": f"https://ec.europa.eu/eurostat/databrowser/view/{dataset}/default/table",
            }
            _cache_set(f"eurostat:{indicator}", result)
            return result
    except Exception as e:
        return {"source": "Eurostat", "indicator": indicator, "error": str(e)}


# ─────────────────────────────────────────────
# FRED (St. Louis Fed)
# ─────────────────────────────────────────────
# Needs a free API key for full access, but the fredgraph CSV endpoint works key-free
# for single-series snapshots.
# Note: For production, request a free key at https://fred.stlouisfed.org/docs/api/api_key.html

FRED_INDICATORS = {
    "us_10y_yield":        ("DGS10",    "US 10Y Treasury Yield",       "%"),
    "us_2y_yield":         ("DGS2",     "US 2Y Treasury Yield",        "%"),
    "fed_funds_rate":      ("DFF",      "Fed Funds Effective Rate",    "%"),
    "us_cpi_yoy":          ("CPIAUCSL", "US CPI",                      "Index"),
    "us_unemployment":     ("UNRATE",   "US Unemployment Rate",        "%"),
    "us_dxy":              ("DTWEXBGS", "USD Broad Index",             "Index"),
    "wti_crude":           ("DCOILWTICO","WTI Crude",                  "USD/bbl"),
    "us_gdp_yoy":          ("A191RL1Q225SBEA", "US Real GDP Growth",   "% QoQ ann."),
}

async def fetch_fred(indicator: str) -> dict:
    cfg = FRED_INDICATORS.get(indicator)
    if not cfg:
        return {"error": f"unknown_fred_indicator: {indicator}", "available": list(FRED_INDICATORS.keys())}

    series_id, label, unit = cfg
    cached = _cache_get(f"fred:{indicator}")
    if cached:
        return {**cached, "from_cache": True}

    # Use fredgraph CSV — no key needed for small queries
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={series_id}"

    try:
        async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"source": "FRED", "indicator": indicator, "error": f"http_{r.status_code}"}

            df = pd.read_csv(io.StringIO(r.text))
            df.columns = ["date", "value"]
            df["value"] = pd.to_numeric(df["value"], errors="coerce")
            df = df.dropna(subset=["value"]).sort_values("date")
            if df.empty:
                return {"source": "FRED", "indicator": indicator, "error": "no_data"}

            latest = df.iloc[-1]
            latest_value = float(latest["value"])
            latest_date = str(latest["date"])

            # YoY for monthly data
            yoy_pct = None
            try:
                df["date_dt"] = pd.to_datetime(df["date"])
                target_dt = pd.to_datetime(latest_date) - pd.DateOffset(years=1)
                prev_rows = df[df["date_dt"] <= target_dt]
                if not prev_rows.empty:
                    prev_value = float(prev_rows.iloc[-1]["value"])
                    if prev_value != 0:
                        yoy_pct = round((latest_value - prev_value) / prev_value * 100, 2)
            except Exception:
                pass

            result = {
                "source": "FRED (St. Louis Fed)",
                "indicator": indicator,
                "label": label,
                "value": round(latest_value, 4),
                "date": latest_date,
                "unit": unit,
                "yoy_pct": yoy_pct,
                "url": f"https://fred.stlouisfed.org/series/{series_id}",
            }
            _cache_set(f"fred:{indicator}", result)
            return result
    except Exception as e:
        return {"source": "FRED", "indicator": indicator, "error": str(e)}


# ─────────────────────────────────────────────
# World Bank (already partially used in main.py — formalize here)
# ─────────────────────────────────────────────

WORLDBANK_INDICATORS = {
    # (country_code, wb_indicator_id, label, unit)
    "italy_gdp_growth":    ("IT",  "NY.GDP.MKTP.KD.ZG",         "Italy GDP Growth",        "% YoY"),
    "italy_cpi":           ("IT",  "FP.CPI.TOTL.ZG",            "Italy CPI Inflation",     "% YoY"),
    "italy_unemployment":  ("IT",  "SL.UEM.TOTL.ZS",            "Italy Unemployment",      "% labor force"),
    "italy_debt_gdp":      ("IT",  "GC.DOD.TOTL.GD.ZS",         "Italy Central Govt Debt", "% GDP"),
    "italy_current_acct":  ("IT",  "BN.CAB.XOKA.GD.ZS",         "Italy Current Account",   "% GDP"),
    "italy_fdi":           ("IT",  "BX.KLT.DINV.WD.GD.ZS",      "Italy FDI Inflows",       "% GDP"),
    "italy_exports_gdp":   ("IT",  "NE.EXP.GNFS.ZS",            "Italy Exports",           "% GDP"),
    "italy_gini":          ("IT",  "SI.POV.GINI",               "Italy Gini Index",        "0-100"),
    "ez_gdp_growth":       ("EMU", "NY.GDP.MKTP.KD.ZG",         "Eurozone GDP Growth",     "% YoY"),
    "world_gdp_growth":    ("WLD", "NY.GDP.MKTP.KD.ZG",         "World GDP Growth",        "% YoY"),
    "us_gdp_growth":       ("US",  "NY.GDP.MKTP.KD.ZG",         "US GDP Growth",           "% YoY"),
    "china_gdp_growth":    ("CN",  "NY.GDP.MKTP.KD.ZG",         "China GDP Growth",        "% YoY"),
    "germany_gdp_growth":  ("DE",  "NY.GDP.MKTP.KD.ZG",         "Germany GDP Growth",      "% YoY"),
}

async def fetch_worldbank(indicator: str) -> dict:
    cfg = WORLDBANK_INDICATORS.get(indicator)
    if not cfg:
        return {"error": f"unknown_wb_indicator: {indicator}", "available": list(WORLDBANK_INDICATORS.keys())}

    country, wb_code, label, unit = cfg
    cached = _cache_get(f"wb:{indicator}", ttl_min=24*60)  # WB is annual data, cache a day
    if cached:
        return {**cached, "from_cache": True}

    url = f"https://api.worldbank.org/v2/country/{country}/indicator/{wb_code}?format=json&mrv=1&per_page=1"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"source": "World Bank", "indicator": indicator, "error": f"http_{r.status_code}"}
            data = r.json()
            if len(data) < 2 or not data[1]:
                return {"source": "World Bank", "indicator": indicator, "error": "no_data"}

            entry = data[1][0]
            result = {
                "source": "World Bank",
                "indicator": indicator,
                "label": label,
                "value": round(entry.get("value"), 4) if entry.get("value") is not None else None,
                "date": str(entry.get("date")),
                "unit": unit,
                "url": f"https://data.worldbank.org/indicator/{wb_code}?locations={country}",
            }
            _cache_set(f"wb:{indicator}", result)
            return result
    except Exception as e:
        return {"source": "World Bank", "indicator": indicator, "error": str(e)}


# ─────────────────────────────────────────────
# IMF SDMX
# ─────────────────────────────────────────────
# Docs: https://datahelp.imf.org/knowledgebase/articles/667681
# Format: https://www.imf.org/external/datamapper/api/v1/{indicator}/{country}

IMF_INDICATORS = {
    "italy_gdp_imf":           ("NGDP_RPCH",      "IT",  "Italy Real GDP Growth (IMF WEO)",      "% YoY"),
    "italy_cpi_imf":           ("PCPIPCH",        "IT",  "Italy CPI (IMF WEO)",                  "% YoY"),
    "italy_debt_imf":          ("GGXWDG_NGDP",    "IT",  "Italy General Govt Gross Debt",        "% GDP"),
    "italy_primary_balance":   ("GGXONLB_NGDP",   "IT",  "Italy Primary Balance",                "% GDP"),
    "italy_current_acct_imf":  ("BCA_NGDPD",      "IT",  "Italy Current Account",                "% GDP"),
    "ez_gdp_imf":              ("NGDP_RPCH",      "EUQ", "Eurozone Real GDP Growth",             "% YoY"),
    "germany_gdp_imf":         ("NGDP_RPCH",      "DEU", "Germany Real GDP Growth",              "% YoY"),
    "france_gdp_imf":          ("NGDP_RPCH",      "FRA", "France Real GDP Growth",               "% YoY"),
}

async def fetch_imf(indicator: str) -> dict:
    cfg = IMF_INDICATORS.get(indicator)
    if not cfg:
        return {"error": f"unknown_imf_indicator: {indicator}", "available": list(IMF_INDICATORS.keys())}

    imf_code, country, label, unit = cfg
    cached = _cache_get(f"imf:{indicator}", ttl_min=24*60)  # IMF WEO is biannual
    if cached:
        return {**cached, "from_cache": True}

    url = f"https://www.imf.org/external/datamapper/api/v1/{imf_code}/{country}"

    try:
        async with httpx.AsyncClient(timeout=15) as client:
            r = await client.get(url)
            if r.status_code != 200:
                return {"source": "IMF", "indicator": indicator, "error": f"http_{r.status_code}"}

            data = r.json()
            values = data.get("values", {}).get(imf_code, {}).get(country, {})
            if not values:
                return {"source": "IMF", "indicator": indicator, "error": "no_data"}

            sorted_years = sorted(values.keys())
            # Last actual + next forecast
            current_year = str(datetime.utcnow().year)
            latest_actual = None
            forecast = None

            for y in sorted_years:
                if y <= current_year:
                    latest_actual = (y, values[y])
                elif forecast is None and values[y] is not None:
                    forecast = (y, values[y])

            result = {
                "source": "IMF World Economic Outlook",
                "indicator": indicator,
                "label": label,
                "value": round(latest_actual[1], 4) if latest_actual and latest_actual[1] is not None else None,
                "date": latest_actual[0] if latest_actual else None,
                "forecast_next_year": {
                    "year": forecast[0],
                    "value": round(forecast[1], 4) if forecast and forecast[1] is not None else None
                } if forecast else None,
                "unit": unit,
                "url": f"https://www.imf.org/external/datamapper/{imf_code}@WEO",
            }
            _cache_set(f"imf:{indicator}", result)
            return result
    except Exception as e:
        return {"source": "IMF", "indicator": indicator, "error": str(e)}


# ─────────────────────────────────────────────
# OECD SDMX
# ─────────────────────────────────────────────
# Endpoint: https://sdmx.oecd.org/public/rest/data/{dataflow}/{key}

OECD_INDICATORS = {
    "italy_business_conf_oecd": ("OECD.SDD.STES,DSD_STES@DF_BCICP,4.0", "M.IT.BCICP.AA.0", "Italy Business Confidence (OECD)", "Index 100=avg"),
    "italy_consumer_conf_oecd": ("OECD.SDD.STES,DSD_STES@DF_CCICP,4.0", "M.IT.CCICP.AA.0", "Italy Consumer Confidence (OECD)", "Index 100=avg"),
    "italy_leading_indicator":  ("OECD.SDD.STES,DSD_STES@DF_CLI,4.0",   "M.IT.LI.IX.AA",   "Italy Composite Leading Indicator", "Index"),
}

async def fetch_oecd(indicator: str) -> dict:
    cfg = OECD_INDICATORS.get(indicator)
    if not cfg:
        return {"error": f"unknown_oecd_indicator: {indicator}", "available": list(OECD_INDICATORS.keys())}

    dataflow, key, label, unit = cfg
    cached = _cache_get(f"oecd:{indicator}")
    if cached:
        return {**cached, "from_cache": True}

    url = f"https://sdmx.oecd.org/public/rest/data/{dataflow}/{key}?startPeriod=2020-01&format=csvfilewithlabels"

    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(url, headers={"Accept": "text/csv"})
            if r.status_code != 200:
                return {"source": "OECD", "indicator": indicator, "error": f"http_{r.status_code}"}

            df = pd.read_csv(io.StringIO(r.text))
            # OECD CSV varies in column names — find TIME_PERIOD and OBS_VALUE
            time_col = next((c for c in df.columns if "TIME" in c.upper()), None)
            val_col = next((c for c in df.columns if "VALUE" in c.upper() or c.upper() == "OBS_VALUE"), None)

            if not time_col or not val_col:
                return {"source": "OECD", "indicator": indicator, "error": "unexpected_format", "columns": list(df.columns)}

            df = df[[time_col, val_col]].dropna().sort_values(time_col)
            if df.empty:
                return {"source": "OECD", "indicator": indicator, "error": "no_data"}

            latest = df.iloc[-1]
            latest_value = float(latest[val_col])
            latest_date = str(latest[time_col])

            yoy_pct = None
            if len(df) >= 13:
                prev = df.iloc[-13]
                prev_value = float(prev[val_col])
                if prev_value != 0:
                    yoy_pct = round((latest_value - prev_value) / prev_value * 100, 2)

            result = {
                "source": "OECD",
                "indicator": indicator,
                "label": label,
                "value": round(latest_value, 4),
                "date": latest_date,
                "unit": unit,
                "yoy_pct": yoy_pct,
                "url": "https://data-explorer.oecd.org/",
            }
            _cache_set(f"oecd:{indicator}", result)
            return result
    except Exception as e:
        return {"source": "OECD", "indicator": indicator, "error": str(e)}


# ─────────────────────────────────────────────
# BIS — bank for international settlements (limited free queries)
# ─────────────────────────────────────────────
# BIS exposes SDMX too. Use sparingly — useful for cross-border banking exposure.

BIS_NOTE = "BIS data available via stats.bis.org/api — implement on demand for banking/cross-border queries"


# ─────────────────────────────────────────────
# Master registry — for /lookup command and fact-check engine
# ─────────────────────────────────────────────

ALL_INDICATORS = {
    "ecb":       ECB_INDICATORS,
    "eurostat":  EUROSTAT_INDICATORS,
    "fred":      FRED_INDICATORS,
    "worldbank": WORLDBANK_INDICATORS,
    "imf":       IMF_INDICATORS,
    "oecd":      OECD_INDICATORS,
}


async def fetch_any(indicator: str) -> dict:
    """
    Try to resolve an indicator across all registered sources.
    Usage: pass full indicator name e.g. 'ecb:eurusd_ecb' or just 'eurusd_ecb'.
    """
    # Explicit source prefix
    if ":" in indicator:
        source, name = indicator.split(":", 1)
        dispatchers = {
            "ecb": fetch_ecb,
            "eurostat": fetch_eurostat,
            "fred": fetch_fred,
            "worldbank": fetch_worldbank,
            "wb": fetch_worldbank,
            "imf": fetch_imf,
            "oecd": fetch_oecd,
        }
        fn = dispatchers.get(source.lower())
        if fn:
            return await fn(name)
        return {"error": f"unknown_source: {source}"}

    # Implicit — try each source
    for source, registry in ALL_INDICATORS.items():
        if indicator in registry:
            dispatcher = {
                "ecb": fetch_ecb, "eurostat": fetch_eurostat, "fred": fetch_fred,
                "worldbank": fetch_worldbank, "imf": fetch_imf, "oecd": fetch_oecd,
            }[source]
            return await dispatcher(indicator)

    return {
        "error": f"indicator_not_found: {indicator}",
        "hint": "Use source:indicator format, e.g. 'ecb:eurusd_ecb'",
        "available_sources": list(ALL_INDICATORS.keys())
    }


def list_all_indicators() -> dict:
    """Return the full catalog of available external indicators."""
    catalog = {}
    for source, registry in ALL_INDICATORS.items():
        catalog[source] = []
        for name, cfg in registry.items():
            # Each registry has slightly different tuple shape
            label = cfg[-2] if len(cfg) >= 2 else name
            catalog[source].append({"name": name, "label": label})
    return {
        "sources": list(ALL_INDICATORS.keys()),
        "total_indicators": sum(len(r) for r in ALL_INDICATORS.values()),
        "catalog": catalog
    }


# ─────────────────────────────────────────────
# Fact-check engine
# ─────────────────────────────────────────────

async def fact_check_claims(claims: list[dict]) -> dict:
    """
    Validate a list of numerical claims against authoritative sources.

    claims format: [
      { "claim": "BTP-Bund spread at 138bps", "indicator": "btp_bund_spread", "expected": 138, "tolerance_pct": 5 },
      { "claim": "EUR/USD at 1.08", "indicator": "ecb:eurusd_ecb", "expected": 1.08, "tolerance_pct": 2 },
      ...
    ]

    Returns: validation report with PASS/FAIL/UNKNOWN for each claim.
    """
    report = {"checks": [], "summary": {"pass": 0, "fail": 0, "unknown": 0}}

    for c in claims:
        indicator = c.get("indicator")
        expected = c.get("expected")
        tolerance_pct = c.get("tolerance_pct", 5)

        if indicator == "btp_bund_spread":
            actual = await fetch_btp_bund_spread()
        else:
            actual = await fetch_any(indicator)

        if "error" in actual or actual.get("value") is None:
            verdict = "UNKNOWN"
            report["summary"]["unknown"] += 1
            check = {
                "claim": c.get("claim"),
                "indicator": indicator,
                "expected": expected,
                "actual": None,
                "verdict": verdict,
                "error": actual.get("error", "no_data"),
            }
        else:
            actual_value = actual["value"]
            diff_pct = abs(actual_value - expected) / max(abs(expected), 1e-9) * 100
            if diff_pct <= tolerance_pct:
                verdict = "PASS"
                report["summary"]["pass"] += 1
            else:
                verdict = "FAIL"
                report["summary"]["fail"] += 1
            check = {
                "claim": c.get("claim"),
                "indicator": indicator,
                "expected": expected,
                "actual": actual_value,
                "diff_pct": round(diff_pct, 2),
                "tolerance_pct": tolerance_pct,
                "verdict": verdict,
                "source": actual.get("source"),
                "date": actual.get("date"),
                "url": actual.get("url"),
            }

        report["checks"].append(check)

    n = len(report["checks"])
    if n > 0:
        report["pass_rate"] = round(report["summary"]["pass"] / n * 100, 1)

    return report
