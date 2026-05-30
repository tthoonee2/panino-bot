"""
Sandwich Club Think Tank — Data Service
FastAPI microservice providing broad economic, market, and research data
to the n8n pipeline via HTTP endpoints.

Covers:
- Macro: FRED (US), Eurostat (EU), World Bank, ECB
- Markets: Yahoo Finance (equities, commodities, FX, bonds)
- Italian economy: ISTAT proxies, BTP spreads, PMI
- Sandwich Index: composite business confidence indicator
- News/sentiment: ANSA RSS, financial RSS feeds
"""

from fastapi import FastAPI, Query, HTTPException
from fastapi.responses import JSONResponse
import httpx
import pandas as pd
import yfinance as yf
from datetime import datetime, timedelta
import json
import os
import asyncio
from typing import Optional
import xml.etree.ElementTree as ET

# ISTAT SDMX integration
from istat import (
    fetch_istat_indicator,
    fetch_istat_italy_snapshot,
    list_istat_dataflows,
    ISTAT_DATAFLOWS,
)

# External APIs for fact-checking & quick lookups
from external_apis import (
    fetch_ecb,
    fetch_btp_bund_spread,
    fetch_eurostat,
    fetch_fred,
    fetch_worldbank,
    fetch_imf,
    fetch_oecd,
    fetch_any,
    list_all_indicators,
    fact_check_claims,
)

app = FastAPI(
    title="Sandwich Club Data Service",
    description="Economic & market data API for the Sandwich Club research pipeline",
    version="1.0.0"
)

# ─────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────

def safe_float(val):
    try:
        return round(float(val), 4)
    except:
        return None

def yf_latest(ticker: str, period: str = "5d") -> dict:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=period)
        if hist.empty:
            return {"error": f"No data for {ticker}"}
        latest = hist.iloc[-1]
        prev = hist.iloc[-2] if len(hist) > 1 else latest
        change = safe_float(((latest["Close"] - prev["Close"]) / prev["Close"]) * 100)
        return {
            "ticker": ticker,
            "close": safe_float(latest["Close"]),
            "change_pct": change,
            "volume": int(latest.get("Volume", 0)),
            "date": hist.index[-1].strftime("%Y-%m-%d")
        }
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}

def yf_series(ticker: str, period: str = "1y") -> dict:
    try:
        t = yf.Ticker(ticker)
        hist = t.history(period=period)
        if hist.empty:
            return {"error": f"No data for {ticker}"}
        return {
            "ticker": ticker,
            "dates": [d.strftime("%Y-%m-%d") for d in hist.index],
            "close": [safe_float(v) for v in hist["Close"].tolist()],
            "period": period
        }
    except Exception as e:
        return {"ticker": ticker, "error": str(e)}


# ─────────────────────────────────────────────
# MARKET DATA ENDPOINTS
# ─────────────────────────────────────────────

@app.get("/markets/snapshot")
async def market_snapshot():
    """
    Full market snapshot: equities, commodities, FX, bonds.
    Used as default context injection for any research task.
    """
    tickers = {
        # European equities
        "FTSE_MIB": "FTSEMIB.MI",
        "DAX": "^GDAXI",
        "CAC40": "^FCHI",
        "EUROSTOXX50": "^STOXX50E",
        # US equities
        "SP500": "^GSPC",
        "NASDAQ": "^IXIC",
        # Commodities
        "BRENT": "BZ=F",
        "WTI": "CL=F",
        "NATGAS_EU": "TTF=F",
        "GOLD": "GC=F",
        "COPPER": "HG=F",
        # FX
        "EURUSD": "EURUSD=X",
        "EURGBP": "EURGBP=X",
        "EURCNY": "EURCNY=X",
        # Bonds
        "BTP10Y": "^FTMIB",  # proxy — real BTP via ECB API below
        "BUND10Y": "^IRX",
    }

    results = {}
    loop = asyncio.get_event_loop()

    def fetch_all():
        return {name: yf_latest(ticker) for name, ticker in tickers.items()}

    results = await loop.run_in_executor(None, fetch_all)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "source": "Yahoo Finance",
        "data": results
    }


@app.get("/markets/series/{ticker}")
async def market_series(
    ticker: str,
    period: str = Query("1y", description="1mo, 3mo, 6mo, 1y, 2y, 5y")
):
    """Time series for any Yahoo Finance ticker."""
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, lambda: yf_series(ticker, period))
    return result


@app.get("/markets/italian")
async def italian_markets():
    """
    Italian-specific market data: FTSE MIB, BTP-Bund spread proxy,
    major Italian stocks (ENI, Enel, Intesa, Stellantis, Generali).
    """
    tickers = {
        "FTSEMIB": "FTSEMIB.MI",
        "ENI": "ENI.MI",
        "ENEL": "ENEL.MI",
        "INTESA": "ISP.MI",
        "STELLANTIS": "STLAM.MI",
        "GENERALI": "G.MI",
        "MEDIOBANCA": "MB.MI",
        "FERRARI": "RACE.MI",
        "PRYSMIAN": "PRY.MI",
        "TENARIS": "TEN.MI",
        # BTP 10Y via ETF proxy
        "BTP_ETF": "BTPS.MI",
    }
    loop = asyncio.get_event_loop()
    def fetch():
        return {k: yf_latest(v) for k, v in tickers.items()}
    data = await loop.run_in_executor(None, fetch)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "market": "Italy",
        "data": data
    }


# ─────────────────────────────────────────────
# MACRO / ECONOMIC DATA
# ─────────────────────────────────────────────

@app.get("/macro/eurozone")
async def eurozone_macro():
    """
    Eurozone macroeconomic indicators via public APIs.
    ECB, Eurostat, World Bank proxies.
    """
    indicators = {}

    # ECB key interest rates via ECB Data Portal
    async with httpx.AsyncClient(timeout=15) as client:
        try:
            # ECB deposit facility rate (key policy rate)
            r = await client.get(
                "https://data-api.ecb.europa.eu/service/data/FM/B.U2.EUR.4F.KR.DFR.LEV"
                "?format=jsondata&lastNObservations=1"
            )
            if r.status_code == 200:
                ecb_data = r.json()
                obs = ecb_data.get("dataSets", [{}])[0].get("series", {})
                # Extract latest value
                for k, v in obs.items():
                    vals = v.get("observations", {})
                    if vals:
                        latest_key = max(vals.keys(), key=int)
                        indicators["ecb_deposit_rate"] = {
                            "value": safe_float(vals[latest_key][0]),
                            "unit": "% per annum",
                            "source": "ECB"
                        }
                    break
        except Exception as e:
            indicators["ecb_deposit_rate"] = {"error": str(e)}

        # HICP Inflation (Eurozone) — ECB
        try:
            r = await client.get(
                "https://data-api.ecb.europa.eu/service/data/ICP/M.U2.N.000000.4.INX"
                "?format=jsondata&lastNObservations=13"
            )
            if r.status_code == 200:
                data = r.json()
                series = data.get("dataSets", [{}])[0].get("series", {})
                for k, v in series.items():
                    obs = v.get("observations", {})
                    sorted_obs = sorted(obs.items(), key=lambda x: int(x[0]))
                    if len(sorted_obs) >= 2:
                        latest_val = safe_float(sorted_obs[-1][1][0])
                        prev_val = safe_float(sorted_obs[-13][1][0]) if len(sorted_obs) >= 13 else None
                        yoy = round(((latest_val - prev_val) / prev_val) * 100, 2) if prev_val else None
                        indicators["hicp_inflation_yoy"] = {
                            "value": yoy,
                            "unit": "% YoY",
                            "source": "ECB"
                        }
                    break
        except Exception as e:
            indicators["hicp_inflation_yoy"] = {"error": str(e)}

        # EUR/USD from Yahoo (already have it but contextualize)
        try:
            loop = asyncio.get_event_loop()
            fx = await loop.run_in_executor(None, lambda: yf_latest("EURUSD=X"))
            indicators["eurusd"] = fx
        except Exception as e:
            indicators["eurusd"] = {"error": str(e)}

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "region": "Eurozone",
        "indicators": indicators
    }


@app.get("/macro/italy")
async def italy_macro():
    """
    Italian macroeconomic context:
    GDP growth, inflation, unemployment, debt/GDP, industrial production.
    Sourced from World Bank API and ECB country data.
    """
    indicators = {}

    async with httpx.AsyncClient(timeout=20) as client:

        # World Bank: Italy GDP growth, inflation, unemployment
        wb_indicators = {
            "gdp_growth": "NY.GDP.MKTP.KD.ZG",
            "cpi_inflation": "FP.CPI.TOTL.ZG",
            "unemployment": "SL.UEM.TOTL.ZS",
            "debt_to_gdp": "GC.DOD.TOTL.GD.ZS",
            "current_account_gdp": "BN.CAB.XOKA.GD.ZS",
            "fdi_inflows_gdp": "BX.KLT.DINV.WD.GD.ZS"
        }

        for label, wb_code in wb_indicators.items():
            try:
                r = await client.get(
                    f"https://api.worldbank.org/v2/country/IT/indicator/{wb_code}"
                    f"?format=json&mrv=1&per_page=1"
                )
                if r.status_code == 200:
                    data = r.json()
                    if len(data) > 1 and data[1]:
                        entry = data[1][0]
                        indicators[label] = {
                            "value": safe_float(entry.get("value")),
                            "year": entry.get("date"),
                            "source": "World Bank"
                        }
            except Exception as e:
                indicators[label] = {"error": str(e)}

        # PMI Manufacturing Italy (via Yahoo proxy — actual PMI via SPGI needs subscription)
        # Use iShares Italy ETF as a proxy for sentiment
        try:
            loop = asyncio.get_event_loop()
            italy_etf = await loop.run_in_executor(None, lambda: yf_latest("EWI"))
            indicators["italy_equity_etf_ewi"] = italy_etf
        except Exception as e:
            indicators["italy_equity_etf_ewi"] = {"error": str(e)}

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "country": "Italy",
        "indicators": indicators
    }


@app.get("/macro/global")
async def global_macro():
    """
    Global macro snapshot: US Fed rate, US CPI, China PMI proxy,
    oil market balance indicators, global risk sentiment (VIX).
    """
    indicators = {}

    async with httpx.AsyncClient(timeout=20) as client:
        # World Bank global indicators
        wb_global = {
            "world_gdp_growth": ("WLD", "NY.GDP.MKTP.KD.ZG"),
            "us_gdp_growth": ("US", "NY.GDP.MKTP.KD.ZG"),
            "china_gdp_growth": ("CN", "NY.GDP.MKTP.KD.ZG"),
            "germany_gdp_growth": ("DE", "NY.GDP.MKTP.KD.ZG"),
        }

        for label, (country, code) in wb_global.items():
            try:
                r = await client.get(
                    f"https://api.worldbank.org/v2/country/{country}/indicator/{code}"
                    f"?format=json&mrv=1&per_page=1"
                )
                if r.status_code == 200:
                    data = r.json()
                    if len(data) > 1 and data[1]:
                        entry = data[1][0]
                        indicators[label] = {
                            "value": safe_float(entry.get("value")),
                            "year": entry.get("date"),
                            "source": "World Bank"
                        }
            except Exception as e:
                indicators[label] = {"error": str(e)}

    # Market-based indicators (Yahoo Finance)
    loop = asyncio.get_event_loop()
    market_indicators = {
        "vix": "^VIX",
        "dxy": "DX-Y.NYB",
        "us10y_yield": "^TNX",
        "brent_crude": "BZ=F",
        "gold": "GC=F",
        "copper_dr": "HG=F",
    }

    def fetch_markets():
        return {k: yf_latest(v) for k, v in market_indicators.items()}

    market_data = await loop.run_in_executor(None, fetch_markets)
    indicators.update(market_data)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "scope": "Global",
        "indicators": indicators
    }


# ─────────────────────────────────────────────
# SANDWICH INDEX
# ─────────────────────────────────────────────

@app.get("/sandwich-index/latest")
async def sandwich_index_latest():
    """
    Sandwich Index — composite Italian business confidence indicator.
    Components: equity momentum, BTP spread proxy, ISTAT business confidence,
    energy cost, EUR/USD, VIX (global risk), Italy vs EuroStoxx alpha.

    Falls back gracefully if ISTAT is unavailable (rate limit, network).
    """

    loop = asyncio.get_event_loop()

    # Try to get ISTAT business confidence as the strongest signal
    istat_biz = None
    try:
        istat_biz = await fetch_istat_indicator("business_confidence")
    except Exception:
        pass

    def compute():
        components = {}
        score = 0.0
        total_weight = 0.0

        # Component 1: FTSE MIB momentum (20% weight, reduced if ISTAT available)
        mib = yf_latest("FTSEMIB.MI")
        if "change_pct" in mib and mib["change_pct"] is not None:
            raw = mib["change_pct"]
            normalized = max(0, min(100, (raw + 2) / 4 * 100))
            components["equity_momentum"] = {
                "raw": raw, "normalized": normalized, "weight": 0.20, "label": "FTSE MIB Daily Return"
            }
            score += normalized * 0.20
            total_weight += 0.20

        # Component 2: ISTAT Business Confidence (25% weight if available — anchors index in real data)
        if istat_biz and istat_biz.get("latest_value") is not None:
            raw = istat_biz["latest_value"]
            # ISTAT business climate index: 100 = long-run average. Range ~80-110.
            normalized = max(0, min(100, (raw - 85) / 25 * 100))
            components["istat_business_confidence"] = {
                "raw": raw,
                "normalized": normalized,
                "weight": 0.25,
                "label": f"ISTAT Business Confidence ({istat_biz.get('latest_period', 'N/A')})",
                "yoy_pct": istat_biz.get("yoy_pct")
            }
            score += normalized * 0.25
            total_weight += 0.25

        # Component 3: EUR/USD (10% weight)
        eurusd = yf_latest("EURUSD=X")
        if "close" in eurusd and eurusd["close"]:
            raw = eurusd["close"]
            normalized = max(0, min(100, (raw - 1.05) / 0.10 * 100))
            components["eurusd"] = {
                "raw": raw, "normalized": normalized, "weight": 0.10, "label": "EUR/USD Rate"
            }
            score += normalized * 0.10
            total_weight += 0.10

        # Component 4: Energy costs (15% weight, inverted)
        brent = yf_latest("BZ=F")
        if "close" in brent and brent["close"]:
            raw = brent["close"]
            normalized = max(0, min(100, (100 - raw) / 40 * 100))
            components["energy_cost"] = {
                "raw": raw, "normalized": normalized, "weight": 0.15, "label": "Brent Crude (inverted)"
            }
            score += normalized * 0.15
            total_weight += 0.15

        # Component 5: VIX — global risk (10% weight, inverted)
        vix = yf_latest("^VIX")
        if "close" in vix and vix["close"]:
            raw = vix["close"]
            normalized = max(0, min(100, (40 - raw) / 30 * 100))
            components["risk_sentiment"] = {
                "raw": raw, "normalized": normalized, "weight": 0.10, "label": "VIX (inverted)"
            }
            score += normalized * 0.10
            total_weight += 0.10

        # Component 6: Italy vs EuroStoxx alpha (20% weight)
        ewi = yf_latest("EWI")
        stoxx = yf_latest("^STOXX50E")
        if (ewi.get("change_pct") is not None and stoxx.get("change_pct") is not None):
            raw = ewi["change_pct"] - stoxx["change_pct"]
            normalized = max(0, min(100, (raw + 1) / 2 * 100))
            components["italy_relative"] = {
                "raw": raw, "normalized": normalized, "weight": 0.20, "label": "Italy vs EuroStoxx Alpha"
            }
            score += normalized * 0.20
            total_weight += 0.20

        final_score = round(score / total_weight, 1) if total_weight > 0 else None

        if final_score is None:
            interpretation = "N/A"
        elif final_score >= 65:
            interpretation = "Positive — Expansion territory"
        elif final_score >= 50:
            interpretation = "Neutral — Stable conditions"
        elif final_score >= 35:
            interpretation = "Cautious — Mild contraction signals"
        else:
            interpretation = "Negative — Stress conditions"

        anchor = "ISTAT Business Confidence" if istat_biz and istat_biz.get("latest_value") else "Market signals only"

        return {
            "score": final_score,
            "interpretation": interpretation,
            "components": components,
            "anchored_in": anchor,
            "methodology": "Weighted composite. Hard data (ISTAT) when available, market signals as overlay. Weights: ISTAT Business Confidence 25%, FTSE MIB 20%, Italy Alpha 20%, Energy 15%, EUR/USD 10%, VIX 10%."
        }

    result = await loop.run_in_executor(None, compute)
    return {
        "timestamp": datetime.utcnow().isoformat(),
        "index": "Sandwich Index",
        "version": "2.0",
        **result
    }


# ─────────────────────────────────────────────
# NEWS & SENTIMENT
# ─────────────────────────────────────────────

@app.get("/news/ansa")
async def ansa_news(topic: Optional[str] = Query(None, description="Filter by keyword")):
    """
    ANSA RSS feed — latest Italian news headlines.
    Filter by topic keyword if provided.
    """
    feeds = {
        "economia": "https://www.ansa.it/sito/notizie/economia/economia_rss.xml",
        "mondo": "https://www.ansa.it/sito/notizie/mondo/mondo_rss.xml",
        "politica": "https://www.ansa.it/sito/notizie/politica/politica_rss.xml",
    }

    articles = []

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for category, url in feeds.items():
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    root = ET.fromstring(r.content)
                    for item in root.findall(".//item")[:10]:
                        title = item.findtext("title", "")
                        desc = item.findtext("description", "")
                        link = item.findtext("link", "")
                        pub_date = item.findtext("pubDate", "")

                        if topic and topic.lower() not in (title + desc).lower():
                            continue

                        articles.append({
                            "category": category,
                            "title": title,
                            "description": desc[:300] if desc else "",
                            "link": link,
                            "published": pub_date
                        })
            except Exception as e:
                articles.append({"category": category, "error": str(e)})

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "source": "ANSA RSS",
        "topic_filter": topic,
        "count": len([a for a in articles if "error" not in a]),
        "articles": articles[:20]
    }


@app.get("/news/financial-rss")
async def financial_rss(topic: Optional[str] = Query(None)):
    """
    Financial RSS feeds: Il Sole 24 Ore, Reuters Business, FT headlines.
    """
    feeds = {
        "sole24ore": "https://www.ilsole24ore.com/rss/economia.xml",
        "reuters_business": "https://feeds.reuters.com/reuters/businessNews",
        "corriere_economia": "https://www.corriere.it/rss/economia.xml",
    }

    articles = []

    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for source, url in feeds.items():
            try:
                r = await client.get(url)
                if r.status_code == 200:
                    root = ET.fromstring(r.content)
                    for item in root.findall(".//item")[:8]:
                        title = item.findtext("title", "")
                        desc = item.findtext("description", "")
                        link = item.findtext("link", "")
                        pub_date = item.findtext("pubDate", "")

                        if topic and topic.lower() not in (title + desc).lower():
                            continue

                        articles.append({
                            "source": source,
                            "title": title,
                            "description": desc[:300] if desc else "",
                            "link": link,
                            "published": pub_date
                        })
            except Exception as e:
                articles.append({"source": source, "error": str(e)})

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "topic_filter": topic,
        "count": len([a for a in articles if "error" not in a]),
        "articles": articles[:20]
    }


# ─────────────────────────────────────────────
# SECTOR DATA
# ─────────────────────────────────────────────

@app.get("/sectors/{sector}")
async def sector_data(sector: str):
    """
    Sector-specific data bundles.
    Sectors: energy, banking, manufacturing, luxury, tech, commodities
    """
    sector_tickers = {
        "energy": {
            "ENI": "ENI.MI", "ENEL": "ENEL.MI", "TERNA": "TRN.MI",
            "BRENT": "BZ=F", "NATGAS": "NG=F", "TTF": "TTF=F",
            "SOLAR_ETF": "TAN", "EU_CARBON": "CARB.L"
        },
        "banking": {
            "INTESA": "ISP.MI", "UNICREDIT": "UCG.MI", "MEDIOBANCA": "MB.MI",
            "BANCO_BPM": "BAMI.MI", "BPER": "BPE.MI",
            "EU_BANKS_ETF": "EXV1.DE", "BTP_BUND_PROXY": "BTPS.MI"
        },
        "manufacturing": {
            "STELLANTIS": "STLAM.MI", "FERRARI": "RACE.MI",
            "PRYSMIAN": "PRY.MI", "PIRELLI": "PIRC.MI",
            "TENARIS": "TEN.MI", "COPPER": "HG=F", "STEEL_ETF": "SLX"
        },
        "luxury": {
            "FERRARI": "RACE.MI", "MONCLER": "MONC.MI", "TOD": "TOD.MI",
            "LVMH": "MC.PA", "HERMES": "RMS.PA", "KERING": "KER.PA"
        },
        "tech": {
            "REPLY": "REY.MI", "EXPRIVIA": "XPR.MI",
            "EU_TECH_ETF": "IUIT.AS", "SOX": "^SOX",
            "NASDAQ": "^IXIC"
        },
        "commodities": {
            "GOLD": "GC=F", "SILVER": "SI=F", "COPPER": "HG=F",
            "BRENT": "BZ=F", "WTI": "CL=F", "NATGAS": "NG=F",
            "WHEAT": "ZW=F", "CORN": "ZC=F", "COFFEE": "KC=F"
        }
    }

    if sector not in sector_tickers:
        raise HTTPException(
            status_code=404,
            detail=f"Sector '{sector}' not found. Available: {list(sector_tickers.keys())}"
        )

    loop = asyncio.get_event_loop()
    def fetch():
        return {k: yf_latest(v) for k, v in sector_tickers[sector].items()}

    data = await loop.run_in_executor(None, fetch)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "sector": sector,
        "data": data
    }


# ─────────────────────────────────────────────
# ISTAT — Italian National Statistics Institute
# ─────────────────────────────────────────────

@app.get("/istat/dataflows")
async def istat_dataflows():
    """List curated ISTAT dataflows wired to the pipeline."""
    return list_istat_dataflows()


@app.get("/istat/indicator/{indicator}")
async def istat_indicator(indicator: str):
    """
    Fetch a single ISTAT indicator by curated name.
    Available: industrial_production, consumer_confidence, business_confidence,
    unemployment, gdp_quarterly, cpi_inflation, retail_trade, exports,
    construction, wages.

    Rate limit: 4/min. Cached aggressively (24h for monthly, 7d for quarterly).
    """
    return await fetch_istat_indicator(indicator)


@app.get("/istat/snapshot")
async def istat_snapshot():
    """
    Full ISTAT Italian macro snapshot — all 10 curated indicators.
    First call after cache miss may take ~2 min (rate-limited sequential).
    Subsequent calls hit cache and return in ms.
    """
    return await fetch_istat_italy_snapshot()


# ─────────────────────────────────────────────
# EXTERNAL APIS — quick lookup & fact-check
# ─────────────────────────────────────────────

@app.get("/lookup/{indicator}")
async def lookup_indicator(indicator: str):
    """
    Quick lookup of any indicator from any source.

    Use 'source:name' format for explicit, or just 'name' to auto-resolve.

    Examples:
      /lookup/ecb:eurusd_ecb
      /lookup/btp_bund_spread          (computed)
      /lookup/fred:us_10y_yield
      /lookup/eurostat:unemployment_it
      /lookup/imf:italy_gdp_imf
    """
    if indicator in ("btp_bund_spread", "btp-bund", "spread"):
        return await fetch_btp_bund_spread()
    return await fetch_any(indicator)


@app.get("/lookup")
async def list_lookups():
    """List all available indicators across all external sources."""
    return list_all_indicators()


@app.get("/ecb/{indicator}")
async def ecb_lookup(indicator: str):
    """Direct ECB Data Portal access."""
    return await fetch_ecb(indicator)


@app.get("/ecb/spread/btp-bund")
async def ecb_btp_bund():
    """Computed BTP-Bund 10Y spread in bps."""
    return await fetch_btp_bund_spread()


@app.get("/eurostat/{indicator}")
async def eurostat_lookup(indicator: str):
    """Direct Eurostat SDMX access."""
    return await fetch_eurostat(indicator)


@app.get("/fred/{indicator}")
async def fred_lookup(indicator: str):
    """Direct FRED (St. Louis Fed) access."""
    return await fetch_fred(indicator)


@app.get("/worldbank/{indicator}")
async def worldbank_lookup(indicator: str):
    """Direct World Bank access."""
    return await fetch_worldbank(indicator)


@app.get("/imf/{indicator}")
async def imf_lookup(indicator: str):
    """Direct IMF WEO access."""
    return await fetch_imf(indicator)


@app.get("/oecd/{indicator}")
async def oecd_lookup(indicator: str):
    """Direct OECD SDMX access."""
    return await fetch_oecd(indicator)


from pydantic import BaseModel
from typing import List

class FactCheckRequest(BaseModel):
    claims: List[dict]

@app.post("/fact-check")
async def fact_check(req: FactCheckRequest):
    """
    Validate numerical claims against authoritative sources.

    Request body:
    {
      "claims": [
        {"claim": "BTP-Bund spread at 138bps", "indicator": "btp_bund_spread", "expected": 138, "tolerance_pct": 5},
        {"claim": "ECB DFR at 3.25%", "indicator": "ecb:ecb_deposit_rate", "expected": 3.25, "tolerance_pct": 2}
      ]
    }
    """
    return await fact_check_claims(req.claims)


# ─────────────────────────────────────────────
# COMPOSITE CONTEXT ENDPOINT (main n8n hook)
# ─────────────────────────────────────────────

@app.get("/context/full")
async def full_context(
    topic: Optional[str] = Query(None),
    sector: Optional[str] = Query(None)
):
    """
    Master endpoint — called by n8n to inject data context into Claude prompts.
    Returns a pre-formatted markdown string ready to paste into a system prompt.
    Combines: market snapshot, macro, Sandwich Index, relevant news.
    """
    # Run in parallel
    tasks = [
        market_snapshot(),
        sandwich_index_latest(),
        eurozone_macro(),
        ansa_news(topic=topic),
        fetch_istat_italy_snapshot(),
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)
    markets, sandwich, macro_ez, news, istat = results

    # Format as markdown context block
    def fmt_market(name, d):
        if "error" in d:
            return f"  {name}: ERROR"
        change = f"{d['change_pct']:+.2f}%" if d.get('change_pct') is not None else ""
        return f"  {name}: {d.get('close', 'N/A')} {change}"

    lines = ["## 📊 Real-Time Data Context\n", f"*Generated: {datetime.utcnow().strftime('%Y-%m-%d %H:%M')} UTC*\n"]

    # Sandwich Index
    if not isinstance(sandwich, Exception):
        idx = sandwich.get("index", {})
        score = sandwich.get("score", "N/A")
        interp = sandwich.get("interpretation", "")
        lines.append(f"\n### 🥪 Sandwich Index\n**Score: {score}/100** — {interp}\n")

    # Key markets
    if not isinstance(markets, Exception):
        lines.append("\n### 📈 Key Markets")
        market_data = markets.get("data", {})
        priority = ["FTSE_MIB", "EUROSTOXX50", "SP500", "BRENT", "NATGAS_EU", "EURUSD", "GOLD"]
        for key in priority:
            if key in market_data:
                lines.append(fmt_market(key, market_data[key]))

    # Macro
    if not isinstance(macro_ez, Exception):
        lines.append("\n### 🏛️ Eurozone Macro")
        for k, v in macro_ez.get("indicators", {}).items():
            if isinstance(v, dict) and v.get("value") is not None:
                lines.append(f"  {k}: {v['value']} {v.get('unit', '')}")

    # ISTAT — Italian hard data
    if not isinstance(istat, Exception):
        istat_summary = istat.get("summary_markdown", "")
        if istat_summary:
            lines.append("\n" + istat_summary)

    # News
    if not isinstance(news, Exception):
        articles = news.get("articles", [])
        if articles:
            lines.append(f"\n### 📰 Latest Headlines (ANSA)")
            for a in articles[:5]:
                if "error" not in a:
                    lines.append(f"  - {a['title']}")

    context_text = "\n".join(lines)

    return {
        "timestamp": datetime.utcnow().isoformat(),
        "topic": topic,
        "context_markdown": context_text,
        "raw": {
            "markets": markets if not isinstance(markets, Exception) else {"error": str(markets)},
            "sandwich_index": sandwich if not isinstance(sandwich, Exception) else {"error": str(sandwich)},
            "macro_eurozone": macro_ez if not isinstance(macro_ez, Exception) else {"error": str(macro_ez)},
            "istat": istat if not isinstance(istat, Exception) else {"error": str(istat)},
            "news": news if not isinstance(news, Exception) else {"error": str(news)},
        }
    }


# ─────────────────────────────────────────────
# HEALTH
# ─────────────────────────────────────────────

@app.get("/health")
async def health():
    return {"status": "ok", "service": "Sandwich Club Data Service", "timestamp": datetime.utcnow().isoformat()}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
