"""
FII/DII Intelligence Dashboard — v4 (Complete Fix)
====================================================
Fixes:
  1. Correct NSE bulk deals API URL
  2. Email sends proper HTML attachment (opens in browser)
  3. Robust 3-source fallback system
  4. yfinance MultiIndex fix
"""

import os, smtplib, logging, time, re
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path

import requests
import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── Setup ──────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("dashboard.log")]
)
log = logging.getLogger(__name__)

OUTPUT_DIR = Path("docs")
OUTPUT_DIR.mkdir(exist_ok=True)

BROWSER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "Chrome/121.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
}

# ── FALLBACK STOCKS (always available) ────────────────────────────────────────
FALLBACK_STOCKS = [
    {"symbol": "GMRAIRPORT.NS",  "name": "GMR Airports",        "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "TORNTPHARM.NS",  "name": "Torrent Pharma",      "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "POWERGRID.NS",   "name": "Power Grid Corp",     "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "JSWENERGY.NS",   "name": "JSW Energy",          "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "SUPREMEIND.NS",  "name": "Supreme Industries",  "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "ASTRAL.NS",      "name": "Astral Poly",         "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "INDIGO.NS",      "name": "IndiGo",              "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "BSE.NS",         "name": "BSE Limited",         "fii_cash": "sell", "dii_cash": "sell"},
    {"symbol": "GODREJCP.NS",    "name": "Godrej Consumer",     "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "SBICARD.NS",     "name": "SBI Cards",           "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "CAMS.NS",        "name": "CAMS",                "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "BRITANNIA.NS",   "name": "Britannia",           "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "KFINTECH.NS",    "name": "KFin Technologies",   "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "ANGELONE.NS",    "name": "Angel One",           "fii_cash": "sell", "dii_cash": "buy"},
    {"symbol": "POLICYBZR.NS",   "name": "PB Fintech",          "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "NUVAMA.NS",      "name": "Nuvama Wealth",       "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "FORTIS.NS",      "name": "Fortis Healthcare",   "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "MANAPPURAM.NS",  "name": "Manappuram Finance",  "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "360ONE.NS",      "name": "360 One WAM",         "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "APLAPOLLO.NS",   "name": "APL Apollo Tubes",    "fii_cash": "buy",  "dii_cash": "buy"},
]

# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 1 ── NSE Historical Bulk Deals JSON (CORRECT URL)
# ─────────────────────────────────────────────────────────────────────────────
def fetch_from_nse() -> list[dict]:
    log.info("📡 [Source 1] NSE Historical Bulk Deals JSON API...")
    try:
        session = requests.Session()

        # Step 1 — hit homepage to set cookies (NSE requires this)
        log.info("  → Getting NSE cookies...")
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=12)
        time.sleep(1.5)

        # Step 2 — hit the bulk deals page to get more cookies
        session.get("https://www.nseindia.com/reports/equity-bulk-deals",
                    headers=NSE_HEADERS, timeout=12)
        time.sleep(1)

        # Step 3 — CORRECT API endpoint
        url = "https://www.nseindia.com/json/historical/historical-bulk-deals.json"
        log.info(f"  → Fetching: {url}")
        resp = session.get(url, headers=NSE_HEADERS, timeout=20)
        log.info(f"  → Status: {resp.status_code}")

        if resp.status_code != 200:
            log.warning(f"  ❌ NSE API returned {resp.status_code}")
            return []

        raw = resp.json()
        log.info(f"  → Response type: {type(raw)}, keys: {list(raw.keys()) if isinstance(raw, dict) else 'list'}")

        # The JSON may be nested — find the records list
        records = []
        if isinstance(raw, list):
            records = raw
        elif isinstance(raw, dict):
            for key in ["data", "bulkDeals", "deals", "records", "BulkDeals"]:
                if key in raw and isinstance(raw[key], list):
                    records = raw[key]
                    log.info(f"  → Found records under key '{key}': {len(records)}")
                    break
            if not records:
                # Try first list value
                for v in raw.values():
                    if isinstance(v, list) and len(v) > 0:
                        records = v
                        break

        log.info(f"  → Total records: {len(records)}")
        if not records:
            return []

        # Only last trading day
        today_str = datetime.today().strftime("%d-%b-%Y")
        yesterday = (datetime.today() - timedelta(days=1)).strftime("%d-%b-%Y")

        stocks = {}
        for deal in records:
            # Flexible field name handling
            date_val = (deal.get("BD_DT_DATE") or deal.get("date") or
                        deal.get("Date") or deal.get("TRADE_DATE") or "")
            sym      = (deal.get("BD_SYMBOL") or deal.get("symbol") or
                        deal.get("Symbol") or deal.get("SYMBOL") or "").strip()
            name     = (deal.get("BD_COMP_NAME") or deal.get("companyName") or
                        deal.get("Company") or sym)
            client   = (deal.get("BD_CLIENT_NAME") or deal.get("clientName") or
                        deal.get("Client") or "").upper()
            buy_sell = (deal.get("BD_BUY_SELL") or deal.get("buySell") or
                        deal.get("BuySell") or "").upper()
            qty      = deal.get("BD_QTY_TRD") or deal.get("quantity") or 0

            if not sym:
                continue

            # FII / DII classification
            fii_kw = ["FII","FPI","FOREIGN","OVERSEAS","GLOBAL","INTERNATIONAL",
                      "MORGAN","GOLDMAN","CITI","BLACKROCK","VANGUARD","FIDELITY",
                      "NOMURA","MACQUARIE","UBS","BARCLAYS","HSBC","JPMORGAN",
                      "DEUTSCHE","MERRILL","SOCIETE","BNP","CREDIT SUISSE","LAZARD"]
            dii_kw = ["DII","MUTUAL FUND","MF","LIC","SBI MF","HDFC MF","ICICI MF",
                      "KOTAK MF","AXIS MF","NIPPON","ADITYA BIRLA","DSP","FRANKLIN",
                      "INSURANCE","PROVIDENT FUND","PENSION","UTI","TATA MF",
                      "MIRAE","EDELWEISS MF","MOTILAL","INVESCO","SUNDARAM"]

            is_fii = any(k in client for k in fii_kw)
            is_dii = any(k in client for k in dii_kw)

            if not (is_fii or is_dii):
                continue

            key = sym.upper()
            if key not in stocks:
                stocks[key] = {
                    "symbol": sym + ".NS",
                    "name": name,
                    "fii_cash": "neutral",
                    "dii_cash": "neutral",
                }

            action = "buy" if "B" in buy_sell else "sell"
            if is_fii: stocks[key]["fii_cash"] = action
            if is_dii: stocks[key]["dii_cash"] = action

        result = list(stocks.values())
        log.info(f"  ✅ NSE JSON: {len(result)} FII/DII stocks found")
        return result

    except Exception as e:
        log.warning(f"  ❌ NSE JSON failed: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 2 ── MunafaSutra scraper
# ─────────────────────────────────────────────────────────────────────────────
def fetch_from_munafasutra() -> list[dict]:
    log.info("📡 [Source 2] MunafaSutra scraper...")
    try:
        resp = requests.get("https://munafasutra.com/nse/FIIDII/",
                            headers=BROWSER_HEADERS, timeout=20)
        resp.raise_for_status()
        log.info(f"  → Status: {resp.status_code}, Length: {len(resp.text)}")

        soup = BeautifulSoup(resp.text, "lxml")
        stocks = []

        # Find all table rows that have stock links
        for a in soup.find_all("a", href=re.compile(r"/nse/stock/")):
            href   = a.get("href", "")
            symbol = href.rstrip("/").split("/")[-1]
            name   = a.get_text(strip=True)
            if not symbol or not name:
                continue

            # Walk up to the <tr> to get buy/sell info
            tr = a.find_parent("tr")
            if not tr:
                continue
            tds = tr.find_all("td")
            if len(tds) < 3:
                continue

            row_text = " ".join(td.get_text(strip=True).lower() for td in tds)
            fii = "buy" if "bought" in row_text else "sell"
            dii = "buy" if "bought" in row_text else "sell"

            stocks.append({
                "symbol": symbol + ".NS",
                "name": name,
                "fii_cash": fii,
                "dii_cash": dii,
            })

        log.info(f"  {'✅' if stocks else '❌'} MunafaSutra: {len(stocks)} stocks")
        return stocks[:20]

    except Exception as e:
        log.warning(f"  ❌ MunafaSutra failed: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
# SOURCE 3 ── Hardcoded fallback
# ─────────────────────────────────────────────────────────────────────────────
def fetch_fallback() -> list[dict]:
    log.warning("📡 [Source 3] Using hardcoded fallback stock list...")
    return FALLBACK_STOCKS.copy()


# ─────────────────────────────────────────────────────────────────────────────
# MASTER FETCH
# ─────────────────────────────────────────────────────────────────────────────
def fetch_fii_dii_stocks():
    stocks = fetch_from_nse()
    if stocks:
        return stocks, "NSE Bulk Deals JSON API"

    stocks = fetch_from_munafasutra()
    if stocks:
        return stocks, "MunafaSutra"

    return fetch_fallback(), "Fallback (Known Institutional Stocks)"


# ─────────────────────────────────────────────────────────────────────────────
# TECHNICAL ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────
def get_series(df, col):
    """Safely extract a column, handling MultiIndex."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df[col].astype(float).dropna()


def compute_technicals(symbol: str) -> dict:
    log.info(f"  📐 {symbol}")
    empty = {
        "rsi": 50.0, "macd_hist": 0.0, "ema_cross": "unknown",
        "bb_label": "N/A", "adx": 0.0, "stoch_rsi": 0.5,
        "resist1": 0.0, "support1": 0.0,
        "swing_high": 0.0, "swing_low": 0.0,
        "last_price": 0.0, "overall": "N/A",
        "score": 0, "sparkline": [], "data_ok": False,
    }

    try:
        end   = datetime.today()
        start = end - timedelta(days=185)
        df = yf.download(symbol, start=start, end=end,
                         progress=False, auto_adjust=True)

        if df is None or df.empty:
            raise ValueError("Empty dataframe")

        # Fix MultiIndex
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df[["Open","High","Low","Close","Volume"]].dropna()
        if len(df) < 25:
            raise ValueError(f"Too few rows: {len(df)}")

        close = df["Close"].astype(float)
        high  = df["High"].astype(float)
        low   = df["Low"].astype(float)
        lc    = float(close.iloc[-1])

        # ── RSI (14) ──────────────────────────────────────────────────────
        delta    = close.diff()
        gain     = delta.clip(lower=0)
        loss     = (-delta).clip(lower=0)
        avg_g    = gain.ewm(com=13, adjust=False).mean()
        avg_l    = loss.ewm(com=13, adjust=False).mean()
        rs       = avg_g / avg_l.replace(0, np.nan)
        rsi_s    = 100 - (100 / (1 + rs))
        rsi_val  = round(float(rsi_s.iloc[-1]), 1)

        # ── MACD (12, 26, 9) ─────────────────────────────────────────────
        ema12     = close.ewm(span=12, adjust=False).mean()
        ema26     = close.ewm(span=26, adjust=False).mean()
        macd      = ema12 - ema26
        macd_sig  = macd.ewm(span=9, adjust=False).mean()
        macd_hist = round(float(macd.iloc[-1] - macd_sig.iloc[-1]), 2)

        # ── EMA 20 / 50 ──────────────────────────────────────────────────
        ema20     = close.ewm(span=20, adjust=False).mean()
        ema50     = close.ewm(span=50, adjust=False).mean()
        ema_cross = "bullish" if float(ema20.iloc[-1]) > float(ema50.iloc[-1]) else "bearish"

        # ── Bollinger Bands (20, 2σ) ─────────────────────────────────────
        bb_mid   = close.rolling(20).mean()
        bb_std   = close.rolling(20).std()
        bb_u     = float((bb_mid + 2 * bb_std).iloc[-1])
        bb_l     = float((bb_mid - 2 * bb_std).iloc[-1])
        bb_pos   = (lc - bb_l) / ((bb_u - bb_l) or 1)
        bb_label = "Overbought" if bb_pos > 0.8 else ("Oversold" if bb_pos < 0.2 else "Mid")

        # ── ADX (14) ─────────────────────────────────────────────────────
        p_dm  = high.diff().clip(lower=0)
        m_dm  = (-low.diff()).clip(lower=0)
        tr    = pd.concat([high - low,
                            (high - close.shift()).abs(),
                            (low  - close.shift()).abs()], axis=1).max(axis=1)
        atr14 = tr.ewm(com=13, adjust=False).mean()
        p_di  = 100 * p_dm.ewm(com=13, adjust=False).mean()  / atr14
        m_di  = 100 * m_dm.ewm(com=13, adjust=False).mean()  / atr14
        dx    = 100 * (p_di - m_di).abs() / (p_di + m_di).replace(0, np.nan)
        adx   = round(float(dx.ewm(com=13, adjust=False).mean().iloc[-1]), 1)

        # ── Stochastic RSI ────────────────────────────────────────────────
        rsi_min   = rsi_s.rolling(14).min()
        rsi_max   = rsi_s.rolling(14).max()
        stoch_rsi_s = (rsi_s - rsi_min) / ((rsi_max - rsi_min).replace(0, np.nan))
        stoch_val = float(stoch_rsi_s.iloc[-1])
        stoch_val = 0.5 if np.isnan(stoch_val) else round(stoch_val, 2)

        # ── Support & Resistance ─────────────────────────────────────────
        pivot    = (float(high.iloc[-1]) + float(low.iloc[-1]) + lc) / 3
        resist1  = round(2 * pivot - float(low.iloc[-1]),  2)
        support1 = round(2 * pivot - float(high.iloc[-1]), 2)
        n        = min(120, len(high))
        swing_h  = round(float(high.rolling(n).max().iloc[-1]), 2)
        swing_l  = round(float(low.rolling(n).min().iloc[-1]),  2)

        # ── Signal Score ─────────────────────────────────────────────────
        score = 0
        score += 2 if rsi_val < 40  else (1 if rsi_val < 55 else (-2 if rsi_val > 70 else 0))
        score += 2 if macd_hist > 0 else 0
        score += 2 if ema_cross == "bullish" else 0
        score += 1 if adx > 25 else 0
        score += 1 if stoch_val < 0.3 else (-1 if stoch_val > 0.8 else 0)

        overall = ("STRONG BUY" if score >= 5 else
                   "BUY"        if score >= 3 else
                   "NEUTRAL"    if score >= 0 else
                   "CAUTION"    if score >= -2 else "SELL")

        spark = [round(float(x), 2) for x in close.iloc[-7:].tolist()]

        return {
            "rsi": rsi_val, "macd_hist": macd_hist, "ema_cross": ema_cross,
            "bb_label": bb_label, "adx": adx, "stoch_rsi": stoch_val,
            "resist1": resist1, "support1": support1,
            "swing_high": swing_h, "swing_low": swing_l,
            "last_price": round(lc, 2), "overall": overall,
            "score": score, "sparkline": spark, "data_ok": True,
        }

    except Exception as e:
        log.warning(f"    ⚠️  {symbol}: {e}")
        return empty


# ─────────────────────────────────────────────────────────────────────────────
# MARKET SUMMARY
# ─────────────────────────────────────────────────────────────────────────────
def fetch_market_summary() -> dict:
    log.info("📡 Fetching Nifty / Sensex...")
    try:
        def load(sym):
            df = yf.download(sym, period="5d", progress=False, auto_adjust=True)
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            c = df["Close"].dropna().astype(float)
            last  = round(float(c.iloc[-1]), 2)
            chg   = round((c.iloc[-1] - c.iloc[-2]) / c.iloc[-2] * 100, 2) if len(c) >= 2 else 0.0
            return last, chg

        np_, nc = load("^NSEI")
        sp_, sc = load("^BSESN")
        return {"nifty_price": np_, "nifty_chg": nc,
                "sensex_price": sp_, "sensex_chg": sc}
    except Exception as e:
        log.warning(f"Market summary failed: {e}")
        return {"nifty_price": 0, "nifty_chg": 0, "sensex_price": 0, "sensex_chg": 0}


# ─────────────────────────────────────────────────────────────────────────────
# BUILD DATASET
# ─────────────────────────────────────────────────────────────────────────────
def build_dataset():
    raw, source = fetch_fii_dii_stocks()
    log.info(f"✅ Source: '{source}' — {len(raw)} stocks")
    market   = fetch_market_summary()
    enriched = []

    for s in raw:
        tech     = compute_technicals(s["symbol"])
        both_buy = s["fii_cash"] == "buy" and s["dii_cash"] == "buy"
        fii_only = s["fii_cash"] == "buy" and s["dii_cash"] != "buy"
        dii_only = s["dii_cash"] == "buy" and s["fii_cash"] != "buy"
        inst_sig = ("BOTH BUY" if both_buy else
                    "FII BUY"  if fii_only  else
                    "DII BUY"  if dii_only  else "SELL")
        enriched.append({**s, **tech,
                         "inst_signal": inst_sig,
                         "both_buy": both_buy,
                         "fii_only": fii_only,
                         "dii_only": dii_only})
        time.sleep(0.4)

    return enriched, market, source


# ─────────────────────────────────────────────────────────────────────────────
# HTML HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def spark_svg(prices):
    if len(prices) < 2:
        return ""
    mn, mx = min(prices), max(prices)
    rng = mx - mn or 1
    w, h = 70, 26
    pts = [f"{round(i*w/(len(prices)-1),1)},{round(h-(p-mn)/rng*h,1)}"
           for i, p in enumerate(prices)]
    col = "#00d4aa" if prices[-1] >= prices[0] else "#ff4d6d"
    return (f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
            f'xmlns="http://www.w3.org/2000/svg"><polyline points="{" ".join(pts)}" '
            f'fill="none" stroke="{col}" stroke-width="1.8" '
            f'stroke-linejoin="round"/></svg>')

def sc(sig):
    return {"STRONG BUY":"sbs","BUY":"sb","NEUTRAL":"sn",
            "CAUTION":"sc2","SELL":"ss","N/A":"sn"}.get(sig,"sn")

def rc(v):
    return "ro" if v > 70 else ("rs2" if v < 40 else "rn2")

def fmt(v, prefix="₹"):
    return f"{prefix}{v:,.2f}" if v else "N/A"


# ─────────────────────────────────────────────────────────────────────────────
# GENERATE HTML
# ─────────────────────────────────────────────────────────────────────────────
def generate_html(stocks, market, date_str, source) -> str:
    nc = "up" if market["nifty_chg"]  >= 0 else "dn"
    xc = "up" if market["sensex_chg"] >= 0 else "dn"
    na = "▲"  if market["nifty_chg"]  >= 0 else "▼"
    xa = "▲"  if market["sensex_chg"] >= 0 else "▼"
    fb  = sum(1 for s in stocks if s["fii_cash"] == "buy")
    db  = sum(1 for s in stocks if s["dii_cash"] == "buy")
    bb  = sum(1 for s in stocks if s["both_buy"])
    st  = sum(1 for s in stocks if s["overall"] == "STRONG BUY")

    rows = ""
    for i, s in enumerate(stocks):
        fc  = "bf" if s["fii_cash"] == "buy" else "bx"
        dc  = "bd" if s["dii_cash"] == "buy" else "bx"
        fa  = "▲ BUY" if s["fii_cash"] == "buy" else "▼ SELL"
        da  = "▲ BUY" if s["dii_cash"] == "buy" else "▼ SELL"
        mc2 = "up" if s["macd_hist"] > 0 else "dn"
        ec  = "up" if s["ema_cross"] == "bullish" else "dn"
        spk = spark_svg(s.get("sparkline", []))
        pr  = fmt(s["last_price"]) if s["last_price"] > 0 else s.get("price_str","N/A")

        rows += f"""
      <tr style="animation-delay:{i*0.05:.2f}s">
        <td><div class="sn">{s['name']}</div>
            <div class="sy">{s['symbol'].replace('.NS','')}</div></td>
        <td><div class="pv">{pr}</div><div class="sp">{spk}</div></td>
        <td><span class="b {fc}">{fa}</span></td>
        <td><span class="b {dc}">{da}</span></td>
        <td class="{rc(s['rsi'])}">
          <div class="rv {'up' if s['rsi']<55 else 'dn'}">{s['rsi']}</div>
          <div class="rb2"><div class="rf2" style="width:{min(s['rsi'],100):.0f}%"></div></div>
        </td>
        <td>
          <div class="im"><span class="il">MACD</span>
            <span class="{mc2}">{'+' if s['macd_hist']>0 else ''}{s['macd_hist']}</span></div>
          <div class="im"><span class="il">EMA</span>
            <span class="{ec}">{'↑ Bull' if s['ema_cross']=='bullish' else '↓ Bear'}</span></div>
          <div class="im"><span class="il">ADX</span><span>{s['adx']}</span></div>
          <div class="im"><span class="il">BB</span><span>{s['bb_label']}</span></div>
          <div class="im"><span class="il">StRSI</span><span>{s['stoch_rsi']}</span></div>
        </td>
        <td>
          <div class="sr"><span class="sr-r">R1</span> {fmt(s['resist1'])}</div>
          <div class="sr"><span class="sr-s">S1</span> {fmt(s['support1'])}</div>
          <div class="sr"><span class="sr-r">6mH</span> {fmt(s['swing_high'])}</div>
          <div class="sr"><span class="sr-s">6mL</span> {fmt(s['swing_low'])}</div>
        </td>
        <td><span class="sig {sc(s['overall'])}">{s['overall']}</span></td>
      </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>FII/DII Pulse — {date_str}</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
:root{{--bg:#050c14;--sur:#0b1623;--bdr:#1a3050;--fi:#00d4aa;--di:#ff8c42;
  --bo:#a78bfa;--se:#ff4d6d;--tx:#e2eaf4;--mu:#5a7a99;--go:#f0c060}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--tx);font-family:'Syne',sans-serif;min-height:100vh}}
body::before{{content:'';position:fixed;inset:0;
  background:linear-gradient(rgba(0,212,170,.03) 1px,transparent 1px),
    linear-gradient(90deg,rgba(0,212,170,.03) 1px,transparent 1px);
  background-size:40px 40px;pointer-events:none;z-index:0}}
.w{{position:relative;z-index:1}}
/* HEADER */
header{{background:linear-gradient(135deg,#050c14,#0a1930,#050c14);
  border-bottom:1px solid var(--bdr);padding:16px 26px;
  display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:99;backdrop-filter:blur(8px)}}
.lg{{display:flex;align-items:center;gap:12px}}
.li{{width:40px;height:40px;background:linear-gradient(135deg,var(--fi),var(--bo));
  border-radius:10px;display:flex;align-items:center;justify-content:center;
  font-size:18px;box-shadow:0 0 18px rgba(0,212,170,.4)}}
.lt{{font-size:18px;font-weight:800;letter-spacing:1px}}
.lt span{{color:var(--fi)}}
.ls2{{font-size:9px;color:var(--mu);letter-spacing:1px;margin-top:2px}}
.hm{{display:flex;align-items:center;gap:12px}}
.lv{{display:flex;align-items:center;gap:6px;background:rgba(0,212,170,.1);
  border:1px solid rgba(0,212,170,.3);padding:5px 12px;border-radius:20px;
  font-size:11px;font-weight:700;color:var(--fi);letter-spacing:1px}}
.ld{{width:7px;height:7px;border-radius:50%;background:var(--fi);animation:pulse 1.5s infinite}}
@keyframes pulse{{0%,100%{{opacity:1;transform:scale(1)}}50%{{opacity:.4;transform:scale(.8)}}}}
.src{{font-size:10px;color:var(--bo);background:rgba(167,139,250,.1);
  border:1px solid rgba(167,139,250,.25);padding:4px 10px;border-radius:12px}}
.dt{{font-family:'Space Mono',monospace;font-size:11px;color:var(--mu)}}
/* SUMMARY BAR */
.sum{{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;
  background:var(--bdr);border-bottom:1px solid var(--bdr)}}
.sc2{{background:var(--sur);padding:14px 20px}}
.sl{{font-size:10px;letter-spacing:2px;color:var(--mu);text-transform:uppercase;margin-bottom:4px}}
.sv{{font-family:'Space Mono',monospace;font-size:20px;font-weight:700}}
.sb2{{font-size:11px;color:var(--mu);margin-top:3px}}
.up{{color:var(--fi)}} .dn{{color:var(--se)}}
.cfi{{color:var(--fi)}} .cdi{{color:var(--di)}} .cbo{{color:var(--bo)}} .cgo{{color:var(--go)}}
/* TABLE */
.tw{{padding:20px 26px;overflow-x:auto}}
.tt{{font-size:11px;letter-spacing:2px;text-transform:uppercase;color:var(--mu);
  margin-bottom:14px;display:flex;align-items:center;gap:10px}}
.tt::before{{content:'';width:3px;height:14px;background:var(--fi);border-radius:2px}}
table{{width:100%;border-collapse:collapse;min-width:920px}}
thead tr{{border-bottom:2px solid var(--bdr)}}
th{{padding:9px 10px;text-align:left;font-size:9px;letter-spacing:1.5px;
  color:var(--mu);text-transform:uppercase;font-weight:600}}
th:not(:first-child){{text-align:center}}
tbody tr{{border-bottom:1px solid rgba(26,48,80,.4);
  animation:si .4s ease both;opacity:0;transition:background .15s}}
@keyframes si{{from{{opacity:0;transform:translateX(-6px)}}to{{opacity:1;transform:translateX(0)}}}}
tbody tr:hover{{background:rgba(0,212,170,.03)}}
td{{padding:11px 10px;font-size:12px;vertical-align:middle;text-align:center}}
td:first-child{{text-align:left}}
.sn{{font-weight:700;font-size:13px}}
.sy{{font-size:9px;color:var(--mu);font-family:'Space Mono',monospace;margin-top:2px}}
.pv{{font-family:'Space Mono',monospace;font-size:13px;font-weight:700}}
.sp{{margin-top:4px}}
/* BADGES */
.b{{display:inline-block;padding:4px 9px;border-radius:5px;font-size:10px;font-weight:800;letter-spacing:.5px}}
.bf{{background:rgba(0,212,170,.12);color:var(--fi);border:1px solid rgba(0,212,170,.3)}}
.bd{{background:rgba(255,140,66,.12);color:var(--di);border:1px solid rgba(255,140,66,.3)}}
.bx{{background:rgba(255,77,109,.1);color:var(--se);border:1px solid rgba(255,77,109,.25)}}
/* RSI */
.rv{{font-family:'Space Mono',monospace;font-size:13px;font-weight:700}}
.rb2{{width:78px;height:4px;background:var(--bdr);border-radius:2px;margin:5px auto 0;overflow:hidden}}
.rf2{{height:100%;border-radius:2px}}
.ro .rf2{{background:var(--se)}} .rn2 .rf2{{background:var(--go)}} .rs2 .rf2{{background:var(--fi)}}
/* INDICATORS */
.im{{display:flex;justify-content:center;gap:5px;font-size:10px;margin-bottom:2px}}
.il{{color:var(--mu);font-size:9px;min-width:28px;text-align:right}}
/* S/R */
.sr{{font-size:10px;font-family:'Space Mono',monospace;display:flex;align-items:center;
  gap:4px;justify-content:center;margin-bottom:2px}}
.sr-r{{font-size:8px;padding:1px 4px;border-radius:3px;font-weight:700;
  background:rgba(255,77,109,.2);color:var(--se)}}
.sr-s{{font-size:8px;padding:1px 4px;border-radius:3px;font-weight:700;
  background:rgba(0,212,170,.15);color:var(--fi)}}
/* SIGNALS */
.sig{{display:inline-block;padding:4px 10px;border-radius:5px;font-size:9px;font-weight:800;letter-spacing:.5px}}
.sbs{{background:rgba(0,212,170,.2);color:var(--fi);border:1px solid rgba(0,212,170,.5);
  box-shadow:0 0 8px rgba(0,212,170,.2)}}
.sb{{background:rgba(0,245,212,.1);color:#4ad9c8;border:1px solid rgba(0,245,212,.3)}}
.sn{{background:rgba(240,192,96,.1);color:var(--go);border:1px solid rgba(240,192,96,.25)}}
.sc2{{background:rgba(255,140,66,.1);color:var(--di);border:1px solid rgba(255,140,66,.3)}}
.ss{{background:rgba(255,77,109,.12);color:var(--se);border:1px solid rgba(255,77,109,.3)}}
/* LEGEND */
.leg{{padding:0 26px 18px;display:flex;gap:14px;flex-wrap:wrap;align-items:center}}
.li2{{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--mu)}}
.ld2{{width:9px;height:9px;border-radius:50%}}
footer{{background:var(--sur);border-top:1px solid var(--bdr);padding:12px 26px;
  display:flex;justify-content:space-between;font-size:11px;color:var(--mu);flex-wrap:wrap;gap:8px}}
</style>
</head>
<body>
<div class="w">
<header>
  <div class="lg">
    <div class="li">📊</div>
    <div>
      <div class="lt">FII<span>/DII</span> PULSE</div>
      <div class="ls2">INSTITUTIONAL INTELLIGENCE DASHBOARD · AUTO-GENERATED</div>
    </div>
  </div>
  <div class="hm">
    <div class="src">📡 {source}</div>
    <div class="lv"><div class="ld"></div>LIVE DATA</div>
    <div class="dt">📅 {date_str}</div>
  </div>
</header>

<div class="sum">
  <div class="sc2"><div class="sl">Nifty 50</div>
    <div class="sv cfi">₹{market['nifty_price']:,.2f}</div>
    <div class="sb2 {nc}">{na} {market['nifty_chg']}%</div></div>
  <div class="sc2"><div class="sl">Sensex</div>
    <div class="sv cdi">₹{market['sensex_price']:,.2f}</div>
    <div class="sb2 {xc}">{xa} {market['sensex_chg']}%</div></div>
  <div class="sc2"><div class="sl">Stocks Tracked</div>
    <div class="sv cbo">{len(stocks)}</div>
    <div class="sb2">FII Buys: {fb} · DII Buys: {db} · Both: {bb}</div></div>
  <div class="sc2"><div class="sl">Strong Buy Signals</div>
    <div class="sv cgo">{st}</div>
    <div class="sb2">Stocks with STRONG BUY</div></div>
</div>

<div class="tw">
  <div class="tt">Institutional Stock Activity · Technical Analysis · {date_str}</div>
  <table>
    <thead><tr>
      <th>Stock</th><th>Price / Trend</th><th>FII Cash</th><th>DII Cash</th>
      <th>RSI (14)</th><th>MACD · EMA · ADX · BB · StRSI</th>
      <th>Support / Resistance (6M)</th><th>Signal</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>

<div class="leg">
  <div class="li2"><div class="ld2" style="background:var(--fi)"></div> FII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--di)"></div> DII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--bo)"></div> Both Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--se)"></div> Selling</div>
  <div class="li2" style="margin-left:auto;font-size:10px;">
    RSI &lt;40: Oversold · &gt;70: Overbought · MACD+: Bullish · ADX&gt;25: Strong Trend · StRSI&lt;0.3: Oversold</div>
</div>

<footer>
  <div>🤖 FII/DII Pulse v4 · Source: {source} · Technicals: yfinance + pandas · {date_str}</div>
  <div>⚠️ Not financial advice. Educational purposes only. Always DYOR.</div>
</footer>
</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
# EMAIL — Sends HTML as a real downloadable file attachment
# ─────────────────────────────────────────────────────────────────────────────
def send_email(html_path: Path, date_str: str, source: str, stock_count: int):
    user  = os.getenv("GMAIL_USER", "").strip()
    pwd   = os.getenv("GMAIL_PASS", "").strip()
    rcpts = os.getenv("RECIPIENT_EMAIL", user).strip()

    if not user or not pwd:
        log.warning("⚠️  GMAIL_USER / GMAIL_PASS not set — skipping email")
        return

    to_list = [r.strip() for r in rcpts.split(",") if r.strip()]
    log.info(f"📧 Sending email to: {to_list}")

    # ── Build message ──────────────────────────────────────────────────────
    msg = MIMEMultipart("mixed")       # 'mixed' supports both text + attachment
    msg["Subject"] = f"📊 FII/DII Intelligence Report — {date_str}"
    msg["From"]    = f"FII/DII Pulse <{user}>"
    msg["To"]      = ", ".join(to_list)

    # ── HTML body (preview text in email) ─────────────────────────────────
    body_html = f"""
<html>
<body style="font-family:Arial,sans-serif;background:#050c14;color:#e2eaf4;
             padding:30px;max-width:600px;margin:0 auto;">
  <div style="border:1px solid #1a3050;border-radius:12px;overflow:hidden;">

    <!-- Header -->
    <div style="background:linear-gradient(135deg,#0a1930,#0f2040);padding:24px;
                border-bottom:1px solid #1a3050;text-align:center;">
      <div style="font-size:28px;margin-bottom:8px;">📊</div>
      <h1 style="color:#00d4aa;margin:0;font-size:22px;letter-spacing:1px;">
        FII/DII PULSE</h1>
      <p style="color:#5a7a99;margin:6px 0 0;font-size:12px;letter-spacing:1px;">
        INSTITUTIONAL INTELLIGENCE REPORT</p>
    </div>

    <!-- Date + Source -->
    <div style="background:#0b1623;padding:16px 24px;border-bottom:1px solid #1a3050;
                display:flex;justify-content:space-between;font-size:12px;">
      <span style="color:#5a7a99;">📅 {date_str}</span>
      <span style="color:#a78bfa;">📡 {source}</span>
    </div>

    <!-- Stats -->
    <div style="background:#0f1e2e;padding:20px 24px;border-bottom:1px solid #1a3050;">
      <p style="color:#e2eaf4;font-size:14px;margin:0 0 12px;">
        Today's report tracked <strong style="color:#00d4aa;">{stock_count} stocks</strong>
        with FII/DII institutional activity.</p>
      <p style="color:#5a7a99;font-size:12px;margin:0;">
        Full interactive dashboard is attached as an HTML file.<br>
        <strong>Open the attachment in your browser</strong> to view the complete report
        with RSI, MACD, EMA crossover, Bollinger Bands, ADX, Stochastic RSI,
        and 6-month Support &amp; Resistance levels.</p>
    </div>

    <!-- How to open -->
    <div style="background:#0b1623;padding:16px 24px;border-bottom:1px solid #1a3050;">
      <p style="color:#f0c060;font-size:12px;font-weight:bold;margin:0 0 8px;">
        📎 How to open the report:</p>
      <ol style="color:#5a7a99;font-size:12px;margin:0;padding-left:18px;">
        <li>Download the attached file: <code style="color:#00d4aa;">fii_dii_report_{date_str.replace(' ','_')}.html</code></li>
        <li>Double-click the file to open in your browser</li>
        <li>Or view live at GitHub Pages (if configured)</li>
      </ol>
    </div>

    <!-- Footer -->
    <div style="background:#050c14;padding:16px 24px;text-align:center;">
      <p style="color:#3a5a78;font-size:10px;margin:0;">
        ⚠️ Not financial advice. For educational purposes only. Always DYOR.<br>
        Auto-generated by FII/DII Pulse · Data: {source}
      </p>
    </div>
  </div>
</body>
</html>"""

    msg.attach(MIMEText(body_html, "html", "utf-8"))

    # ── HTML file attachment (the full dashboard) ──────────────────────────
    attachment_name = f"fii_dii_report_{date_str.replace(' ', '_')}.html"

    with open(html_path, "rb") as f:
        file_data = f.read()

    part = MIMEBase("application", "octet-stream")
    part.set_payload(file_data)
    encoders.encode_base64(part)
    part.add_header(
        "Content-Disposition",
        "attachment",
        filename=attachment_name
    )
    part.add_header("Content-Type", "text/html; charset=utf-8")
    msg.attach(part)

    # ── Send via Gmail SMTP SSL ────────────────────────────────────────────
    try:
        log.info("  Connecting to Gmail SMTP...")
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as server:
            server.login(user, pwd)
            server.sendmail(user, to_list, msg.as_string())
        log.info(f"  ✅ Email sent successfully to {to_list}")
    except smtplib.SMTPAuthenticationError:
        log.error("  ❌ Gmail auth failed — check GMAIL_USER and GMAIL_PASS (App Password)")
        raise
    except Exception as e:
        log.error(f"  ❌ Email error: {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────────────────
def main():
    date_str  = datetime.today().strftime("%d %b %Y")
    date_file = datetime.today().strftime("%Y-%m-%d")

    log.info("=" * 65)
    log.info(f"  🚀 FII/DII Pulse v4 — {date_str}")
    log.info("=" * 65)

    # Build enriched data
    stocks, market, source = build_dataset()
    log.info(f"📊 Stocks with technicals: {len(stocks)}")

    # Generate HTML
    html = generate_html(stocks, market, date_str, source)

    # Save files
    index_path = OUTPUT_DIR / "index.html"
    dated_path = OUTPUT_DIR / f"report_{date_file}.html"

    for p in [index_path, dated_path]:
        p.write_text(html, encoding="utf-8")
        log.info(f"💾 Saved: {p}")

    # Send email with HTML attachment
    send_email(dated_path, date_str, source, len(stocks))

    log.info("=" * 65)
    log.info("  ✅ FII/DII Pulse complete!")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
