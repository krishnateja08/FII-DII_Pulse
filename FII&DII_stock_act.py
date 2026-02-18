"""
FII/DII Intelligence Dashboard — v6
=====================================
NSE API: https://www.nseindia.com/api/historicalOR/bulk-block-short-deals
         ?optionType=bulk_deals&from=DD-MM-YYYY&to=DD-MM-YYYY

Date Logic:
  - Runs after 12 AM IST daily
  - Automatically picks last trading day (skips weekends + NSE holidays)
  - from_date = last trading day, to_date = last trading day
"""

import os, smtplib, logging, time, re
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from pathlib import Path
import pytz

import requests
import pandas as pd
import numpy as np
import yfinance as yf
from bs4 import BeautifulSoup
from dotenv import load_dotenv

# ── Setup ─────────────────────────────────────────────────────────────────────
load_dotenv()
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(), logging.FileHandler("dashboard.log")]
)
log = logging.getLogger(__name__)
OUTPUT_DIR = Path("docs")
OUTPUT_DIR.mkdir(exist_ok=True)

# ── NSE INDIA HOLIDAYS (NSE official list — update yearly) ────────────────────
NSE_HOLIDAYS_2025 = {
    "2025-01-26","2025-02-26","2025-03-14","2025-03-31",
    "2025-04-10","2025-04-14","2025-04-18","2025-05-01",
    "2025-08-15","2025-08-27","2025-10-02","2025-10-02",
    "2025-10-21","2025-10-22","2025-11-05","2025-12-25",
}
NSE_HOLIDAYS_2026 = {
    "2026-01-26","2026-03-19","2026-03-20","2026-04-02",
    "2026-04-03","2026-04-14","2026-04-17","2026-05-01",
    "2026-06-19","2026-08-15","2026-08-31","2026-10-09",
    "2026-10-28","2026-11-25","2026-12-25",
}
NSE_HOLIDAYS = NSE_HOLIDAYS_2025 | NSE_HOLIDAYS_2026

# ── Headers ───────────────────────────────────────────────────────────────────
BROWSER_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
}
NSE_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Referer": "https://www.nseindia.com/",
    "X-Requested-With": "XMLHttpRequest",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
}

# ── FII / DII keyword classifiers ─────────────────────────────────────────────
FII_KW = [
    "FII","FPI","FOREIGN","OVERSEAS","GLOBAL","INTERNATIONAL",
    "MORGAN STANLEY","GOLDMAN SACHS","CITIGROUP","CITI BANK",
    "BLACKROCK","VANGUARD","FIDELITY","NOMURA","MACQUARIE",
    "UBS","BARCLAYS","HSBC","JPMORGAN","JP MORGAN","DEUTSCHE",
    "MERRILL LYNCH","SOCIETE GENERALE","BNP PARIBAS","LAZARD",
    "WARBURG","WELLINGTON","ABERDEEN","SCHRODERS","ASHMORE",
    "EASTSPRING","MATTHEWS ASIA","DIMENSIONAL","NEUBERGER",
    "INVESCO DEVELOPING","CREDIT SUISSE","CLSA","JEFFERIES",
    "MOTILAL OSWAL FOREIGN","MIRAE ASSET GLOBAL",
]
DII_KW = [
    "MUTUAL FUND"," MF ","LIC OF INDIA","LIC ","SBI LIFE","SBI MF",
    "HDFC MUTUAL","HDFC MF","ICICI PRUDENTIAL MF","ICICI PRU MF",
    "KOTAK MAHINDRA MF","KOTAK MF","AXIS MUTUAL","AXIS MF",
    "NIPPON INDIA MF","NIPPON MF","ADITYA BIRLA SUN LIFE",
    "DSP MUTUAL","DSP MF","FRANKLIN TEMPLETON","TATA MUTUAL","TATA MF",
    "MIRAE ASSET MF","EDELWEISS MF","MOTILAL OSWAL MF","SUNDARAM MF",
    "UTI MUTUAL","UTI MF","CANARA ROBECO","PGIM INDIA MF",
    "INSURANCE","LIFE INSURANCE","GENERAL INSURANCE",
    "PROVIDENT FUND","PENSION FUND","NATIONAL PENSION",
    "EMPLOYEES PROVIDENT","NPS TRUST","GIC RE","UNITED INDIA",
    "NEW INDIA ASSURANCE","ORIENTAL INSURANCE","NATIONAL INSURANCE",
    "BAJAJ ALLIANZ","HDFC ERGO","ICICI LOMBARD","STAR HEALTH",
]

# ── FALLBACK stocks (always shown if all APIs fail) ───────────────────────────
FALLBACK_STOCKS = [
    {"symbol":"GMRAIRPORT.NS", "name":"GMR Airports",       "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"TORNTPHARM.NS", "name":"Torrent Pharma",     "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"POWERGRID.NS",  "name":"Power Grid Corp",    "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"JSWENERGY.NS",  "name":"JSW Energy",         "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"SUPREMEIND.NS", "name":"Supreme Industries", "fii_cash":"buy",  "dii_cash":"sell"},
    {"symbol":"ASTRAL.NS",     "name":"Astral Poly",        "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"INDIGO.NS",     "name":"IndiGo",             "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"BSE.NS",        "name":"BSE Limited",        "fii_cash":"sell", "dii_cash":"sell"},
    {"symbol":"GODREJCP.NS",   "name":"Godrej Consumer",    "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"SBICARD.NS",    "name":"SBI Cards",          "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"CAMS.NS",       "name":"CAMS",               "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"BRITANNIA.NS",  "name":"Britannia",          "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"KFINTECH.NS",   "name":"KFin Technologies",  "fii_cash":"buy",  "dii_cash":"sell"},
    {"symbol":"ANGELONE.NS",   "name":"Angel One",          "fii_cash":"sell", "dii_cash":"buy"},
    {"symbol":"POLICYBZR.NS",  "name":"PB Fintech",         "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"NUVAMA.NS",     "name":"Nuvama Wealth",      "fii_cash":"buy",  "dii_cash":"sell"},
    {"symbol":"FORTIS.NS",     "name":"Fortis Healthcare",  "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"MANAPPURAM.NS", "name":"Manappuram Finance", "fii_cash":"buy",  "dii_cash":"sell"},
    {"symbol":"360ONE.NS",     "name":"360 One WAM",        "fii_cash":"buy",  "dii_cash":"buy"},
    {"symbol":"APLAPOLLO.NS",  "name":"APL Apollo Tubes",   "fii_cash":"buy",  "dii_cash":"buy"},
]


# ─────────────────────────────────────────────────────────────────────────────
#  DATE UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def is_trading_day(dt: datetime) -> bool:
    """Return True if dt is a weekday and not an NSE holiday."""
    if dt.weekday() >= 5:           # Saturday=5, Sunday=6
        return False
    key = dt.strftime("%Y-%m-%d")
    return key not in NSE_HOLIDAYS


def last_trading_day() -> datetime:
    """
    Return the last completed NSE trading day as of current IST time.
    If it's before market close (15:30 IST) we use yesterday's trading day.
    """
    IST = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(IST)

    # Use today if market has closed (after 15:30), else use previous day
    if now_ist.hour > 15 or (now_ist.hour == 15 and now_ist.minute >= 30):
        candidate = now_ist.replace(tzinfo=None)
    else:
        candidate = (now_ist - timedelta(days=1)).replace(tzinfo=None)

    # Walk backwards until we find a trading day
    for _ in range(10):
        if is_trading_day(candidate):
            return candidate
        candidate -= timedelta(days=1)

    raise RuntimeError("Could not find a trading day in last 10 days")


def fmt_nse_date(dt: datetime) -> str:
    """Format date as DD-MM-YYYY for NSE API."""
    return dt.strftime("%d-%m-%Y")


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 1 — NSE Correct Bulk Deals API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_from_nse() -> list[dict]:
    log.info("📡 [Source 1] NSE Bulk-Block-Short Deals API...")

    try:
        trade_day   = last_trading_day()
        date_str    = fmt_nse_date(trade_day)
        log.info(f"  → Last trading day: {date_str}")

        session = requests.Session()

        # Step 1 — seed cookies
        log.info("  → Seeding NSE cookies (homepage)...")
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=12)
        time.sleep(2)

        # Step 2 — visit the reports page
        session.get(
            "https://www.nseindia.com/market-data/bulk-block-short-selling-deals",
            headers=NSE_HEADERS, timeout=12
        )
        time.sleep(1.5)

        # Step 3 — call the CORRECT API with proper dates
        url = (
            "https://www.nseindia.com/api/historicalOR/"
            "bulk-block-short-deals"
            f"?optionType=bulk_deals&from={date_str}&to={date_str}"
        )
        log.info(f"  → GET {url}")
        resp = session.get(url, headers=NSE_HEADERS, timeout=25)
        log.info(f"  → HTTP {resp.status_code}")
        log.info(f"  → Content-Type: {resp.headers.get('Content-Type','')}")

        if resp.status_code != 200:
            log.warning(f"  ❌ NSE returned HTTP {resp.status_code}")
            return []

        # ── Parse response ─────────────────────────────────────────────────
        raw = resp.json()

        # Log top-level structure for debugging
        if isinstance(raw, dict):
            log.info(f"  → Top-level keys: {list(raw.keys())}")
            for k, v in raw.items():
                log.info(f"     '{k}': {type(v).__name__}"
                         f"{' len='+str(len(v)) if isinstance(v,(list,dict)) else ' = '+str(v)[:60]}")
        elif isinstance(raw, list):
            log.info(f"  → Direct list with {len(raw)} items")
            if raw:
                log.info(f"  → First item type: {type(raw[0])}, sample: {str(raw[0])[:120]}")

        # ── Convert to DataFrame regardless of shape ───────────────────────
        df = None

        if isinstance(raw, list) and raw:
            # Direct list of dicts  OR  list of lists
            if isinstance(raw[0], dict):
                df = pd.DataFrame(raw)
            else:
                df = pd.DataFrame(raw)   # will create col 0,1,2...

        elif isinstance(raw, dict):
            # Try common wrapper keys
            for key in ["data","Data","results","bulkDeals","deals","bulkDealData","records"]:
                val = raw.get(key)
                if isinstance(val, list) and val:
                    if isinstance(val[0], dict):
                        df = pd.DataFrame(val)
                    else:
                        # list-of-lists with separate columns key
                        cols = raw.get("columns", raw.get("Columns", None))
                        df   = pd.DataFrame(val, columns=cols) if cols else pd.DataFrame(val)
                    log.info(f"  → Used key='{key}', shape={df.shape}")
                    break

            # If still None try columns+data pattern
            if df is None and "columns" in raw and "data" in raw:
                df = pd.DataFrame(raw["data"], columns=raw["columns"])
                log.info(f"  → columns+data pattern, shape={df.shape}")

        if df is None or df.empty:
            log.warning("  ⚠️  DataFrame empty — market holiday or no bulk deals today")
            return []

        log.info(f"  → DataFrame shape: {df.shape}")
        log.info(f"  → Columns: {list(df.columns)}")
        if len(df):
            log.info(f"  → Sample row: {df.iloc[0].to_dict()}")

        # ── Normalise column names ─────────────────────────────────────────
        df.columns = [str(c).strip() for c in df.columns]
        rename = {}
        for c in df.columns:
            cu = c.upper()
            if "SYMBOL"   in cu:                          rename[c] = "SYMBOL"
            elif any(x in cu for x in ["COMP","SCRIP_NAME","COMPANY","NAME"]): rename[c] = "COMPANY"
            elif any(x in cu for x in ["CLIENT","PARTY","BUYER","SELLER"]):    rename[c] = "CLIENT"
            elif any(x in cu for x in ["BUY","SELL","BS","TYPE"]):             rename[c] = "BUYSELL"
            elif any(x in cu for x in ["QTY","QUANTITY","SHARES"]):            rename[c] = "QTY"
            elif any(x in cu for x in ["DATE","DT"]):                          rename[c] = "DATE"
            elif any(x in cu for x in ["PRICE","RATE","VALUE"]):               rename[c] = "PRICE"
        df = df.rename(columns=rename)
        log.info(f"  → Normalised columns: {list(df.columns)}")

        if "CLIENT" not in df.columns:
            log.warning("  ❌ CLIENT column missing — cannot classify FII/DII")
            return []

        # ── Classify FII / DII ─────────────────────────────────────────────
        stocks = {}
        matched = 0
        for _, row in df.iterrows():
            sym     = str(row.get("SYMBOL",  "")).strip().upper()
            name    = str(row.get("COMPANY", sym)).strip()
            client  = str(row.get("CLIENT",  "")).strip().upper()
            bs      = str(row.get("BUYSELL", "")).strip().upper()

            if not sym or not client:
                continue

            is_fii = any(k in client for k in FII_KW)
            is_dii = any(k in client for k in DII_KW)

            if not (is_fii or is_dii):
                continue

            matched += 1
            action = "buy" if bs.startswith("B") else "sell"
            if sym not in stocks:
                stocks[sym] = {"symbol": sym+".NS", "name": name,
                               "fii_cash": "neutral", "dii_cash": "neutral"}
            if is_fii: stocks[sym]["fii_cash"] = action
            if is_dii: stocks[sym]["dii_cash"] = action

        result = [v for v in stocks.values()
                  if v["fii_cash"] != "neutral" or v["dii_cash"] != "neutral"]

        log.info(f"  → Scanned {len(df)} rows, matched {matched} FII/DII rows → {len(result)} unique stocks")

        if not result and "CLIENT" in df.columns:
            sample = df["CLIENT"].dropna().unique()[:10].tolist()
            log.info(f"  → Sample CLIENTs (not matching): {sample}")

        return result

    except Exception as e:
        log.warning(f"  ❌ NSE fetch error: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 2 — MunafaSutra
# ─────────────────────────────────────────────────────────────────────────────

def fetch_from_munafasutra() -> list[dict]:
    log.info("📡 [Source 2] MunafaSutra scraper...")
    try:
        resp = requests.get("https://munafasutra.com/nse/FIIDII/",
                            headers=BROWSER_HEADERS, timeout=20)
        resp.raise_for_status()
        soup   = BeautifulSoup(resp.text, "lxml")
        stocks = []
        for a in soup.find_all("a", href=re.compile(r"/nse/stock/")):
            href   = a.get("href","")
            symbol = href.rstrip("/").split("/")[-1]
            name   = a.get_text(strip=True)
            if not symbol or not name:
                continue
            tr = a.find_parent("tr")
            if not tr:
                continue
            tds      = tr.find_all("td")
            row_text = " ".join(t.get_text(" ",strip=True).lower() for t in tds)
            fii = "buy" if "bought" in row_text else "sell"
            dii = "buy" if "bought" in row_text else "sell"
            stocks.append({"symbol": symbol+".NS", "name": name,
                           "fii_cash": fii, "dii_cash": dii})
        log.info(f"  {'✅' if stocks else '❌'} MunafaSutra: {len(stocks)} stocks")
        return stocks[:20]
    except Exception as e:
        log.warning(f"  ❌ MunafaSutra: {e}")
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 3 — Fallback
# ─────────────────────────────────────────────────────────────────────────────

def fetch_fallback() -> list[dict]:
    log.warning("📡 [Source 3] Hardcoded fallback stocks")
    return FALLBACK_STOCKS.copy()


def fetch_fii_dii_stocks():
    s = fetch_from_nse()
    if s: return s, "NSE Bulk Deals API"
    s = fetch_from_munafasutra()
    if s: return s, "MunafaSutra"
    return fetch_fallback(), "Fallback (Known Institutional Stocks)"


# ─────────────────────────────────────────────────────────────────────────────
#  TECHNICAL ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def fix_df(df):
    """Flatten MultiIndex columns from yfinance."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = df.columns.get_level_values(0)
    return df


def compute_technicals(symbol: str) -> dict:
    log.info(f"  📐 {symbol}")
    empty = dict(rsi=50.0, macd_hist=0.0, ema_cross="unknown", bb_label="N/A",
                 adx=0.0, stoch_rsi=0.5, resist1=0.0, support1=0.0,
                 swing_high=0.0, swing_low=0.0, last_price=0.0,
                 overall="N/A", score=0, sparkline=[], data_ok=False)
    try:
        end   = datetime.today()
        start = end - timedelta(days=185)
        df    = yf.download(symbol, start=start, end=end,
                            progress=False, auto_adjust=True)
        if df is None or df.empty:
            raise ValueError("Empty data")
        df = fix_df(df)
        df = df[["Open","High","Low","Close","Volume"]].dropna()
        if len(df) < 25:
            raise ValueError(f"Only {len(df)} rows")

        c  = df["Close"].astype(float)
        h  = df["High"].astype(float)
        lo = df["Low"].astype(float)
        lc = float(c.iloc[-1])

        # RSI(14)
        dlt  = c.diff()
        gain = dlt.clip(lower=0);  loss = (-dlt).clip(lower=0)
        ag   = gain.ewm(com=13,adjust=False).mean()
        al   = loss.ewm(com=13,adjust=False).mean()
        rsi_s= 100 - (100/(1 + ag/al.replace(0,np.nan)))
        rsi  = round(float(rsi_s.iloc[-1]),1)

        # MACD(12,26,9)
        macd  = c.ewm(span=12,adjust=False).mean() - c.ewm(span=26,adjust=False).mean()
        mhist = round(float((macd - macd.ewm(span=9,adjust=False).mean()).iloc[-1]),2)

        # EMA 20/50
        ema20 = c.ewm(span=20,adjust=False).mean()
        ema50 = c.ewm(span=50,adjust=False).mean()
        ecross= "bullish" if float(ema20.iloc[-1])>float(ema50.iloc[-1]) else "bearish"

        # Bollinger Bands
        bm  = c.rolling(20).mean(); bs = c.rolling(20).std()
        bu  = float((bm+2*bs).iloc[-1]); bl2= float((bm-2*bs).iloc[-1])
        bp  = (lc-bl2)/((bu-bl2) or 1)
        bbl = "Overbought" if bp>0.8 else ("Oversold" if bp<0.2 else "Mid")

        # ADX(14)
        pdm = h.diff().clip(lower=0); mdm = (-lo.diff()).clip(lower=0)
        tr  = pd.concat([h-lo,(h-c.shift()).abs(),(lo-c.shift()).abs()],axis=1).max(axis=1)
        atr = tr.ewm(com=13,adjust=False).mean()
        pdi = 100*pdm.ewm(com=13,adjust=False).mean()/atr
        mdi = 100*mdm.ewm(com=13,adjust=False).mean()/atr
        dx  = 100*(pdi-mdi).abs()/(pdi+mdi).replace(0,np.nan)
        adx = round(float(dx.ewm(com=13,adjust=False).mean().iloc[-1]),1)

        # Stoch RSI
        srsi = (rsi_s-rsi_s.rolling(14).min()) / \
               (rsi_s.rolling(14).max()-rsi_s.rolling(14).min()).replace(0,np.nan)
        sv   = float(srsi.iloc[-1]); sv = 0.5 if np.isnan(sv) else round(sv,2)

        # Support/Resistance
        n  = min(120,len(h))
        pv = (float(h.iloc[-1])+float(lo.iloc[-1])+lc)/3
        r1 = round(2*pv - float(lo.iloc[-1]),2)
        s1 = round(2*pv - float(h.iloc[-1]),2)
        sh = round(float(h.rolling(n).max().iloc[-1]),2)
        sl = round(float(lo.rolling(n).min().iloc[-1]),2)

        # Signal score
        sc = 0
        sc += 2 if rsi<40 else (1 if rsi<55 else (-2 if rsi>70 else 0))
        sc += 2 if mhist>0 else 0
        sc += 2 if ecross=="bullish" else 0
        sc += 1 if adx>25 else 0
        sc += 1 if sv<0.3 else (-1 if sv>0.8 else 0)
        ov  = ("STRONG BUY" if sc>=5 else "BUY" if sc>=3
               else "NEUTRAL" if sc>=0 else "CAUTION" if sc>=-2 else "SELL")

        spark = [round(float(x),2) for x in c.iloc[-7:].tolist()]
        return dict(rsi=rsi,macd_hist=mhist,ema_cross=ecross,bb_label=bbl,
                    adx=adx,stoch_rsi=sv,resist1=r1,support1=s1,
                    swing_high=sh,swing_low=sl,last_price=round(lc,2),
                    overall=ov,score=sc,sparkline=spark,data_ok=True)
    except Exception as e:
        log.warning(f"    ⚠️  {symbol}: {e}")
        return empty


# ─────────────────────────────────────────────────────────────────────────────
#  MARKET SUMMARY
# ─────────────────────────────────────────────────────────────────────────────

def fetch_market_summary() -> dict:
    log.info("📡 Nifty / Sensex...")
    try:
        def load(sym):
            df = yf.download(sym, period="5d", progress=False, auto_adjust=True)
            df = fix_df(df)
            c  = df["Close"].dropna().astype(float)
            return (round(float(c.iloc[-1]),2),
                    round(float((c.iloc[-1]-c.iloc[-2])/c.iloc[-2]*100),2) if len(c)>=2 else 0.0)
        np_, nc = load("^NSEI")
        sp_, sc = load("^BSESN")
        return dict(nifty_price=np_,nifty_chg=nc,sensex_price=sp_,sensex_chg=sc)
    except Exception as e:
        log.warning(f"Market summary failed: {e}")
        return dict(nifty_price=0,nifty_chg=0,sensex_price=0,sensex_chg=0)


# ─────────────────────────────────────────────────────────────────────────────
#  BUILD FULL DATASET
# ─────────────────────────────────────────────────────────────────────────────

def build_dataset():
    raw, source = fetch_fii_dii_stocks()
    log.info(f"✅ Source: '{source}' — {len(raw)} stocks")
    market   = fetch_market_summary()
    enriched = []
    for s in raw:
        tech     = compute_technicals(s["symbol"])
        both_buy = s["fii_cash"]=="buy" and s["dii_cash"]=="buy"
        fii_only = s["fii_cash"]=="buy" and s["dii_cash"]!="buy"
        dii_only = s["dii_cash"]=="buy" and s["fii_cash"]!="buy"
        inst_sig = ("BOTH BUY" if both_buy else
                    "FII BUY"  if fii_only  else
                    "DII BUY"  if dii_only  else "SELL")
        enriched.append({**s, **tech,
                         "inst_signal":inst_sig,
                         "both_buy":both_buy,
                         "fii_only":fii_only,
                         "dii_only":dii_only})
        time.sleep(0.4)
    return enriched, market, source


# ─────────────────────────────────────────────────────────────────────────────
#  HTML HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def spark_svg(prices):
    if len(prices)<2: return ""
    mn,mx = min(prices),max(prices); rng=mx-mn or 1
    w,h   = 70,26
    pts   = [f"{round(i*w/(len(prices)-1),1)},{round(h-(p-mn)/rng*h,1)}"
             for i,p in enumerate(prices)]
    col   = "#00d4aa" if prices[-1]>=prices[0] else "#ff4d6d"
    return (f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
            f'xmlns="http://www.w3.org/2000/svg"><polyline points="{" ".join(pts)}" '
            f'fill="none" stroke="{col}" stroke-width="1.8" stroke-linejoin="round"/></svg>')

def sigcls(s):
    return {"STRONG BUY":"sbs","BUY":"sbuy","NEUTRAL":"sn",
            "CAUTION":"sca","SELL":"sse","N/A":"sn"}.get(s,"sn")

def rsicls(v):
    return "ro" if v>70 else ("rs2" if v<40 else "rn2")

def fmt(v):
    return f"₹{v:,.2f}" if v else "N/A"


# ─────────────────────────────────────────────────────────────────────────────
#  GENERATE HTML
# ─────────────────────────────────────────────────────────────────────────────

def generate_html(stocks, market, date_str, source) -> str:
    nc  = "up" if market["nifty_chg"]  >=0 else "dn"
    xc  = "up" if market["sensex_chg"] >=0 else "dn"
    na  = "▲"  if market["nifty_chg"]  >=0 else "▼"
    xa  = "▲"  if market["sensex_chg"] >=0 else "▼"
    fb  = sum(1 for s in stocks if s["fii_cash"]=="buy")
    db  = sum(1 for s in stocks if s["dii_cash"]=="buy")
    bb  = sum(1 for s in stocks if s["both_buy"])
    st  = sum(1 for s in stocks if s["overall"]=="STRONG BUY")

    rows = ""
    for i, s in enumerate(stocks):
        fc  = "bf" if s["fii_cash"]=="buy"  else "bx"
        dc  = "bd" if s["dii_cash"]=="buy"  else "bx"
        fa  = "▲ BUY"  if s["fii_cash"]=="buy"  else "▼ SELL"
        da  = "▲ BUY"  if s["dii_cash"]=="buy"  else "▼ SELL"
        mc2 = "up" if s["macd_hist"]>0 else "dn"
        ec  = "up" if s["ema_cross"]=="bullish" else "dn"
        spk = spark_svg(s.get("sparkline",[]))
        pr  = fmt(s["last_price"]) if s["last_price"]>0 else s.get("price_str","N/A")

        rows += f"""
      <tr style="animation-delay:{i*0.05:.2f}s">
        <td><div class="sn">{s['name']}</div>
            <div class="sy">{s['symbol'].replace('.NS','')}</div></td>
        <td><div class="pv">{pr}</div><div class="sp">{spk}</div></td>
        <td><span class="b {fc}">{fa}</span></td>
        <td><span class="b {dc}">{da}</span></td>
        <td class="{rsicls(s['rsi'])}">
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
        <td><span class="sig {sigcls(s['overall'])}">{s['overall']}</span></td>
      </tr>"""

    css = """
:root{--bg:#050c14;--sur:#0b1623;--bdr:#1a3050;--fi:#00d4aa;--di:#ff8c42;
  --bo:#a78bfa;--se:#ff4d6d;--tx:#e2eaf4;--mu:#5a7a99;--go:#f0c060}
*{margin:0;padding:0;box-sizing:border-box}
body{background:var(--bg);color:var(--tx);font-family:'Syne',sans-serif;min-height:100vh}
body::before{content:'';position:fixed;inset:0;
  background:linear-gradient(rgba(0,212,170,.03) 1px,transparent 1px),
    linear-gradient(90deg,rgba(0,212,170,.03) 1px,transparent 1px);
  background-size:40px 40px;pointer-events:none;z-index:0}
.w{position:relative;z-index:1}
header{background:linear-gradient(135deg,#050c14,#0a1930,#050c14);
  border-bottom:1px solid var(--bdr);padding:16px 26px;
  display:flex;align-items:center;justify-content:space-between;
  position:sticky;top:0;z-index:99;backdrop-filter:blur(8px)}
.lg{display:flex;align-items:center;gap:12px}
.li{width:40px;height:40px;background:linear-gradient(135deg,var(--fi),var(--bo));
  border-radius:10px;display:flex;align-items:center;justify-content:center;
  font-size:18px;box-shadow:0 0 18px rgba(0,212,170,.4)}
.lt{font-size:18px;font-weight:800;letter-spacing:1px}
.lt span{color:var(--fi)}
.ls2{font-size:9px;color:var(--mu);letter-spacing:1px;margin-top:2px}
.hm{display:flex;align-items:center;gap:12px}
.lv{display:flex;align-items:center;gap:6px;background:rgba(0,212,170,.1);
  border:1px solid rgba(0,212,170,.3);padding:5px 12px;border-radius:20px;
  font-size:11px;font-weight:700;color:var(--fi);letter-spacing:1px}
.ld{width:7px;height:7px;border-radius:50%;background:var(--fi);animation:pulse 1.5s infinite}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.4;transform:scale(.8)}}
.src{font-size:10px;color:var(--bo);background:rgba(167,139,250,.1);
  border:1px solid rgba(167,139,250,.25);padding:4px 10px;border-radius:12px}
.dt{font-family:'Space Mono',monospace;font-size:11px;color:var(--mu)}
.sum{display:grid;grid-template-columns:repeat(4,1fr);gap:1px;
  background:var(--bdr);border-bottom:1px solid var(--bdr)}
.sc2{background:var(--sur);padding:14px 20px}
.sl{font-size:10px;letter-spacing:2px;color:var(--mu);text-transform:uppercase;margin-bottom:4px}
.sv{font-family:'Space Mono',monospace;font-size:20px;font-weight:700}
.sb2{font-size:11px;color:var(--mu);margin-top:3px}
.up{color:var(--fi)} .dn{color:var(--se)}
.cfi{color:var(--fi)} .cdi{color:var(--di)} .cbo{color:var(--bo)} .cgo{color:var(--go)}
.tw{padding:20px 26px;overflow-x:auto}
.tt{font-size:11px;letter-spacing:2px;text-transform:uppercase;color:var(--mu);
  margin-bottom:14px;display:flex;align-items:center;gap:10px}
.tt::before{content:'';width:3px;height:14px;background:var(--fi);border-radius:2px}
table{width:100%;border-collapse:collapse;min-width:920px}
thead tr{border-bottom:2px solid var(--bdr)}
th{padding:9px 10px;text-align:left;font-size:9px;letter-spacing:1.5px;
  color:var(--mu);text-transform:uppercase;font-weight:600}
th:not(:first-child){text-align:center}
tbody tr{border-bottom:1px solid rgba(26,48,80,.4);
  animation:si .4s ease both;opacity:0;transition:background .15s}
@keyframes si{from{opacity:0;transform:translateX(-6px)}to{opacity:1;transform:translateX(0)}}
tbody tr:hover{background:rgba(0,212,170,.03)}
td{padding:11px 10px;font-size:12px;vertical-align:middle;text-align:center}
td:first-child{text-align:left}
.sn{font-weight:700;font-size:13px}
.sy{font-size:9px;color:var(--mu);font-family:'Space Mono',monospace;margin-top:2px}
.pv{font-family:'Space Mono',monospace;font-size:13px;font-weight:700}
.sp{margin-top:4px}
.b{display:inline-block;padding:4px 9px;border-radius:5px;font-size:10px;font-weight:800;letter-spacing:.5px}
.bf{background:rgba(0,212,170,.12);color:var(--fi);border:1px solid rgba(0,212,170,.3)}
.bd{background:rgba(255,140,66,.12);color:var(--di);border:1px solid rgba(255,140,66,.3)}
.bx{background:rgba(255,77,109,.1);color:var(--se);border:1px solid rgba(255,77,109,.25)}
.rv{font-family:'Space Mono',monospace;font-size:13px;font-weight:700}
.rb2{width:78px;height:4px;background:var(--bdr);border-radius:2px;margin:5px auto 0;overflow:hidden}
.rf2{height:100%;border-radius:2px}
.ro .rf2{background:var(--se)} .rn2 .rf2{background:var(--go)} .rs2 .rf2{background:var(--fi)}
.im{display:flex;justify-content:center;gap:5px;font-size:10px;margin-bottom:2px}
.il{color:var(--mu);font-size:9px;min-width:30px;text-align:right}
.sr{font-size:10px;font-family:'Space Mono',monospace;display:flex;align-items:center;
  gap:4px;justify-content:center;margin-bottom:2px}
.sr-r{font-size:8px;padding:1px 4px;border-radius:3px;font-weight:700;
  background:rgba(255,77,109,.2);color:var(--se)}
.sr-s{font-size:8px;padding:1px 4px;border-radius:3px;font-weight:700;
  background:rgba(0,212,170,.15);color:var(--fi)}
.sig{display:inline-block;padding:4px 10px;border-radius:5px;font-size:9px;font-weight:800;letter-spacing:.5px}
.sbs{background:rgba(0,212,170,.2);color:var(--fi);border:1px solid rgba(0,212,170,.5);
  box-shadow:0 0 8px rgba(0,212,170,.2)}
.sbuy{background:rgba(0,245,212,.1);color:#4ad9c8;border:1px solid rgba(0,245,212,.3)}
.sn{background:rgba(240,192,96,.1);color:var(--go);border:1px solid rgba(240,192,96,.25)}
.sca{background:rgba(255,140,66,.1);color:var(--di);border:1px solid rgba(255,140,66,.3)}
.sse{background:rgba(255,77,109,.12);color:var(--se);border:1px solid rgba(255,77,109,.3)}
.leg{padding:0 26px 18px;display:flex;gap:14px;flex-wrap:wrap;align-items:center}
.li2{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--mu)}
.ld2{width:9px;height:9px;border-radius:50%}
footer{background:var(--sur);border-top:1px solid var(--bdr);padding:12px 26px;
  display:flex;justify-content:space-between;font-size:11px;color:var(--mu);flex-wrap:wrap;gap:8px}
"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>FII/DII Pulse — {date_str}</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>{css}</style>
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
  <div class="tt">Institutional Activity · Technical Analysis · {date_str}</div>
  <table>
    <thead><tr>
      <th>Stock</th><th>Price / Trend</th><th>FII Cash</th><th>DII Cash</th>
      <th>RSI(14)</th><th>MACD · EMA · ADX · BB · StRSI</th>
      <th>Support / Resistance (6M)</th><th>Signal</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>

<div class="leg">
  <div class="li2"><div class="ld2" style="background:var(--fi)"></div>FII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--di)"></div>DII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--bo)"></div>Both Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--se)"></div>Selling</div>
  <div class="li2" style="margin-left:auto;font-size:10px;">
    RSI &lt;40: Oversold · &gt;70: Overbought · MACD+: Bullish · ADX&gt;25: Strong Trend</div>
</div>

<footer>
  <div>🤖 FII/DII Pulse v6 · Source: {source} · Technicals: yfinance+pandas · {date_str}</div>
  <div>⚠️ Not financial advice. Educational purposes only. Always DYOR.</div>
</footer>
</div></body></html>"""


# ─────────────────────────────────────────────────────────────────────────────
#  EMAIL  — proper HTML file attachment
# ─────────────────────────────────────────────────────────────────────────────

def send_email(html_path: Path, date_str: str, source: str, count: int):
    user  = os.getenv("GMAIL_USER","").strip()
    pwd   = os.getenv("GMAIL_PASS","").strip()
    rcpts = os.getenv("RECIPIENT_EMAIL", user).strip()

    if not user or not pwd:
        log.warning("⚠️  GMAIL_USER / GMAIL_PASS not set — skipping email")
        return

    to_list = [r.strip() for r in rcpts.split(",") if r.strip()]
    log.info(f"📧 Sending to: {to_list}")

    msg            = MIMEMultipart("mixed")
    msg["Subject"] = f"📊 FII/DII Intelligence Report — {date_str}"
    msg["From"]    = f"FII/DII Pulse <{user}>"
    msg["To"]      = ", ".join(to_list)

    # ── Email body HTML ────────────────────────────────────────────────────
    fname = html_path.name
    body  = f"""<html><body style="font-family:Arial,sans-serif;background:#050c14;
color:#e2eaf4;padding:30px;max-width:600px;margin:0 auto;">
<div style="border:1px solid #1a3050;border-radius:12px;overflow:hidden;">
  <div style="background:linear-gradient(135deg,#0a1930,#0f2040);padding:24px;
              border-bottom:1px solid #1a3050;text-align:center;">
    <div style="font-size:32px;margin-bottom:8px;">📊</div>
    <h1 style="color:#00d4aa;margin:0;font-size:22px;letter-spacing:1px;">FII/DII PULSE</h1>
    <p style="color:#5a7a99;margin:6px 0 0;font-size:12px;letter-spacing:1px;">
      INSTITUTIONAL INTELLIGENCE REPORT</p>
  </div>
  <div style="background:#0b1623;padding:14px 24px;border-bottom:1px solid #1a3050;
              font-size:12px;display:flex;justify-content:space-between;">
    <span style="color:#5a7a99;">📅 {date_str}</span>
    <span style="color:#a78bfa;">📡 {source}</span>
  </div>
  <div style="background:#0f1e2e;padding:20px 24px;border-bottom:1px solid #1a3050;">
    <p style="color:#e2eaf4;font-size:14px;margin:0 0 10px;">
      Today's report tracked <strong style="color:#00d4aa;">{count} stocks</strong>
      with FII/DII institutional activity.</p>
    <p style="color:#5a7a99;font-size:12px;margin:0;">
      The full interactive dashboard is <strong>attached below</strong> as an HTML file.
    </p>
  </div>
  <div style="background:#0b1623;padding:16px 24px;border-bottom:1px solid #1a3050;">
    <p style="color:#f0c060;font-size:12px;font-weight:bold;margin:0 0 8px;">
      📎 How to open your report:</p>
    <ol style="color:#5a7a99;font-size:12px;margin:0;padding-left:18px;line-height:1.8;">
      <li>Find the attachment: <code style="color:#00d4aa;">{fname}</code></li>
      <li>Save it to your desktop</li>
      <li>Double-click → opens in Chrome / Firefox / Edge</li>
    </ol>
  </div>
  <div style="background:#050c14;padding:14px 24px;text-align:center;">
    <p style="color:#3a5a78;font-size:10px;margin:0;">
      ⚠️ Not financial advice. Educational purposes only. Always DYOR.<br>
      Auto-generated by FII/DII Pulse v6
    </p>
  </div>
</div></body></html>"""

    msg.attach(MIMEText(body, "html", "utf-8"))

    # ── Attach HTML file (proper MIME encoding) ────────────────────────────
    with open(html_path, "rb") as f:
        data = f.read()

    att = MIMEBase("application", "octet-stream")
    att.set_payload(data)
    encoders.encode_base64(att)
    att.add_header("Content-Disposition", "attachment", filename=html_path.name)
    att.add_header("Content-Type", f'text/html; name="{html_path.name}"')
    msg.attach(att)

    # ── Send ───────────────────────────────────────────────────────────────
    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as srv:
            srv.login(user, pwd)
            srv.sendmail(user, to_list, msg.as_string())
        log.info(f"  ✅ Email sent to {to_list}")
    except smtplib.SMTPAuthenticationError:
        log.error("  ❌ Gmail auth failed — use App Password, not your main password")
        raise
    except Exception as e:
        log.error(f"  ❌ Email error: {e}")
        raise


# ─────────────────────────────────────────────────────────────────────────────
#  MAIN
# ─────────────────────────────────────────────────────────────────────────────

def main():
    IST      = pytz.timezone("Asia/Kolkata")
    now_ist  = datetime.now(IST)
    date_str = now_ist.strftime("%d %b %Y")
    date_file= now_ist.strftime("%Y-%m-%d")

    log.info("=" * 65)
    log.info(f"  🚀 FII/DII Pulse v6 — {date_str}  (IST: {now_ist.strftime('%H:%M')})")
    log.info("=" * 65)

    stocks, market, source = build_dataset()
    log.info(f"📊 Stocks enriched: {len(stocks)}")

    html = generate_html(stocks, market, date_str, source)

    index_path = OUTPUT_DIR / "index.html"
    dated_path = OUTPUT_DIR / f"report_{date_file}.html"
    for p in [index_path, dated_path]:
        p.write_text(html, encoding="utf-8")
        log.info(f"💾 Saved: {p}")

    send_email(dated_path, date_str, source, len(stocks))

    log.info("=" * 65)
    log.info("  ✅ Complete!")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
