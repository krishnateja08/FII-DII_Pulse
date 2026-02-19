"""
FII/DII Intelligence Dashboard — v9 · Warm Parchment Theme
===================================================
KEY CHANGE from v8:
  Theme: Warm Parchment — Cream/Ivory base · Navy header · Forest green
  accents · Instrument Serif + DM Sans typography · Fully responsive
  (Mobile 320px → Tablet 768px → Laptop 1024px → Desktop 1440px+)

NSE CSV endpoints:
  Bulk: https://www.nseindia.com/api/historicalOR/bulk-block-short-deals
        ?optionType=bulk_deals&from=DD-MM-YYYY&to=DD-MM-YYYY&csv=true
  Block: https://www.nseindia.com/api/historicalOR/bulk-block-short-deals
        ?optionType=block_deals&from=DD-MM-YYYY&to=DD-MM-YYYY&csv=true

Date Logic (VERIFIED):
  - Block Deal window closes at 06:30 PM IST daily
  - After  18:30 IST → to_date = TODAY  (deals are final)
  - Before 18:30 IST → to_date = last completed trading day
  - from_date = 5 trading days BEFORE to_date (6 total)

RESPONSIVE BREAKPOINTS:
  Mobile  : < 640px  — stacked cards, hidden sidebar, compact stats
  Tablet  : 640–1023px — 2-col stats, collapsible sidebar overlay
  Laptop  : 1024–1279px — sidebar visible, 3-col stats
  Desktop : 1280px+ — full layout, 6-col stats
"""

import io, os, smtplib, logging, time, re
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

# ── NSE INDIA HOLIDAYS ────────────────────────────────────────────────────────
NSE_HOLIDAYS_2025 = {
    "2025-01-26","2025-02-26","2025-03-14","2025-03-31",
    "2025-04-10","2025-04-14","2025-04-18","2025-05-01",
    "2025-08-15","2025-08-27","2025-10-02",
    "2025-10-21","2025-10-22","2025-11-05","2025-12-25",
}
NSE_HOLIDAYS_2026 = {
    "2026-01-26","2026-03-19","2026-03-20","2026-04-02",
    "2026-04-03","2026-04-14","2026-04-17","2026-05-01",
    "2026-06-19","2026-08-15","2026-08-31","2026-10-09",
    "2026-10-28","2026-11-25","2026-12-25",
}
NSE_HOLIDAYS = NSE_HOLIDAYS_2025 | NSE_HOLIDAYS_2026

# ── Browser / NSE Headers ─────────────────────────────────────────────────────
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
    "FII","FPI","FOREIGN","OVERSEAS","GLOBAL","INTERNATIONAL","NON RESIDENT",
    "MORGAN STANLEY","GOLDMAN SACHS","CITIGROUP","CITI BANK","CITIBANK",
    "BLACKROCK","VANGUARD","FIDELITY","NOMURA","MACQUARIE","MORGANSTANLEY",
    "UBS","BARCLAYS","HSBC","JPMORGAN","JP MORGAN","DEUTSCHE","DB INTERNATIONAL",
    "MERRILL LYNCH","SOCIETE GENERALE","BNP PARIBAS","LAZARD","NATIXIS",
    "WARBURG","WELLINGTON","ABERDEEN","SCHRODERS","ASHMORE","INVESCO",
    "EASTSPRING","MATTHEWS ASIA","DIMENSIONAL","NEUBERGER","PICTET",
    "CREDIT SUISSE","CLSA","JEFFERIES","BOFA","BANK OF AMERICA","CITI GROUP",
    "MOTILAL OSWAL FOREIGN","MIRAE ASSET GLOBAL","AMUNDI","FRANKLIN OVERSEAS",
    "FIRST STATE","OPPENHEIMER","ARTISAN","DRIEHAUS","CAUSEWAY","COMMONWEALTH",
    "DODGE & COX","HARBOR","WASATCH","WILLIAM BLAIR","MANNING","THORNBURG",
    "GENESIS","CORONATION","ALLAN GRAY","AFRICA","EMERGING MARKETS",
    "SINGAPORE","CAYMAN","MAURITIUS","CYPRUS","NETHERLANDS ANTILLES",
]

DII_KW = [
    "MUTUAL FUND","TRUSTEE","AMC LIMITED","ASSET MANAGEMENT",
    " MF ","MF-","- MF","(MF)","_MF_",
    "SBI MF","SBI MUTUAL","SBI BLUECHIP","SBI MAGNUM",
    "HDFC MF","HDFC MUTUAL","HDFC BALANCED","HDFC EQUITY",
    "ICICI PRUDENTIAL MF","ICICI PRU MF","ICICI PRUDENTIAL MUTUAL",
    "KOTAK MAHINDRA MF","KOTAK MF","KOTAK MUTUAL",
    "AXIS MUTUAL","AXIS MF","AXIS LONG TERM",
    "NIPPON INDIA MF","NIPPON MF","NIPPON MUTUAL","NIPPON INDIA MUTUAL",
    "ADITYA BIRLA SUN LIFE","ABSL MF","ADITYA BIRLA MF",
    "DSP MUTUAL","DSP MF","DSP BLACKROCK",
    "FRANKLIN TEMPLETON","FRANKLIN INDIA",
    "TATA MUTUAL","TATA MF","TATA AIA",
    "MIRAE ASSET MF","MIRAE ASSET MUTUAL",
    "EDELWEISS MF","EDELWEISS MUTUAL",
    "MOTILAL OSWAL MF","MOTILAL OSWAL MUTUAL",
    "SUNDARAM MF","SUNDARAM MUTUAL",
    "UTI MUTUAL","UTI MF","UTI TRUSTEE",
    "CANARA ROBECO","PGIM INDIA MF","PGIM INDIA MUTUAL",
    "WHITEOAK CAPITAL MF","WHITEOAK MF",
    "QUANT MUTUAL","QUANT MF",
    "BANDHAN MF","BANDHAN MUTUAL",
    "NAVI MF","NAVI MUTUAL","360 ONE MF","360ONE MF",
    "GROWW MF","GROWW MUTUAL",
    "SAMCO MF","SAMCO MUTUAL","TRUST MF","TRUST MUTUAL",
    "LIC OF INDIA","LIC MF","LIFE INSURANCE CORPORATION",
    "SBI LIFE","HDFC LIFE","ICICI PRUDENTIAL LIFE","MAX LIFE","BAJAJ LIFE",
    "INSURANCE","LIFE INSURANCE","GENERAL INSURANCE","REINSURANCE",
    "NEW INDIA ASSURANCE","ORIENTAL INSURANCE","NATIONAL INSURANCE CO",
    "BAJAJ ALLIANZ","HDFC ERGO","ICICI LOMBARD","STAR HEALTH","CARE HEALTH",
    "GIC RE","GIC OF INDIA","UNITED INDIA","AGRICULTURE INSURANCE",
    "PROVIDENT FUND","PENSION FUND","NATIONAL PENSION","NPS TRUST",
    "EMPLOYEES PROVIDENT","EPFO","COAL MINES","SEAMEN PROVIDENT",
    "NATIONAL INVESTMENT AND INFRASTRUCTURE","NIIF",
    "INDIA INFRASTRUCTURE FINANCE","IIFCL",
    "POWER FINANCE","PFC","REC LIMITED","REC LTD",
    "NABARD","SIDBI","EXIM BANK","NATIONAL HOUSING BANK",
    "ALTERNATIVE INVESTMENT FUND","AIF","CAT III AIF","CAT II AIF",
    "PORTFOLIO MANAGEMENT","PMS ",
]

# ── FALLBACK stocks ───────────────────────────────────────────────────────────
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
    if dt.weekday() >= 5:
        return False
    return dt.strftime("%Y-%m-%d") not in NSE_HOLIDAYS


def fmt_nse_date(dt: datetime) -> str:
    return dt.strftime("%d-%m-%Y")


def get_date_range() -> tuple:
    IST = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(IST)
    today   = now_ist.replace(tzinfo=None).replace(
        hour=0, minute=0, second=0, microsecond=0
    )

    past_cutoff = now_ist.hour > 18 or (now_ist.hour == 18 and now_ist.minute >= 30)

    if past_cutoff and is_trading_day(today):
        to_date = today
        log.info("  → Past 18:30 IST — TODAY is to_date")
    else:
        to_date = today - timedelta(days=1)
        for _ in range(10):
            if is_trading_day(to_date):
                break
            to_date -= timedelta(days=1)
        log.info("  → Before 18:30 IST — last trading day is to_date")

    from_date = to_date
    steps     = 0
    candidate = to_date - timedelta(days=1)
    while True:
        if (to_date - candidate).days > 30:
            log.warning("  ⚠️  Could not find 5 trading days back in 30 days")
            break
        if is_trading_day(candidate):
            steps    += 1
            from_date = candidate
            if steps == 5:
                break
        candidate -= timedelta(days=1)

    label = f"{fmt_nse_date(from_date)} → {fmt_nse_date(to_date)}"
    log.info(f"  → Date range: {label}  ({steps + 1} trading days)")
    return from_date, to_date, label


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 1 — NSE CSV Download API
# ─────────────────────────────────────────────────────────────────────────────

def fetch_from_nse() -> list:
    log.info("[Source 1] NSE Bulk/Block Deals — CSV Download API...")
    try:
        from_date, to_date, date_range_label = get_date_range()
        from_str = fmt_nse_date(from_date)
        to_str   = fmt_nse_date(to_date)
        log.info(f"  -> Range: {from_str} to {to_str}")

        csv_endpoints = [
            {
                "url": "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals",
                "params": {"optionType": "bulk_deals", "from": from_str, "to": to_str, "csv": "true"},
                "deal_type": "bulk_deals",
            },
            {
                "url": "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals",
                "params": {"optionType": "block_deals", "from": from_str, "to": to_str, "csv": "true"},
                "deal_type": "block_deals",
            },
        ]

        session_obj = None
        use_cffi    = False

        try:
            from curl_cffi import requests as cffi_req
            log.info("  -> Using curl_cffi Chrome120 (Akamai bypass)")
            session_obj = cffi_req.Session(impersonate="chrome120")
            session_obj.get("https://www.nseindia.com/", timeout=15)
            time.sleep(2)
            session_obj.get(
                "https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
                timeout=15,
            )
            time.sleep(2)
            use_cffi = True
        except ImportError:
            log.warning("  -> curl_cffi not installed — using requests")
        except Exception as e:
            log.warning(f"  -> curl_cffi error: {e} — using requests")

        if not use_cffi:
            session_obj = requests.Session()
            session_obj.headers.update(NSE_HEADERS)
            r = session_obj.get("https://www.nseindia.com/", timeout=15)
            log.info(f"  -> Homepage HTTP {r.status_code} | cookies: {list(session_obj.cookies.keys())}")
            time.sleep(2.5)
            session_obj.get(
                "https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
                timeout=15,
            )
            time.sleep(2)

        csv_req_headers = {
            "Referer": "https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
            "Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        all_dfs = []

        for ep in csv_endpoints:
            deal_type = ep["deal_type"]
            log.info(f"  -> Fetching CSV: {deal_type} ...")
            csv_df = None

            for attempt in range(1, 4):
                try:
                    if use_cffi:
                        resp = session_obj.get(ep["url"], params=ep["params"],
                                               headers=csv_req_headers, timeout=30)
                    else:
                        resp = session_obj.get(ep["url"], params=ep["params"],
                                               headers={**NSE_HEADERS, **csv_req_headers}, timeout=30)

                    body    = resp.content
                    preview = body[:300].decode("utf-8", errors="replace").strip()
                    log.info(f"  -> [{deal_type}] HTTP {resp.status_code} | {len(body)} bytes | {preview[:80]!r}")

                    if resp.status_code != 200:
                        log.warning(f"  !! HTTP {resp.status_code} on attempt {attempt}")
                        time.sleep(3); continue
                    if len(body) == 0:
                        log.warning(f"  !! Empty body on attempt {attempt}")
                        time.sleep(3); continue
                    if preview.lstrip().startswith("<"):
                        log.warning(f"  !! HTML returned (bot-blocked) on attempt {attempt}")
                        time.sleep(4); continue

                    try:
                        csv_df = pd.read_csv(io.StringIO(body.decode("utf-8", errors="replace")))
                        log.info(f"  ✅ [{deal_type}] CSV: {len(csv_df)} rows | cols: {list(csv_df.columns)}")
                        break
                    except Exception as csv_err:
                        log.warning(f"  !! CSV parse error: {csv_err} — trying JSON fallback")

                    try:
                        raw_json = resp.json()
                        if isinstance(raw_json, list) and raw_json:
                            csv_df = pd.DataFrame(raw_json)
                        elif isinstance(raw_json, dict):
                            for key in ["data","Data","results","records","bulkDeals","blockDeals"]:
                                val = raw_json.get(key)
                                if isinstance(val, list) and val:
                                    cols = raw_json.get("columns")
                                    csv_df = (pd.DataFrame(val, columns=cols)
                                              if (cols and not isinstance(val[0], dict))
                                              else pd.DataFrame(val))
                                    break
                        if csv_df is not None and not csv_df.empty:
                            log.info(f"  ✅ [{deal_type}] JSON fallback: {len(csv_df)} rows")
                            break
                    except Exception as json_err:
                        log.warning(f"  !! JSON fallback also failed: {json_err}")

                    time.sleep(3)

                except Exception as e:
                    log.warning(f"  !! [{deal_type}] attempt {attempt} exception: {e}")
                    time.sleep(3)

            if csv_df is not None and not csv_df.empty:
                csv_df["_deal_type"] = deal_type
                all_dfs.append(csv_df)
            else:
                log.warning(f"  !! [{deal_type}] No usable data — skipping")

            time.sleep(1.5)

        if use_cffi and hasattr(session_obj, "close"):
            try: session_obj.close()
            except Exception: pass

        if not all_dfs:
            log.warning("  !! No CSV data from any endpoint — falling back")
            return []

        df = pd.concat(all_dfs, ignore_index=True)
        df.columns = [str(c).strip() for c in df.columns]

        NSE_EXACT = {
            "BD_SYMBOL":"SYMBOL","BD_SCRIP_NAME":"COMPANY","BD_CLIENT_NAME":"CLIENT",
            "BD_BUY_SELL":"BUYSELL","BD_QTY_TRD":"QTY","BD_DT_DATE":"DATE",
            "BD_TP_WATP":"PRICE","BD_REMARKS":"REMARKS",
            "Symbol":"SYMBOL","Security Name":"COMPANY","Client Name":"CLIENT",
            "Buy / Sell":"BUYSELL","Quantity Traded":"QTY",
            "Trade Price / Wght. Avg. Price":"PRICE","Remarks":"REMARKS","Date":"DATE",
            "SYMBOL":"SYMBOL","SECURITY NAME":"COMPANY","CLIENT NAME":"CLIENT",
            "BUY / SELL":"BUYSELL","QUANTITY TRADED":"QTY",
            "TRADE PRICE / WGHT. AVG. PRICE":"PRICE",
            "SCRIP_NAME":"COMPANY","CLIENT_NAME":"CLIENT","BUY_SELL":"BUYSELL",
            "QTY_TRD":"QTY","TRADE_DATE":"DATE","TRADE_PRICE":"PRICE",
        }
        nse_upper = {k.upper(): v for k, v in NSE_EXACT.items()}

        rename = {}
        mapped = set()
        for c in df.columns:
            cu = c.strip()
            target = NSE_EXACT.get(cu) or nse_upper.get(cu.upper())
            if target and target not in mapped:
                rename[c] = target; mapped.add(target); continue
            cuu = cu.upper()
            if "CLIENT" in cuu and "CLIENT" not in mapped:
                rename[c] = "CLIENT"; mapped.add("CLIENT")
            elif "PARTY" in cuu and "CLIENT" not in mapped:
                rename[c] = "CLIENT"; mapped.add("CLIENT")
            elif "SYMBOL" in cuu and "SYMBOL" not in mapped:
                rename[c] = "SYMBOL"; mapped.add("SYMBOL")
            elif "SECURITY" in cuu and "NAME" in cuu and "COMPANY" not in mapped:
                rename[c] = "COMPANY"; mapped.add("COMPANY")
            elif "SCRIP" in cuu and "COMPANY" not in mapped:
                rename[c] = "COMPANY"; mapped.add("COMPANY")
            elif "BUY" in cuu and "SELL" in cuu and "BUYSELL" not in mapped:
                rename[c] = "BUYSELL"; mapped.add("BUYSELL")
            elif "QTY" in cuu and "QTY" not in mapped:
                rename[c] = "QTY"; mapped.add("QTY")
            elif "PRICE" in cuu and "PRICE" not in mapped:
                rename[c] = "PRICE"; mapped.add("PRICE")

        df = df.rename(columns=rename)

        if "CLIENT" not in df.columns:
            log.warning("  ❌ CLIENT column missing after normalisation")
            return []

        stocks, matched = {}, 0
        for _, row in df.iterrows():
            sym    = str(row.get("SYMBOL",  "")).strip().upper()
            name   = str(row.get("COMPANY", sym)).strip()
            client = str(row.get("CLIENT",  "")).strip().upper()
            bs     = str(row.get("BUYSELL", "")).strip().upper()

            if not sym or sym in ("NAN","") or not client or client == "NAN":
                continue

            is_fii = any(k in client for k in FII_KW)
            is_dii = any(k in client for k in DII_KW)
            action = "buy" if bs.startswith("B") else "sell"

            if sym not in stocks:
                stocks[sym] = {"symbol": sym+".NS","name": name,
                               "fii_cash":"neutral","dii_cash":"neutral","client_name":client}
            if is_fii:
                stocks[sym]["fii_cash"] = action; matched += 1
            if is_dii:
                stocks[sym]["dii_cash"] = action; matched += 1

        result = list(stocks.values())
        log.info(f"  → rows={len(df)} | FII/DII matched={matched} | stocks={len(result)}")
        return result

    except Exception as e:
        log.warning(f"  ❌ fetch_from_nse error: {e}")
        import traceback; log.warning(traceback.format_exc())
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 2 — MunafaSutra
# ─────────────────────────────────────────────────────────────────────────────

def fetch_from_munafasutra() -> list:
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
            if not symbol or not name: continue
            tr = a.find_parent("tr")
            if not tr: continue
            row_text = " ".join(t.get_text(" ",strip=True).lower() for t in tr.find_all("td"))
            fii = "buy" if "bought" in row_text else "sell"
            dii = "buy" if "bought" in row_text else "sell"
            stocks.append({"symbol":symbol+".NS","name":name,"fii_cash":fii,"dii_cash":dii})
        log.info(f"  {'✅' if stocks else '❌'} MunafaSutra: {len(stocks)} stocks")
        return stocks[:20]
    except Exception as e:
        log.warning(f"  ❌ MunafaSutra: {e}")
        return []


def fetch_fallback() -> list:
    log.warning("📡 [Source 3] Hardcoded fallback stocks")
    return FALLBACK_STOCKS.copy()


def fetch_fii_dii_stocks():
    s = fetch_from_nse()
    if s: return s, "NSE Bulk Deals CSV API"
    s = fetch_from_munafasutra()
    if s: return s, "MunafaSutra"
    return fetch_fallback(), "Fallback (Known Institutional Stocks)"


# ─────────────────────────────────────────────────────────────────────────────
#  TECHNICAL ANALYSIS
# ─────────────────────────────────────────────────────────────────────────────

def fix_df(df):
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
        df    = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=True)
        if df is None or df.empty: raise ValueError("Empty data")
        df = fix_df(df)
        df = df[["Open","High","Low","Close","Volume"]].dropna()
        if len(df) < 25: raise ValueError(f"Only {len(df)} rows")

        c  = df["Close"].astype(float)
        h  = df["High"].astype(float)
        lo = df["Low"].astype(float)
        lc = float(c.iloc[-1])

        # RSI(14)
        dlt  = c.diff()
        gain = dlt.clip(lower=0)
        loss = (-dlt).clip(lower=0)
        ag   = gain.ewm(com=13, adjust=False).mean()
        al   = loss.ewm(com=13, adjust=False).mean()
        rsi_s= 100 - (100 / (1 + ag / al.replace(0, np.nan)))
        rsi  = round(float(rsi_s.iloc[-1]), 1)

        # MACD(12,26,9)
        macd  = (c.ewm(span=12, adjust=False).mean() - c.ewm(span=26, adjust=False).mean())
        mhist = round(float((macd - macd.ewm(span=9, adjust=False).mean()).iloc[-1]), 2)

        # EMA 20/50
        ema20  = c.ewm(span=20, adjust=False).mean()
        ema50  = c.ewm(span=50, adjust=False).mean()
        ecross = "bullish" if float(ema20.iloc[-1]) > float(ema50.iloc[-1]) else "bearish"

        # Bollinger Bands
        bm  = c.rolling(20).mean()
        bsd = c.rolling(20).std()
        bu  = float((bm + 2*bsd).iloc[-1])
        bl2 = float((bm - 2*bsd).iloc[-1])
        bp  = (lc - bl2) / ((bu - bl2) or 1)
        bbl = "Overbought" if bp > 0.8 else ("Oversold" if bp < 0.2 else "Mid")

        # ADX(14)
        pdm = h.diff().clip(lower=0)
        mdm = (-lo.diff()).clip(lower=0)
        tr  = pd.concat([h-lo,(h-c.shift()).abs(),(lo-c.shift()).abs()],axis=1).max(axis=1)
        atr = tr.ewm(com=13, adjust=False).mean()
        pdi = 100 * pdm.ewm(com=13, adjust=False).mean() / atr
        mdi = 100 * mdm.ewm(com=13, adjust=False).mean() / atr
        dx  = 100 * (pdi-mdi).abs() / (pdi+mdi).replace(0, np.nan)
        adx = round(float(dx.ewm(com=13, adjust=False).mean().iloc[-1]), 1)

        # Stoch RSI
        srsi = ((rsi_s - rsi_s.rolling(14).min()) /
                (rsi_s.rolling(14).max() - rsi_s.rolling(14).min()).replace(0, np.nan))
        sv = float(srsi.iloc[-1])
        sv = 0.5 if np.isnan(sv) else round(sv, 2)

        # Pivot S/R
        n  = min(120, len(h))
        pv = (float(h.iloc[-1]) + float(lo.iloc[-1]) + lc) / 3
        r1 = round(2*pv - float(lo.iloc[-1]), 2)
        s1 = round(2*pv - float(h.iloc[-1]), 2)
        sh = round(float(h.rolling(n).max().iloc[-1]), 2)
        sl = round(float(lo.rolling(n).min().iloc[-1]), 2)

        # Signal score
        sc = 0
        sc += 2 if rsi < 40    else (1 if rsi < 55 else (-2 if rsi > 70 else 0))
        sc += 2 if mhist > 0   else 0
        sc += 2 if ecross == "bullish" else 0
        sc += 1 if adx > 25    else 0
        sc += 1 if sv < 0.3    else (-1 if sv > 0.8 else 0)
        ov = ("STRONG BUY" if sc >= 5 else "BUY" if sc >= 3 else
              "NEUTRAL"    if sc >= 0 else "CAUTION" if sc >= -2 else "SELL")

        spark = [round(float(x), 2) for x in c.iloc[-7:].tolist()]
        return dict(rsi=rsi, macd_hist=mhist, ema_cross=ecross, bb_label=bbl,
                    adx=adx, stoch_rsi=sv, resist1=r1, support1=s1,
                    swing_high=sh, swing_low=sl, last_price=round(lc, 2),
                    overall=ov, score=sc, sparkline=spark, data_ok=True)
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
            return (
                round(float(c.iloc[-1]), 2),
                round(float((c.iloc[-1]-c.iloc[-2])/c.iloc[-2]*100), 2) if len(c) >= 2 else 0.0
            )
        np_, nc = load("^NSEI")
        sp_, sc = load("^BSESN")
        return dict(nifty_price=np_, nifty_chg=nc, sensex_price=sp_, sensex_chg=sc)
    except Exception as e:
        log.warning(f"Market summary failed: {e}")
        return dict(nifty_price=0, nifty_chg=0, sensex_price=0, sensex_chg=0)


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
        both_buy = s["fii_cash"] == "buy"  and s["dii_cash"] == "buy"
        fii_only = s["fii_cash"] == "buy"  and s["dii_cash"] != "buy"
        dii_only = s["dii_cash"] == "buy"  and s["fii_cash"] != "buy"
        both_sel = s["fii_cash"] == "sell" and s["dii_cash"] == "sell"
        neither  = s["fii_cash"] == "neutral" and s["dii_cash"] == "neutral"
        inst_sig = ("BOTH BUY"   if both_buy else "FII BUY"   if fii_only else
                    "DII BUY"    if dii_only  else "BOTH SELL" if both_sel else
                    "BULK/BLOCK" if neither   else "SELL")
        enriched.append({**s, **tech, "inst_signal":inst_sig,
                         "both_buy":both_buy,"fii_only":fii_only,"dii_only":dii_only})
        time.sleep(0.4)
    return enriched, market, source


# ─────────────────────────────────────────────────────────────────────────────
#  SECTOR MAP & ICONS
# ─────────────────────────────────────────────────────────────────────────────

SECTOR_MAP = {
    "HDFCBANK":"Banking & Finance","ICICIBANK":"Banking & Finance","SBIN":"Banking & Finance",
    "AXISBANK":"Banking & Finance","KOTAKBANK":"Banking & Finance","INDUSINDBK":"Banking & Finance",
    "BANDHANBNK":"Banking & Finance","FEDERALBNK":"Banking & Finance","IDFCFIRSTB":"Banking & Finance",
    "PNB":"Banking & Finance","BANKBARODA":"Banking & Finance","CANARABANK":"Banking & Finance",
    "AUBANK":"Banking & Finance","RBLBANK":"Banking & Finance","YESBANK":"Banking & Finance",
    "UJJIVANSFB":"Banking & Finance","EQUITASBNK":"Banking & Finance","ESAFSFB":"Banking & Finance",
    "BAJFINANCE":"NBFC & Fintech","BAJAJFINSV":"NBFC & Fintech","CHOLAFIN":"NBFC & Fintech",
    "MUTHOOTFIN":"NBFC & Fintech","MANAPPURAM":"NBFC & Fintech","SBICARD":"NBFC & Fintech",
    "ANGELONE":"NBFC & Fintech","POLICYBZR":"NBFC & Fintech","CAMS":"NBFC & Fintech",
    "KFINTECH":"NBFC & Fintech","NUVAMA":"NBFC & Fintech","360ONE":"NBFC & Fintech","IIFL":"NBFC & Fintech",
    "MOTHERSON":"Auto & Auto Ancillaries",
    "TCS":"IT & Technology","INFY":"IT & Technology","WIPRO":"IT & Technology",
    "HCLTECH":"IT & Technology","TECHM":"IT & Technology","LTIM":"IT & Technology",
    "MPHASIS":"IT & Technology","COFORGE":"IT & Technology","PERSISTENT":"IT & Technology",
    "OFSS":"IT & Technology","LTTS":"IT & Technology","HEXAWARE":"IT & Technology",
    "KPITTECH":"IT & Technology","TATAELXSI":"IT & Technology",
    "SUNPHARMA":"Pharma & Healthcare","DRREDDY":"Pharma & Healthcare","CIPLA":"Pharma & Healthcare",
    "DIVISLAB":"Pharma & Healthcare","TORNTPHARM":"Pharma & Healthcare","AUROPHARMA":"Pharma & Healthcare",
    "LUPIN":"Pharma & Healthcare","ALKEM":"Pharma & Healthcare","IPCALAB":"Pharma & Healthcare",
    "GLAND":"Pharma & Healthcare","FORTIS":"Pharma & Healthcare","APOLLOHOSP":"Pharma & Healthcare",
    "MAXHEALTH":"Pharma & Healthcare","KIMS":"Pharma & Healthcare","MEDANTA":"Pharma & Healthcare",
    "NARAYANA":"Pharma & Healthcare",
    "RELIANCE":"Oil, Gas & Energy","ONGC":"Oil, Gas & Energy","IOC":"Oil, Gas & Energy",
    "BPCL":"Oil, Gas & Energy","HINDPETRO":"Oil, Gas & Energy","GAIL":"Oil, Gas & Energy",
    "OIL":"Oil, Gas & Energy","MGL":"Oil, Gas & Energy","IGL":"Oil, Gas & Energy",
    "PETRONET":"Oil, Gas & Energy","GUJGASLTD":"Oil, Gas & Energy","ATGL":"Oil, Gas & Energy",
    "NTPC":"Power & Utilities","POWERGRID":"Power & Utilities","ADANIPOWER":"Power & Utilities",
    "TATAPOWER":"Power & Utilities","JSWENERGY":"Power & Utilities","TORNTPOWER":"Power & Utilities",
    "CESC":"Power & Utilities","NHPC":"Power & Utilities","SJVN":"Power & Utilities",
    "IREDA":"Power & Utilities","PFC":"Power & Utilities","RECLTD":"Power & Utilities",
    "TATASTEEL":"Metals & Mining","JSWSTEEL":"Metals & Mining","HINDALCO":"Metals & Mining",
    "VEDL":"Metals & Mining","SAIL":"Metals & Mining","NMDC":"Metals & Mining",
    "NATIONALUM":"Metals & Mining","WELCORP":"Metals & Mining","APLAPOLLO":"Metals & Mining",
    "JINDALSTEL":"Metals & Mining","MOIL":"Metals & Mining","RATNAMANI":"Metals & Mining",
    "MARUTI":"Auto & Auto Ancillaries","TATAMOTORS":"Auto & Auto Ancillaries",
    "M&M":"Auto & Auto Ancillaries","BAJAJ-AUTO":"Auto & Auto Ancillaries",
    "HEROMOTOCO":"Auto & Auto Ancillaries","EICHERMOT":"Auto & Auto Ancillaries",
    "TVSMOTORS":"Auto & Auto Ancillaries","ASHOKLEY":"Auto & Auto Ancillaries",
    "ESCORTS":"Auto & Auto Ancillaries","BOSCHLTD":"Auto & Auto Ancillaries",
    "BHARATFORG":"Auto & Auto Ancillaries","EXIDEIND":"Auto & Auto Ancillaries",
    "AMARAJABAT":"Auto & Auto Ancillaries","BALKRISIND":"Auto & Auto Ancillaries",
    "TIINDIA":"Auto & Auto Ancillaries","APOLLOTYRE":"Auto & Auto Ancillaries",
    "HINDUNILVR":"FMCG & Consumer","ITC":"FMCG & Consumer","NESTLEIND":"FMCG & Consumer",
    "BRITANNIA":"FMCG & Consumer","DABUR":"FMCG & Consumer","MARICO":"FMCG & Consumer",
    "COLPAL":"FMCG & Consumer","GODREJCP":"FMCG & Consumer","EMAMILTD":"FMCG & Consumer",
    "TATACONSUM":"FMCG & Consumer","VARUN":"FMCG & Consumer","RADICO":"FMCG & Consumer",
    "UBL":"FMCG & Consumer","MCDOWELL-N":"FMCG & Consumer",
    "ULTRACEMCO":"Cement & Construction","AMBUJACEM":"Cement & Construction",
    "ACC":"Cement & Construction","SHREECEM":"Cement & Construction",
    "DALMIACEMENTBHARAT":"Cement & Construction","RAMCOCEM":"Cement & Construction",
    "JKCEMENT":"Cement & Construction","HEIDELBERG":"Cement & Construction",
    "LT":"Cement & Construction","NCC":"Cement & Construction",
    "KNRCON":"Cement & Construction","PNCINFRA":"Cement & Construction",
    "RVNL":"Cement & Construction","IRCON":"Cement & Construction",
    "DLF":"Real Estate","GODREJPROP":"Real Estate","OBEROIRLTY":"Real Estate",
    "PRESTIGE":"Real Estate","PHOENIXLTD":"Real Estate","BRIGADE":"Real Estate",
    "SOBHA":"Real Estate","MAHLIFE":"Real Estate","LODHA":"Real Estate","SUNTECK":"Real Estate",
    "SIEMENS":"Capital Goods & Industrials","ABB":"Capital Goods & Industrials",
    "HAVELLS":"Capital Goods & Industrials","BHEL":"Capital Goods & Industrials",
    "BEL":"Capital Goods & Industrials","HAL":"Capital Goods & Industrials",
    "COCHINSHIP":"Capital Goods & Industrials","MAZDOCK":"Capital Goods & Industrials",
    "GRINDWELL":"Capital Goods & Industrials","THERMAX":"Capital Goods & Industrials",
    "CUMMINSIND":"Capital Goods & Industrials","KALYANKJIL":"Capital Goods & Industrials",
    "BHARTIARTL":"Telecom & Media","IDEA":"Telecom & Media","INDUSTOWER":"Telecom & Media",
    "TATACOMM":"Telecom & Media","ZEEL":"Telecom & Media","SUNTV":"Telecom & Media","PVRINOX":"Telecom & Media",
    "PIDILITIND":"Chemicals & Specialty","ASIANPAINT":"Chemicals & Specialty",
    "BERGEPAINT":"Chemicals & Specialty","ATUL":"Chemicals & Specialty",
    "NAVINFLUOR":"Chemicals & Specialty","SOLARINDS":"Chemicals & Specialty",
    "FINEORG":"Chemicals & Specialty","CLEAN":"Chemicals & Specialty",
    "DEEPAKNITR":"Chemicals & Specialty","ALKYLAMINE":"Chemicals & Specialty",
    "SBILIFE":"Insurance","HDFCLIFE":"Insurance","ICICIPRULI":"Insurance",
    "MAXFINSERV":"Insurance","GICRE":"Insurance","NIACL":"Insurance",
    "STARHEALTH":"Insurance","GODIGIT":"Insurance",
    "BSE":"Exchange & Capital Markets","MCX":"Exchange & Capital Markets",
    "CDSL":"Exchange & Capital Markets","NSDL":"Exchange & Capital Markets",
    "CRISIL":"Exchange & Capital Markets","ICRA":"Exchange & Capital Markets",
    "INDIGO":"Aviation & Logistics","SPICEJET":"Aviation & Logistics",
    "GMRAIRPORT":"Aviation & Logistics","ADANIPORTS":"Aviation & Logistics",
    "CONCOR":"Aviation & Logistics","BLUEDART":"Aviation & Logistics",
    "DELHIVERY":"Aviation & Logistics","MAHINDRA LOG":"Aviation & Logistics",
    "DMART":"Retail & E-Commerce","TRENT":"Retail & E-Commerce","NYKAA":"Retail & E-Commerce",
    "ZOMATO":"Retail & E-Commerce","CARTRADE":"Retail & E-Commerce","SHOPERSTOP":"Retail & E-Commerce",
    "UPL":"Agri & Fertilisers","COROMANDEL":"Agri & Fertilisers","CHAMBLFERT":"Agri & Fertilisers",
    "GNFC":"Agri & Fertilisers","GSFC":"Agri & Fertilisers","NFL":"Agri & Fertilisers",
    "RALLIS":"Agri & Fertilisers","BAYER":"Agri & Fertilisers",
}

SECTOR_ICONS = {
    "Banking & Finance":"🏦","NBFC & Fintech":"💳","IT & Technology":"💻",
    "Pharma & Healthcare":"💊","Oil, Gas & Energy":"⛽","Power & Utilities":"⚡",
    "Metals & Mining":"⚙️","Auto & Auto Ancillaries":"🚗","FMCG & Consumer":"🛒",
    "Cement & Construction":"🏗️","Real Estate":"🏢","Capital Goods & Industrials":"🏭",
    "Telecom & Media":"📡","Chemicals & Specialty":"🧪","Insurance":"🛡️",
    "Exchange & Capital Markets":"📈","Aviation & Logistics":"✈️",
    "Retail & E-Commerce":"🛍️","Agri & Fertilisers":"🌾","Others":"🔷",
}

SIGNAL_ORDER = {
    "STRONG BUY":0,"BUY":1,"NEUTRAL":2,"CAUTION":3,"BULK/BLOCK":4,"N/A":5,"SELL":6,"BOTH SELL":7,
}


def get_sector(symbol: str) -> str:
    sym = symbol.replace(".NS","").strip().upper()
    return SECTOR_MAP.get(sym, "Others")


# ─────────────────────────────────────────────────────────────────────────────
#  HTML HELPERS — Warm Parchment Theme
# ─────────────────────────────────────────────────────────────────────────────

def spark_svg(prices):
    if len(prices) < 2: return ""
    mn, mx = min(prices), max(prices)
    rng = mx - mn or 1
    w, h = 72, 22
    bar_w = max(1, w // len(prices) - 1)
    bars = ""
    up = prices[-1] >= prices[0]
    for i, p in enumerate(prices):
        bar_h = max(2, round((p - mn) / rng * h))
        x = i * (w // len(prices))
        y = h - bar_h
        col = "#0f5c3a" if up else "#c0392b"
        bars += f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bar_h}" fill="{col}" rx="1"/>'
    return (f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
            f'xmlns="http://www.w3.org/2000/svg" style="display:block">{bars}</svg>')


def rsi_class(v):
    if v > 70:  return "rsi-hot"
    if v < 40:  return "rsi-cold"
    return "rsi-warm"


def sig_class(overall):
    return {
        "STRONG BUY":"sig-sb","BUY":"sig-buy","NEUTRAL":"sig-neutral",
        "CAUTION":"sig-caution","SELL":"sig-sell","BOTH SELL":"sig-sell",
        "BULK/BLOCK":"sig-blk","N/A":"sig-neutral",
    }.get(overall, "sig-neutral")


def fmt_price(v):
    return f"&#8377;{v:,.2f}" if v else "N/A"


def fmt_macd(v):
    sign = "+" if v >= 0 else ""
    cls  = "macd-pos" if v >= 0 else "macd-neg"
    return f'<span class="{cls}">{sign}{v:.2f}</span>'


def fmt_ema(cross):
    if cross == "bullish":
        return '<span class="ema-bull">EMA ▲</span>'
    return '<span class="ema-bear">EMA ▼</span>'


# ─────────────────────────────────────────────────────────────────────────────
#  GENERATE HTML — Warm Parchment · Fully Responsive
# ─────────────────────────────────────────────────────────────────────────────

def generate_html(stocks, market, date_str, source, date_range_label="") -> str:

    nc  = "up" if market["nifty_chg"]  >= 0 else "dn"
    xc  = "up" if market["sensex_chg"] >= 0 else "dn"
    na  = "▲"  if market["nifty_chg"]  >= 0 else "▼"
    xa  = "▲"  if market["sensex_chg"] >= 0 else "▼"

    fb  = sum(1 for s in stocks if s["fii_cash"] == "buy")
    db  = sum(1 for s in stocks if s["dii_cash"] == "buy")
    bb  = sum(1 for s in stocks if s["both_buy"])
    st  = sum(1 for s in stocks if s["overall"] == "STRONG BUY")
    sel = sum(1 for s in stocks if s["overall"] in ("SELL","BOTH SELL"))

    for s in stocks:
        s["sector"] = get_sector(s["symbol"])

    from collections import defaultdict
    sector_groups = defaultdict(list)
    for s in stocks:
        sector_groups[s["sector"]].append(s)

    def signal_sort_key(s):
        return SIGNAL_ORDER.get(s.get("overall","N/A"), 5)

    for sec in sector_groups:
        sector_groups[sec].sort(key=signal_sort_key)

    def sector_best(items):
        return min(SIGNAL_ORDER.get(s.get("overall","N/A"), 5) for s in items)

    sorted_sectors = sorted(sector_groups.items(), key=lambda kv: sector_best(kv[1]))

    # ── Sidebar ───────────────────────────────────────────────────────────────
    sidebar_items = ""
    for sector_name, sec_stocks in sorted_sectors:
        icon     = SECTOR_ICONS.get(sector_name,"🔷")
        best_sig = min(sec_stocks, key=signal_sort_key)["overall"]
        if best_sig in ("STRONG BUY","BUY","BOTH BUY"):
            sig_cls, sig_lbl = "buy", "↑ BUY"
        elif best_sig in ("SELL","BOTH SELL"):
            sig_cls, sig_lbl = "sell","↓ SELL"
        else:
            sig_cls, sig_lbl = "hold","→ HOLD"
        anchor = sector_name.replace(" ","_").replace("&","and")
        sidebar_items += f"""
        <a href="#{anchor}" class="sb-item" onclick="closeSidebar()">
          <div>
            <div class="sb-item-name">{icon} {sector_name}</div>
            <div class="sb-item-count">{len(sec_stocks)} securities</div>
          </div>
          <span class="sb-sig {sig_cls}">{sig_lbl}</span>
        </a>"""

    # ── Sector cards ──────────────────────────────────────────────────────────
    sector_cards = ""
    for sector_name, sec_stocks in sorted_sectors:
        icon      = SECTOR_ICONS.get(sector_name,"🔷")
        anchor    = sector_name.replace(" ","_").replace("&","and")
        sec_count = len(sec_stocks)
        sec_sb    = sum(1 for s in sec_stocks if s["overall"] == "STRONG BUY")
        sec_buy   = sum(1 for s in sec_stocks if s["overall"] == "BUY")
        sec_sell  = sum(1 for s in sec_stocks if s["overall"] in ("SELL","BOTH SELL"))

        header_pills = ""
        if sec_sb:   header_pills += f'<span class="hpill sb">⚡ {sec_sb} Strong Buy</span>'
        if sec_buy:  header_pills += f'<span class="hpill buy">▲ {sec_buy} Buy</span>'
        if sec_sell: header_pills += f'<span class="hpill sell">▼ {sec_sell} Sell</span>'

        stock_rows = ""
        for s in sec_stocks:
            sym          = s["symbol"].replace(".NS","")
            price        = fmt_price(s["last_price"]) if s["last_price"] > 0 else "—"
            spk          = spark_svg(s.get("sparkline",[]))
            rsi_v        = s["rsi"]
            rc           = rsi_class(rsi_v)
            macd_h       = fmt_macd(s["macd_hist"])
            ema_h        = fmt_ema(s["ema_cross"])
            overall      = s["overall"]
            sc_val       = sig_class(overall)
            is_up        = (s.get("sparkline") and len(s["sparkline"]) >= 2
                            and s["sparkline"][-1] >= s["sparkline"][0])
            price_cls    = "price-up" if is_up else "price-dn"

            if overall == "STRONG BUY":   sig_label = "⚡ STRONG BUY"
            elif overall == "BUY":        sig_label = "▲ BUY"
            elif overall in ("SELL","BOTH SELL"): sig_label = "▼ SELL"
            elif overall == "CAUTION":    sig_label = "⚠ CAUTION"
            elif overall == "BULK/BLOCK": sig_label = "■ BULK/BLOCK"
            else:                         sig_label = "— NEUTRAL"

            # Mobile card + Desktop row both generated; CSS toggles visibility
            stock_rows += f"""
            <tr class="stock-row">
              <td class="td-stock">
                <div class="stock-name">{s['name']}</div>
                <div class="stock-sym">{sym}</div>
              </td>
              <td class="td-r">
                <div class="price-val {price_cls}">{price}</div>
                <div class="spark-wrap">{spk}</div>
              </td>
              <td class="td-c">
                <div class="rsi-badge {rc}">{rsi_v}</div>
                <div class="rsi-track"><div class="rsi-fill {rc}" style="width:{min(rsi_v,100):.0f}%"></div></div>
              </td>
              <td class="td-c td-sr">
                <div class="sr-grid">
                  <div class="sr-row"><span class="sr-tag r">R1</span><span class="sr-val r">{fmt_price(s['resist1'])}</span></div>
                  <div class="sr-row"><span class="sr-tag s">S1</span><span class="sr-val s">{fmt_price(s['support1'])}</span></div>
                  <div class="sr-row"><span class="sr-tag r">6mH</span><span class="sr-val r">{fmt_price(s['swing_high'])}</span></div>
                  <div class="sr-row"><span class="sr-tag s">6mL</span><span class="sr-val s">{fmt_price(s['swing_low'])}</span></div>
                </div>
              </td>
              <td class="td-c td-macd">
                <div class="macd-val">{macd_h}</div>
                <div class="ema-val">{ema_h}</div>
              </td>
              <td class="td-c">
                <span class="sig-pill {sc_val}">{sig_label}</span>
              </td>
            </tr>"""

        sector_cards += f"""
        <div class="sector-card" id="{anchor}">
          <div class="sec-hdr">
            <div class="sec-hdr-left">
              <span class="sec-icon">{icon}</span>
              <span class="sec-name">{sector_name}</span>
              <span class="sec-count">{sec_count} securities</span>
            </div>
            <div class="sec-pills">{header_pills}</div>
          </div>
          <div class="tbl-wrap">
            <table class="sec-table">
              <thead>
                <tr>
                  <th>SECURITY</th>
                  <th class="th-r">PRICE / TREND</th>
                  <th class="th-c">RSI (14)</th>
                  <th class="th-c td-sr">S/R LEVELS</th>
                  <th class="th-c td-macd">MACD / EMA</th>
                  <th class="th-c">SIGNAL</th>
                </tr>
              </thead>
              <tbody>{stock_rows}</tbody>
            </table>
          </div>
        </div>"""

    IST     = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")

    # ── Ticker ────────────────────────────────────────────────────────────────
    ticker_items = [
        ("NIFTY 50",   f"&#8377;{market['nifty_price']:,.2f}",
         "up" if market["nifty_chg"] >= 0 else "dn",
         f"{'▲' if market['nifty_chg']>=0 else '▼'}{abs(market['nifty_chg']):.2f}%"),
        ("SENSEX",     f"&#8377;{market['sensex_price']:,.2f}",
         "up" if market["sensex_chg"] >= 0 else "dn",
         f"{'▲' if market['sensex_chg']>=0 else '▼'}{abs(market['sensex_chg']):.2f}%"),
        ("TRACKED",    str(len(stocks)), "up",  f"FII:{fb} · DII:{db}"),
        ("BOTH BUY",   str(bb),          "up",  "securities"),
        ("STRONG BUY", str(st),          "up",  "signals"),
        ("SELL ALERT", str(sel),         "dn" if sel > 0 else "up", "caution"),
        ("SOURCE",     source[:20],      "up",  "NSE CSV"),
        ("RANGE",      date_range_label, "up",  "window"),
    ]
    ticker_html = ""
    for sym_t, val, cls, extra in ticker_items:
        ticker_html += (f'<div class="t-item"><span class="t-sym">{sym_t}</span>'
                        f'<span class="t-val {cls}">{val}</span>'
                        f'<span class="t-extra">{extra}</span></div>')
    ticker_html = ticker_html * 2

    # ══════════════════════════════════════════════════════════════════════════
    #  CSS — Warm Parchment · Full Responsive System
    # ══════════════════════════════════════════════════════════════════════════
    css = """
@import url('https://fonts.googleapis.com/css2?family=Instrument+Serif:ital@0;1&family=DM+Sans:opsz,wght@9..40,300;9..40,400;9..40,500;9..40,600;9..40,700&family=DM+Mono:wght@400;500&display=swap');

/* ═══════════════════════════════════════════════════════
   WARM PARCHMENT PALETTE
   --bg        #f5f0e8   warm cream background
   --surface   #fffdf7   card / panel white
   --surface2  #f0ead8   slightly deeper cream
   --border    #d8cdb0   warm tan border
   --border2   #c4b894   richer tan
   --navy      #1a2744   deep navy header
   --navy2     #253460   header accent
   --forest    #0f5c3a   buy signals / positive
   --red       #c0392b   sell / negative
   --gold      #b8860b   strong buy
   --amber     #d97706   caution
   --text      #1a1a1a   primary text
   --text2     #4a4a4a   secondary text
   --text3     #7a7060   muted text
   --muted     #9a9080   dim text
════════════════════════════════════════════════════════ */

:root {
  --bg:       #f5f0e8;
  --surface:  #fffdf7;
  --surface2: #f0ead8;
  --border:   #d8cdb0;
  --border2:  #c4b894;
  --navy:     #1a2744;
  --navy2:    #253460;
  --forest:   #0f5c3a;
  --forest-d: rgba(15,92,58,.1);
  --red:      #c0392b;
  --red-d:    rgba(192,57,43,.1);
  --gold:     #b8860b;
  --gold-d:   rgba(184,134,11,.12);
  --amber:    #d97706;
  --amber-d:  rgba(217,119,6,.1);
  --purple:   #6432a0;
  --purple-d: rgba(100,50,160,.08);
  --text:     #1a1a1a;
  --text2:    #4a4a4a;
  --text3:    #7a7060;
  --muted:    #9a9080;
  --radius:   8px;
  --radius-sm:4px;
  --shadow:   0 2px 12px rgba(0,0,0,.08);
  --shadow-md:0 4px 24px rgba(0,0,0,.12);
}

/* ── RESET & BASE ── */
*, *::before, *::after { margin:0; padding:0; box-sizing:border-box; }
html { scroll-behavior:smooth; font-size:16px; }
body {
  background: var(--bg);
  background-image:
    repeating-linear-gradient(0deg, rgba(200,190,170,.08) 0px, transparent 1px, transparent 48px),
    repeating-linear-gradient(90deg, rgba(200,190,170,.08) 0px, transparent 1px, transparent 48px);
  color: var(--text);
  font-family: 'DM Sans', 'Segoe UI', system-ui, sans-serif;
  min-height: 100vh;
  font-size: 14px;
  line-height: 1.55;
  overflow-x: hidden;
}

/* ════════════════════════════════════════════════════════
   HEADER
════════════════════════════════════════════════════════ */
header {
  background: var(--navy);
  border-bottom: 3px solid var(--gold);
  display: grid;
  grid-template-columns: auto 1fr auto;
  align-items: stretch;
  position: sticky; top: 0; z-index: 700;
  box-shadow: 0 4px 20px rgba(0,0,0,.3);
}

/* Hamburger button (mobile only) */
.h-burger {
  display: none;
  align-items: center; justify-content: center;
  padding: 0 16px;
  border: none; background: none; cursor: pointer;
  color: rgba(255,255,255,.7);
  font-size: 22px; line-height:1;
  border-right: 1px solid rgba(255,255,255,.1);
}
.h-burger:hover { color: #f0c060; background: rgba(255,255,255,.05); }

.h-brand {
  padding: 12px 22px;
  border-right: 1px solid rgba(255,255,255,.12);
  display: flex; align-items: center; gap: 10px;
  min-width: 0;
}
.logo { font-family:'Instrument Serif',serif; font-size:20px; font-weight:700; color:#f0ead8; display:flex; align-items:baseline; gap:3px; white-space:nowrap; }
.logo-fii { color:#f0c060; }
.logo-sep { color:rgba(255,255,255,.3); font-family:'DM Sans',sans-serif; font-weight:300; }
.logo-dii { color:#80d4a0; }
.logo-sub { font-size:9px; letter-spacing:1.8px; color:rgba(255,255,255,.4); font-weight:600; text-transform:uppercase; margin-top:2px; white-space:nowrap; }

.h-nav {
  display: flex; align-items: stretch; padding: 0 6px;
  overflow-x: auto; scrollbar-width: none;
}
.h-nav::-webkit-scrollbar { display:none; }
.h-tab {
  padding: 0 14px; display:flex; align-items:center;
  font-size: 11px; font-weight: 700; letter-spacing: .5px; text-transform: uppercase;
  color: rgba(255,255,255,.45); cursor: pointer;
  border-bottom: 2px solid transparent; transition: all .15s; white-space: nowrap;
}
.h-tab:hover { color:rgba(255,255,255,.8); background:rgba(255,255,255,.05); }
.h-tab.active { color:#f0c060; border-bottom-color:#f0c060; }

.h-meta { display:flex; align-items:center; border-left:1px solid rgba(255,255,255,.12); }
.h-meta-item {
  padding: 9px 16px; border-right:1px solid rgba(255,255,255,.08);
  display:flex; flex-direction:column; gap:2px;
}
.h-meta-label { font-size:9px; letter-spacing:1.5px; color:rgba(255,255,255,.35); text-transform:uppercase; font-weight:700; }
.h-meta-val { font-size:11px; font-weight:600; color:#f0ead8; font-family:'DM Mono',monospace; white-space:nowrap; }
.h-live { padding:9px 18px; display:flex; align-items:center; gap:7px; font-size:10px; font-weight:700; letter-spacing:1px; color:#80d4a0; white-space:nowrap; }
.led { width:7px; height:7px; border-radius:50%; background:#80d4a0; box-shadow:0 0 6px #80d4a0; animation:blink 2.2s ease-in-out infinite; }
@keyframes blink { 0%,100%{opacity:1} 50%{opacity:.2} }

/* ════════════════════════════════════════════════════════
   TICKER
════════════════════════════════════════════════════════ */
.ticker-wrap {
  height: 30px; overflow:hidden;
  background: var(--navy2);
  border-bottom: 2px solid var(--gold);
  display: flex; align-items: center;
}
.ticker-inner { display:inline-flex; white-space:nowrap; animation:ticker 55s linear infinite; }
@keyframes ticker { from{transform:translateX(0)} to{transform:translateX(-50%)} }
.t-item { display:inline-flex; align-items:center; gap:8px; padding:0 20px; font-size:11px; border-right:1px solid rgba(255,255,255,.08); color:rgba(255,255,255,.45); }
.t-sym { color:rgba(255,255,255,.75); font-weight:700; font-size:10px; letter-spacing:.6px; }
.t-val { font-weight:700; font-family:'DM Mono',monospace; }
.t-val.up { color:#80d4a0; }
.t-val.dn { color:#ff8080; }
.t-extra { font-size:10px; color:rgba(255,255,255,.3); }

/* ════════════════════════════════════════════════════════
   STATS BAR
════════════════════════════════════════════════════════ */
.stats-bar {
  display: grid;
  grid-template-columns: repeat(6, 1fr);
  background: var(--surface);
  border-bottom: 2px solid var(--border);
}
.stat {
  padding: 14px 18px;
  border-right: 1px solid var(--border);
  transition: background .2s; cursor:default;
}
.stat:last-child { border-right:none; }
.stat:hover { background:var(--surface2); }
.stat-lbl { font-size:9px; letter-spacing:1.5px; color:var(--muted); text-transform:uppercase; margin-bottom:5px; font-weight:700; }
.stat-val { font-size:22px; font-weight:700; color:var(--navy); font-family:'DM Mono',monospace; line-height:1; }
.stat-val.forest { color:var(--forest); }
.stat-val.gold   { color:var(--gold); }
.stat-chg { font-size:11px; margin-top:4px; font-weight:500; }
.stat-chg.up  { color:var(--forest); }
.stat-chg.dn  { color:var(--red); }
.stat-chg.neu { color:var(--muted); }

/* ════════════════════════════════════════════════════════
   OVERLAY (mobile sidebar backdrop)
════════════════════════════════════════════════════════ */
.overlay {
  display:none; position:fixed; inset:0; z-index:600;
  background:rgba(0,0,0,.45); backdrop-filter:blur(2px);
}
.overlay.active { display:block; }

/* ════════════════════════════════════════════════════════
   MAIN LAYOUT
════════════════════════════════════════════════════════ */
.main { display:grid; grid-template-columns:240px 1fr; min-height:calc(100vh - 200px); }

/* ════════════════════════════════════════════════════════
   SIDEBAR
════════════════════════════════════════════════════════ */
.sidebar {
  background: var(--surface);
  border-right: 2px solid var(--border);
  overflow-y: auto;
  position: sticky; top: 87px; /* header height */
  height: calc(100vh - 87px);
}
.sb-section { border-bottom:1px solid var(--border); padding-bottom:4px; }
.sb-title {
  font-size:9px; letter-spacing:2px; font-weight:700; color:var(--muted); text-transform:uppercase;
  padding:10px 16px 7px;
  background:var(--surface2); border-bottom:1px solid var(--border);
}
.sb-item {
  padding:8px 14px; display:flex; justify-content:space-between; align-items:center;
  cursor:pointer; border-left:3px solid transparent; transition:all .15s;
  text-decoration:none; color:inherit; font-size:12px;
}
.sb-item:hover { background:var(--surface2); border-left-color:var(--gold); }
.sb-item-name { color:var(--text); font-weight:600; font-size:12px; }
.sb-item-count { font-size:10px; color:var(--muted); margin-top:1px; }
.sb-sig { font-size:9px; font-weight:700; padding:2px 7px; border-radius:var(--radius-sm); }
.sb-sig.buy  { color:var(--forest); background:var(--forest-d); border:1px solid rgba(15,92,58,.25); }
.sb-sig.sell { color:var(--red);    background:var(--red-d);    border:1px solid rgba(192,57,43,.25); }
.sb-sig.hold { color:var(--muted);  background:rgba(0,0,0,.04); border:1px solid var(--border); }
.sb-legend { padding:10px 14px; }
.sb-leg { display:flex; align-items:center; gap:8px; font-size:11px; color:var(--text3); padding:3px 0; }
.sb-dot { width:8px; height:8px; border-radius:2px; flex-shrink:0; }

/* ════════════════════════════════════════════════════════
   CONTENT AREA
════════════════════════════════════════════════════════ */
.content { overflow:auto; background:var(--bg); }
.content-hdr {
  padding:12px 18px; background:var(--surface); border-bottom:2px solid var(--border);
  display:flex; align-items:center; gap:10px; flex-wrap:wrap;
  position:sticky; top:87px; z-index:10;
}
.content-hdr-title { font-family:'Instrument Serif',serif; font-style:italic; font-size:14px; font-weight:600; color:var(--navy); }
.date-badge { font-size:11px; color:var(--text3); background:var(--surface2); border:1px solid var(--border); padding:3px 10px; border-radius:var(--radius-sm); white-space:nowrap; }
.src-badge  { margin-left:auto; font-size:10px; color:var(--muted); }
.cards-wrap { padding:14px; display:flex; flex-direction:column; gap:14px; }

/* ════════════════════════════════════════════════════════
   SECTOR CARDS
════════════════════════════════════════════════════════ */
.sector-card {
  background: var(--surface);
  border: 2px solid var(--border);
  border-radius: var(--radius);
  overflow: hidden;
  box-shadow: var(--shadow);
  transition: box-shadow .2s, border-color .2s;
}
.sector-card:hover { box-shadow:var(--shadow-md); border-color:var(--border2); }

.sec-hdr {
  display: flex; align-items: center; justify-content: space-between;
  padding: 11px 16px;
  background: var(--navy);
  border-bottom: 2px solid var(--gold);
  gap:8px; flex-wrap:wrap;
}
.sec-hdr-left { display:flex; align-items:center; gap:8px; }
.sec-icon { font-size:15px; }
.sec-name { font-family:'Instrument Serif',serif; font-size:14px; font-weight:700; color:#f0ead8; }
.sec-count { font-size:10px; color:rgba(255,255,255,.4); background:rgba(255,255,255,.08); border:1px solid rgba(255,255,255,.12); padding:2px 8px; border-radius:var(--radius-sm); }
.sec-pills { display:flex; gap:5px; flex-wrap:wrap; }
.hpill { font-size:10px; font-weight:700; padding:3px 9px; border-radius:var(--radius-sm); }
.hpill.sb   { color:#f0c060; background:rgba(240,192,96,.15); border:1px solid rgba(240,192,96,.3); }
.hpill.buy  { color:#80d4a0; background:rgba(128,212,160,.15); border:1px solid rgba(128,212,160,.3); }
.hpill.sell { color:#ff8080; background:rgba(255,128,128,.15); border:1px solid rgba(255,128,128,.3); }

/* ════════════════════════════════════════════════════════
   TABLE
════════════════════════════════════════════════════════ */
.tbl-wrap { overflow-x:auto; -webkit-overflow-scrolling:touch; }
.sec-table { width:100%; border-collapse:collapse; font-size:12px; min-width:700px; }

.sec-table thead tr { background:var(--surface2); border-bottom:2px solid var(--border); }
.sec-table th {
  padding:8px 12px; font-size:9px; font-weight:700; letter-spacing:1.5px;
  color:var(--muted); text-transform:uppercase; text-align:left; white-space:nowrap;
}
.th-c { text-align:center; }
.th-r { text-align:right; }

.stock-row { border-bottom:1px solid var(--border); transition:background .12s; }
.stock-row:hover { background:rgba(0,0,0,.02); }
.stock-row:last-child { border-bottom:none; }
.sec-table td { padding:11px 12px; vertical-align:middle; }
.td-c { text-align:center; }
.td-r { text-align:right; }
.td-stock { min-width:130px; }

.stock-name { font-family:'Instrument Serif',serif; font-size:13px; font-weight:700; color:var(--navy); line-height:1.2; }
.stock-sym  { font-size:10px; color:var(--muted); margin-top:2px; font-family:'DM Mono',monospace; letter-spacing:.4px; }

.price-val { font-size:13px; font-weight:700; font-family:'DM Mono',monospace; }
.price-up { color:var(--forest); }
.price-dn { color:var(--red); }
.spark-wrap { margin-top:4px; display:flex; justify-content:flex-end; }

.rsi-badge { font-size:14px; font-weight:700; font-family:'DM Mono',monospace; display:inline-block; }
.rsi-badge.rsi-hot  { color:var(--red); }
.rsi-badge.rsi-warm { color:var(--gold); }
.rsi-badge.rsi-cold { color:var(--forest); }
.rsi-track { width:64px; height:4px; background:rgba(0,0,0,.08); border-radius:2px; margin:4px auto 0; overflow:hidden; }
.rsi-fill { height:100%; border-radius:2px; transition:width .3s; }
.rsi-fill.rsi-hot  { background:var(--red); }
.rsi-fill.rsi-warm { background:var(--gold); }
.rsi-fill.rsi-cold { background:var(--forest); }

.sr-grid { font-size:10px; font-family:'DM Mono',monospace; }
.sr-row  { display:flex; align-items:center; gap:4px; justify-content:center; line-height:1.9; }
.sr-tag  { font-size:8px; font-weight:700; padding:0 4px; border-radius:2px; min-width:24px; text-align:center; }
.sr-tag.r { background:var(--red-d);    color:var(--red); }
.sr-tag.s { background:var(--forest-d); color:var(--forest); }
.sr-val.r { color:var(--red); }
.sr-val.s { color:var(--forest); }

.macd-val { font-size:12px; font-weight:700; font-family:'DM Mono',monospace; }
.macd-pos { color:var(--forest); }
.macd-neg { color:var(--red); }
.ema-val  { margin-top:3px; }
.ema-bull { font-size:9px; font-weight:700; padding:2px 7px; border-radius:var(--radius-sm); color:var(--forest); background:var(--forest-d); border:1px solid rgba(15,92,58,.25); }
.ema-bear { font-size:9px; font-weight:700; padding:2px 7px; border-radius:var(--radius-sm); color:var(--red);    background:var(--red-d);    border:1px solid rgba(192,57,43,.25); }

.sig-pill { display:inline-block; padding:5px 11px; border-radius:var(--radius-sm); font-size:10px; font-weight:700; letter-spacing:.3px; white-space:nowrap; border:1px solid; }
.sig-sb      { background:var(--gold-d);   color:var(--gold);   border-color:rgba(184,134,11,.4); }
.sig-buy     { background:var(--forest-d); color:var(--forest); border-color:rgba(15,92,58,.35); }
.sig-neutral { background:rgba(0,0,0,.04); color:var(--muted);  border-color:var(--border); }
.sig-caution { background:var(--amber-d);  color:var(--amber);  border-color:rgba(217,119,6,.3); }
.sig-sell    { background:var(--red-d);    color:var(--red);    border-color:rgba(192,57,43,.4); }
.sig-blk     { background:var(--purple-d); color:var(--purple); border-color:rgba(100,50,160,.25); }

/* ════════════════════════════════════════════════════════
   FOOTER & STATUS
════════════════════════════════════════════════════════ */
footer {
  background: var(--navy);
  border-top: 3px solid var(--gold);
  padding: 12px 18px;
  display: flex; justify-content:space-between; align-items:center;
  font-size: 11px; color:rgba(255,255,255,.45);
  flex-wrap: wrap; gap:8px;
}
.footer-brand { color:#f0ead8; font-weight:700; font-family:'Instrument Serif',serif; }
.footer-warn  { color:#f0c060; }

.status-bar {
  background: var(--navy2); border-top:1px solid rgba(255,255,255,.1);
  padding: 5px 18px; display:flex; gap:16px; align-items:center;
  font-size: 10px; color:rgba(255,255,255,.4); flex-wrap:wrap;
}
.si { display:flex; align-items:center; gap:5px; }
.sd { width:5px; height:5px; border-radius:50%; }
.sd.ok   { background:#80d4a0; box-shadow:0 0 4px rgba(128,212,160,.5); }
.sd.warn { background:#f0c060; }
.sd.err  { background:#ff8080; }
.sts-ts { margin-left:auto; color:rgba(255,255,255,.6); font-family:'DM Mono',monospace; font-size:10px; }

/* ════════════════════════════════════════════════════════
   RESPONSIVE — LAPTOP (≤ 1279px)
════════════════════════════════════════════════════════ */
@media (max-width:1279px) {
  .stats-bar { grid-template-columns:repeat(3,1fr); }
  .stat-val  { font-size:20px; }
  .sidebar   { width:210px; }
  .main      { grid-template-columns:210px 1fr; }
  .h-meta-item:last-of-type { display:none; }
}

/* ════════════════════════════════════════════════════════
   RESPONSIVE — TABLET (≤ 1023px)
════════════════════════════════════════════════════════ */
@media (max-width:1023px) {
  /* Sidebar becomes off-canvas overlay */
  .h-burger { display:flex; }
  .h-nav    { display:none; }
  header    { grid-template-columns:auto auto 1fr auto; }

  .main { grid-template-columns:1fr; }

  .sidebar {
    position: fixed;
    top: 0; left: -260px; bottom: 0;
    width: 260px; z-index:800;
    height: 100vh;
    transition: left .28s cubic-bezier(.4,0,.2,1);
    box-shadow: none;
    border-right: 2px solid var(--border2);
  }
  .sidebar.open {
    left: 0;
    box-shadow: 4px 0 24px rgba(0,0,0,.2);
  }

  .content-hdr { top:0; position:relative; }

  /* Hide S/R and MACD columns on tablet to keep table readable */
  .td-sr   { display:none; }
  .td-macd { display:none; }

  .stats-bar { grid-template-columns:repeat(3,1fr); }
  .stat-val  { font-size:20px; }

  .h-meta-item:nth-child(1) { display:none; }

  .cards-wrap { padding:10px; gap:10px; }
}

/* ════════════════════════════════════════════════════════
   RESPONSIVE — MOBILE (≤ 639px)
════════════════════════════════════════════════════════ */
@media (max-width:639px) {
  /* Header collapses */
  header { grid-template-columns:auto 1fr auto; }
  .h-meta { display:none; }
  .logo   { font-size:17px; }
  .logo-sub { display:none; }
  .h-brand { padding:10px 14px; }

  /* Ticker scrolls faster on mobile */
  .ticker-inner { animation-duration:30s; }
  .t-extra { display:none; }

  /* Stats 2-col grid */
  .stats-bar { grid-template-columns:repeat(2,1fr); }
  .stat { padding:11px 14px; }
  .stat-val  { font-size:18px; }
  .stat-lbl  { font-size:8px; }

  /* Content header stacks */
  .content-hdr { padding:10px 12px; gap:6px; }
  .content-hdr-title { font-size:13px; }
  .src-badge  { display:none; }
  .date-badge { font-size:10px; }

  /* Cards flush to edge */
  .cards-wrap { padding:8px 6px; gap:8px; }
  .sector-card { border-radius:6px; }
  .sec-hdr { padding:9px 12px; }
  .sec-name { font-size:13px; }
  .sec-count { display:none; }

  /* Table: only show Security, Price, Signal on mobile */
  .td-sr       { display:none; }
  .td-macd     { display:none; }
  .sec-table   { min-width:0; }
  .sec-table th:nth-child(3), /* RSI header */
  .sec-table td:nth-child(3)  /* RSI cell */
              { display:none; }

  .stock-name { font-size:12px; }
  .stock-sym  { font-size:9px; }
  .price-val  { font-size:12px; }
  .sig-pill   { font-size:9px; padding:4px 8px; }

  .spark-wrap svg { width:56px; }

  /* Footer compact */
  footer { padding:10px 14px; font-size:10px; }
  footer > div:nth-child(2) { display:none; }
  .status-bar { gap:10px; padding:4px 14px; }
  .si:nth-child(3), .si:nth-child(4) { display:none; }

  /* Sidebar wider on small screens */
  .sidebar { width:85vw; max-width:300px; }
}

/* ════════════════════════════════════════════════════════
   RESPONSIVE — SMALL MOBILE (≤ 380px)
════════════════════════════════════════════════════════ */
@media (max-width:380px) {
  .stats-bar { grid-template-columns:repeat(2,1fr); }
  .stat:nth-child(5), .stat:nth-child(6) { display:none; }
  .logo { font-size:15px; }
  body  { font-size:13px; }
}

/* ════════════════════════════════════════════════════════
   ANIMATIONS
════════════════════════════════════════════════════════ */
.stock-row { animation:rowIn .25s ease both; }
@keyframes rowIn { from{opacity:0;transform:translateY(3px)} to{opacity:1;transform:translateY(0)} }
.sector-card { animation:cardIn .3s ease both; }
@keyframes cardIn { from{opacity:0;transform:translateY(6px)} to{opacity:1;transform:translateY(0)} }
"""

    # ── HTML template ─────────────────────────────────────────────────────────
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<meta name="theme-color" content="#1a2744">
<title>FII/DII Pulse &mdash; Warm Parchment &mdash; {date_str}</title>
<style>{css}</style>
</head>
<body>

<!-- ═══ OVERLAY (mobile sidebar backdrop) ═══ -->
<div class="overlay" id="overlay" onclick="closeSidebar()"></div>

<!-- ═══ HEADER ═══ -->
<header>
  <button class="h-burger" onclick="toggleSidebar()" aria-label="Menu">&#9776;</button>
  <div class="h-brand">
    <div>
      <div class="logo">
        <span class="logo-fii">FII</span>
        <span class="logo-sep">/</span>
        <span class="logo-dii">DII</span>
      </div>
      <div class="logo-sub">Institutional Intelligence &middot; Warm Parchment &middot; v9</div>
    </div>
  </div>
  <div class="h-nav">
    <span class="h-tab active">OVERVIEW</span>
    <span class="h-tab">FII FLOWS</span>
    <span class="h-tab">DII FLOWS</span>
    <span class="h-tab">SECTORS</span>
    <span class="h-tab">TECHNICALS</span>
    <span class="h-tab">ALERTS</span>
  </div>
  <div class="h-meta">
    <div class="h-meta-item">
      <div class="h-meta-label">Range</div>
      <div class="h-meta-val">{date_range_label or date_str}</div>
    </div>
    <div class="h-meta-item">
      <div class="h-meta-label">Date</div>
      <div class="h-meta-val">{date_str}</div>
    </div>
    <div class="h-live"><div class="led"></div>LIVE</div>
  </div>
</header>

<!-- ═══ TICKER ═══ -->
<div class="ticker-wrap">
  <div class="ticker-inner">{ticker_html}</div>
</div>

<!-- ═══ STATS BAR ═══ -->
<div class="stats-bar">
  <div class="stat">
    <div class="stat-lbl">Nifty 50</div>
    <div class="stat-val">&#8377;{market['nifty_price']:,.0f}</div>
    <div class="stat-chg {nc}">{na} {abs(market['nifty_chg']):.2f}%</div>
  </div>
  <div class="stat">
    <div class="stat-lbl">Sensex</div>
    <div class="stat-val">&#8377;{market['sensex_price']:,.0f}</div>
    <div class="stat-chg {xc}">{xa} {abs(market['sensex_chg']):.2f}%</div>
  </div>
  <div class="stat">
    <div class="stat-lbl">Tracked</div>
    <div class="stat-val">{len(stocks)}</div>
    <div class="stat-chg neu">Securities</div>
  </div>
  <div class="stat">
    <div class="stat-lbl">FII Buy</div>
    <div class="stat-val forest">{fb}</div>
    <div class="stat-chg up">▲ Active</div>
  </div>
  <div class="stat">
    <div class="stat-lbl">DII Buy</div>
    <div class="stat-val forest">{db}</div>
    <div class="stat-chg up">▲ Active</div>
  </div>
  <div class="stat">
    <div class="stat-lbl">Strong Buy</div>
    <div class="stat-val gold">{st}</div>
    <div class="stat-chg up">⚡ Signals</div>
  </div>
</div>

<!-- ═══ MAIN ═══ -->
<div class="main">

  <!-- SIDEBAR -->
  <div class="sidebar" id="sidebar">
    <div class="sb-section">
      <div class="sb-title">Sectors</div>
      {sidebar_items}
    </div>
    <div class="sb-section">
      <div class="sb-title">Signal Guide</div>
      <div class="sb-legend">
        <div class="sb-leg"><div class="sb-dot" style="background:var(--gold)"></div>⚡ Strong Buy</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--forest)"></div>▲ Buy</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--purple)"></div>■ Bulk/Block</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--muted)"></div>— Neutral</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--amber)"></div>⚠ Caution</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--red)"></div>▼ Sell</div>
      </div>
    </div>
    <div class="sb-section">
      <div class="sb-title">RSI Guide</div>
      <div class="sb-legend">
        <div class="sb-leg"><div class="sb-dot" style="background:var(--red)"></div>&gt;70 Overbought</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--gold)"></div>40–70 Mid zone</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--forest)"></div>&lt;40 Oversold</div>
      </div>
    </div>
    <div class="sb-section">
      <div class="sb-title">Flow Key</div>
      <div class="sb-legend">
        <div class="sb-leg"><div class="sb-dot" style="background:var(--forest)"></div>FII Buying</div>
        <div class="sb-leg"><div class="sb-dot" style="background:#3b82f6"></div>DII Buying</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--red)"></div>Selling</div>
        <div class="sb-leg"><div class="sb-dot" style="background:var(--muted)"></div>No activity</div>
      </div>
    </div>
    <div class="sb-section">
      <div class="sb-title">Data Source</div>
      <div class="sb-legend">
        <div class="sb-leg" style="flex-direction:column;align-items:flex-start;gap:3px">
          <span style="color:var(--text2);font-weight:600;font-size:11px">📡 {source}</span>
          <span style="font-size:10px;color:var(--muted)">📅 {date_range_label}</span>
          <span style="font-size:10px;color:var(--muted)">🕐 {now_ist}</span>
        </div>
      </div>
    </div>
  </div>

  <!-- CONTENT -->
  <div class="content">
    <div class="content-hdr">
      <div class="content-hdr-title">Sector-wise Institutional Flow &mdash; Strong Buy &rarr; Sell</div>
      {f'<span class="date-badge">&#128197; {date_range_label}</span>' if date_range_label else ''}
      <div class="src-badge">&#128225; {source} &middot; yfinance technicals</div>
    </div>

    <div class="cards-wrap">
      {sector_cards}
    </div>
  </div>

</div><!-- /main -->

<!-- ═══ FOOTER ═══ -->
<footer>
  <div><span class="footer-brand">FII/DII Pulse</span> &middot; Warm Parchment &middot; v9 &middot; {source} &middot; {date_str}</div>
  <div>Sorted: Strong Buy &rarr; Buy &rarr; Neutral &rarr; Caution &rarr; Sell</div>
  <div class="footer-warn">&#9888; NOT FINANCIAL ADVICE &middot; EDUCATIONAL ONLY &middot; DYOR</div>
</footer>

<!-- ═══ STATUS BAR ═══ -->
<div class="status-bar">
  <div class="si"><div class="sd ok"></div>NSE CSV: OK</div>
  <div class="si"><div class="sd ok"></div>yfinance: OK</div>
  <div class="si"><div class="sd ok"></div>{len(stocks)} stocks</div>
  <div class="si"><div class="sd ok"></div>Technicals computed</div>
  <div class="sts-ts">LAST UPDATE: {now_ist}</div>
</div>

<!-- ═══ JS — responsive sidebar toggle ═══ -->
<script>
  function toggleSidebar() {{
    const sb  = document.getElementById('sidebar');
    const ov  = document.getElementById('overlay');
    const open = sb.classList.toggle('open');
    ov.classList.toggle('active', open);
    document.body.style.overflow = open ? 'hidden' : '';
  }}
  function closeSidebar() {{
    document.getElementById('sidebar').classList.remove('open');
    document.getElementById('overlay').classList.remove('active');
    document.body.style.overflow = '';
  }}
  // Close sidebar on Escape key
  document.addEventListener('keydown', e => {{ if(e.key === 'Escape') closeSidebar(); }});
  // Smooth scroll offset for sticky header
  document.querySelectorAll('.sb-item').forEach(a => {{
    a.addEventListener('click', function(e) {{
      const href = this.getAttribute('href');
      if(href && href.startsWith('#')) {{
        e.preventDefault();
        const target = document.querySelector(href);
        if(target) {{
          const offset = 100;
          const top = target.getBoundingClientRect().top + window.scrollY - offset;
          window.scrollTo({{ top, behavior:'smooth' }});
        }}
      }}
    }});
  }});
</script>

</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
#  EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def send_email(html_path: Path, date_str: str, source: str,
               count: int, date_range_label: str = ""):
    user  = os.getenv("GMAIL_USER","").strip()
    pwd   = os.getenv("GMAIL_PASS","").strip()
    rcpts = os.getenv("RECIPIENT_EMAIL", user).strip()

    if not user or not pwd:
        log.warning("⚠️  GMAIL_USER / GMAIL_PASS not set — skipping email")
        return

    to_list = [r.strip() for r in rcpts.split(",") if r.strip()]
    log.info(f"📧 Sending full HTML dashboard to: {to_list}")

    full_html = html_path.read_text(encoding="utf-8")

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = f"📊 FII/DII Pulse · Warm Parchment — {date_str}"
    msg["From"]    = f"FII/DII Pulse <{user}>"
    msg["To"]      = ", ".join(to_list)

    plain = (
        f"FII/DII Pulse — Warm Parchment Theme\n"
        f"Date: {date_str}\nSource: {source}\n"
        f"Date range: {date_range_label}\nStocks tracked: {count}\n\n"
        f"Please open this email in an HTML-capable client to view the full dashboard.\n"
        f"Not financial advice. Educational purposes only."
    )
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(full_html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as srv:
            srv.login(user, pwd)
            srv.sendmail(user, to_list, msg.as_string())
        log.info(f"  ✅ Dashboard emailed to {to_list}")
    except smtplib.SMTPAuthenticationError:
        log.error("  ❌ Gmail auth failed — use App Password")
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
    log.info(f"  📊 FII/DII Pulse v9 Warm Parchment — {date_str}  (IST: {now_ist.strftime('%H:%M')})")
    log.info("=" * 65)

    try:
        from_date, to_date, date_range_label = get_date_range()
    except Exception:
        date_range_label = ""

    stocks, market, source = build_dataset()
    log.info(f"📊 Stocks enriched: {len(stocks)}")

    html = generate_html(stocks, market, date_str, source, date_range_label)

    index_path = OUTPUT_DIR / "index.html"
    dated_path = OUTPUT_DIR / f"report_{date_file}.html"
    for p in [index_path, dated_path]:
        p.write_text(html, encoding="utf-8")
        log.info(f"💾 Saved: {p}")

    send_email(dated_path, date_str, source, len(stocks), date_range_label)

    log.info("=" * 65)
    log.info(f"  ✅ Complete! Range: {date_range_label} | Stocks: {len(stocks)}")
    log.info("=" * 65)


if __name__ == "__main__":
    main()
