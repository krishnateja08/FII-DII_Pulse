"""
FII/DII Intelligence Dashboard — v8 (CSV Edition) · Stealth Slate Theme
===================================================
KEY CHANGE from original v8:
  fetch_from_nse() now uses the CSV download endpoint instead of the
  JSON API. The JSON API caps at 50 records; the CSV API returns ALL
  records for the full date window — matching the "Download (.csv)"
  button on the NSE website.

NSE CSV endpoint (bulk):
  https://www.nseindia.com/api/historicalOR/bulk-block-short-deals
  ?optionType=bulk_deals&from=DD-MM-YYYY&to=DD-MM-YYYY&csv=true

NSE CSV endpoint (block):
  https://www.nseindia.com/api/historicalOR/bulk-block-short-deals
  ?optionType=block_deals&from=DD-MM-YYYY&to=DD-MM-YYYY&csv=true

Date Logic (VERIFIED):
  - Block Deal window closes at 06:30 PM IST daily
  - After  18:30 IST → to_date = TODAY  (deals are final)
  - Before 18:30 IST → to_date = last completed trading day
  - from_date = 5 trading days BEFORE to_date
    → to_date is day-1, from_date is day-6 = 6 trading days total
    → Matches NSE website window: e.g. 10-02-2026 → 17-02-2026

THEME: Stealth Slate — Dark Charcoal · Teal Accents · Clean Card Layout
  DM Sans · Card-based sectors · Pill badges · Gradient signals
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
    log.info("[Source 1] NSE Bulk/Block Deals — CSV Download API (no 50-row cap)...")

    try:
        from_date, to_date, date_range_label = get_date_range()
        from_str = fmt_nse_date(from_date)
        to_str   = fmt_nse_date(to_date)
        log.info(f"  -> Range: {from_str} to {to_str}")

        csv_endpoints = [
            {
                "url": "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals",
                "params": {
                    "optionType": "bulk_deals",
                    "from": from_str,
                    "to":   to_str,
                    "csv":  "true",
                },
                "deal_type": "bulk_deals",
            },
            {
                "url": "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals",
                "params": {
                    "optionType": "block_deals",
                    "from": from_str,
                    "to":   to_str,
                    "csv":  "true",
                },
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
                        resp = session_obj.get(
                            ep["url"],
                            params=ep["params"],
                            headers=csv_req_headers,
                            timeout=30,
                        )
                    else:
                        resp = session_obj.get(
                            ep["url"],
                            params=ep["params"],
                            headers={**NSE_HEADERS, **csv_req_headers},
                            timeout=30,
                        )

                    body    = resp.content
                    preview = body[:300].decode("utf-8", errors="replace").strip()
                    log.info(
                        f"  -> [{deal_type}] HTTP {resp.status_code} | "
                        f"{len(body)} bytes | {preview[:80]!r}"
                    )

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
                        csv_df = pd.read_csv(
                            io.StringIO(body.decode("utf-8", errors="replace"))
                        )
                        log.info(
                            f"  ✅ [{deal_type}] CSV: {len(csv_df)} rows | "
                            f"cols: {list(csv_df.columns)}"
                        )
                        break
                    except Exception as csv_err:
                        log.warning(f"  !! CSV parse error: {csv_err} — trying JSON fallback")

                    try:
                        raw_json = resp.json()
                        if isinstance(raw_json, list) and raw_json:
                            csv_df = pd.DataFrame(raw_json)
                        elif isinstance(raw_json, dict):
                            for key in ["data", "Data", "results", "records",
                                        "bulkDeals", "blockDeals"]:
                                val = raw_json.get(key)
                                if isinstance(val, list) and val:
                                    cols  = raw_json.get("columns")
                                    csv_df = (
                                        pd.DataFrame(val, columns=cols)
                                        if (cols and not isinstance(val[0], dict))
                                        else pd.DataFrame(val)
                                    )
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
                log.info(f"  -> [{deal_type}] {len(csv_df)} rows queued")
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
        log.info(f"  -> Combined: {df.shape[0]} rows from {len(all_dfs)} endpoint(s)")
        df.columns = [str(c).strip() for c in df.columns]
        log.info(f"  -> Raw columns: {list(df.columns)}")

        NSE_EXACT = {
            "BD_SYMBOL":      "SYMBOL",
            "BD_SCRIP_NAME":  "COMPANY",
            "BD_CLIENT_NAME": "CLIENT",
            "BD_BUY_SELL":    "BUYSELL",
            "BD_QTY_TRD":     "QTY",
            "BD_DT_DATE":     "DATE",
            "BD_TP_WATP":     "PRICE",
            "BD_REMARKS":     "REMARKS",
            "Symbol":                          "SYMBOL",
            "Security Name":                   "COMPANY",
            "Client Name":                     "CLIENT",
            "Buy / Sell":                      "BUYSELL",
            "Quantity Traded":                 "QTY",
            "Trade Price / Wght. Avg. Price":  "PRICE",
            "Remarks":                         "REMARKS",
            "Date":                            "DATE",
            "SYMBOL":          "SYMBOL",
            "SECURITY NAME":   "COMPANY",
            "CLIENT NAME":     "CLIENT",
            "BUY / SELL":      "BUYSELL",
            "QUANTITY TRADED": "QTY",
            "TRADE PRICE / WGHT. AVG. PRICE": "PRICE",
            "SCRIP_NAME":  "COMPANY",
            "CLIENT_NAME": "CLIENT",
            "BUY_SELL":    "BUYSELL",
            "QTY_TRD":     "QTY",
            "TRADE_DATE":  "DATE",
            "TRADE_PRICE": "PRICE",
        }

        nse_upper = {k.upper(): v for k, v in NSE_EXACT.items()}

        rename = {}
        mapped = set()
        for c in df.columns:
            cu = c.strip()
            target = NSE_EXACT.get(cu) or nse_upper.get(cu.upper())
            if target and target not in mapped:
                rename[c] = target
                mapped.add(target)
                continue
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
        log.info(f"  -> Normalised columns: {list(df.columns)}")

        if "CLIENT" not in df.columns:
            log.warning("  ❌ CLIENT column missing after normalisation")
            log.info(f"  -> All columns present: {list(df.columns)}")
            return []

        stocks, matched = {}, 0
        for _, row in df.iterrows():
            sym    = str(row.get("SYMBOL",  "")).strip().upper()
            name   = str(row.get("COMPANY", sym)).strip()
            client = str(row.get("CLIENT",  "")).strip().upper()
            bs     = str(row.get("BUYSELL", "")).strip().upper()

            if not sym or sym in ("NAN", "") or not client or client == "NAN":
                continue

            is_fii = any(k in client for k in FII_KW)
            is_dii = any(k in client for k in DII_KW)
            action = "buy" if bs.startswith("B") else "sell"

            if sym not in stocks:
                stocks[sym] = {
                    "symbol":      sym + ".NS",
                    "name":        name,
                    "fii_cash":    "neutral",
                    "dii_cash":    "neutral",
                    "client_name": client,
                }

            if is_fii:
                stocks[sym]["fii_cash"] = action
                matched += 1
            if is_dii:
                stocks[sym]["dii_cash"] = action
                matched += 1

        result = list(stocks.values())
        log.info(
            f"  → Total rows={len(df)} | FII/DII matched={matched} | "
            f"unique stocks={len(result)} (ALL included)"
        )
        return result

    except Exception as e:
        log.warning(f"  ❌ NSE fetch_from_nse error: {e}")
        import traceback
        log.warning(traceback.format_exc())
        return []


# ─────────────────────────────────────────────────────────────────────────────
#  SOURCE 2 — MunafaSutra fallback
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
            href   = a.get("href", "")
            symbol = href.rstrip("/").split("/")[-1]
            name   = a.get_text(strip=True)
            if not symbol or not name:
                continue
            tr = a.find_parent("tr")
            if not tr:
                continue
            row_text = " ".join(
                t.get_text(" ", strip=True).lower() for t in tr.find_all("td")
            )
            fii = "buy" if "bought" in row_text else "sell"
            dii = "buy" if "bought" in row_text else "sell"
            stocks.append({"symbol": symbol + ".NS", "name": name,
                           "fii_cash": fii, "dii_cash": dii})
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
        gain = dlt.clip(lower=0)
        loss = (-dlt).clip(lower=0)
        ag   = gain.ewm(com=13, adjust=False).mean()
        al   = loss.ewm(com=13, adjust=False).mean()
        rsi_s= 100 - (100 / (1 + ag / al.replace(0, np.nan)))
        rsi  = round(float(rsi_s.iloc[-1]), 1)

        # MACD(12,26,9)
        macd  = (c.ewm(span=12, adjust=False).mean()
                 - c.ewm(span=26, adjust=False).mean())
        mhist = round(float(
            (macd - macd.ewm(span=9, adjust=False).mean()).iloc[-1]
        ), 2)

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
        tr  = pd.concat(
            [h-lo, (h-c.shift()).abs(), (lo-c.shift()).abs()], axis=1
        ).max(axis=1)
        atr = tr.ewm(com=13, adjust=False).mean()
        pdi = 100 * pdm.ewm(com=13, adjust=False).mean() / atr
        mdi = 100 * mdm.ewm(com=13, adjust=False).mean() / atr
        dx  = 100 * (pdi-mdi).abs() / (pdi+mdi).replace(0, np.nan)
        adx = round(float(dx.ewm(com=13, adjust=False).mean().iloc[-1]), 1)

        # Stoch RSI
        srsi = ((rsi_s - rsi_s.rolling(14).min()) /
                (rsi_s.rolling(14).max() - rsi_s.rolling(14).min())
                .replace(0, np.nan))
        sv = float(srsi.iloc[-1])
        sv = 0.5 if np.isnan(sv) else round(sv, 2)

        # Pivot S/R
        n  = min(120, len(h))
        pv = (float(h.iloc[-1]) + float(lo.iloc[-1]) + lc) / 3
        r1 = round(2*pv - float(lo.iloc[-1]), 2)
        s1 = round(2*pv - float(h.iloc[-1]),  2)
        sh = round(float(h.rolling(n).max().iloc[-1]),  2)
        sl = round(float(lo.rolling(n).min().iloc[-1]), 2)

        # Signal score
        sc = 0
        sc += 2 if rsi < 40    else (1 if rsi < 55 else (-2 if rsi > 70 else 0))
        sc += 2 if mhist > 0   else 0
        sc += 2 if ecross == "bullish" else 0
        sc += 1 if adx > 25    else 0
        sc += 1 if sv < 0.3    else (-1 if sv > 0.8 else 0)
        ov = ("STRONG BUY" if sc >= 5
              else "BUY"     if sc >= 3
              else "NEUTRAL" if sc >= 0
              else "CAUTION" if sc >= -2
              else "SELL")

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
                round(float((c.iloc[-1]-c.iloc[-2])/c.iloc[-2]*100), 2)
                if len(c) >= 2 else 0.0
            )
        np_, nc = load("^NSEI")
        sp_, sc = load("^BSESN")
        return dict(nifty_price=np_, nifty_chg=nc,
                    sensex_price=sp_, sensex_chg=sc)
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
        inst_sig = ("BOTH BUY"   if both_buy else
                    "FII BUY"    if fii_only  else
                    "DII BUY"    if dii_only  else
                    "BOTH SELL"  if both_sel  else
                    "BULK/BLOCK" if neither   else "SELL")
        enriched.append({**s, **tech,
                         "inst_signal": inst_sig,
                         "both_buy":    both_buy,
                         "fii_only":    fii_only,
                         "dii_only":    dii_only})
        time.sleep(0.4)
    return enriched, market, source


# ─────────────────────────────────────────────────────────────────────────────
#  SECTOR MAP & ICONS
# ─────────────────────────────────────────────────────────────────────────────

SECTOR_MAP = {
    "HDFCBANK":"Banking & Finance","ICICIBANK":"Banking & Finance",
    "SBIN":"Banking & Finance","AXISBANK":"Banking & Finance",
    "KOTAKBANK":"Banking & Finance","INDUSINDBK":"Banking & Finance",
    "BANDHANBNK":"Banking & Finance","FEDERALBNK":"Banking & Finance",
    "IDFCFIRSTB":"Banking & Finance","PNB":"Banking & Finance",
    "BANKBARODA":"Banking & Finance","CANARABANK":"Banking & Finance",
    "AUBANK":"Banking & Finance","RBLBANK":"Banking & Finance",
    "YESBANK":"Banking & Finance","UJJIVANSFB":"Banking & Finance",
    "EQUITASBNK":"Banking & Finance","ESAFSFB":"Banking & Finance",
    "BAJFINANCE":"NBFC & Fintech","BAJAJFINSV":"NBFC & Fintech",
    "CHOLAFIN":"NBFC & Fintech","MUTHOOTFIN":"NBFC & Fintech",
    "MANAPPURAM":"NBFC & Fintech","SBICARD":"NBFC & Fintech",
    "ANGELONE":"NBFC & Fintech","POLICYBZR":"NBFC & Fintech",
    "CAMS":"NBFC & Fintech","KFINTECH":"NBFC & Fintech",
    "NUVAMA":"NBFC & Fintech","360ONE":"NBFC & Fintech",
    "IIFL":"NBFC & Fintech","MOTHERSON":"Auto & Auto Ancillaries",
    "TCS":"IT & Technology","INFY":"IT & Technology",
    "WIPRO":"IT & Technology","HCLTECH":"IT & Technology",
    "TECHM":"IT & Technology","LTIM":"IT & Technology",
    "MPHASIS":"IT & Technology","COFORGE":"IT & Technology",
    "PERSISTENT":"IT & Technology","OFSS":"IT & Technology",
    "LTTS":"IT & Technology","HEXAWARE":"IT & Technology",
    "KPITTECH":"IT & Technology","TATAELXSI":"IT & Technology",
    "SUNPHARMA":"Pharma & Healthcare","DRREDDY":"Pharma & Healthcare",
    "CIPLA":"Pharma & Healthcare","DIVISLAB":"Pharma & Healthcare",
    "TORNTPHARM":"Pharma & Healthcare","AUROPHARMA":"Pharma & Healthcare",
    "LUPIN":"Pharma & Healthcare","ALKEM":"Pharma & Healthcare",
    "IPCALAB":"Pharma & Healthcare","GLAND":"Pharma & Healthcare",
    "FORTIS":"Pharma & Healthcare","APOLLOHOSP":"Pharma & Healthcare",
    "MAXHEALTH":"Pharma & Healthcare","KIMS":"Pharma & Healthcare",
    "MEDANTA":"Pharma & Healthcare","NARAYANA":"Pharma & Healthcare",
    "RELIANCE":"Oil, Gas & Energy","ONGC":"Oil, Gas & Energy",
    "IOC":"Oil, Gas & Energy","BPCL":"Oil, Gas & Energy",
    "HINDPETRO":"Oil, Gas & Energy","GAIL":"Oil, Gas & Energy",
    "OIL":"Oil, Gas & Energy","MGL":"Oil, Gas & Energy",
    "IGL":"Oil, Gas & Energy","PETRONET":"Oil, Gas & Energy",
    "GUJGASLTD":"Oil, Gas & Energy","ATGL":"Oil, Gas & Energy",
    "NTPC":"Power & Utilities","POWERGRID":"Power & Utilities",
    "ADANIPOWER":"Power & Utilities","TATAPOWER":"Power & Utilities",
    "JSWENERGY":"Power & Utilities","TORNTPOWER":"Power & Utilities",
    "CESC":"Power & Utilities","NHPC":"Power & Utilities",
    "SJVN":"Power & Utilities","IREDA":"Power & Utilities",
    "PFC":"Power & Utilities","RECLTD":"Power & Utilities",
    "TATASTEEL":"Metals & Mining","JSWSTEEL":"Metals & Mining",
    "HINDALCO":"Metals & Mining","VEDL":"Metals & Mining",
    "SAIL":"Metals & Mining","NMDC":"Metals & Mining",
    "NATIONALUM":"Metals & Mining","WELCORP":"Metals & Mining",
    "APLAPOLLO":"Metals & Mining","JINDALSTEL":"Metals & Mining",
    "MOIL":"Metals & Mining","RATNAMANI":"Metals & Mining",
    "MARUTI":"Auto & Auto Ancillaries","TATAMOTORS":"Auto & Auto Ancillaries",
    "M&M":"Auto & Auto Ancillaries","BAJAJ-AUTO":"Auto & Auto Ancillaries",
    "HEROMOTOCO":"Auto & Auto Ancillaries","EICHERMOT":"Auto & Auto Ancillaries",
    "TVSMOTORS":"Auto & Auto Ancillaries","ASHOKLEY":"Auto & Auto Ancillaries",
    "ESCORTS":"Auto & Auto Ancillaries","BOSCHLTD":"Auto & Auto Ancillaries",
    "BHARATFORG":"Auto & Auto Ancillaries","EXIDEIND":"Auto & Auto Ancillaries",
    "AMARAJABAT":"Auto & Auto Ancillaries","BALKRISIND":"Auto & Auto Ancillaries",
    "TIINDIA":"Auto & Auto Ancillaries","APOLLOTYRE":"Auto & Auto Ancillaries",
    "HINDUNILVR":"FMCG & Consumer","ITC":"FMCG & Consumer",
    "NESTLEIND":"FMCG & Consumer","BRITANNIA":"FMCG & Consumer",
    "DABUR":"FMCG & Consumer","MARICO":"FMCG & Consumer",
    "COLPAL":"FMCG & Consumer","GODREJCP":"FMCG & Consumer",
    "EMAMILTD":"FMCG & Consumer","TATACONSUM":"FMCG & Consumer",
    "VARUN":"FMCG & Consumer","RADICO":"FMCG & Consumer",
    "UBL":"FMCG & Consumer","MCDOWELL-N":"FMCG & Consumer",
    "ULTRACEMCO":"Cement & Construction","AMBUJACEM":"Cement & Construction",
    "ACC":"Cement & Construction","SHREECEM":"Cement & Construction",
    "DALMIACEMENTBHARAT":"Cement & Construction","RAMCOCEM":"Cement & Construction",
    "JKCEMENT":"Cement & Construction","HEIDELBERG":"Cement & Construction",
    "LT":"Cement & Construction","NCC":"Cement & Construction",
    "KNRCON":"Cement & Construction","PNCINFRA":"Cement & Construction",
    "RVNL":"Cement & Construction","IRCON":"Cement & Construction",
    "DLF":"Real Estate","GODREJPROP":"Real Estate",
    "OBEROIRLTY":"Real Estate","PRESTIGE":"Real Estate",
    "PHOENIXLTD":"Real Estate","BRIGADE":"Real Estate",
    "SOBHA":"Real Estate","MAHLIFE":"Real Estate",
    "LODHA":"Real Estate","SUNTECK":"Real Estate",
    "SIEMENS":"Capital Goods & Industrials","ABB":"Capital Goods & Industrials",
    "HAVELLS":"Capital Goods & Industrials","BHEL":"Capital Goods & Industrials",
    "BEL":"Capital Goods & Industrials","HAL":"Capital Goods & Industrials",
    "COCHINSHIP":"Capital Goods & Industrials","MAZDOCK":"Capital Goods & Industrials",
    "GRINDWELL":"Capital Goods & Industrials","THERMAX":"Capital Goods & Industrials",
    "CUMMINSIND":"Capital Goods & Industrials","KALYANKJIL":"Capital Goods & Industrials",
    "BHARTIARTL":"Telecom & Media","IDEA":"Telecom & Media",
    "INDUSTOWER":"Telecom & Media","TATACOMM":"Telecom & Media",
    "ZEEL":"Telecom & Media","SUNTV":"Telecom & Media",
    "PVRINOX":"Telecom & Media",
    "PIDILITIND":"Chemicals & Specialty","ASIANPAINT":"Chemicals & Specialty",
    "BERGEPAINT":"Chemicals & Specialty","ATUL":"Chemicals & Specialty",
    "NAVINFLUOR":"Chemicals & Specialty","SOLARINDS":"Chemicals & Specialty",
    "FINEORG":"Chemicals & Specialty","CLEAN":"Chemicals & Specialty",
    "DEEPAKNITR":"Chemicals & Specialty","ALKYLAMINE":"Chemicals & Specialty",
    "SBILIFE":"Insurance","HDFCLIFE":"Insurance",
    "ICICIPRULI":"Insurance","MAXFINSERV":"Insurance",
    "GICRE":"Insurance","NIACL":"Insurance",
    "STARHEALTH":"Insurance","GODIGIT":"Insurance",
    "BSE":"Exchange & Capital Markets","MCX":"Exchange & Capital Markets",
    "CDSL":"Exchange & Capital Markets","NSDL":"Exchange & Capital Markets",
    "CRISIL":"Exchange & Capital Markets","ICRA":"Exchange & Capital Markets",
    "INDIGO":"Aviation & Logistics","SPICEJET":"Aviation & Logistics",
    "GMRAIRPORT":"Aviation & Logistics","ADANIPORTS":"Aviation & Logistics",
    "CONCOR":"Aviation & Logistics","BLUEDART":"Aviation & Logistics",
    "DELHIVERY":"Aviation & Logistics","MAHINDRA LOG":"Aviation & Logistics",
    "DMART":"Retail & E-Commerce","TRENT":"Retail & E-Commerce",
    "NYKAA":"Retail & E-Commerce","ZOMATO":"Retail & E-Commerce",
    "CARTRADE":"Retail & E-Commerce","SHOPERSTOP":"Retail & E-Commerce",
    "UPL":"Agri & Fertilisers","COROMANDEL":"Agri & Fertilisers",
    "CHAMBLFERT":"Agri & Fertilisers","GNFC":"Agri & Fertilisers",
    "GSFC":"Agri & Fertilisers","NFL":"Agri & Fertilisers",
    "RALLIS":"Agri & Fertilisers","BAYER":"Agri & Fertilisers",
}

SECTOR_ICONS = {
    "Banking & Finance":           "🏦",
    "NBFC & Fintech":              "💳",
    "IT & Technology":             "💻",
    "Pharma & Healthcare":         "💊",
    "Oil, Gas & Energy":           "⛽",
    "Power & Utilities":           "⚡",
    "Metals & Mining":             "⚙️",
    "Auto & Auto Ancillaries":     "🚗",
    "FMCG & Consumer":             "🛒",
    "Cement & Construction":       "🏗️",
    "Real Estate":                 "🏢",
    "Capital Goods & Industrials": "🏭",
    "Telecom & Media":             "📡",
    "Chemicals & Specialty":       "🧪",
    "Insurance":                   "🛡️",
    "Exchange & Capital Markets":  "📈",
    "Aviation & Logistics":        "✈️",
    "Retail & E-Commerce":         "🛍️",
    "Agri & Fertilisers":          "🌾",
    "Others":                      "🔷",
}

SIGNAL_ORDER = {
    "STRONG BUY": 0,
    "BUY":        1,
    "NEUTRAL":    2,
    "CAUTION":    3,
    "BULK/BLOCK": 4,
    "N/A":        5,
    "SELL":       6,
    "BOTH SELL":  7,
}


def get_sector(symbol: str) -> str:
    sym = symbol.replace(".NS", "").strip().upper()
    return SECTOR_MAP.get(sym, "Others")


# ─────────────────────────────────────────────────────────────────────────────
#  HTML HELPERS  — Stealth Slate Theme
# ─────────────────────────────────────────────────────────────────────────────

def spark_svg(prices):
    """Mini bar-chart sparkline — teal/red palette matching Stealth Slate."""
    if len(prices) < 2:
        return ""
    mn, mx = min(prices), max(prices)
    rng = mx - mn or 1
    w, h = 72, 22
    # Bar chart style matching the screenshot
    bar_w = max(1, w // len(prices) - 1)
    bars = ""
    up = prices[-1] >= prices[0]
    for i, p in enumerate(prices):
        bar_h = max(2, round((p - mn) / rng * h))
        x = i * (w // len(prices))
        y = h - bar_h
        col = "#10b981" if up else "#ef4444"
        bars += f'<rect x="{x}" y="{y}" width="{bar_w}" height="{bar_h}" fill="{col}" rx="1"/>'
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
        f'xmlns="http://www.w3.org/2000/svg" style="display:block">'
        f'{bars}'
        f'</svg>'
    )


def rsi_class(v):
    """Return Stealth Slate RSI CSS class."""
    if v > 70:  return "rsi-hot"
    if v < 40:  return "rsi-cold"
    return "rsi-warm"


def sig_class(overall):
    """Map overall signal → Stealth Slate CSS badge class."""
    return {
        "STRONG BUY": "sig-sb",
        "BUY":        "sig-buy",
        "NEUTRAL":    "sig-neutral",
        "CAUTION":    "sig-caution",
        "SELL":       "sig-sell",
        "BOTH SELL":  "sig-sell",
        "BULK/BLOCK": "sig-blk",
        "N/A":        "sig-neutral",
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
#  GENERATE HTML  —  Stealth Slate Theme
# ─────────────────────────────────────────────────────────────────────────────

def generate_html(stocks, market, date_str, source, date_range_label="") -> str:

    # ── Market direction helpers ──────────────────────────────────────────────
    nc  = "up"  if market["nifty_chg"]  >= 0 else "dn"
    xc  = "up"  if market["sensex_chg"] >= 0 else "dn"
    na  = "▲"   if market["nifty_chg"]  >= 0 else "▼"
    xa  = "▲"   if market["sensex_chg"] >= 0 else "▼"

    # ── Counts ────────────────────────────────────────────────────────────────
    fb  = sum(1 for s in stocks if s["fii_cash"] == "buy")
    db  = sum(1 for s in stocks if s["dii_cash"] == "buy")
    bb  = sum(1 for s in stocks if s["both_buy"])
    st  = sum(1 for s in stocks if s["overall"] == "STRONG BUY")
    sel = sum(1 for s in stocks if s["overall"] in ("SELL", "BOTH SELL"))

    # ── Sector grouping + sorting ─────────────────────────────────────────────
    for s in stocks:
        s["sector"] = get_sector(s["symbol"])

    from collections import defaultdict
    sector_groups = defaultdict(list)
    for s in stocks:
        sector_groups[s["sector"]].append(s)

    def signal_sort_key(s):
        return SIGNAL_ORDER.get(s.get("overall", "N/A"), 5)

    for sec in sector_groups:
        sector_groups[sec].sort(key=signal_sort_key)

    def sector_best(items):
        return min(SIGNAL_ORDER.get(s.get("overall", "N/A"), 5) for s in items)

    sorted_sectors = sorted(
        sector_groups.items(), key=lambda kv: sector_best(kv[1])
    )

    # ── Sidebar sector list ───────────────────────────────────────────────────
    sidebar_items = ""
    for sector_name, sec_stocks in sorted_sectors:
        icon       = SECTOR_ICONS.get(sector_name, "🔷")
        best_sig   = min(sec_stocks, key=signal_sort_key)["overall"]
        if best_sig in ("STRONG BUY", "BUY", "BOTH BUY"):
            sig_cls, sig_lbl = "buy",  "↑ BUY"
        elif best_sig in ("SELL", "BOTH SELL"):
            sig_cls, sig_lbl = "sell", "↓ SELL"
        else:
            sig_cls, sig_lbl = "hold", "→ HOLD"
        anchor = sector_name.replace(" ", "_").replace("&", "and")
        sidebar_items += f"""
        <a href="#{anchor}" class="sb-item">
          <div>
            <div class="sb-item-name">{icon} {sector_name}</div>
            <div class="sb-item-count">{len(sec_stocks)} securities</div>
          </div>
          <span class="sb-item-sig {sig_cls}">{sig_lbl}</span>
        </a>"""

    # ── Sector card rows ──────────────────────────────────────────────────────
    sector_cards = ""

    for sector_name, sec_stocks in sorted_sectors:
        icon      = SECTOR_ICONS.get(sector_name, "🔷")
        anchor    = sector_name.replace(" ", "_").replace("&", "and")
        sec_count = len(sec_stocks)
        sec_sb    = sum(1 for s in sec_stocks if s["overall"] == "STRONG BUY")
        sec_buy   = sum(1 for s in sec_stocks if s["overall"] == "BUY")
        sec_sell  = sum(1 for s in sec_stocks if s["overall"] in ("SELL", "BOTH SELL"))

        # Sector header pills
        header_pills = ""
        if sec_sb:
            header_pills += f'<span class="hdr-pill sb">⚡ {sec_sb} Strong Buy</span>'
        if sec_buy:
            header_pills += f'<span class="hdr-pill buy">▲ {sec_buy} Buy</span>'
        if sec_sell:
            header_pills += f'<span class="hdr-pill sell">▼ {sec_sell} Sell</span>'

        # Build stock rows for this sector card
        stock_rows = ""
        for s in sec_stocks:
            sym         = s["symbol"].replace(".NS", "")
            price       = fmt_price(s["last_price"]) if s["last_price"] > 0 else "—"
            spk         = spark_svg(s.get("sparkline", []))
            rsi_v       = s["rsi"]
            rsi_cls     = rsi_class(rsi_v)
            macd_h      = fmt_macd(s["macd_hist"])
            ema_h       = fmt_ema(s["ema_cross"])
            overall     = s["overall"]
            sig_cls_val = sig_class(overall)
            is_up       = s.get("sparkline") and len(s["sparkline"]) >= 2 and s["sparkline"][-1] >= s["sparkline"][0]

            # Signal label
            if overall == "STRONG BUY":
                sig_label = "⚡ STRONG BUY"
            elif overall == "BUY":
                sig_label = "▲ BUY"
            elif overall in ("SELL", "BOTH SELL"):
                sig_label = "▼ SELL"
            elif overall == "CAUTION":
                sig_label = "⚠ CAUTION"
            elif overall == "BULK/BLOCK":
                sig_label = "■ BULK/BLOCK"
            else:
                sig_label = "— NEUTRAL"

            price_dir_cls = "price-up" if is_up else "price-dn"

            stock_rows += f"""
            <tr class="stock-row">
              <td class="td-stock">
                <div class="stock-name">{s['name']}</div>
                <div class="stock-sym">{sym}</div>
              </td>
              <td class="td-r">
                <div class="price-val {price_dir_cls}">{price}</div>
                <div class="spark-wrap">{spk}</div>
              </td>
              <td class="td-c">
                <div class="rsi-badge {rsi_cls}">{rsi_v}</div>
                <div class="rsi-track">
                  <div class="rsi-fill {rsi_cls}" style="width:{min(rsi_v,100):.0f}%"></div>
                </div>
              </td>
              <td class="td-c">
                <div class="sr-grid">
                  <div class="sr-row"><span class="sr-tag r">R1</span><span class="sr-val r">{fmt_price(s['resist1'])}</span></div>
                  <div class="sr-row"><span class="sr-tag s">S1</span><span class="sr-val s">{fmt_price(s['support1'])}</span></div>
                  <div class="sr-row"><span class="sr-tag r">6mH</span><span class="sr-val r">{fmt_price(s['swing_high'])}</span></div>
                  <div class="sr-row"><span class="sr-tag s">6mL</span><span class="sr-val s">{fmt_price(s['swing_low'])}</span></div>
                </div>
              </td>
              <td class="td-c">
                <div class="macd-val">{macd_h}</div>
                <div class="ema-val">{ema_h}</div>
              </td>
              <td class="td-c">
                <span class="sig-pill {sig_cls_val}">{sig_label}</span>
              </td>
            </tr>"""

        sector_cards += f"""
        <div class="sector-card" id="{anchor}">
          <div class="sec-card-hdr">
            <div class="sec-card-left">
              <span class="sec-icon">{icon}</span>
              <span class="sec-card-name">{sector_name}</span>
              <span class="sec-count-badge">{sec_count} securities</span>
            </div>
            <div class="sec-hdr-pills">{header_pills}</div>
          </div>
          <div class="sec-table-wrap">
            <table class="sec-table">
              <thead>
                <tr>
                  <th>SECURITY</th>
                  <th class="th-r">PRICE / TREND</th>
                  <th class="th-c">RSI (14)</th>
                  <th class="th-c">S/R LEVELS</th>
                  <th class="th-c">MACD / EMA</th>
                  <th class="th-c">SIGNAL</th>
                </tr>
              </thead>
              <tbody>{stock_rows}</tbody>
            </table>
          </div>
        </div>"""

    # ── IST timestamp ─────────────────────────────────────────────────────────
    IST = pytz.timezone("Asia/Kolkata")
    now_ist = datetime.now(IST).strftime("%d-%b-%Y %H:%M IST")

    # ── Ticker tape ───────────────────────────────────────────────────────────
    ticker_items = [
        ("NIFTY 50",
         f"&#8377;{market['nifty_price']:,.2f}",
         "up" if market["nifty_chg"] >= 0 else "dn",
         f"{'▲' if market['nifty_chg']>=0 else '▼'}{abs(market['nifty_chg']):.2f}%"),
        ("SENSEX",
         f"&#8377;{market['sensex_price']:,.2f}",
         "up" if market["sensex_chg"] >= 0 else "dn",
         f"{'▲' if market['sensex_chg']>=0 else '▼'}{abs(market['sensex_chg']):.2f}%"),
        ("TRACKED",   str(len(stocks)), "up",  f"FII:{fb} · DII:{db}"),
        ("BOTH BUY",  str(bb),          "up",  "securities"),
        ("STRONG BUY",str(st),          "up",  "signals"),
        ("SELL ALERT",str(sel),         "dn" if sel > 0 else "up", "caution"),
        ("SOURCE",    source[:20],      "up",  "NSE CSV"),
        ("RANGE",     date_range_label, "up",  "window"),
    ]
    ticker_html = ""
    for sym, val, cls, extra in ticker_items:
        ticker_html += (
            f'<div class="t-item">'
            f'<span class="t-sym">{sym}</span>'
            f'<span class="t-val {cls}">{val}</span>'
            f'<span class="t-extra">{extra}</span>'
            f'</div>'
        )
    ticker_html = ticker_html * 2  # duplicate for seamless scroll

    # ══════════════════════════════════════════════════════════════════════════
    #  CSS — Stealth Slate Theme
    # ══════════════════════════════════════════════════════════════════════════
    css = """
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

/* ═══════════════════════════════════════════════════════
   STEALTH SLATE PALETTE
   --bg        #0f1117  deep charcoal
   --surface   #161b27  card surfaces
   --surface2  #1c2333  elevated panels
   --border    #252d3d  subtle borders
   --border2   #2e3a50  card borders
   --teal      #10b981  primary accent / buy
   --teal-dim  #065f46  teal bg tint
   --blue      #3b82f6  secondary accent
   --red       #ef4444  sell / negative
   --yellow    #f59e0b  caution
   --text      #e2e8f0  primary text
   --text2     #94a3b8  muted text
   --text3     #64748b  dim text
   --white     #f8fafc  headings
═══════════════════════════════════════════════════════ */

:root {
  --bg:       #0f1117;
  --surface:  #161b27;
  --surface2: #1c2333;
  --surface3: #212940;
  --border:   #252d3d;
  --border2:  #2e3a50;
  --teal:     #10b981;
  --teal2:    #34d399;
  --teal-dim: rgba(16,185,129,.12);
  --blue:     #3b82f6;
  --blue-dim: rgba(59,130,246,.12);
  --red:      #ef4444;
  --red-dim:  rgba(239,68,68,.12);
  --yellow:   #f59e0b;
  --yel-dim:  rgba(245,158,11,.12);
  --purple:   #8b5cf6;
  --text:     #e2e8f0;
  --text2:    #94a3b8;
  --text3:    #64748b;
  --white:    #f8fafc;
  --radius:   10px;
  --radius-sm:6px;
}

*{margin:0;padding:0;box-sizing:border-box}
html{scroll-behavior:smooth}

body{
  background:var(--bg);
  color:var(--text);
  font-family:'DM Sans','Segoe UI',system-ui,sans-serif;
  min-height:100vh;
  overflow-x:hidden;
  font-size:14px;
  line-height:1.5;
}

/* Subtle grain texture */
body::before{
  content:'';position:fixed;inset:0;pointer-events:none;z-index:0;
  background-image:url("data:image/svg+xml,%3Csvg viewBox='0 0 256 256' xmlns='http://www.w3.org/2000/svg'%3E%3Cfilter id='noise'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.9' numOctaves='4' stitchTiles='stitch'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23noise)' opacity='0.03'/%3E%3C/svg%3E");
  opacity:.5;
}

.w{position:relative;z-index:1;min-height:100vh;display:flex;flex-direction:column}

/* ── HEADER ── */
header{
  background:var(--surface);
  border-bottom:1px solid var(--border2);
  display:grid;
  grid-template-columns:auto 1fr auto;
  align-items:stretch;
  position:sticky;top:0;z-index:500;
  box-shadow:0 4px 24px rgba(0,0,0,.4);
}

.h-brand{
  padding:12px 24px;
  border-right:1px solid var(--border);
  display:flex;align-items:center;gap:12px;
}
.h-logo{
  font-size:18px;font-weight:700;letter-spacing:.5px;
  color:var(--white);
  display:flex;align-items:center;gap:2px;
}
.h-logo-fii{color:var(--teal)}
.h-logo-sep{color:var(--text3);margin:0 2px}
.h-logo-dii{color:var(--blue)}
.h-tagline{
  font-size:10px;color:var(--text3);margin-top:2px;
  font-weight:400;letter-spacing:.3px;
}

.h-nav{display:flex;align-items:stretch}
.h-tab{
  padding:0 20px;
  display:flex;align-items:center;
  font-size:11px;font-weight:600;letter-spacing:.5px;text-transform:uppercase;
  color:var(--text3);
  cursor:pointer;transition:all .15s;white-space:nowrap;
  position:relative;text-decoration:none;border-bottom:2px solid transparent;
}
.h-tab:hover{color:var(--text);background:rgba(255,255,255,.03)}
.h-tab.active{color:var(--teal);border-bottom-color:var(--teal)}

.h-meta{display:flex;align-items:center;border-left:1px solid var(--border)}
.h-meta-item{
  padding:10px 16px;border-right:1px solid var(--border);
  display:flex;flex-direction:column;gap:2px;
}
.h-meta-label{font-size:9px;letter-spacing:1.5px;color:var(--text3);text-transform:uppercase;font-weight:500}
.h-meta-val{font-size:12px;font-weight:600;color:var(--white);font-family:'DM Mono',monospace}

.h-live{
  padding:10px 20px;
  display:flex;align-items:center;gap:8px;
  font-size:10px;font-weight:700;letter-spacing:1px;color:var(--teal);
}
.led{
  width:7px;height:7px;border-radius:50%;background:var(--teal);
  box-shadow:0 0 6px var(--teal),0 0 12px rgba(16,185,129,.4);
  animation:blink 2s ease-in-out infinite;
}
@keyframes blink{0%,100%{opacity:1}50%{opacity:.25}}

/* ── TICKER ── */
.ticker-wrap{
  background:rgba(0,0,0,.3);
  border-bottom:1px solid var(--border);
  height:28px;overflow:hidden;display:flex;align-items:center;
}
.ticker-inner{
  display:inline-flex;white-space:nowrap;
  animation:scroll-ticker 60s linear infinite;
}
@keyframes scroll-ticker{from{transform:translateX(0)}to{transform:translateX(-50%)}}
.t-item{
  display:inline-flex;align-items:center;gap:8px;
  padding:0 24px;font-size:11px;
  border-right:1px solid var(--border);color:var(--text3);
}
.t-sym{color:var(--text2);font-weight:600;font-size:10px;letter-spacing:.5px}
.t-val{font-weight:700;font-family:'DM Mono',monospace}
.t-val.up{color:var(--teal)}
.t-val.dn{color:var(--red)}
.t-extra{color:var(--text3);font-size:10px}

/* ── STATS BAR ── */
.stats-bar{
  display:grid;grid-template-columns:repeat(6,1fr);
  background:var(--surface);
  border-bottom:1px solid var(--border);
  padding:0;
}
.stat{
  padding:16px 20px;
  border-right:1px solid var(--border);
  transition:background .2s;
}
.stat:hover{background:var(--surface2)}
.stat-lbl{
  font-size:9px;letter-spacing:1.5px;color:var(--text3);
  text-transform:uppercase;margin-bottom:6px;font-weight:600;
}
.stat-val{
  font-size:26px;font-weight:700;color:var(--white);
  font-variant-numeric:tabular-nums;line-height:1;
  font-family:'DM Mono',monospace;
}
.stat-val.teal{color:var(--teal)}
.stat-val.red{color:var(--red)}
.stat-chg{font-size:11px;font-weight:500;margin-top:4px;display:flex;align-items:center;gap:4px}
.stat-chg.up{color:var(--teal)}
.stat-chg.dn{color:var(--red)}
.stat-chg.neu{color:var(--text3)}

/* ── LAYOUT ── */
.main{display:grid;grid-template-columns:240px 1fr;flex:1}

/* ── SIDEBAR ── */
.sidebar{
  background:var(--surface);
  border-right:1px solid var(--border);
  display:flex;flex-direction:column;
  overflow-y:auto;
}
.sb-section{padding-bottom:8px;border-bottom:1px solid var(--border)}
.sb-title{
  font-size:9px;letter-spacing:2px;font-weight:700;
  color:var(--text3);text-transform:uppercase;
  padding:12px 16px 8px;
}

.sb-item{
  padding:8px 16px;
  display:flex;justify-content:space-between;align-items:center;
  cursor:pointer;
  border-left:2px solid transparent;
  transition:all .15s;font-size:12px;
  text-decoration:none;color:inherit;
}
.sb-item:hover{background:var(--surface2);border-left-color:var(--teal)}
.sb-item-name{color:var(--text);font-weight:500}
.sb-item-count{font-size:10px;color:var(--text3);margin-top:2px}
.sb-item-sig{
  font-size:9px;font-weight:700;padding:2px 8px;border-radius:20px;
  letter-spacing:.3px;white-space:nowrap;
}
.sb-item-sig.buy {color:var(--teal);background:var(--teal-dim);border:1px solid rgba(16,185,129,.25)}
.sb-item-sig.sell{color:var(--red);background:var(--red-dim);border:1px solid rgba(239,68,68,.25)}
.sb-item-sig.hold{color:var(--text2);background:rgba(255,255,255,.05);border:1px solid var(--border2)}

.sb-legend{padding:10px 16px}
.sb-leg-item{display:flex;align-items:center;gap:8px;font-size:11px;color:var(--text3);padding:3px 0}
.sb-leg-dot{width:8px;height:8px;border-radius:2px;flex-shrink:0}

/* ── CONTENT ── */
.content{overflow:auto;display:flex;flex-direction:column;gap:0}

.content-hdr{
  padding:14px 20px;
  background:var(--surface);
  border-bottom:1px solid var(--border);
  display:flex;align-items:center;gap:12px;flex-wrap:wrap;
}
.content-hdr-title{
  font-size:12px;font-weight:600;color:var(--text2);
  display:flex;align-items:center;gap:8px;
}
.content-hdr-range{
  font-size:11px;color:var(--text3);
  background:var(--surface2);border:1px solid var(--border2);
  padding:3px 12px;border-radius:20px;
}
.content-hdr-src{font-size:10px;color:var(--text3);margin-left:auto}

/* ── SECTOR CARDS ── */
.cards-wrap{
  padding:16px;
  display:flex;flex-direction:column;gap:16px;
}

.sector-card{
  background:var(--surface);
  border:1px solid var(--border2);
  border-radius:var(--radius);
  overflow:hidden;
  transition:box-shadow .2s;
}
.sector-card:hover{
  box-shadow:0 0 0 1px rgba(16,185,129,.15),0 8px 32px rgba(0,0,0,.3);
}

.sec-card-hdr{
  display:flex;align-items:center;justify-content:space-between;
  padding:12px 18px;
  background:var(--surface2);
  border-bottom:1px solid var(--border);
}
.sec-card-left{display:flex;align-items:center;gap:10px}
.sec-icon{font-size:16px}
.sec-card-name{
  font-size:14px;font-weight:700;color:var(--white);
}
.sec-count-badge{
  font-size:10px;color:var(--text3);
  background:rgba(255,255,255,.06);
  border:1px solid var(--border2);
  padding:2px 10px;border-radius:20px;font-weight:500;
}
.sec-hdr-pills{display:flex;gap:6px;flex-wrap:wrap}
.hdr-pill{
  font-size:10px;font-weight:700;padding:3px 10px;border-radius:20px;
}
.hdr-pill.sb  {color:var(--teal);background:var(--teal-dim);border:1px solid rgba(16,185,129,.25)}
.hdr-pill.buy {color:var(--blue);background:var(--blue-dim);border:1px solid rgba(59,130,246,.25)}
.hdr-pill.sell{color:var(--red); background:var(--red-dim); border:1px solid rgba(239,68,68,.25)}

/* ── SECTOR TABLE ── */
.sec-table-wrap{overflow-x:auto}
.sec-table{
  width:100%;border-collapse:collapse;
  font-size:12px;min-width:840px;
}

.sec-table thead tr{
  background:rgba(0,0,0,.2);
  border-bottom:1px solid var(--border);
}
.sec-table th{
  padding:8px 14px;font-size:9px;font-weight:700;letter-spacing:1.5px;
  color:var(--text3);text-transform:uppercase;text-align:left;white-space:nowrap;
}
.th-c{text-align:center}
.th-r{text-align:right}

/* Data rows */
.stock-row{
  border-bottom:1px solid rgba(255,255,255,.04);
  transition:background .12s;
  animation:row-in .3s ease both;
}
@keyframes row-in{from{opacity:0;transform:translateY(4px)}to{opacity:1;transform:translateY(0)}}
.stock-row:hover{background:rgba(255,255,255,.03)}
.stock-row:last-child{border-bottom:none}

.sec-table td{padding:12px 14px;vertical-align:middle;text-align:left}
.td-c{text-align:center}
.td-r{text-align:right}
.td-stock{min-width:160px}

.stock-name{
  font-size:13px;font-weight:700;color:var(--white);letter-spacing:.2px;line-height:1.2;
}
.stock-sym{
  font-size:10px;color:var(--text3);margin-top:2px;
  letter-spacing:.5px;font-family:'DM Mono',monospace;
}

/* Price */
.price-val{
  font-size:14px;font-weight:700;font-variant-numeric:tabular-nums;
  font-family:'DM Mono',monospace;
}
.price-up{color:var(--teal)}
.price-dn{color:var(--red)}
.spark-wrap{margin-top:5px;display:flex;justify-content:flex-end}

/* RSI */
.rsi-badge{
  font-size:15px;font-weight:700;font-variant-numeric:tabular-nums;
  font-family:'DM Mono',monospace;display:inline-block;
}
.rsi-badge.rsi-hot {color:var(--red)}
.rsi-badge.rsi-warm{color:var(--yellow)}
.rsi-badge.rsi-cold{color:var(--teal)}
.rsi-track{
  width:72px;height:3px;background:rgba(255,255,255,.08);
  border-radius:2px;margin:5px auto 0;overflow:hidden;
}
.rsi-fill{height:100%;border-radius:2px;transition:width .3s}
.rsi-fill.rsi-hot {background:var(--red)}
.rsi-fill.rsi-warm{background:var(--yellow)}
.rsi-fill.rsi-cold{background:var(--teal)}

/* S/R Levels */
.sr-grid{font-size:10px;line-height:1.9;font-variant-numeric:tabular-nums;font-family:'DM Mono',monospace}
.sr-row{display:flex;align-items:center;gap:5px;justify-content:center}
.sr-tag{
  font-size:8px;font-weight:700;padding:0 4px;border-radius:3px;min-width:26px;text-align:center;
}
.sr-tag.r{background:var(--red-dim);color:var(--red)}
.sr-tag.s{background:var(--teal-dim);color:var(--teal)}
.sr-val.r{color:var(--red)}
.sr-val.s{color:var(--teal)}

/* MACD / EMA */
.macd-val{font-size:13px;font-weight:700;font-variant-numeric:tabular-nums;font-family:'DM Mono',monospace}
.macd-pos{color:var(--teal)}
.macd-neg{color:var(--red)}
.ema-val{margin-top:4px}
.ema-bull{
  font-size:9px;font-weight:700;padding:2px 8px;border-radius:20px;
  color:var(--teal);background:var(--teal-dim);border:1px solid rgba(16,185,129,.25);
}
.ema-bear{
  font-size:9px;font-weight:700;padding:2px 8px;border-radius:20px;
  color:var(--red);background:var(--red-dim);border:1px solid rgba(239,68,68,.25);
}

/* Flow badges */
.flow-row{display:flex;gap:4px;justify-content:center;flex-wrap:wrap}
.flow-badge{
  font-size:9px;font-weight:700;padding:3px 8px;border-radius:20px;
  letter-spacing:.3px;border:1px solid;white-space:nowrap;
}
.fii-b    {color:var(--teal);border-color:rgba(16,185,129,.35);background:var(--teal-dim)}
.fii-s    {color:var(--red); border-color:rgba(239,68,68,.35); background:var(--red-dim)}
.dii-b    {color:var(--blue);border-color:rgba(59,130,246,.35);background:var(--blue-dim)}
.dii-s    {color:#f87171;    border-color:rgba(248,113,113,.3);background:rgba(248,113,113,.08)}
.flow-neutral{color:var(--text3);border-color:var(--border2);background:transparent}

/* Signal pills */
.sig-pill{
  display:inline-block;padding:5px 12px;border-radius:20px;
  font-size:10px;font-weight:700;letter-spacing:.3px;
  white-space:nowrap;border:1px solid;
}
.sig-sb{
  background:var(--teal-dim);color:var(--teal2);border-color:rgba(16,185,129,.4);
  box-shadow:0 0 12px rgba(16,185,129,.12);
}
.sig-buy{
  background:var(--blue-dim);color:var(--blue);border-color:rgba(59,130,246,.35);
}
.sig-neutral{background:rgba(255,255,255,.04);color:var(--text3);border-color:var(--border2)}
.sig-caution{
  background:var(--yel-dim);color:var(--yellow);border-color:rgba(245,158,11,.35);
}
.sig-sell{
  background:var(--red-dim);color:var(--red);border-color:rgba(239,68,68,.4);
}
.sig-blk{
  background:rgba(139,92,246,.1);color:var(--purple);border-color:rgba(139,92,246,.3);
}

/* ── FOOTER ── */
footer{
  background:var(--surface);border-top:1px solid var(--border);
  padding:12px 20px;
  display:flex;justify-content:space-between;align-items:center;
  font-size:11px;color:var(--text3);
  flex-wrap:wrap;gap:8px;
}
.footer-brand{color:var(--white);font-weight:700}
.footer-warn{color:var(--yellow)}

/* ── STATUS BAR ── */
.status-bar{
  background:rgba(0,0,0,.4);border-top:1px solid var(--border);
  padding:4px 20px;display:flex;gap:20px;align-items:center;
  font-size:10px;color:var(--text3);flex-wrap:wrap;
}
.status-item{display:flex;align-items:center;gap:5px}
.status-dot{width:5px;height:5px;border-radius:50%}
.status-dot.ok  {background:var(--teal);box-shadow:0 0 4px rgba(16,185,129,.5)}
.status-dot.warn{background:var(--yellow)}
.status-dot.err {background:var(--red)}
.status-ts{margin-left:auto;color:var(--text2);font-weight:600;font-family:'DM Mono',monospace;font-size:10px}

/* ── RESPONSIVE ── */
@media(max-width:1100px){
  .main{grid-template-columns:200px 1fr}
  .stat-val{font-size:22px}
  .stats-bar{grid-template-columns:repeat(3,1fr)}
}
@media(max-width:860px){
  .main{grid-template-columns:1fr}
  .sidebar{display:none}
  .stats-bar{grid-template-columns:repeat(3,1fr)}
  header{grid-template-columns:1fr auto}
  .h-nav{display:none}
}
@media(max-width:600px){
  .stats-bar{grid-template-columns:repeat(2,1fr)}
  .cards-wrap{padding:8px}
  .h-brand{padding:10px 14px}
  footer{font-size:10px}
}
"""

    # ══════════════════════════════════════════════════════════════════════════
    #  HTML TEMPLATE — Stealth Slate
    # ══════════════════════════════════════════════════════════════════════════
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>FII/DII Pulse &mdash; Institutional Intelligence &mdash; {date_str}</title>
<style>{css}</style>
</head>
<body>
<div class="w">

<!-- ═══ HEADER ═══ -->
<header>
  <div class="h-brand">
    <div>
      <div class="h-logo">
        <span class="h-logo-fii">FII</span>
        <span class="h-logo-sep">/</span>
        <span class="h-logo-dii">DII</span>
      </div>
      <div class="h-tagline">Institutional Intelligence Dashboard &middot; Stealth Slate &middot; NSE Bulk/Block CSV API &middot; v8</div>
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
    <div class="h-live">
      <div class="led"></div>Live
    </div>
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
    <div class="stat-val teal">{fb}</div>
    <div class="stat-chg up">▲ Active</div>
  </div>
  <div class="stat">
    <div class="stat-lbl">DII Buy</div>
    <div class="stat-val teal">{db}</div>
    <div class="stat-chg up">▲ Active</div>
  </div>
  <div class="stat">
    <div class="stat-lbl">Strong Buy</div>
    <div class="stat-val teal">{st}</div>
    <div class="stat-chg up">⚡ Signals</div>
  </div>
</div>

<!-- ═══ MAIN ═══ -->
<div class="main">

  <!-- SIDEBAR -->
  <div class="sidebar">
    <div class="sb-section">
      <div class="sb-title">Sectors</div>
      {sidebar_items}
    </div>
    <div class="sb-section">
      <div class="sb-title">Signal Guide</div>
      <div class="sb-legend">
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--teal)"></div>⚡ Strong Buy</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--blue)"></div>▲ Buy</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--purple)"></div>■ Bulk/Block</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--text3)"></div>— Neutral</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--yellow)"></div>⚠ Caution</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--red)"></div>▼ Sell</div>
      </div>
    </div>
    <div class="sb-section">
      <div class="sb-title">RSI Guide</div>
      <div class="sb-legend">
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--red)"></div>&gt;70 Overbought</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--yellow)"></div>40–70 Mid zone</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--teal)"></div>&lt;40 Oversold</div>
      </div>
    </div>
    <div class="sb-section">
      <div class="sb-title">Flow Key</div>
      <div class="sb-legend">
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--teal)"></div>FII Buying</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--blue)"></div>DII Buying</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--red)"></div>Selling</div>
        <div class="sb-leg-item"><div class="sb-leg-dot" style="background:var(--text3)"></div>No activity</div>
      </div>
    </div>
  </div>

  <!-- CONTENT -->
  <div class="content">
    <div class="content-hdr">
      <div class="content-hdr-title">
        Sector-wise Institutional Flow &mdash; Strong Buy &rarr; Sell
      </div>
      {f'<span class="content-hdr-range">📅 {date_range_label}</span>' if date_range_label else ''}
      <div class="content-hdr-src">📡 {source} &middot; yfinance technicals</div>
    </div>

    <div class="cards-wrap">
      {sector_cards}
    </div>
  </div><!-- /content -->

</div><!-- /main -->

<!-- ═══ FOOTER ═══ -->
<footer>
  <div>
    <span class="footer-brand">FII/DII PULSE</span>
    &middot; Stealth Slate &middot; v8 &middot; {source} &middot; {date_str}
  </div>
  <div>Sorted: Strong Buy &rarr; Buy &rarr; Neutral &rarr; Caution &rarr; Sell</div>
  <div class="footer-warn">⚠ NOT FINANCIAL ADVICE &middot; EDUCATIONAL ONLY &middot; DYOR</div>
</footer>

<!-- ═══ STATUS BAR ═══ -->
<div class="status-bar">
  <div class="status-item"><div class="status-dot ok"></div>NSE CSV API: OK</div>
  <div class="status-item"><div class="status-dot ok"></div>yfinance: OK</div>
  <div class="status-item"><div class="status-dot ok"></div>{len(stocks)} stocks loaded</div>
  <div class="status-item"><div class="status-dot ok"></div>Technicals computed</div>
  <div class="status-ts">LAST UPDATE: {now_ist}</div>
</div>

</div><!-- /w -->
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
#  EMAIL
# ─────────────────────────────────────────────────────────────────────────────

def send_email(html_path: Path, date_str: str, source: str,
               count: int, date_range_label: str = ""):
    user  = os.getenv("GMAIL_USER", "").strip()
    pwd   = os.getenv("GMAIL_PASS", "").strip()
    rcpts = os.getenv("RECIPIENT_EMAIL", user).strip()

    if not user or not pwd:
        log.warning("⚠️  GMAIL_USER / GMAIL_PASS not set — skipping email")
        return

    to_list = [r.strip() for r in rcpts.split(",") if r.strip()]
    log.info(f"📧 Sending full HTML dashboard to: {to_list}")

    full_html = html_path.read_text(encoding="utf-8")

    msg            = MIMEMultipart("alternative")
    msg["Subject"] = f"📊 FII/DII Pulse · Stealth Slate — {date_str}"
    msg["From"]    = f"FII/DII Pulse <{user}>"
    msg["To"]      = ", ".join(to_list)

    plain = (
        f"FII/DII Pulse — Stealth Slate Theme\n"
        f"Date: {date_str}\n"
        f"Source: {source}\n"
        f"Date range: {date_range_label}\n"
        f"Stocks tracked: {count}\n\n"
        f"Please open this email in an HTML-capable client to view the full dashboard.\n"
        f"Not financial advice. Educational purposes only."
    )
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(full_html, "html", "utf-8"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465, timeout=30) as srv:
            srv.login(user, pwd)
            srv.sendmail(user, to_list, msg.as_string())
        log.info(f"  ✅ Full HTML dashboard emailed to {to_list}")
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
    log.info(f"  📊 FII/DII Pulse v8 Stealth Slate — {date_str}  (IST: {now_ist.strftime('%H:%M')})")
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
