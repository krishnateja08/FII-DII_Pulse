"""
FII/DII Intelligence Dashboard — v8 (CSV Edition) · Jade Garden Theme
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

THEME: Jade Garden — Sage green + white, clean modern fintech
  (Only HTML/CSS output changed; all logic is identical to v8 CSV Edition)
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
    """
    Returns (from_date, to_date, label).
    to_date  = today if past 18:30 IST, else last completed trading day.
    from_date = exactly 5 trading days before to_date (6-day window).
    """
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
#  SOURCE 1 — NSE CSV Download API  (★ KEY CHANGE — full data, no 50-row cap)
# ─────────────────────────────────────────────────────────────────────────────

def fetch_from_nse() -> list:
    """
    Fetch NSE bulk + block deals via the CSV download endpoint.
    Adding &csv=true to the historicalOR API triggers the full CSV export —
    the same file produced by clicking "Download (.csv)" on the NSE website.

    This removes the 50-record cap that the plain JSON API enforces.
    """
    log.info("[Source 1] NSE Bulk/Block Deals — CSV Download API (no 50-row cap)...")

    try:
        from_date, to_date, date_range_label = get_date_range()
        from_str = fmt_nse_date(from_date)
        to_str   = fmt_nse_date(to_date)
        log.info(f"  -> Range: {from_str} to {to_str}")

        # ── Two endpoints: bulk + block ────────────────────────────────────
        csv_endpoints = [
            {
                "url": "https://www.nseindia.com/api/historicalOR/bulk-block-short-deals",
                "params": {
                    "optionType": "bulk_deals",
                    "from": from_str,
                    "to":   to_str,
                    "csv":  "true",          # ← triggers full CSV export
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

        # ── Session: curl_cffi preferred (bypasses Akamai bot check) ──────
        session_obj = None
        use_cffi    = False

        try:
            from curl_cffi import requests as cffi_req
            log.info("  -> Using curl_cffi Chrome120 (Akamai bypass)")
            session_obj = cffi_req.Session(impersonate="chrome120")
            session_obj.get("https://www.nseindia.com/", timeout=15)
            time.sleep(2)
            # Seed the referrer page — critical for the CSV download to work
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

        # Headers that mimic clicking the "Download (.csv)" button
        csv_req_headers = {
            "Referer": "https://www.nseindia.com/report-detail/display-bulk-and-block-deals",
            "Accept":  "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }

        # ── Fetch each endpoint ────────────────────────────────────────────
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

                    # ── Try CSV parse first ────────────────────────────────
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

                    # ── JSON fallback (NSE occasionally returns JSON even with csv=true) ──
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

            time.sleep(1.5)   # polite pause between endpoints

        # ── Cleanup ───────────────────────────────────────────────────────
        if use_cffi and hasattr(session_obj, "close"):
            try: session_obj.close()
            except Exception: pass

        if not all_dfs:
            log.warning("  !! No CSV data from any endpoint — falling back")
            return []

        # ── Merge all deal types ───────────────────────────────────────────
        df = pd.concat(all_dfs, ignore_index=True)
        log.info(f"  -> Combined: {df.shape[0]} rows from {len(all_dfs)} endpoint(s)")
        df.columns = [str(c).strip() for c in df.columns]
        log.info(f"  -> Raw columns: {list(df.columns)}")

        # ── Normalise column names ─────────────────────────────────────────
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
#  HTML HELPERS
# ─────────────────────────────────────────────────────────────────────────────

def spark_svg(prices):
    if len(prices) < 2:
        return ""
    mn, mx = min(prices), max(prices)
    rng = mx - mn or 1
    w, h = 70, 26
    pts = [
        f"{round(i*w/(len(prices)-1),1)},{round(h-(p-mn)/rng*h,1)}"
        for i, p in enumerate(prices)
    ]
    # Jade Garden: use sage green for up, soft coral for down
    col = "#3d9970" if prices[-1] >= prices[0] else "#e05c5c"
    return (
        f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
        f'xmlns="http://www.w3.org/2000/svg">'
        f'<polyline points="{" ".join(pts)}" fill="none" stroke="{col}" '
        f'stroke-width="1.8" stroke-linejoin="round"/></svg>'
    )


def sigcls(s):
    return {"STRONG BUY":"sbs","BUY":"sbuy","NEUTRAL":"sna",
            "CAUTION":"sca","SELL":"sse","N/A":"sna",
            "BOTH BUY":"sbs","FII BUY":"sbuy","DII BUY":"sdii",
            "BOTH SELL":"sse","BULK/BLOCK":"sblk"}.get(s, "sna")


def rsicls(v):
    return "ro" if v > 70 else ("rs2" if v < 40 else "rn2")


def fmt(v):
    return f"₹{v:,.2f}" if v else "N/A"


# ─────────────────────────────────────────────────────────────────────────────
#  SECTOR MAP & ICONS  (unchanged)
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
    "Banking & Finance":        "🏦",
    "NBFC & Fintech":           "💳",
    "IT & Technology":          "💻",
    "Pharma & Healthcare":      "💊",
    "Oil, Gas & Energy":        "⛽",
    "Power & Utilities":        "⚡",
    "Metals & Mining":          "⚙️",
    "Auto & Auto Ancillaries":  "🚗",
    "FMCG & Consumer":          "🛒",
    "Cement & Construction":    "🏗️",
    "Real Estate":              "🏢",
    "Capital Goods & Industrials": "🏭",
    "Telecom & Media":          "📡",
    "Chemicals & Specialty":    "🧪",
    "Insurance":                "🛡️",
    "Exchange & Capital Markets":"📈",
    "Aviation & Logistics":     "✈️",
    "Retail & E-Commerce":      "🛍️",
    "Agri & Fertilisers":       "🌾",
    "Others":                   "🔷",
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
#  GENERATE HTML  —  🌿 JADE GARDEN THEME
#  Only this function's CSS string and structural HTML skin has changed.
#  All data-building logic, sector grouping, sorting, row generation = identical.
# ─────────────────────────────────────────────────────────────────────────────

def generate_html(stocks, market, date_str, source, date_range_label="") -> str:
    nc = "up" if market["nifty_chg"]  >= 0 else "dn"
    xc = "up" if market["sensex_chg"] >= 0 else "dn"
    na = "▲"  if market["nifty_chg"]  >= 0 else "▼"
    xa = "▲"  if market["sensex_chg"] >= 0 else "▼"
    fb = sum(1 for s in stocks if s["fii_cash"] == "buy")
    db = sum(1 for s in stocks if s["dii_cash"] == "buy")
    bb = sum(1 for s in stocks if s["both_buy"])
    st = sum(1 for s in stocks if s["overall"] == "STRONG BUY")

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
        return min(SIGNAL_ORDER.get(s.get("overall","N/A"), 5) for s in items)

    sorted_sectors = sorted(sector_groups.items(), key=lambda kv: sector_best(kv[1]))

    # ── Build table rows ──────────────────────────────────────────────────
    rows = ""
    row_idx = 0
    for sector_name, sec_stocks in sorted_sectors:
        icon      = SECTOR_ICONS.get(sector_name, "🔷")
        sec_count = len(sec_stocks)
        sec_sb    = sum(1 for s in sec_stocks if s["overall"] == "STRONG BUY")
        sec_buy   = sum(1 for s in sec_stocks if s["overall"] == "BUY")
        sec_sell  = sum(1 for s in sec_stocks if s["overall"] in ("SELL","BOTH SELL"))

        rows += f"""
    <tr class="sec-hdr">
      <td colspan="5">
        <div class="sec-hdr-inner">
          <span class="sec-icon">{icon}</span>
          <span class="sec-name">{sector_name}</span>
          <span class="sec-count">{sec_count} stocks</span>
          <span class="sec-pills">"""
        if sec_sb:
            rows += f'<span class="sec-pill sbs">⚡ {sec_sb} STRONG BUY</span>'
        if sec_buy:
            rows += f'<span class="sec-pill sbuy">▲ {sec_buy} BUY</span>'
        if sec_sell:
            rows += f'<span class="sec-pill sse">▼ {sec_sell} SELL</span>'
        rows += """</span>
        </div>
      </td>
    </tr>"""

        for s in sec_stocks:
            spk = spark_svg(s.get("sparkline", []))
            pr  = fmt(s["last_price"]) if s["last_price"] > 0 else s.get("price_str","N/A")

            rows += f"""
    <tr style="animation-delay:{row_idx*0.04:.2f}s">
      <td data-label="Stock">
        <div class="sn">{s['name']}</div>
        <div class="sy">{s['symbol'].replace('.NS','')}</div>
      </td>
      <td data-label="Price">
        <div class="pv">{pr}</div>
        <div class="sp">{spk}</div>
      </td>
      <td data-label="RSI(14)" class="{rsicls(s['rsi'])}">
        <div class="rv {'up' if s['rsi']<55 else 'dn'}">{s['rsi']}</div>
        <div class="rb2"><div class="rf2" style="width:{min(s['rsi'],100):.0f}%"></div></div>
      </td>
      <td data-label="Levels">
        <div class="sr"><span class="sr-r">R1</span> {fmt(s['resist1'])}</div>
        <div class="sr"><span class="sr-s">S1</span> {fmt(s['support1'])}</div>
        <div class="sr"><span class="sr-r">6mH</span> {fmt(s['swing_high'])}</div>
        <div class="sr"><span class="sr-s">6mL</span> {fmt(s['swing_low'])}</div>
      </td>
      <td data-label="Signal"><span class="sig {sigcls(s['overall'])}">{s['overall']}</span></td>
    </tr>"""
            row_idx += 1

    # ════════════════════════════════════════════════════════════════════════
    #  🌿 JADE GARDEN CSS  — Light, clean, sage-green fintech aesthetic
    #  Palette:
    #    --jade     #2d6a4f  (deep jade green — primary brand)
    #    --sage     #3d9970  (medium sage — accents, buy signals)
    #    --mint     #52b788  (light mint — hover, subtle highlights)
    #    --foam     #d8f3dc  (pale green foam — backgrounds, tags)
    #    --ivory    #f8faf8  (near-white page background)
    #    --paper    #ffffff  (card/table background)
    #    --ink      #1b3a2d  (dark green-tinted text)
    #    --mist     #6c9b80  (muted secondary text)
    #    --coral    #e05c5c  (sell / negative — warm coral, not harsh red)
    #    --sand     #f4a261  (caution / DII — warm amber)
    #    --sky      #457b9d  (neutral / bulk-block — calm blue)
    #    --border   #c8e6c9  (subtle green-tinted border)
    # ════════════════════════════════════════════════════════════════════════
    css = """
@import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

:root{
  --jade:#2d6a4f;
  --jade2:#1b4332;
  --sage:#3d9970;
  --mint:#52b788;
  --foam:#d8f3dc;
  --foam2:#b7e4c7;
  --ivory:#f4f9f5;
  --paper:#ffffff;
  --ink:#1b3a2d;
  --mist:#5a8a6e;
  --coral:#d64045;
  --sand:#e98a4e;
  --sky:#457b9d;
  --border:#c8e6c9;
  --border2:#e8f5e9;
  --shadow:0 1px 3px rgba(45,106,79,.08),0 4px 16px rgba(45,106,79,.06);
  --shadow-sm:0 1px 4px rgba(45,106,79,.1);
}

*{margin:0;padding:0;box-sizing:border-box}
html{scroll-behavior:smooth}

body{
  background:var(--ivory);
  color:var(--ink);
  font-family:'DM Sans',system-ui,sans-serif;
  min-height:100vh;
  line-height:1.5;
}

/* Subtle leaf-vein texture overlay */
body::before{
  content:'';
  position:fixed;inset:0;
  background-image:
    radial-gradient(ellipse 800px 600px at 10% 20%,rgba(82,183,136,.06),transparent),
    radial-gradient(ellipse 600px 800px at 90% 80%,rgba(45,106,79,.04),transparent);
  pointer-events:none;z-index:0;
}

.w{position:relative;z-index:1}

/* ── HEADER ── */
header{
  background:var(--jade2);
  border-bottom:3px solid var(--sage);
  padding:14px 24px;
  display:flex;align-items:center;justify-content:space-between;
  flex-wrap:wrap;gap:10px;
  position:sticky;top:0;z-index:99;
  box-shadow:0 2px 12px rgba(27,67,50,.25);
}

.lg{display:flex;align-items:center;gap:14px}

.logo-icon{
  width:42px;height:42px;min-width:42px;
  background:linear-gradient(135deg,var(--sage),var(--mint));
  border-radius:12px;
  display:flex;align-items:center;justify-content:center;
  font-size:20px;
  box-shadow:0 2px 10px rgba(82,183,136,.35);
}

.lt{font-size:17px;font-weight:700;letter-spacing:.3px;color:#fff}
.lt span{color:var(--mint)}
.ls2{font-size:9px;color:rgba(255,255,255,.45);letter-spacing:1.5px;margin-top:2px;text-transform:uppercase}

.hm{display:flex;align-items:center;gap:8px;flex-wrap:wrap}

.lv{
  display:flex;align-items:center;gap:6px;
  background:rgba(82,183,136,.2);
  border:1px solid rgba(82,183,136,.4);
  padding:4px 12px;border-radius:20px;
  font-size:10px;font-weight:600;color:var(--mint);letter-spacing:1px;
}
.led{
  width:7px;height:7px;border-radius:50%;
  background:var(--mint);
  animation:pulse 1.8s infinite;
}
@keyframes pulse{0%,100%{opacity:1;transform:scale(1)}50%{opacity:.3;transform:scale(.7)}}

.src{
  font-size:10px;color:rgba(255,255,255,.65);
  background:rgba(255,255,255,.08);
  border:1px solid rgba(255,255,255,.15);
  padding:4px 12px;border-radius:20px;
}

.drb{
  font-size:10px;color:var(--mint);
  background:rgba(82,183,136,.15);
  border:1px solid rgba(82,183,136,.3);
  padding:4px 12px;border-radius:20px;
  font-family:'DM Mono',monospace;
}

.dt{font-family:'DM Mono',monospace;font-size:10px;color:rgba(255,255,255,.4)}

/* ── SUMMARY STRIP ── */
.sum{
  display:grid;grid-template-columns:repeat(4,1fr);
  gap:1px;
  background:var(--border);
  border-bottom:1px solid var(--border);
}

.sc2{
  background:var(--paper);
  padding:16px 20px;
  transition:background .2s;
}
.sc2:hover{background:var(--foam2)}

.sl{
  font-size:9px;letter-spacing:2px;color:var(--mist);
  text-transform:uppercase;margin-bottom:6px;font-weight:500;
}

.sv{
  font-family:'DM Mono',monospace;
  font-size:20px;font-weight:700;
  color:var(--ink);
}

.sb2{font-size:10px;color:var(--mist);margin-top:4px;font-weight:500}

.up{color:var(--sage)!important}
.dn{color:var(--coral)!important}

/* ── SECTOR NAV ── */
.snav{
  padding:10px 24px;
  display:flex;gap:8px;flex-wrap:wrap;
  background:var(--paper);
  border-bottom:1px solid var(--border);
  position:sticky;top:70px;z-index:90;
  box-shadow:var(--shadow-sm);
  overflow-x:auto;
}

.snav-link{
  font-size:10px;font-weight:600;
  color:var(--jade);
  text-decoration:none;
  padding:4px 12px;border-radius:20px;
  border:1px solid var(--border);
  background:var(--foam);
  white-space:nowrap;
  transition:all .2s;
}
.snav-link:hover{
  background:var(--jade);color:#fff;
  border-color:var(--jade);
}

/* ── TABLE WRAPPER ── */
.tw{padding:20px 24px;overflow-x:auto;-webkit-overflow-scrolling:touch}

.tt{
  font-size:10px;letter-spacing:1.5px;text-transform:uppercase;
  color:var(--mist);margin-bottom:14px;
  display:flex;align-items:center;gap:10px;flex-wrap:wrap;font-weight:600;
}
.tt::before{
  content:'';width:3px;height:14px;
  background:linear-gradient(var(--sage),var(--mint));
  border-radius:2px;flex-shrink:0;
}

/* ── TABLE ── */
table{
  width:100%;border-collapse:separate;border-spacing:0;
  min-width:860px;
  background:var(--paper);
  border-radius:16px;
  border:1px solid var(--border);
  overflow:hidden;
  box-shadow:var(--shadow);
}

thead tr{border-bottom:2px solid var(--border)}

th{
  padding:11px 14px;text-align:left;
  font-size:9px;letter-spacing:1.5px;
  color:var(--mist);text-transform:uppercase;font-weight:700;
  white-space:nowrap;background:var(--foam);
}
th:not(:first-child){text-align:center}

tbody tr:not(.sec-hdr){
  border-bottom:1px solid var(--border2);
  animation:si .35s ease both;
  opacity:0;
  transition:background .15s;
}

@keyframes si{
  from{opacity:0;transform:translateY(4px)}
  to{opacity:1;transform:translateY(0)}
}

tbody tr:not(.sec-hdr):hover{background:var(--foam)}

td{
  padding:12px 14px;font-size:12px;
  vertical-align:middle;text-align:center;
  color:var(--ink);
}
td:first-child{text-align:left}

.sn{font-weight:700;font-size:13px;line-height:1.3;color:var(--ink)}
.sy{
  font-size:9px;color:var(--mist);
  font-family:'DM Mono',monospace;margin-top:2px;
  letter-spacing:.5px;
}

.pv{
  font-family:'DM Mono',monospace;
  font-size:13px;font-weight:700;color:var(--ink);
}
.sp{margin-top:5px}

/* RSI */
.rv{font-family:'DM Mono',monospace;font-size:13px;font-weight:700}
.rb2{
  width:78px;height:4px;
  background:var(--border);border-radius:2px;
  margin:5px auto 0;overflow:hidden;
}
.rf2{height:100%;border-radius:2px}
.ro .rf2{background:var(--coral)}
.rn2 .rf2{background:var(--sand)}
.rs2 .rf2{background:var(--sage)}

/* Levels */
.sr{
  font-size:10px;font-family:'DM Mono',monospace;
  display:flex;align-items:center;gap:5px;
  justify-content:center;margin-bottom:3px;
}
.sr-r{
  font-size:8px;padding:1px 5px;border-radius:4px;font-weight:700;
  background:rgba(214,64,69,.1);color:var(--coral);
}
.sr-s{
  font-size:8px;padding:1px 5px;border-radius:4px;font-weight:700;
  background:rgba(61,153,112,.12);color:var(--sage);
}

/* ── SECTOR HEADER ROWS ── */
tr.sec-hdr td{
  background:linear-gradient(90deg,var(--foam),rgba(216,243,220,.4),transparent);
  border-top:2px solid var(--border);
  border-bottom:1px solid var(--foam2);
  padding:10px 14px;
}
.sec-hdr-inner{display:flex;align-items:center;gap:10px;flex-wrap:wrap}
.sec-icon{font-size:16px}
.sec-name{
  font-size:13px;font-weight:700;letter-spacing:.2px;
  color:var(--jade);
}
.sec-count{
  font-size:10px;color:var(--mist);font-weight:600;
  background:var(--foam2);
  padding:2px 10px;border-radius:20px;
  border:1px solid var(--border);
}
.sec-pills{display:flex;gap:6px;flex-wrap:wrap;margin-left:2px}
.sec-pill{
  font-size:9px;font-weight:700;
  padding:2px 9px;border-radius:20px;letter-spacing:.3px;
}

/* ── SIGNAL BADGES ── */
.sig{
  display:inline-block;padding:5px 12px;border-radius:8px;
  font-size:9px;font-weight:700;letter-spacing:.6px;
  white-space:nowrap;
}

/* Strong Buy — jade green filled */
.sbs{
  background:var(--jade);color:#fff;
  box-shadow:0 2px 8px rgba(45,106,79,.25);
}
/* Buy — sage outline */
.sbuy{
  background:var(--foam);color:var(--jade);
  border:1.5px solid var(--sage);
}
/* DII Buy — amber */
.sdii{
  background:rgba(233,138,78,.1);color:#c06a1f;
  border:1.5px solid rgba(233,138,78,.4);
}
/* Bulk/Block — sky blue */
.sblk{
  background:rgba(69,123,157,.08);color:var(--sky);
  border:1.5px solid rgba(69,123,157,.3);
}
/* Neutral — warm sand */
.sna{
  background:rgba(233,138,78,.08);color:#a0622a;
  border:1.5px solid rgba(233,138,78,.25);
}
/* Caution — light coral outline */
.sca{
  background:rgba(214,64,69,.07);color:var(--coral);
  border:1.5px solid rgba(214,64,69,.25);
}
/* Sell — coral filled */
.sse{
  background:var(--coral);color:#fff;
  box-shadow:0 2px 8px rgba(214,64,69,.2);
}

/* Sector pill reuses same classes */
.sec-pill.sbs{background:rgba(45,106,79,.15);color:var(--jade);border:1px solid rgba(45,106,79,.3)}
.sec-pill.sbuy{background:rgba(61,153,112,.12);color:var(--sage);border:1px solid rgba(61,153,112,.3)}
.sec-pill.sse{background:rgba(214,64,69,.1);color:var(--coral);border:1px solid rgba(214,64,69,.25)}

/* ── LEGEND ── */
.leg{
  padding:0 24px 20px;
  display:flex;gap:14px;flex-wrap:wrap;align-items:center;
}
.li2{display:flex;align-items:center;gap:7px;font-size:11px;color:var(--mist);font-weight:500}
.ld2{width:10px;height:10px;border-radius:50%;flex-shrink:0}

/* ── FOOTER ── */
footer{
  background:var(--jade2);
  border-top:2px solid var(--sage);
  padding:14px 24px;
  display:flex;justify-content:space-between;
  font-size:10px;color:rgba(255,255,255,.45);
  flex-wrap:wrap;gap:8px;
  font-weight:500;letter-spacing:.3px;
}

/* ── RESPONSIVE ── */
@media(max-width:900px){
  .sum{grid-template-columns:repeat(2,1fr)}.sv{font-size:17px}
  .lt{font-size:15px}.ls2{display:none}
}

@media(max-width:600px){
  header{padding:10px 16px}.lt{font-size:13px}
  .lv,.src{font-size:9px;padding:3px 9px}.dt{display:none}.ls2{display:none}
  .sum{grid-template-columns:repeat(2,1fr)}.sc2{padding:12px 14px}
  .sv{font-size:16px}.sl{font-size:8px}.sb2{font-size:9px}
  .tw{padding:12px;overflow-x:visible}
  table,thead,tbody,th,td,tr{display:block}
  thead{display:none}
  tr.sec-hdr{display:block;margin-top:14px}
  tr.sec-hdr td{border-radius:10px;padding:10px 12px}
  .sec-hdr-inner{gap:6px}.sec-name{font-size:12px}
  tbody tr:not(.sec-hdr){
    background:var(--paper);
    border:1px solid var(--border);
    border-radius:12px;margin-bottom:10px;padding:14px;
    animation:si .35s ease both;
    box-shadow:var(--shadow-sm);
  }
  tbody tr:not(.sec-hdr):hover{background:var(--foam)}
  td{
    text-align:left;padding:5px 0;border:none;
    display:flex;align-items:center;
    justify-content:space-between;gap:8px;font-size:12px;
  }
  td:first-child{
    flex-direction:column;align-items:flex-start;
    margin-bottom:8px;border-bottom:1px solid var(--border);padding-bottom:10px;
  }
  td::before{
    content:attr(data-label);font-size:9px;letter-spacing:1px;
    text-transform:uppercase;color:var(--mist);flex-shrink:0;min-width:80px;
    font-weight:600;
  }
  td:first-child::before{display:none}
  .rb2{margin:4px 0 0}.sr{justify-content:flex-end}
  .leg{padding:0 12px 14px;gap:10px}.li2{font-size:10px}
  footer{padding:10px 16px;font-size:9px}
  .tt{font-size:9px}.drb{font-size:9px;padding:3px 9px}
  .snav{top:66px;padding:8px 14px}
}
"""

    drb_html = (f'<div class="drb">🗓 {date_range_label}</div>'
                if date_range_label else "")

    sector_nav = ""
    for sector_name, _ in sorted_sectors:
        icon   = SECTOR_ICONS.get(sector_name, "🔷")
        anchor = sector_name.replace(" ","_").replace("&","and")
        sector_nav += f'<a href="#{anchor}" class="snav-link">{icon} {sector_name}</a>'

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>FII/DII Pulse — Jade Garden — {date_str}</title>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap" rel="stylesheet">
<style>
{css}
</style>
</head>
<body>
<div class="w">

<header>
  <div class="lg">
    <div class="logo-icon">📊</div>
    <div>
      <div class="lt">FII<span>/DII</span> PULSE</div>
      <div class="ls2">Institutional Intelligence · Sector-wise · Jade Garden</div>
    </div>
  </div>
  <div class="hm">
    {drb_html}
    <div class="src">📡 {source}</div>
    <div class="lv"><div class="led"></div>LIVE</div>
    <div class="dt">{date_str}</div>
  </div>
</header>

<div class="snav">{sector_nav}</div>

<div class="sum">
  <div class="sc2">
    <div class="sl">Nifty 50</div>
    <div class="sv">₹{market['nifty_price']:,.2f}</div>
    <div class="sb2 {nc}">{na} {market['nifty_chg']}%</div>
  </div>
  <div class="sc2">
    <div class="sl">Sensex</div>
    <div class="sv">₹{market['sensex_price']:,.2f}</div>
    <div class="sb2 {xc}">{xa} {market['sensex_chg']}%</div>
  </div>
  <div class="sc2">
    <div class="sl">Stocks Tracked</div>
    <div class="sv">{len(stocks)}</div>
    <div class="sb2">FII: {fb} · DII: {db} · Both: {bb}</div>
  </div>
  <div class="sc2">
    <div class="sl">Strong Buy</div>
    <div class="sv">{st}</div>
    <div class="sb2">Stocks signalled</div>
  </div>
</div>

<div class="tw">
  <div class="tt">
    Sector-wise Institutional Activity · Strong Buy → Buy → Neutral → Sell ·
    <span style="color:var(--jade);font-family:'DM Mono',monospace;font-weight:600">
      {date_range_label or date_str}
    </span>
  </div>
  <table>
    <thead><tr>
      <th>Stock</th>
      <th>Price / Trend</th>
      <th>RSI(14)</th>
      <th>Support / Resistance (6M)</th>
      <th>Signal</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>

<div class="leg">
  <div class="li2"><div class="ld2" style="background:var(--sage)"></div>FII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--sand)"></div>DII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--jade)"></div>Both Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--coral)"></div>Selling</div>
  <div class="li2"><div class="ld2" style="background:var(--sky)"></div>Bulk/Block</div>
  <div class="li2" style="margin-left:auto;font-size:10px;text-align:right;color:var(--mist)">
    🌿 Jade Garden Theme · Sorted by signal strength within sectors
  </div>
</div>

<footer>
  <div>🌿 FII/DII Pulse v8 · Jade Garden · {source} · yfinance · {date_str}</div>
  <div>⚠️ Not financial advice. Educational purposes only. Always DYOR.</div>
</footer>

</div>
</body>
</html>"""


# ─────────────────────────────────────────────────────────────────────────────
#  EMAIL  (unchanged)
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
    msg["Subject"] = f"🌿 FII/DII Pulse · Jade Garden — {date_str}"
    msg["From"]    = f"FII/DII Pulse <{user}>"
    msg["To"]      = ", ".join(to_list)

    plain = (
        f"FII/DII Pulse — {date_str}\n"
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
#  MAIN  (unchanged)
# ─────────────────────────────────────────────────────────────────────────────

def main():
    IST      = pytz.timezone("Asia/Kolkata")
    now_ist  = datetime.now(IST)
    date_str = now_ist.strftime("%d %b %Y")
    date_file= now_ist.strftime("%Y-%m-%d")

    log.info("=" * 65)
    log.info(f"  🌿 FII/DII Pulse v8 Jade Garden — {date_str}  (IST: {now_ist.strftime('%H:%M')})")
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
