"""
FII/DII Intelligence Dashboard — v3 (Robust Multi-Source)
==========================================================
Data sources (tried in order):
  1. NSE India Bulk Deals API  (official, most reliable)
  2. MunafaSutra scraper        (stock-level FII/DII activity)
  3. Hardcoded fallback list    (always works — recent known active stocks)

Technical Analysis: RSI, MACD, EMA 20/50, Bollinger Bands, ADX, Stoch RSI
Support/Resistance: Pivot points + 6-month swing high/low via yfinance
Email: Gmail SMTP with HTML attachment
GitHub Pages: saves to docs/index.html
"""

import os, json, smtplib, logging, traceback, time
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication
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

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
    "Referer": "https://www.nseindia.com/",
    "Accept-Language": "en-US,en;q=0.9",
}

# ── HARDCODED FALLBACK — Always-available stocks with known FII/DII interest ──
# Updated regularly — these are well-known institutional favourites
FALLBACK_STOCKS = [
    {"symbol": "GMRAIRPORT.NS",  "name": "GMR Airports",       "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "TORNTPHARM.NS",  "name": "Torrent Pharma",     "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "POWERGRID.NS",   "name": "Power Grid Corp",    "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "JSWENERGY.NS",   "name": "JSW Energy",         "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "SUPREMEIND.NS",  "name": "Supreme Industries", "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "ASTRAL.NS",      "name": "Astral Poly",        "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "INDIGO.NS",      "name": "IndiGo",             "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "BSE.NS",         "name": "BSE Limited",        "fii_cash": "sell", "dii_cash": "sell"},
    {"symbol": "GODREJCP.NS",    "name": "Godrej Consumer",    "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "SBICARD.NS",     "name": "SBI Cards",          "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "CAMS.NS",        "name": "CAMS",               "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "BRITANNIA.NS",   "name": "Britannia",          "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "KFINTECH.NS",    "name": "KFin Technologies",  "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "ANGELONE.NS",    "name": "Angel One",          "fii_cash": "sell", "dii_cash": "buy"},
    {"symbol": "POLICYBZR.NS",   "name": "PB Fintech",         "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "NUVAMA.NS",      "name": "Nuvama Wealth",      "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "FORTIS.NS",      "name": "Fortis Healthcare",  "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "MANAPPURAM.NS",  "name": "Manappuram Finance", "fii_cash": "buy",  "dii_cash": "sell"},
    {"symbol": "360ONE.NS",      "name": "360 One WAM",        "fii_cash": "buy",  "dii_cash": "buy"},
    {"symbol": "APLAPOLLO.NS",   "name": "APL Apollo Tubes",   "fii_cash": "buy",  "dii_cash": "buy"},
]

# ── SOURCE 1: NSE Bulk Deals API ───────────────────────────────────────────────
def fetch_from_nse_bulk_deals() -> list[dict]:
    """Fetch from NSE's official bulk deals API — most reliable source."""
    log.info("📡 Trying NSE Bulk Deals API...")
    try:
        session = requests.Session()
        # First hit NSE homepage to get cookies
        session.get("https://www.nseindia.com/", headers=NSE_HEADERS, timeout=10)
        time.sleep(1)

        # Fetch bulk deals for today
        today = datetime.today().strftime("%d-%m-%Y")
        url = f"https://www.nseindia.com/api/bulk-deals?from_date={today}&to_date={today}"
        resp = session.get(url, headers=NSE_HEADERS, timeout=15)
        resp.raise_for_status()
        data = resp.json()

        log.info(f"  NSE API response keys: {list(data.keys()) if isinstance(data, dict) else 'list'}")

        deals = data.get("data", data) if isinstance(data, dict) else data
        if not deals:
            log.warning("  NSE Bulk Deals: empty response (market holiday or no deals today)")
            return []

        stocks = {}
        for deal in deals:
            sym = deal.get("symbol", "") or deal.get("Symbol", "")
            name = deal.get("companyName", sym) or deal.get("Company", sym)
            client = (deal.get("clientName", "") or "").upper()
            qty = float(deal.get("quantity", 0) or 0)
            price = float(deal.get("tradePrice", 0) or 0)
            buy_sell = (deal.get("buySell", "") or "").upper()

            if not sym:
                continue

            # Classify as FII or DII based on client name keywords
            is_fii = any(k in client for k in ["FII", "FOREIGN", "FPI", "OVERSEAS", "GLOBAL",
                                                 "INTERNATIONAL", "MORGAN", "GOLDMAN", "CITI",
                                                 "BLACKROCK", "VANGUARD", "FIDELITY", "NOMURA",
                                                 "MACQUARIE", "UBS", "BARCLAYS", "HSBC"])
            is_dii = any(k in client for k in ["DII", "MF", "MUTUAL FUND", "LIC", "SBI",
                                                 "HDFC MF", "ICICI MF", "KOTAK", "AXIS MF",
                                                 "NIPPON", "ADITYA", "DSP", "FRANKLIN",
                                                 "INSURANCE", "PROVIDENT", "PENSION"])

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
            if is_fii:
                stocks[key]["fii_cash"] = action
            if is_dii:
                stocks[key]["dii_cash"] = action

        result = list(stocks.values())
        log.info(f"  ✅ NSE Bulk Deals: found {len(result)} stocks")
        return result

    except Exception as e:
        log.warning(f"  ❌ NSE Bulk Deals failed: {e}")
        return []


# ── SOURCE 2: MunafaSutra Scraper ─────────────────────────────────────────────
def fetch_from_munafasutra() -> list[dict]:
    """Scrape MunafaSutra for FII/DII stock activity."""
    log.info("📡 Trying MunafaSutra scraper...")
    try:
        resp = requests.get("https://munafasutra.com/nse/FIIDII/",
                            headers=HEADERS, timeout=20)
        resp.raise_for_status()
        log.info(f"  Status: {resp.status_code}, Content length: {len(resp.text)}")

        soup = BeautifulSoup(resp.text, "lxml")

        # Find the stock activity table
        tables = soup.find_all("table")
        log.info(f"  Found {len(tables)} tables on page")

        if not tables:
            # Try finding rows directly
            rows = soup.find_all("tr")
            log.info(f"  Found {len(rows)} tr elements directly")

        stocks = []
        for tbl_idx, table in enumerate(tables):
            rows = table.find_all("tr")
            log.info(f"  Table {tbl_idx}: {len(rows)} rows")

            for row in rows[1:]:  # skip header
                cols = row.find_all("td")
                if len(cols) < 4:
                    continue

                link = cols[0].find("a")
                if not link:
                    continue

                href = link.get("href", "")
                symbol = href.rstrip("/").split("/")[-1] if href else ""
                name = link.get_text(strip=True)

                # Get buy/sell text from columns
                col_texts = [c.get_text(strip=True).lower() for c in cols]
                full_text = " ".join(col_texts)

                fii_cash = "buy" if "bought" in col_texts[1] else "sell" if "sold" in col_texts[1] else "neutral"
                dii_cash = "buy" if "bought" in col_texts[1] else "sell" if "sold" in col_texts[1] else "neutral"

                if symbol and name:
                    stocks.append({
                        "symbol": symbol + ".NS",
                        "name": name,
                        "fii_cash": fii_cash,
                        "dii_cash": dii_cash,
                    })

            if stocks:
                break  # found stocks in this table

        log.info(f"  {'✅' if stocks else '❌'} MunafaSutra: {len(stocks)} stocks")
        return stocks[:20]

    except Exception as e:
        log.warning(f"  ❌ MunafaSutra failed: {e}")
        return []


# ── SOURCE 3: Fallback — Recent high-activity FII/DII stocks ─────────────────
def fetch_fallback() -> list[dict]:
    """Always-available fallback using known institutional favourite stocks."""
    log.info("📡 Using hardcoded fallback stock list (reliable baseline)...")
    return FALLBACK_STOCKS.copy()


# ── MASTER FETCH — Try all sources in order ────────────────────────────────────
def fetch_fii_dii_stocks() -> tuple[list[dict], str]:
    """Try NSE → MunafaSutra → Fallback. Returns (stocks, source_name)."""
    # Try NSE bulk deals first
    stocks = fetch_from_nse_bulk_deals()
    if stocks:
        return stocks, "NSE Bulk Deals API"

    # Try MunafaSutra
    stocks = fetch_from_munafasutra()
    if stocks:
        return stocks, "MunafaSutra"

    # Always-available fallback
    log.warning("⚠️  All live sources failed — using fallback stock list")
    return fetch_fallback(), "Fallback (Known Institutional Stocks)"


# ── TECHNICAL ANALYSIS ────────────────────────────────────────────────────────
def compute_technicals(symbol: str) -> dict:
    """Download 6-month OHLCV and compute all indicators."""
    log.info(f"  📐 Technicals: {symbol}")
    end   = datetime.today()
    start = end - timedelta(days=185)

    try:
        df = yf.download(symbol, start=start, end=end,
                         progress=False, auto_adjust=True)

        if df is None or df.empty:
            raise ValueError("No data returned")

        # Handle MultiIndex columns from yfinance
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        df = df.dropna()
        if len(df) < 20:
            raise ValueError(f"Only {len(df)} rows — insufficient")

        log.info(f"    Got {len(df)} rows for {symbol}")

        close = df["Close"].astype(float)
        high  = df["High"].astype(float)
        low   = df["Low"].astype(float)

        # ── RSI (14) ──────────────────────────────────────────────────────
        delta    = close.diff()
        gain     = delta.clip(lower=0)
        loss     = (-delta).clip(lower=0)
        avg_gain = gain.ewm(com=13, adjust=False).mean()
        avg_loss = loss.ewm(com=13, adjust=False).mean()
        rs       = avg_gain / avg_loss.replace(0, np.nan)
        rsi_val  = round(float((100 - (100 / (1 + rs))).iloc[-1]), 1)

        # ── MACD (12, 26, 9) ─────────────────────────────────────────────
        ema12     = close.ewm(span=12, adjust=False).mean()
        ema26     = close.ewm(span=26, adjust=False).mean()
        macd      = ema12 - ema26
        sig       = macd.ewm(span=9, adjust=False).mean()
        macd_hist = round(float(macd.iloc[-1] - sig.iloc[-1]), 2)

        # ── EMA 20 / 50 crossover ────────────────────────────────────────
        ema20     = close.ewm(span=20, adjust=False).mean()
        ema50     = close.ewm(span=50, adjust=False).mean()
        ema_cross = "bullish" if float(ema20.iloc[-1]) > float(ema50.iloc[-1]) else "bearish"

        # ── Bollinger Bands (20, 2σ) ─────────────────────────────────────
        bb_mid   = close.rolling(20).mean()
        bb_std   = close.rolling(20).std()
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std
        lc       = float(close.iloc[-1])
        bb_rng   = float(bb_upper.iloc[-1]) - float(bb_lower.iloc[-1])
        bb_pos   = (lc - float(bb_lower.iloc[-1])) / (bb_rng if bb_rng else 1)
        bb_label = "Overbought" if bb_pos > 0.8 else ("Oversold" if bb_pos < 0.2 else "Mid")

        # ── ADX (14) ─────────────────────────────────────────────────────
        plus_dm  = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)
        tr       = pd.concat([high - low,
                               (high - close.shift()).abs(),
                               (low  - close.shift()).abs()], axis=1).max(axis=1)
        atr14    = tr.ewm(com=13, adjust=False).mean()
        plus_di  = 100 * plus_dm.ewm(com=13, adjust=False).mean()  / atr14
        minus_di = 100 * minus_dm.ewm(com=13, adjust=False).mean() / atr14
        dx       = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
        adx_val  = round(float(dx.ewm(com=13, adjust=False).mean().iloc[-1]), 1)

        # ── Stochastic RSI ────────────────────────────────────────────────
        rsi_ser = 100 - (100 / (1 + avg_gain / avg_loss.replace(0, np.nan)))
        rmin    = rsi_ser.rolling(14).min()
        rmax    = rsi_ser.rolling(14).max()
        stoch_rsi = round(float(((rsi_ser - rmin) / ((rmax - rmin).replace(0, np.nan))).iloc[-1]), 2)

        # ── Support & Resistance ─────────────────────────────────────────
        pivot    = (float(high.iloc[-1]) + float(low.iloc[-1]) + lc) / 3
        resist1  = round(2 * pivot - float(low.iloc[-1]),  2)
        support1 = round(2 * pivot - float(high.iloc[-1]), 2)
        swing_h  = round(float(high.rolling(min(120, len(high))).max().iloc[-1]), 2)
        swing_l  = round(float(low.rolling(min(120, len(low))).min().iloc[-1]),   2)

        # ── Overall Signal Score ─────────────────────────────────────────
        score = 0
        if rsi_val < 40:          score += 2
        elif rsi_val < 55:        score += 1
        elif rsi_val > 70:        score -= 2
        if macd_hist > 0:         score += 2
        if ema_cross == "bullish": score += 2
        if adx_val > 25:          score += 1
        if not np.isnan(stoch_rsi):
            if stoch_rsi < 0.3:   score += 1
            elif stoch_rsi > 0.8: score -= 1

        overall = ("STRONG BUY" if score >= 5 else
                   "BUY"        if score >= 3 else
                   "NEUTRAL"    if score >= 0 else
                   "CAUTION"    if score >= -2 else "SELL")

        # 7-day sparkline
        spark = [round(float(x), 2) for x in close.iloc[-7:].tolist()]

        return {
            "rsi": rsi_val, "macd_hist": macd_hist,
            "ema_cross": ema_cross, "bb_label": bb_label,
            "adx": adx_val, "stoch_rsi": stoch_rsi if not np.isnan(stoch_rsi) else 0.5,
            "resist1": resist1, "support1": support1,
            "swing_high": swing_h, "swing_low": swing_l,
            "last_price": round(lc, 2),
            "overall": overall, "score": score,
            "sparkline": spark, "data_ok": True,
        }

    except Exception as e:
        log.warning(f"    ⚠️  Technicals failed for {symbol}: {e}")
        return {
            "rsi": 50.0, "macd_hist": 0.0, "ema_cross": "unknown",
            "bb_label": "N/A", "adx": 0.0, "stoch_rsi": 0.5,
            "resist1": 0.0, "support1": 0.0,
            "swing_high": 0.0, "swing_low": 0.0,
            "last_price": 0.0, "overall": "N/A", "score": 0,
            "sparkline": [], "data_ok": False,
        }


# ── MARKET SUMMARY ────────────────────────────────────────────────────────────
def fetch_market_summary() -> dict:
    log.info("📡 Fetching Nifty/Sensex...")
    try:
        nifty  = yf.download("^NSEI",  period="5d", progress=False, auto_adjust=True)
        sensex = yf.download("^BSESN", period="5d", progress=False, auto_adjust=True)

        def safe_last(df):
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            c = df["Close"].dropna().astype(float)
            return float(c.iloc[-1]) if len(c) >= 1 else 0.0

        def safe_pct(df):
            if isinstance(df.columns, pd.MultiIndex):
                df.columns = df.columns.get_level_values(0)
            c = df["Close"].dropna().astype(float)
            return round((c.iloc[-1] - c.iloc[-2]) / c.iloc[-2] * 100, 2) if len(c) >= 2 else 0.0

        return {
            "nifty_price":  round(safe_last(nifty), 2),
            "nifty_chg":    safe_pct(nifty),
            "sensex_price": round(safe_last(sensex), 2),
            "sensex_chg":   safe_pct(sensex),
        }
    except Exception as e:
        log.warning(f"Market summary failed: {e}")
        return {"nifty_price": 0, "nifty_chg": 0, "sensex_price": 0, "sensex_chg": 0}


# ── BUILD FULL DATASET ────────────────────────────────────────────────────────
def build_dataset():
    raw_stocks, source = fetch_fii_dii_stocks()
    log.info(f"✅ Using source: {source} — {len(raw_stocks)} stocks")

    market   = fetch_market_summary()
    enriched = []

    for s in raw_stocks:
        tech = compute_technicals(s["symbol"])
        both_buy  = s["fii_cash"] == "buy"  and s["dii_cash"] == "buy"
        fii_only  = s["fii_cash"] == "buy"  and s["dii_cash"] != "buy"
        dii_only  = s["dii_cash"] == "buy"  and s["fii_cash"] != "buy"
        inst_sig  = ("BOTH BUY" if both_buy else
                     "FII BUY"  if fii_only else
                     "DII BUY"  if dii_only else "SELL")
        enriched.append({
            **s, **tech,
            "inst_signal": inst_sig,
            "both_buy": both_buy,
            "fii_only": fii_only,
            "dii_only": dii_only,
        })
        time.sleep(0.3)  # gentle rate limiting

    return enriched, market, source


# ── HTML GENERATION ───────────────────────────────────────────────────────────
def spark_svg(prices):
    if len(prices) < 2:
        return ""
    mn, mx = min(prices), max(prices)
    rng = mx - mn if mx != mn else 1
    w, h = 70, 26
    pts  = [f"{round(i*w/(len(prices)-1),1)},{round(h-(p-mn)/rng*h,1)}" for i, p in enumerate(prices)]
    col  = "#00d4aa" if prices[-1] >= prices[0] else "#ff4d6d"
    return (f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" xmlns="http://www.w3.org/2000/svg">'
            f'<polyline points="{" ".join(pts)}" fill="none" stroke="{col}" '
            f'stroke-width="1.8" stroke-linejoin="round"/></svg>')

def sig_cls(s):
    return {"STRONG BUY":"signal-strong-buy","BUY":"signal-buy","NEUTRAL":"signal-neutral",
            "CAUTION":"signal-caution","SELL":"signal-sell"}.get(s,"signal-neutral")

def badge_cls(s):
    return {"BOTH BUY":"badge-both","FII BUY":"badge-fii","DII BUY":"badge-dii",
            "SELL":"badge-sell"}.get(s,"badge-sell")

def rsi_cls(v):
    return "rsi-overbought" if v>70 else ("rsi-oversold" if v<40 else "rsi-neutral")


def generate_html(stocks, market, date_str, source):
    nc = "up" if market["nifty_chg"]  >= 0 else "down"
    sc = "up" if market["sensex_chg"] >= 0 else "down"
    na = "▲"  if market["nifty_chg"]  >= 0 else "▼"
    sa = "▲"  if market["sensex_chg"] >= 0 else "▼"
    fii_b  = sum(1 for s in stocks if s["fii_cash"] == "buy")
    dii_b  = sum(1 for s in stocks if s["dii_cash"] == "buy")
    both_b = sum(1 for s in stocks if s["both_buy"])
    strong = sum(1 for s in stocks if s["overall"] == "STRONG BUY")

    rows = ""
    for s in stocks:
        fb  = "badge-fii"  if s["fii_cash"] == "buy"  else "badge-sell"
        db  = "badge-dii"  if s["dii_cash"] == "buy"  else "badge-sell"
        fat = "▲ BUY" if s["fii_cash"] == "buy" else "▼ SELL"
        dat = "▲ BUY" if s["dii_cash"] == "buy" else "▼ SELL"
        mc  = "up"    if s["macd_hist"] > 0 else "down"
        ec  = "up"    if s["ema_cross"] == "bullish" else "down"
        spk = spark_svg(s.get("sparkline", []))
        price_disp = f"₹{s['last_price']:,.2f}" if s['last_price'] > 0 else s.get("price_str", "N/A")

        rows += f"""
        <tr>
          <td><div class="sn">{s['name']}</div>
              <div class="ss">{s['symbol'].replace('.NS','')}</div></td>
          <td><div class="pm">{price_disp}</div><div class="spk">{spk}</div></td>
          <td><span class="badge {fb}">{fat}</span></td>
          <td><span class="badge {db}">{dat}</span></td>
          <td>
            <div class="{rsi_cls(s['rsi'])}">
              <span class="rn {'up' if s['rsi']<55 else 'down'}">{s['rsi']}</span>
              <div class="rb"><div class="rf" style="width:{min(s['rsi'],100):.0f}%"></div></div>
            </div>
          </td>
          <td>
            <div class="im"><span class="il">MACD</span><span class="{mc}">{'+' if s['macd_hist']>0 else ''}{s['macd_hist']}</span></div>
            <div class="im"><span class="il">EMA</span><span class="{ec}">{'↑ Bull' if s['ema_cross']=='bullish' else '↓ Bear'}</span></div>
            <div class="im"><span class="il">ADX</span><span>{s['adx']}</span></div>
            <div class="im"><span class="il">BB</span><span>{s['bb_label']}</span></div>
          </td>
          <td>
            <div class="sr"><span class="sr-r">R1</span> {f"₹{s['resist1']:,.2f}" if s['resist1'] else 'N/A'}</div>
            <div class="sr"><span class="sr-s">S1</span> {f"₹{s['support1']:,.2f}" if s['support1'] else 'N/A'}</div>
            <div class="sr"><span class="sr-r">6mH</span> {f"₹{s['swing_high']:,.2f}" if s['swing_high'] else 'N/A'}</div>
            <div class="sr"><span class="sr-s">6mL</span> {f"₹{s['swing_low']:,.2f}" if s['swing_low'] else 'N/A'}</div>
          </td>
          <td><span class="signal {sig_cls(s['overall'])}">{s['overall']}</span></td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>FII/DII Pulse — {date_str}</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
:root{{--bg:#050c14;--sur:#0b1623;--card:#0f1e2e;--bdr:#1a3050;
  --fii:#00d4aa;--dii:#ff8c42;--both:#a78bfa;--sell:#ff4d6d;
  --txt:#e2eaf4;--mut:#5a7a99;--gold:#f0c060;--grn:#00d4aa;--red:#ff4d6d;}}
*{{margin:0;padding:0;box-sizing:border-box}}
body{{background:var(--bg);color:var(--txt);font-family:'Syne',sans-serif;min-height:100vh}}
body::before{{content:'';position:fixed;inset:0;
  background-image:linear-gradient(rgba(0,212,170,.03) 1px,transparent 1px),
    linear-gradient(90deg,rgba(0,212,170,.03) 1px,transparent 1px);
  background-size:40px 40px;pointer-events:none;z-index:0}}
.w{{position:relative;z-index:1}}
header{{background:linear-gradient(135deg,#050c14,#0a1930,#050c14);
  border-bottom:1px solid var(--bdr);padding:18px 28px;
  display:flex;align-items:center;justify-content:space-between}}
.logo{{display:flex;align-items:center;gap:12px}}
.li{{width:40px;height:40px;background:linear-gradient(135deg,var(--fii),var(--both));
  border-radius:10px;display:flex;align-items:center;justify-content:center;
  font-size:18px;box-shadow:0 0 20px rgba(0,212,170,.4)}}
.lt{{font-size:19px;font-weight:800;letter-spacing:1px}}
.lt span{{color:var(--fii)}}
.ls{{font-size:10px;color:var(--mut);letter-spacing:1px;margin-top:2px}}
.hm{{display:flex;align-items:center;gap:14px}}
.lb{{display:flex;align-items:center;gap:6px;background:rgba(0,212,170,.1);
  border:1px solid rgba(0,212,170,.3);padding:5px 13px;border-radius:20px;
  font-size:11px;font-weight:700;color:var(--fii);letter-spacing:1px}}
.ld{{width:7px;height:7px;border-radius:50%;background:var(--fii);animation:pulse 1.5s infinite}}
@keyframes pulse{{0%,100%{{opacity:1;transform:scale(1)}}50%{{opacity:.4;transform:scale(.8)}}}}
.dt{{font-family:'Space Mono',monospace;font-size:11px;color:var(--mut)}}
.src{{font-size:10px;color:var(--both);background:rgba(167,139,250,.1);
  border:1px solid rgba(167,139,250,.2);padding:4px 10px;border-radius:12px}}
.sum{{display:grid;grid-template-columns:repeat(4,1fr);background:var(--bdr);gap:1px;
  border-bottom:1px solid var(--bdr)}}
.sc{{background:var(--sur);padding:16px 22px}}
.sl{{font-size:10px;letter-spacing:2px;color:var(--mut);text-transform:uppercase;margin-bottom:5px}}
.sv{{font-family:'Space Mono',monospace;font-size:20px;font-weight:700}}
.sb{{font-size:11px;color:var(--mut);margin-top:4px}}
.up{{color:var(--grn)}} .down{{color:var(--red)}}
.cf{{color:var(--fii)}} .cd{{color:var(--dii)}} .cb{{color:var(--both)}} .cg{{color:var(--gold)}}
.tw{{padding:22px 28px;overflow-x:auto}}
.tt{{font-size:12px;letter-spacing:2px;text-transform:uppercase;color:var(--mut);
  margin-bottom:16px;display:flex;align-items:center;gap:10px}}
.tt::before{{content:'';width:3px;height:14px;background:var(--fii);border-radius:2px}}
table{{width:100%;border-collapse:collapse;min-width:880px}}
thead tr{{border-bottom:2px solid var(--bdr)}}
th{{padding:9px 11px;text-align:left;font-size:10px;letter-spacing:1.5px;
  color:var(--mut);text-transform:uppercase}}
th:not(:first-child){{text-align:center}}
tbody tr{{border-bottom:1px solid rgba(26,48,80,.4);transition:background .15s;
  animation:si .4s ease forwards;opacity:0}}
@keyframes si{{from{{opacity:0;transform:translateX(-8px)}}to{{opacity:1;transform:translateX(0)}}}}
tbody tr:nth-child(1){{animation-delay:.05s}} tbody tr:nth-child(2){{animation-delay:.1s}}
tbody tr:nth-child(3){{animation-delay:.15s}} tbody tr:nth-child(4){{animation-delay:.2s}}
tbody tr:nth-child(5){{animation-delay:.25s}} tbody tr:nth-child(6){{animation-delay:.3s}}
tbody tr:nth-child(7){{animation-delay:.35s}} tbody tr:nth-child(8){{animation-delay:.4s}}
tbody tr:nth-child(9){{animation-delay:.45s}} tbody tr:nth-child(10){{animation-delay:.5s}}
tbody tr:nth-child(n+11){{animation-delay:.55s}}
tbody tr:hover{{background:rgba(0,212,170,.03)}}
td{{padding:12px 11px;font-size:12px;vertical-align:middle;text-align:center}}
td:first-child{{text-align:left}}
.sn{{font-weight:700;font-size:13px}} .ss{{font-size:10px;color:var(--mut);font-family:'Space Mono',monospace;margin-top:2px}}
.pm{{font-family:'Space Mono',monospace;font-size:13px;font-weight:700}} .spk{{margin-top:4px}}
.badge{{display:inline-block;padding:4px 9px;border-radius:5px;font-size:10px;font-weight:800;letter-spacing:.5px}}
.badge-fii{{background:rgba(0,212,170,.12);color:var(--fii);border:1px solid rgba(0,212,170,.25)}}
.badge-dii{{background:rgba(255,140,66,.12);color:var(--dii);border:1px solid rgba(255,140,66,.25)}}
.badge-both{{background:rgba(167,139,250,.15);color:var(--both);border:1px solid rgba(167,139,250,.3)}}
.badge-sell{{background:rgba(255,77,109,.1);color:var(--sell);border:1px solid rgba(255,77,109,.25)}}
.rn{{font-family:'Space Mono',monospace;font-size:13px;font-weight:700}}
.rb{{width:80px;height:4px;background:var(--bdr);border-radius:2px;margin:5px auto 0;overflow:hidden}}
.rf{{height:100%;border-radius:2px}}
.rsi-overbought .rf{{background:var(--red)}} .rsi-neutral .rf{{background:var(--gold)}} .rsi-oversold .rf{{background:var(--grn)}}
.im{{display:flex;justify-content:center;gap:5px;font-size:10px;margin-bottom:2px}}
.il{{color:var(--mut);font-size:9px}}
.sr{{font-size:10px;font-family:'Space Mono',monospace;display:flex;align-items:center;gap:4px;justify-content:center;margin-bottom:2px}}
.sr-r{{font-size:9px;padding:1px 4px;border-radius:3px;font-weight:700;background:rgba(255,77,109,.2);color:var(--red)}}
.sr-s{{font-size:9px;padding:1px 4px;border-radius:3px;font-weight:700;background:rgba(0,212,170,.15);color:var(--grn)}}
.signal{{display:inline-block;padding:4px 10px;border-radius:5px;font-size:10px;font-weight:800;letter-spacing:.5px}}
.signal-strong-buy{{background:rgba(0,212,170,.2);color:var(--grn);border:1px solid rgba(0,212,170,.5);box-shadow:0 0 8px rgba(0,212,170,.2)}}
.signal-buy{{background:rgba(0,245,212,.1);color:#4ad9c8;border:1px solid rgba(0,245,212,.25)}}
.signal-neutral{{background:rgba(240,192,96,.1);color:var(--gold);border:1px solid rgba(240,192,96,.25)}}
.signal-caution{{background:rgba(255,140,66,.1);color:var(--dii);border:1px solid rgba(255,140,66,.3)}}
.signal-sell{{background:rgba(255,77,109,.12);color:var(--red);border:1px solid rgba(255,77,109,.3)}}
.leg{{padding:0 28px 20px;display:flex;gap:16px;flex-wrap:wrap;align-items:center}}
.li2{{display:flex;align-items:center;gap:6px;font-size:11px;color:var(--mut)}}
.ld2{{width:9px;height:9px;border-radius:50%}}
footer{{background:var(--sur);border-top:1px solid var(--bdr);padding:13px 28px;
  display:flex;justify-content:space-between;font-size:11px;color:var(--mut);flex-wrap:wrap;gap:8px}}
</style>
</head>
<body>
<div class="w">
<header>
  <div class="logo">
    <div class="li">📊</div>
    <div>
      <div class="lt">FII<span>/DII</span> PULSE</div>
      <div class="ls">INSTITUTIONAL INTELLIGENCE DASHBOARD · AUTO-GENERATED</div>
    </div>
  </div>
  <div class="hm">
    <div class="src">📡 {source}</div>
    <div class="lb"><div class="ld"></div> LIVE DATA</div>
    <div class="dt">📅 {date_str}</div>
  </div>
</header>

<div class="sum">
  <div class="sc"><div class="sl">Nifty 50</div>
    <div class="sv cf">₹{market['nifty_price']:,.2f}</div>
    <div class="sb {nc}">{na} {market['nifty_chg']}%</div></div>
  <div class="sc"><div class="sl">Sensex</div>
    <div class="sv cd">₹{market['sensex_price']:,.2f}</div>
    <div class="sb {sc}">{sa} {market['sensex_chg']}%</div></div>
  <div class="sc"><div class="sl">Stocks Tracked</div>
    <div class="sv cb">{len(stocks)}</div>
    <div class="sb">FII Buys: {fii_b} · DII Buys: {dii_b} · Both: {both_b}</div></div>
  <div class="sc"><div class="sl">Strong Buy Signals</div>
    <div class="sv cg">{strong}</div>
    <div class="sb">Stocks with STRONG BUY rating</div></div>
</div>

<div class="tw">
  <div class="tt">Institutional Stock Activity · Technical Analysis · {date_str}</div>
  <table>
    <thead><tr>
      <th>Stock</th><th>Price / Trend</th><th>FII Cash</th><th>DII Cash</th>
      <th>RSI (14)</th><th>MACD · EMA · ADX · BB</th>
      <th>Support / Resistance (6M)</th><th>Signal</th>
    </tr></thead>
    <tbody>{rows}</tbody>
  </table>
</div>

<div class="leg">
  <div class="li2"><div class="ld2" style="background:var(--fii)"></div> FII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--dii)"></div> DII Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--both)"></div> Both Buying</div>
  <div class="li2"><div class="ld2" style="background:var(--red)"></div> Selling</div>
  <div class="li2" style="margin-left:auto;font-size:10px;">
    RSI: &lt;40 Oversold · &gt;70 Overbought · MACD+: Bullish · ADX&gt;25: Strong Trend</div>
</div>

<footer>
  <div>🤖 Auto-generated by FII/DII Pulse · Source: {source} · Technicals: yfinance+pandas</div>
  <div>⚠️ Not financial advice. For educational purposes only. Always DYOR.</div>
</footer>
</div></body></html>"""


# ── EMAIL ────────────────────────────────────────────────────────────────────
def send_email(html_content, date_str, html_path, source):
    user  = os.getenv("GMAIL_USER")
    pwd   = os.getenv("GMAIL_PASS")
    rcpts = os.getenv("RECIPIENT_EMAIL", user)
    if not user or not pwd:
        log.warning("GMAIL_USER/GMAIL_PASS not set — skipping email")
        return

    to_list = [r.strip() for r in rcpts.split(",")]
    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📊 FII/DII Intelligence Report — {date_str}"
    msg["From"]    = f"FII/DII Pulse <{user}>"
    msg["To"]      = ", ".join(to_list)

    body = f"""<html><body style="font-family:sans-serif;background:#050c14;color:#e2eaf4;padding:24px;">
      <h2 style="color:#00d4aa;">📊 FII/DII Pulse — {date_str}</h2>
      <p style="color:#5a7a99;">Data Source: <strong style="color:#a78bfa;">{source}</strong></p>
      <p>Your daily institutional intelligence report is attached.</p>
      <p>Open <strong>fii_dii_report_{date_str.replace(' ','_')}.html</strong> in your browser for the full dashboard.</p>
      <hr style="border-color:#1a3050;margin:16px 0;">
      <p style="font-size:11px;color:#5a7a99;">⚠️ Not financial advice. Educational use only.</p>
    </body></html>"""

    msg.attach(MIMEText(body, "html"))
    with open(html_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=html_path.name)
        part["Content-Disposition"] = f'attachment; filename="{html_path.name}"'
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as srv:
            srv.login(user, pwd)
            srv.sendmail(user, to_list, msg.as_string())
        log.info("✅ Email sent!")
    except Exception as e:
        log.error(f"❌ Email failed: {e}")


# ── MAIN ──────────────────────────────────────────────────────────────────────
def main():
    date_str  = datetime.today().strftime("%d %b %Y")
    date_file = datetime.today().strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info(f"  FII/DII Pulse — {date_str}")
    log.info("=" * 60)

    stocks, market, source = build_dataset()
    log.info(f"📊 Total stocks with technicals: {len(stocks)}")

    html = generate_html(stocks, market, date_str, source)

    index_path = OUTPUT_DIR / "index.html"
    dated_path = OUTPUT_DIR / f"report_{date_file}.html"
    for p in [index_path, dated_path]:
        p.write_text(html, encoding="utf-8")
        log.info(f"💾 Saved: {p}")

    send_email(html, date_str, dated_path, source)
    log.info("✅ Complete!")


if __name__ == "__main__":
    main()
