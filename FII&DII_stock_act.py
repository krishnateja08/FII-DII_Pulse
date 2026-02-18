"""
FII/DII Intelligence Dashboard
================================
Fetches last-day FII/DII stock activity, computes technical indicators,
generates an HTML report, and emails it via Gmail.

Requirements:
    pip install requests beautifulsoup4 yfinance pandas numpy
                ta lxml smtplib jinja2 python-dotenv

Environment Variables (set in .env or GitHub Secrets):
    GMAIL_USER     = your_email@gmail.com
    GMAIL_PASS     = your_gmail_app_password   (16-char App Password)
    RECIPIENT_EMAIL = recipient@example.com    (comma-separated for multiple)
"""

import os
import json
import smtplib
import logging
import traceback
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

OUTPUT_DIR = Path("docs")          # GitHub Pages serves from /docs
OUTPUT_DIR.mkdir(exist_ok=True)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}

# ── 1. FETCH FII/DII STOCK DATA ────────────────────────────────────────────────

def fetch_fii_dii_stocks() -> list[dict]:
    """
    Scrape MunafaSutra for today's FII/DII stock-level activity.
    Falls back to sample data if scraping fails (e.g., market holiday).
    """
    log.info("Fetching FII/DII stock data from MunafaSutra...")
    url = "https://munafasutra.com/nse/FIIDII/"

    try:
        resp = requests.get(url, headers=HEADERS, timeout=15)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "lxml")

        table = soup.find("table")
        if not table:
            raise ValueError("No table found on page")

        rows = table.find_all("tr")[1:]  # skip header
        stocks = []

        for row in rows:
            cols = row.find_all("td")
            if len(cols) < 5:
                continue

            name_tag = cols[0].find("a")
            if not name_tag:
                continue

            href = name_tag.get("href", "")
            symbol = href.split("/")[-1] if href else ""
            name = name_tag.get_text(strip=True)

            fii_cash  = cols[1].get_text(strip=True).lower()
            fii_fno   = cols[2].get_text(strip=True).lower()
            change    = cols[3].get_text(strip=True)
            price     = cols[4].get_text(strip=True)

            # Determine DII from context (MunafaSutra combines FII/DII)
            stocks.append({
                "symbol":    symbol + ".NS",
                "name":      name,
                "fii_cash":  "buy" if "bought" in fii_cash else "sell",
                "fii_fno":   "buy" if "bought" in fii_fno  else "sell",
                "dii_cash":  "buy" if "bought" in fii_cash else "sell",  # approximated
                "price_str": price,
                "change_str": change,
            })

        log.info(f"Found {len(stocks)} stocks")
        return stocks[:20]  # top 20

    except Exception as e:
        log.warning(f"Scraping failed ({e}), using fallback sample data")
        return _fallback_stocks()


def _fallback_stocks() -> list[dict]:
    """Sample stocks for weekends / holidays / scrape failures."""
    return [
        {"symbol": "GMRINFRA.NS",    "name": "GMR Airports",      "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹100.52", "change_str": "+6.90%"},
        {"symbol": "TORNTPHARM.NS",  "name": "Torrent Pharma",    "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹4261",   "change_str": "+4.50%"},
        {"symbol": "POWERGRID.NS",   "name": "Power Grid",        "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹300.50", "change_str": "+4.63%"},
        {"symbol": "JSWENERGY.NS",   "name": "JSW Energy",        "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹489.95", "change_str": "+3.21%"},
        {"symbol": "SUPREMEIND.NS",  "name": "Supreme Industries","fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "sell", "price_str": "₹3885",   "change_str": "+2.94%"},
        {"symbol": "ASTRAL.NS",      "name": "Astral Poly",       "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹1639",   "change_str": "+2.71%"},
        {"symbol": "INDIGO.NS",      "name": "IndiGo",            "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹4940",   "change_str": "+0.24%"},
        {"symbol": "BSE.NS",         "name": "BSE Limited",       "fii_cash": "sell", "fii_fno": "buy",  "dii_cash": "sell", "price_str": "₹2804",   "change_str": "-7.30%"},
        {"symbol": "GODREJCP.NS",    "name": "Godrej Consumer",   "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹1203",   "change_str": "+0.70%"},
        {"symbol": "SBICARD.NS",     "name": "SBI Cards",         "fii_cash": "buy",  "fii_fno": "sell", "dii_cash": "buy",  "price_str": "₹772",    "change_str": "+1.51%"},
        {"symbol": "CAMS.NS",        "name": "CAMS",              "fii_cash": "buy",  "fii_fno": "buy",  "dii_cash": "buy",  "price_str": "₹737",    "change_str": "+2.23%"},
        {"symbol": "BRITANNIA.NS",   "name": "Britannia",         "fii_cash": "buy",  "fii_fno": "sell", "dii_cash": "buy",  "price_str": "₹6106",   "change_str": "+2.10%"},
    ]


# ── 2. TECHNICAL ANALYSIS ─────────────────────────────────────────────────────

def compute_technicals(symbol: str) -> dict:
    """
    Downloads 6-month OHLCV data and computes:
      RSI, MACD, EMA crossover, Bollinger Bands, ADX, Stochastic RSI,
      Support & Resistance (pivot-based).
    """
    log.info(f"  Computing technicals for {symbol}...")
    end   = datetime.today()
    start = end - timedelta(days=180)  # 6 months

    try:
        df = yf.download(symbol, start=start, end=end, progress=False, auto_adjust=True)
        if df.empty or len(df) < 30:
            raise ValueError("Insufficient data")

        close = df["Close"].squeeze()
        high  = df["High"].squeeze()
        low   = df["Low"].squeeze()
        vol   = df["Volume"].squeeze()

        # ── RSI (14) ──────────────────────────────────────────────────────
        delta = close.diff()
        gain  = delta.clip(lower=0)
        loss  = (-delta).clip(lower=0)
        avg_gain = gain.ewm(com=13, adjust=False).mean()
        avg_loss = loss.ewm(com=13, adjust=False).mean()
        rs  = avg_gain / avg_loss.replace(0, np.nan)
        rsi = (100 - (100 / (1 + rs))).iloc[-1]

        # ── MACD (12,26,9) ────────────────────────────────────────────────
        ema12  = close.ewm(span=12, adjust=False).mean()
        ema26  = close.ewm(span=26, adjust=False).mean()
        macd   = ema12 - ema26
        signal = macd.ewm(span=9,  adjust=False).mean()
        macd_val  = round(float(macd.iloc[-1]),  2)
        signal_val= round(float(signal.iloc[-1]),2)
        macd_hist = round(macd_val - signal_val, 2)

        # ── EMA 20 / 50 crossover ────────────────────────────────────────
        ema20 = close.ewm(span=20, adjust=False).mean()
        ema50 = close.ewm(span=50, adjust=False).mean()
        ema_cross = "bullish" if float(ema20.iloc[-1]) > float(ema50.iloc[-1]) else "bearish"

        # ── Bollinger Bands (20, 2σ) ─────────────────────────────────────
        bb_mid   = close.rolling(20).mean()
        bb_std   = close.rolling(20).std()
        bb_upper = bb_mid + 2 * bb_std
        bb_lower = bb_mid - 2 * bb_std
        last_close = float(close.iloc[-1])
        bb_pos = (last_close - float(bb_lower.iloc[-1])) / \
                 (float(bb_upper.iloc[-1]) - float(bb_lower.iloc[-1]) + 1e-9)
        bb_label = "Overbought" if bb_pos > 0.8 else ("Oversold" if bb_pos < 0.2 else "Mid")

        # ── ADX (14) ─────────────────────────────────────────────────────
        plus_dm  = high.diff().clip(lower=0)
        minus_dm = (-low.diff()).clip(lower=0)
        tr       = pd.concat([high - low,
                               (high - close.shift()).abs(),
                               (low  - close.shift()).abs()], axis=1).max(axis=1)
        atr14     = tr.ewm(com=13, adjust=False).mean()
        plus_di   = 100 * plus_dm.ewm(com=13,  adjust=False).mean() / atr14
        minus_di  = 100 * minus_dm.ewm(com=13, adjust=False).mean() / atr14
        dx        = 100 * (plus_di - minus_di).abs() / (plus_di + minus_di).replace(0, np.nan)
        adx_val   = round(float(dx.ewm(com=13, adjust=False).mean().iloc[-1]), 1)

        # ── Stochastic RSI ────────────────────────────────────────────────
        rsi_series = 100 - (100 / (1 + gain.ewm(com=13, adjust=False).mean() /
                                   loss.ewm(com=13, adjust=False).mean().replace(0, np.nan)))
        rsi_min = rsi_series.rolling(14).min()
        rsi_max = rsi_series.rolling(14).max()
        stoch_rsi = float(((rsi_series - rsi_min) / (rsi_max - rsi_min + 1e-9)).iloc[-1])

        # ── Support & Resistance (Pivot Points from last 6m) ─────────────
        pivot   = (float(high.iloc[-1]) + float(low.iloc[-1]) + last_close) / 3
        resist1 = round(2 * pivot - float(low.iloc[-1]),  2)
        support1= round(2 * pivot - float(high.iloc[-1]), 2)
        resist2 = round(pivot + (float(high.iloc[-1]) - float(low.iloc[-1])), 2)
        support2= round(pivot - (float(high.iloc[-1]) - float(low.iloc[-1])), 2)
        # 6-month swing high/low as broader S/R
        swing_high = round(float(high.rolling(120).max().iloc[-1]), 2)
        swing_low  = round(float(low.rolling(120).min().iloc[-1]),  2)

        # ── Overall Signal ────────────────────────────────────────────────
        score = 0
        if rsi < 40:        score += 2
        elif rsi < 55:      score += 1
        elif rsi > 70:      score -= 2
        if macd_hist > 0:   score += 2
        if ema_cross == "bullish": score += 2
        if adx_val > 25:    score += 1
        if stoch_rsi < 0.3: score += 1
        elif stoch_rsi > 0.8: score -= 1

        if   score >= 5:    overall = "STRONG BUY"
        elif score >= 3:    overall = "BUY"
        elif score >= 0:    overall = "NEUTRAL"
        elif score >= -2:   overall = "CAUTION"
        else:               overall = "SELL"

        # Last 7 days of closing prices for sparkline
        spark = [round(float(x), 2) for x in close.iloc[-7:]]

        return {
            "rsi":        round(float(rsi), 1),
            "macd":       macd_val,
            "macd_hist":  macd_hist,
            "ema_cross":  ema_cross,
            "bb_label":   bb_label,
            "adx":        adx_val,
            "stoch_rsi":  round(stoch_rsi, 2),
            "resist1":    resist1,
            "support1":   support1,
            "resist2":    resist2,
            "support2":   support2,
            "swing_high": swing_high,
            "swing_low":  swing_low,
            "last_price": round(last_close, 2),
            "overall":    overall,
            "score":      score,
            "sparkline":  spark,
        }

    except Exception as e:
        log.warning(f"  Failed for {symbol}: {e}")
        return _empty_technicals()


def _empty_technicals() -> dict:
    return {
        "rsi": 50.0, "macd": 0.0, "macd_hist": 0.0,
        "ema_cross": "unknown", "bb_label": "N/A", "adx": 0.0,
        "stoch_rsi": 0.5, "resist1": 0.0, "support1": 0.0,
        "resist2": 0.0, "support2": 0.0, "swing_high": 0.0, "swing_low": 0.0,
        "last_price": 0.0, "overall": "N/A", "score": 0, "sparkline": [],
    }


# ── 3. FETCH NIFTY SUMMARY ────────────────────────────────────────────────────

def fetch_market_summary() -> dict:
    log.info("Fetching Nifty/Sensex data...")
    try:
        nifty  = yf.download("^NSEI",  period="2d", progress=False, auto_adjust=True)
        sensex = yf.download("^BSESN", period="2d", progress=False, auto_adjust=True)

        def pct(df):
            c = df["Close"].squeeze()
            return round(float((c.iloc[-1] - c.iloc[-2]) / c.iloc[-2] * 100), 2)

        def last(df):
            return round(float(df["Close"].squeeze().iloc[-1]), 2)

        return {
            "nifty_price":  last(nifty),
            "nifty_chg":    pct(nifty),
            "sensex_price": last(sensex),
            "sensex_chg":   pct(sensex),
        }
    except Exception as e:
        log.warning(f"Market summary failed: {e}")
        return {"nifty_price": 0, "nifty_chg": 0, "sensex_price": 0, "sensex_chg": 0}


# ── 4. BUILD ENRICHED DATASET ─────────────────────────────────────────────────

def build_dataset() -> list[dict]:
    raw_stocks = fetch_fii_dii_stocks()
    market     = fetch_market_summary()
    enriched   = []

    for s in raw_stocks:
        tech = compute_technicals(s["symbol"])
        # Determine combined signal badge
        both_buy  = s["fii_cash"] == "buy" and s["dii_cash"] == "buy"
        fii_only  = s["fii_cash"] == "buy" and s["dii_cash"] != "buy"
        dii_only  = s["dii_cash"] == "buy" and s["fii_cash"] != "buy"
        both_sell = s["fii_cash"] != "buy" and s["dii_cash"] != "buy"

        if both_buy:   inst_signal = "BOTH BUY"
        elif fii_only: inst_signal = "FII BUY"
        elif dii_only: inst_signal = "DII BUY"
        else:          inst_signal = "SELL"

        enriched.append({**s, **tech,
                         "inst_signal": inst_signal,
                         "both_buy": both_buy,
                         "fii_only": fii_only,
                         "dii_only": dii_only,
                         "both_sell": both_sell})

    return enriched, market


# ── 5. HTML REPORT GENERATOR ──────────────────────────────────────────────────

def signal_class(sig: str) -> str:
    return {
        "STRONG BUY": "signal-strong-buy",
        "BUY":        "signal-buy",
        "NEUTRAL":    "signal-neutral",
        "CAUTION":    "signal-caution",
        "SELL":       "signal-sell",
        "N/A":        "signal-neutral",
    }.get(sig, "signal-neutral")


def inst_badge_class(sig: str) -> str:
    return {
        "BOTH BUY": "badge-both",
        "FII BUY":  "badge-fii",
        "DII BUY":  "badge-dii",
        "SELL":     "badge-sell",
    }.get(sig, "badge-sell")


def rsi_class(val: float) -> str:
    if val > 70:  return "rsi-overbought"
    if val < 40:  return "rsi-oversold"
    return "rsi-neutral"


def spark_svg(prices: list) -> str:
    """Generate a tiny inline SVG sparkline from price list."""
    if len(prices) < 2:
        return ""
    mn, mx = min(prices), max(prices)
    rng = mx - mn if mx != mn else 1
    w, h = 70, 26
    pts = []
    for i, p in enumerate(prices):
        x = round(i * w / (len(prices) - 1), 1)
        y = round(h - (p - mn) / rng * h, 1)
        pts.append(f"{x},{y}")
    color = "#00d4aa" if prices[-1] >= prices[0] else "#ff4d6d"
    return (f'<svg width="{w}" height="{h}" viewBox="0 0 {w} {h}" '
            f'xmlns="http://www.w3.org/2000/svg">'
            f'<polyline points="{" ".join(pts)}" fill="none" '
            f'stroke="{color}" stroke-width="1.8" stroke-linejoin="round"/>'
            f'</svg>')


def generate_html(stocks: list[dict], market: dict, date_str: str) -> str:
    nifty_color  = "up" if market["nifty_chg"]  >= 0 else "down"
    sensex_color = "up" if market["sensex_chg"] >= 0 else "down"
    nifty_arrow  = "▲" if market["nifty_chg"]  >= 0 else "▼"
    sensex_arrow = "▲" if market["sensex_chg"] >= 0 else "▼"

    fii_buys  = sum(1 for s in stocks if s["fii_cash"] == "buy")
    dii_buys  = sum(1 for s in stocks if s["dii_cash"] == "buy")
    both_buys = sum(1 for s in stocks if s["both_buy"])
    strong    = sum(1 for s in stocks if s["overall"] == "STRONG BUY")

    rows_html = ""
    for s in stocks:
        sc   = signal_class(s["overall"])
        bc   = inst_badge_class(s["inst_signal"])
        rc   = rsi_class(s["rsi"])
        spk  = spark_svg(s.get("sparkline", []))
        fii_arrow = "▲ BUY" if s["fii_cash"] == "buy" else "▼ SELL"
        dii_arrow = "▲ BUY" if s["dii_cash"] == "buy" else "▼ SELL"
        fii_bc = "badge-fii" if s["fii_cash"] == "buy" else "badge-sell"
        dii_bc = "badge-dii" if s["dii_cash"] == "buy" else "badge-sell"
        macd_color = "up" if s["macd_hist"] > 0 else "down"
        ema_color  = "up" if s["ema_cross"] == "bullish" else "down"

        rows_html += f"""
        <tr>
          <td>
            <div class="stock-name">{s['name']}</div>
            <div class="stock-sym">{s['symbol'].replace('.NS','')}</div>
          </td>
          <td class="price-cell">
            <div class="price-main">₹{s['last_price']:,}</div>
            <div class="spark">{spk}</div>
          </td>
          <td class="act-cell"><span class="badge {fii_bc}">{fii_arrow}</span></td>
          <td class="act-cell"><span class="badge {dii_bc}">{dii_arrow}</span></td>
          <td class="rsi-cell">
            <div class="{rc}">
              <span class="rsi-num {'up' if s['rsi']<55 else 'down'}">{s['rsi']}</span>
              <div class="rsi-bar-bg"><div class="rsi-bar-fill" style="width:{min(s['rsi'],100)}%"></div></div>
            </div>
          </td>
          <td>
            <div class="ind-mini">
              <span class="ind-lbl">MACD</span>
              <span class="{macd_color}">{'+' if s['macd_hist']>0 else ''}{s['macd_hist']}</span>
            </div>
            <div class="ind-mini">
              <span class="ind-lbl">EMA</span>
              <span class="{ema_color}">{'↑ Bull' if s['ema_cross']=='bullish' else '↓ Bear'}</span>
            </div>
            <div class="ind-mini">
              <span class="ind-lbl">ADX</span>
              <span>{s['adx']}</span>
            </div>
          </td>
          <td>
            <div class="sr-row"><span class="sr-r">R1</span> ₹{s['resist1']:,}</div>
            <div class="sr-row"><span class="sr-s">S1</span> ₹{s['support1']:,}</div>
            <div class="sr-row"><span class="sr-r">6m H</span> ₹{s['swing_high']:,}</div>
            <div class="sr-row"><span class="sr-s">6m L</span> ₹{s['swing_low']:,}</div>
          </td>
          <td class="sig-cell"><span class="signal {sc}">{s['overall']}</span></td>
        </tr>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>FII/DII Pulse — {date_str}</title>
<link href="https://fonts.googleapis.com/css2?family=Space+Mono:wght@400;700&family=Syne:wght@400;600;700;800&display=swap" rel="stylesheet">
<style>
  :root {{
    --bg:#050c14; --surface:#0b1623; --card:#0f1e2e;
    --border:#1a3050; --fii:#00d4aa; --dii:#ff8c42;
    --both:#a78bfa; --sell:#ff4d6d; --text:#e2eaf4;
    --muted:#5a7a99; --gold:#f0c060; --green:#00d4aa;
    --red:#ff4d6d; --neon:#00f5d4;
  }}
  * {{ margin:0; padding:0; box-sizing:border-box; }}
  body {{ background:var(--bg); color:var(--text); font-family:'Syne',sans-serif;
          min-height:100vh; }}
  body::before {{ content:''; position:fixed; inset:0;
    background-image:linear-gradient(rgba(0,212,170,.03) 1px,transparent 1px),
      linear-gradient(90deg,rgba(0,212,170,.03) 1px,transparent 1px);
    background-size:40px 40px; pointer-events:none; z-index:0; }}
  .wrap {{ position:relative; z-index:1; }}

  /* HEADER */
  header {{ background:linear-gradient(135deg,#050c14,#0a1930,#050c14);
    border-bottom:1px solid var(--border); padding:18px 32px;
    display:flex; align-items:center; justify-content:space-between; }}
  .logo {{ display:flex; align-items:center; gap:12px; }}
  .logo-icon {{ width:42px; height:42px;
    background:linear-gradient(135deg,var(--fii),var(--both));
    border-radius:10px; display:flex; align-items:center;
    justify-content:center; font-size:20px;
    box-shadow:0 0 20px rgba(0,212,170,.4); }}
  .logo-title {{ font-size:20px; font-weight:800; letter-spacing:1px; }}
  .logo-title span {{ color:var(--fii); }}
  .logo-sub {{ font-size:10px; color:var(--muted); letter-spacing:1px; margin-top:2px; }}
  .live-badge {{ display:flex; align-items:center; gap:6px;
    background:rgba(0,212,170,.1); border:1px solid rgba(0,212,170,.3);
    padding:6px 14px; border-radius:20px; font-size:12px; font-weight:700;
    color:var(--fii); letter-spacing:1px; }}
  .live-dot {{ width:7px; height:7px; border-radius:50%;
    background:var(--fii); animation:pulse 1.5s infinite; }}
  @keyframes pulse {{ 0%,100%{{opacity:1;transform:scale(1)}} 50%{{opacity:.4;transform:scale(.8)}} }}
  .date-tag {{ font-family:'Space Mono',monospace; font-size:12px; color:var(--muted); }}

  /* SUMMARY */
  .summary {{ display:grid; grid-template-columns:repeat(4,1fr);
    background:var(--border); gap:1px; border-bottom:1px solid var(--border); }}
  .s-card {{ background:var(--surface); padding:18px 24px; }}
  .s-label {{ font-size:10px; letter-spacing:2px; color:var(--muted);
    text-transform:uppercase; margin-bottom:6px; }}
  .s-val {{ font-family:'Space Mono',monospace; font-size:22px; font-weight:700; }}
  .s-sub {{ font-size:11px; color:var(--muted); margin-top:4px; }}
  .c-fii{{color:var(--fii)}} .c-dii{{color:var(--dii)}} .c-both{{color:var(--both)}} .c-gold{{color:var(--gold)}}
  .up{{color:var(--green)}} .down{{color:var(--red)}}

  /* TABLE */
  .table-wrap {{ padding:24px 32px; overflow-x:auto; }}
  .sec-title {{ font-size:12px; letter-spacing:2px; text-transform:uppercase;
    color:var(--muted); margin-bottom:18px;
    display:flex; align-items:center; gap:10px; }}
  .sec-title::before {{ content:''; width:3px; height:16px;
    background:var(--fii); border-radius:2px; }}
  table {{ width:100%; border-collapse:collapse; min-width:900px; }}
  thead tr {{ border-bottom:2px solid var(--border); }}
  th {{ padding:10px 12px; text-align:left; font-size:10px;
    letter-spacing:1.5px; color:var(--muted); text-transform:uppercase; }}
  th:not(:first-child) {{ text-align:center; }}
  tbody tr {{ border-bottom:1px solid rgba(26,48,80,.4);
    transition:background .15s; animation:slideIn .4s ease forwards; opacity:0; }}
  @keyframes slideIn {{ from{{opacity:0;transform:translateX(-8px)}} to{{opacity:1;transform:translateX(0)}} }}
  tbody tr:nth-child(1){{animation-delay:.05s}} tbody tr:nth-child(2){{animation-delay:.1s}}
  tbody tr:nth-child(3){{animation-delay:.15s}} tbody tr:nth-child(4){{animation-delay:.2s}}
  tbody tr:nth-child(5){{animation-delay:.25s}} tbody tr:nth-child(6){{animation-delay:.3s}}
  tbody tr:nth-child(7){{animation-delay:.35s}} tbody tr:nth-child(8){{animation-delay:.4s}}
  tbody tr:nth-child(9){{animation-delay:.45s}} tbody tr:nth-child(10){{animation-delay:.5s}}
  tbody tr:hover {{ background:rgba(0,212,170,.03); }}
  td {{ padding:13px 12px; font-size:13px; vertical-align:middle; text-align:center; }}
  td:first-child {{ text-align:left; }}

  .stock-name {{ font-weight:700; font-size:14px; }}
  .stock-sym {{ font-size:10px; color:var(--muted); font-family:'Space Mono',monospace;
    letter-spacing:.5px; margin-top:2px; }}

  .price-cell {{ text-align:center; }}
  .price-main {{ font-family:'Space Mono',monospace; font-size:13px; font-weight:700; }}
  .spark {{ margin-top:4px; }}

  .badge {{ display:inline-block; padding:4px 9px; border-radius:5px;
    font-size:10px; font-weight:800; letter-spacing:.5px; }}
  .badge-fii {{ background:rgba(0,212,170,.12); color:var(--fii); border:1px solid rgba(0,212,170,.25); }}
  .badge-dii {{ background:rgba(255,140,66,.12); color:var(--dii); border:1px solid rgba(255,140,66,.25); }}
  .badge-both {{ background:rgba(167,139,250,.15); color:var(--both); border:1px solid rgba(167,139,250,.3); }}
  .badge-sell {{ background:rgba(255,77,109,.1); color:var(--sell); border:1px solid rgba(255,77,109,.25); }}

  .rsi-num {{ font-family:'Space Mono',monospace; font-size:13px; font-weight:700; }}
  .rsi-bar-bg {{ width:80px; height:4px; background:var(--border);
    border-radius:2px; margin:5px auto 0; overflow:hidden; }}
  .rsi-bar-fill {{ height:100%; border-radius:2px; }}
  .rsi-overbought .rsi-bar-fill {{ background:var(--red); }}
  .rsi-neutral    .rsi-bar-fill {{ background:var(--gold); }}
  .rsi-oversold   .rsi-bar-fill {{ background:var(--green); }}

  .ind-mini {{ display:flex; justify-content:center; gap:5px;
    font-size:11px; margin-bottom:3px; }}
  .ind-lbl {{ color:var(--muted); font-size:10px; }}

  .sr-row {{ font-size:10px; font-family:'Space Mono',monospace;
    display:flex; align-items:center; gap:4px; justify-content:center;
    margin-bottom:2px; }}
  .sr-r {{ font-size:9px; padding:1px 5px; border-radius:3px; font-weight:700;
    background:rgba(255,77,109,.2); color:var(--red); }}
  .sr-s {{ font-size:9px; padding:1px 5px; border-radius:3px; font-weight:700;
    background:rgba(0,212,170,.15); color:var(--green); }}

  .signal {{ display:inline-block; padding:5px 12px; border-radius:5px;
    font-size:10px; font-weight:800; letter-spacing:.5px; }}
  .signal-strong-buy {{ background:rgba(0,212,170,.2); color:var(--green);
    border:1px solid rgba(0,212,170,.5); box-shadow:0 0 10px rgba(0,212,170,.2); }}
  .signal-buy     {{ background:rgba(0,245,212,.1); color:#4ad9c8; border:1px solid rgba(0,245,212,.25); }}
  .signal-neutral {{ background:rgba(240,192,96,.1); color:var(--gold); border:1px solid rgba(240,192,96,.25); }}
  .signal-caution {{ background:rgba(255,140,66,.1); color:var(--dii); border:1px solid rgba(255,140,66,.3); }}
  .signal-sell    {{ background:rgba(255,77,109,.12); color:var(--red); border:1px solid rgba(255,77,109,.3); }}

  /* LEGEND */
  .legend {{ padding:0 32px 24px; display:flex; gap:16px; flex-wrap:wrap; }}
  .leg-item {{ display:flex; align-items:center; gap:6px; font-size:11px; color:var(--muted); }}
  .leg-dot {{ width:10px; height:10px; border-radius:50%; }}

  /* FOOTER */
  footer {{ background:var(--surface); border-top:1px solid var(--border);
    padding:14px 32px; display:flex; justify-content:space-between;
    font-size:11px; color:var(--muted); flex-wrap:wrap; gap:8px; }}
  .disclaimer {{ font-style:italic; font-size:10px; }}
</style>
</head>
<body>
<div class="wrap">
<header>
  <div class="logo">
    <div class="logo-icon">📊</div>
    <div>
      <div class="logo-title">FII<span>/DII</span> PULSE</div>
      <div class="logo-sub">INSTITUTIONAL INTELLIGENCE DASHBOARD · AUTO-GENERATED</div>
    </div>
  </div>
  <div style="display:flex;align-items:center;gap:16px;">
    <div class="live-badge"><div class="live-dot"></div> LIVE DATA</div>
    <div class="date-tag">📅 {date_str}</div>
  </div>
</header>

<div class="summary">
  <div class="s-card">
    <div class="s-label">Nifty 50</div>
    <div class="s-val c-fii">₹{market['nifty_price']:,}</div>
    <div class="s-sub {nifty_color}">{nifty_arrow} {market['nifty_chg']}%</div>
  </div>
  <div class="s-card">
    <div class="s-label">Sensex</div>
    <div class="s-val c-dii">₹{market['sensex_price']:,}</div>
    <div class="s-sub {sensex_color}">{sensex_arrow} {market['sensex_chg']}%</div>
  </div>
  <div class="s-card">
    <div class="s-label">Stocks Tracked</div>
    <div class="s-val c-both">{len(stocks)}</div>
    <div class="s-sub">FII Buys: {fii_buys} · DII Buys: {dii_buys} · Both: {both_buys}</div>
  </div>
  <div class="s-card">
    <div class="s-label">Strong Buy Signals</div>
    <div class="s-val c-gold">{strong}</div>
    <div class="s-sub">Stocks with STRONG BUY rating</div>
  </div>
</div>

<div class="table-wrap">
  <div class="sec-title">Institutional Stock Activity with Technical Analysis · {date_str}</div>
  <table>
    <thead>
      <tr>
        <th>Stock</th>
        <th>Price / Trend</th>
        <th>FII Cash</th>
        <th>DII Cash</th>
        <th>RSI (14)</th>
        <th>MACD · EMA · ADX</th>
        <th>Support / Resistance (6M)</th>
        <th>Signal</th>
      </tr>
    </thead>
    <tbody>{rows_html}</tbody>
  </table>
</div>

<div class="legend">
  <div class="leg-item"><div class="leg-dot" style="background:var(--fii)"></div> FII Buying</div>
  <div class="leg-item"><div class="leg-dot" style="background:var(--dii)"></div> DII Buying</div>
  <div class="leg-item"><div class="leg-dot" style="background:var(--both)"></div> Both Buying</div>
  <div class="leg-item"><div class="leg-dot" style="background:var(--red)"></div> Selling</div>
  <div class="leg-item" style="margin-left:auto;font-size:10px;">RSI: &lt;40 Oversold · &gt;70 Overbought · MACD: + Bullish · ADX: &gt;25 Strong Trend</div>
</div>

<footer>
  <div>🤖 Auto-generated by FII/DII Pulse · Data: NSE/MunafaSutra · Technicals: yfinance + pandas</div>
  <div class="disclaimer">⚠️ Not financial advice. For educational purposes only. Always DYOR.</div>
</footer>
</div>
</body>
</html>"""
    return html


# ── 6. SEND EMAIL ─────────────────────────────────────────────────────────────

def send_email(html_content: str, date_str: str, html_path: Path):
    gmail_user = os.getenv("GMAIL_USER")
    gmail_pass = os.getenv("GMAIL_PASS")
    recipients = os.getenv("RECIPIENT_EMAIL", gmail_user)

    if not gmail_user or not gmail_pass:
        log.warning("GMAIL_USER / GMAIL_PASS not set — skipping email")
        return

    to_list = [r.strip() for r in recipients.split(",")]
    log.info(f"Sending email to: {to_list}")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = f"📊 FII/DII Intelligence Report — {date_str}"
    msg["From"]    = f"FII/DII Pulse <{gmail_user}>"
    msg["To"]      = ", ".join(to_list)

    # Inline HTML body (truncated for email; full file attached)
    body = f"""
    <html><body style="font-family:sans-serif;background:#050c14;color:#e2eaf4;padding:20px;">
      <h2 style="color:#00d4aa;">📊 FII/DII Pulse Report — {date_str}</h2>
      <p>Your daily institutional intelligence report is ready.</p>
      <p>Please open the attached <strong>fii_dii_report_{date_str}.html</strong>
         file in your browser for the full interactive dashboard.</p>
      <p>Or view it online (if GitHub Pages enabled):
         <a href="https://YOUR_GITHUB_USERNAME.github.io/fii-dii-dashboard/"
            style="color:#00d4aa;">GitHub Pages Link</a></p>
      <hr style="border-color:#1a3050;margin:16px 0;">
      <p style="font-size:11px;color:#5a7a99;">
        ⚠️ Not financial advice. For educational purposes only.<br>
        Auto-generated by FII/DII Pulse · Data: NSE/MunafaSutra
      </p>
    </body></html>"""

    msg.attach(MIMEText(body, "html"))

    # Attach full HTML report
    with open(html_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=html_path.name)
        part["Content-Disposition"] = f'attachment; filename="{html_path.name}"'
        msg.attach(part)

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(gmail_user, gmail_pass)
            server.sendmail(gmail_user, to_list, msg.as_string())
        log.info("✅ Email sent successfully!")
    except Exception as e:
        log.error(f"❌ Email failed: {e}")
        raise


# ── 7. MAIN ───────────────────────────────────────────────────────────────────

def main():
    date_str = datetime.today().strftime("%d %b %Y")
    date_file = datetime.today().strftime("%Y-%m-%d")

    log.info("=" * 60)
    log.info(f"FII/DII Pulse — {date_str}")
    log.info("=" * 60)

    # Build data
    stocks, market = build_dataset()

    # Generate HTML
    html = generate_html(stocks, market, date_str)

    # Save to docs/ (GitHub Pages) as index.html (latest) + dated copy
    index_path = OUTPUT_DIR / "index.html"
    dated_path = OUTPUT_DIR / f"report_{date_file}.html"

    for path in [index_path, dated_path]:
        path.write_text(html, encoding="utf-8")
        log.info(f"Saved: {path}")

    # Send email
    send_email(html, date_str, dated_path)

    log.info("✅ Done!")


if __name__ == "__main__":
    main()
