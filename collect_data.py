#!/usr/bin/env python3
"""
₿ Polymarket BTC 5-Min TICK RECORDER

Records EVERYTHING needed to backtest with realistic fill simulation.

Data captured EVERY tick (default 3s):
  ┌─────────────────────────────────────────────────────────────────┐
  │  POLYMARKET (per market window)                                 │
  │    • UP + DOWN token midpoint prices                            │
  │    • FULL orderbook: all bids & asks, both UP and DOWN tokens   │
  │    • Spread, depth levels, total liquidity per side             │
  │    • Gamma API prices (for resolution detection)                │
  │    • Market volume, liquidity, condition_id                     │
  ├─────────────────────────────────────────────────────────────────┤
  │  BINANCE                                                        │
  │    • Real-time spot price (ticker/price endpoint)               │
  │    • Current 5m candle OHLCV + seconds into candle              │
  │    • 1m candle for momentum                                     │
  │    • RSI-14, EMA-9/21, Bollinger Bands, ATR, VWAP              │
  │    • Funding rate, taker buy/sell ratio                          │
  ├─────────────────────────────────────────────────────────────────┤
  │  WINDOW TRACKING                                                │
  │    • Pre-window: captures data 60s BEFORE window opens          │
  │    • In-window: every tick while window is active                │
  │    • Post-window: captures resolution ~30s after window closes  │
  │    • Window lifecycle: UPCOMING → ACTIVE → RESOLVING → RESOLVED │
  └─────────────────────────────────────────────────────────────────┘

Output: Two files per run:
  1. ticks_{date}.jsonl     — every tick, every market (for replay)
  2. windows_{date}.jsonl   — one row per resolved window (for quick analysis)

Needs ~1000 windows for statistical confidence.
  5-min windows = 288/day → ~3.5 days minimum, 10 days ideal.

Usage:
    python collect_data.py                    # Default: 3s tick interval
    python collect_data.py --interval 2       # 2s ticks (more granular)
    python collect_data.py --interval 5       # 5s ticks (less API load)
"""

import os
import sys
import json
import time
import signal
import argparse
import requests
import threading
import gzip
import glob
from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime, timezone, timedelta
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

DATA_DIR = os.environ.get("DATA_DIR",
    os.path.join(os.path.dirname(os.path.abspath(__file__)), "data"))


# ═══════════════════════════════════════════════════════════════════════════════
#  HTTP File Server (for downloading data from Railway)
# ═══════════════════════════════════════════════════════════════════════════════

class DataFileHandler(BaseHTTPRequestHandler):
    """Serves data files for download via browser."""

    def log_message(self, fmt, *args):
        pass  # Suppress default access logs

    def do_GET(self):
        data_dir = DATA_DIR

        if self.path == "/" or self.path == "/data":
            # List all data files
            files = []
            if os.path.isdir(data_dir):
                for f in sorted(os.listdir(data_dir)):
                    fpath = os.path.join(data_dir, f)
                    if os.path.isfile(fpath):
                        size_mb = os.path.getsize(fpath) / (1024 * 1024)
                        files.append(f"<a href='/data/{f}'>{f}</a> ({size_mb:.1f} MB)")
            html = "<h2>₿ Recorded Data Files</h2><ul>"
            html += "".join(f"<li>{f}</li>" for f in files)
            html += "</ul><p>Click to download.</p>"
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(html.encode())
            return

        if self.path.startswith("/data/"):
            filename = self.path.split("/data/")[-1]
            # Security: only serve files, no path traversal
            if "/" in filename or ".." in filename:
                self.send_error(403)
                return
            filepath = os.path.join(data_dir, filename)
            if os.path.isfile(filepath):
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Disposition",
                                 f"attachment; filename={filename}")
                self.send_header("Content-Length", str(os.path.getsize(filepath)))
                self.end_headers()
                with open(filepath, "rb") as f:
                    self.wfile.write(f.read())
                return

        if self.path.startswith("/delete/"):
            # Require secret key: /delete/filename?key=YOUR_SECRET
            delete_key = os.environ.get("DELETE_KEY", "")
            if not delete_key:
                self.send_error(403, "Set DELETE_KEY env variable first")
                return
            if f"?key={delete_key}" not in self.path:
                self.send_error(403, "Invalid key")
                return
            filename = self.path.split("/delete/")[-1].split("?")[0]
            if "/" in filename or ".." in filename:
                self.send_error(403)
                return
            filepath = os.path.join(data_dir, filename)
            if os.path.isfile(filepath):
                size_mb = os.path.getsize(filepath) / (1024 * 1024)
                os.remove(filepath)
                html = f"<h2>Deleted {filename} ({size_mb:.1f} MB)</h2>"
                html += "<a href='/'>Back to files</a>"
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                self.wfile.write(html.encode())
                return

        self.send_error(404)


def start_http_server(port: int = 8080):
    """Start HTTP file server in a daemon thread."""
    server = HTTPServer(("0.0.0.0", port), DataFileHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    print(f"  📡 HTTP server on port {port} — download files at /data/")
    return server


# ═══════════════════════════════════════════════════════════════════════════════
#  Binance Data
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_spot_price() -> float:
    """Real-time BTC/USDT spot price with retry + fallback."""
    # Try Binance first (fastest)
    for attempt in range(3):
        try:
            r = requests.get(
                "https://api.binance.com/api/v3/ticker/price?symbol=BTCUSDT",
                timeout=2,
            )
            if r.status_code == 200:
                price = float(r.json()["price"])
                if price > 1000:
                    return price
        except Exception:
            pass
        time.sleep(0.2)

    # Fallback: Coinbase
    try:
        r = requests.get(
            "https://api.coinbase.com/v2/prices/BTC-USD/spot",
            timeout=3,
        )
        if r.status_code == 200:
            price = float(r.json()["data"]["amount"])
            if price > 1000:
                return price
    except Exception:
        pass
    return 0.0


def fetch_candle_snapshot() -> dict:
    """Current 5m + 1m candle snapshot from Binance."""
    data = {}
    try:
        # 5m candle
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "5m", "limit": 2},
            timeout=3,
        )
        if r.status_code == 200:
            klines = r.json()
            if klines:
                cur = klines[-1]
                data["candle_5m_open"] = float(cur[1])
                data["candle_5m_high"] = float(cur[2])
                data["candle_5m_low"] = float(cur[3])
                data["candle_5m_close"] = float(cur[4])
                data["candle_5m_vol"] = float(cur[5])
                data["candle_5m_open_ts"] = int(cur[0])
                if len(klines) > 1:
                    prev = klines[-2]
                    data["prev_candle_5m_open"] = float(prev[1])
                    data["prev_candle_5m_close"] = float(prev[4])
                    data["prev_candle_5m_vol"] = float(prev[5])

        # 1m candle (for sub-5m momentum)
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "1m", "limit": 3},
            timeout=3,
        )
        if r.status_code == 200:
            klines = r.json()
            if klines:
                cur = klines[-1]
                data["candle_1m_open"] = float(cur[1])
                data["candle_1m_close"] = float(cur[4])
                data["candle_1m_vol"] = float(cur[5])
    except Exception:
        pass
    return data


def fetch_technicals() -> dict:
    """Full technicals snapshot — RSI, EMAs, BB, ATR, VWAP."""
    data = {}
    try:
        r = requests.get(
            "https://api.binance.com/api/v3/klines",
            params={"symbol": "BTCUSDT", "interval": "5m", "limit": 50},
            timeout=5,
        )
        if r.status_code != 200:
            return data
        klines = r.json()
        closes = [float(k[4]) for k in klines]
        volumes = [float(k[5]) for k in klines]

        # RSI-14
        if len(closes) >= 15:
            gains, losses = [], []
            for i in range(1, len(closes)):
                d = closes[i] - closes[i - 1]
                gains.append(max(d, 0))
                losses.append(max(-d, 0))
            ag = sum(gains[-14:]) / 14
            al = sum(losses[-14:]) / 14
            data["rsi_14"] = round(100 - (100 / (1 + ag / al)), 2) if al > 0 else 100.0

        # EMAs
        def ema(vals, p):
            if len(vals) < p:
                return sum(vals) / len(vals)
            k = 2 / (p + 1)
            e = sum(vals[:p]) / p
            for v in vals[p:]:
                e = v * k + e * (1 - k)
            return round(e, 2)

        data["ema_9"] = ema(closes, 9)
        data["ema_21"] = ema(closes, 21)

        # Bollinger Bands
        if len(closes) >= 20:
            window = closes[-20:]
            mean = sum(window) / 20
            std = (sum((x - mean) ** 2 for x in window) / 20) ** 0.5
            data["bb_upper"] = round(mean + 2 * std, 2)
            data["bb_lower"] = round(mean - 2 * std, 2)

        # ATR-14
        if len(klines) >= 15:
            trs = []
            for i in range(1, len(klines)):
                h, l = float(klines[i][2]), float(klines[i][3])
                pc = float(klines[i - 1][4])
                trs.append(max(h - l, abs(h - pc), abs(l - pc)))
            data["atr_14"] = round(sum(trs[-14:]) / 14, 2)

        # VWAP
        total_pv = sum(
            (float(k[2]) + float(k[3]) + float(k[4])) / 3 * float(k[5])
            for k in klines
        )
        total_vol = sum(float(k[5]) for k in klines)
        data["vwap"] = round(total_pv / total_vol, 2) if total_vol > 0 else 0

        # Vol ratio
        avg_vol = sum(volumes[-11:-1]) / 10 if len(volumes) >= 11 else 1
        data["vol_ratio_5m"] = round(volumes[-1] / avg_vol, 2) if avg_vol > 0 else 1

    except Exception:
        pass
    return data


def fetch_funding_rate() -> float:
    """Binance BTCUSDT perps funding rate."""
    try:
        r = requests.get(
            "https://fapi.binance.com/fapi/v1/fundingRate",
            params={"symbol": "BTCUSDT", "limit": 1}, timeout=3,
        )
        if r.status_code == 200 and r.json():
            return round(float(r.json()[0].get("fundingRate", 0)), 6)
    except Exception:
        pass
    return 0.0


def fetch_taker_ratio() -> float:
    """Binance taker buy/sell volume ratio."""
    try:
        r = requests.get(
            "https://fapi.binance.com/futures/data/takerlongshortRatio",
            params={"symbol": "BTCUSDT", "period": "5m", "limit": 1}, timeout=3,
        )
        if r.status_code == 200 and r.json():
            return round(float(r.json()[0].get("buySellRatio", 1.0)), 4)
    except Exception:
        pass
    return 1.0


def fetch_chainlink_btc_price() -> float:
    """
    Fetch BTC/USD price from Chainlink oracle on Polygon.
    This is the EXACT price source that resolves Polymarket BTC markets.
    Contract: 0xc907E116054Ad103354f2D350FD2514433D57F6f (Polygon PoS)
    Method: latestRoundData() → (roundId, answer, startedAt, updatedAt, answeredInRound)
    Answer has 8 decimals.
    """
    CHAINLINK_BTC_USD = "0xc907E116054Ad103354f2D350FD2514433D57F6f"
    # latestRoundData() function selector
    FUNC_SELECTOR = "0xfeaf968c"

    # Try multiple Polygon RPC endpoints
    rpcs = [
        "https://polygon-rpc.com",
        "https://rpc-mainnet.matic.quiknode.pro",
        "https://polygon-mainnet.g.alchemy.com/v2/demo",
    ]

    for rpc in rpcs:
        try:
            r = requests.post(
                rpc,
                json={
                    "jsonrpc": "2.0",
                    "method": "eth_call",
                    "params": [
                        {"to": CHAINLINK_BTC_USD, "data": FUNC_SELECTOR},
                        "latest",
                    ],
                    "id": 1,
                },
                timeout=3,
            )
            if r.status_code == 200:
                result = r.json().get("result", "")
                if result and len(result) >= 130:
                    # answer is the 2nd 32-byte word (bytes 66-130)
                    answer_hex = result[66:130]
                    answer = int(answer_hex, 16)
                    # Chainlink BTC/USD has 8 decimals
                    price = answer / 1e8
                    if price > 1000:  # sanity check
                        return round(price, 2)
        except Exception:
            continue
    return 0.0


# ═══════════════════════════════════════════════════════════════════════════════
#  Polymarket Data — FULL DEPTH
# ═══════════════════════════════════════════════════════════════════════════════

def fetch_full_book(token_id: str) -> dict:
    """
    Fetch FULL orderbook for a token: all bids, all asks, with price + size.
    This is the critical data for simulating realistic fills.
    """
    try:
        r = requests.get(
            "https://clob.polymarket.com/book",
            params={"token_id": token_id},
            timeout=3,
        )
        if r.status_code == 200:
            book = r.json()
            bids = [
                {"p": float(b["price"]), "s": float(b["size"])}
                for b in book.get("bids", [])
            ]
            asks = [
                {"p": float(a["price"]), "s": float(a["size"])}
                for a in book.get("asks", [])
            ]
            return {"bids": bids, "asks": asks}
    except Exception:
        pass
    return {"bids": [], "asks": []}


def fetch_midpoint(token_id: str) -> float:
    """Fetch CLOB midpoint price for a single token."""
    try:
        r = requests.get(
            "https://clob.polymarket.com/midpoint",
            params={"token_id": token_id},
            timeout=2,
        )
        if r.status_code == 200:
            return float(r.json().get("mid", 0.5))
    except Exception:
        pass
    return 0.5


def discover_windows(look_ahead_minutes: int = 15, look_behind: int = 1) -> list[dict]:
    """
    Discover BTC 5-min market windows.
    Returns upcoming, active, AND recently-closed windows for resolution tracking.
    """
    now = datetime.now(timezone.utc)
    ts_now = int(now.timestamp())
    ts_5m = ts_now - (ts_now % 300)

    windows = []
    # Look behind for resolution capture, ahead for upcoming
    for offset in range(-look_behind - 1, look_ahead_minutes // 5 + 2):
        check_ts = ts_5m + (offset * 300)
        slug = f"btc-updown-5m-{check_ts}"

        try:
            resp = requests.get(
                f"https://gamma-api.polymarket.com/events?slug={slug}",
                timeout=3,
            )
            if resp.status_code != 200 or not resp.json():
                continue

            event = resp.json()[0]
            event_markets = event.get("markets", [])
            if not event_markets:
                continue

            mkt = event_markets[0]
            end_date = mkt.get("endDate", "")
            condition_id = mkt.get("conditionId", "")

            try:
                end_dt = datetime.fromisoformat(end_date.replace("Z", "+00:00"))
                seconds_left = (end_dt - now).total_seconds()
            except Exception:
                seconds_left = 0

            # Skip windows more than 2 minutes past resolution
            if seconds_left < -120:
                continue

            # Token IDs
            tokens_str = mkt.get("clobTokenIds", "")
            try:
                token_ids = eval(tokens_str) if isinstance(tokens_str, str) else tokens_str
                up_token = str(token_ids[0]) if token_ids else ""
                down_token = str(token_ids[1]) if len(token_ids) > 1 else ""
            except Exception:
                up_token, down_token = "", ""

            # Gamma prices (for resolution detection)
            gamma_up, gamma_down = 0.5, 0.5
            prices_str = mkt.get("outcomePrices", "[]")
            try:
                prices = eval(prices_str) if isinstance(prices_str, str) else prices_str
                gamma_up = float(prices[0])
                gamma_down = float(prices[1])
            except Exception:
                pass

            # Window lifecycle state
            if seconds_left > 300:
                state = "UPCOMING"
            elif seconds_left > 0:
                state = "ACTIVE"
            elif gamma_up > 0.95 or gamma_down > 0.95:
                state = "RESOLVED"
            else:
                state = "RESOLVING"

            resolution = ""
            if gamma_up > 0.95:
                resolution = "UP"
            elif gamma_down > 0.95:
                resolution = "DOWN"

            windows.append({
                "slug": slug,
                "condition_id": condition_id,
                "question": mkt.get("question", ""),
                "start_ts": check_ts,
                "end_ts": check_ts + 300,
                "end_date": end_date,
                "seconds_left": round(seconds_left, 1),
                "state": state,
                "up_token": up_token,
                "down_token": down_token,
                "gamma_up": gamma_up,
                "gamma_down": gamma_down,
                "resolution": resolution,
                "volume": float(event.get("volume", 0)),
                "liquidity": float(event.get("liquidity", 0)),
            })

        except Exception:
            continue

    return windows


# ═══════════════════════════════════════════════════════════════════════════════
#  Book Summary (compact representation for quick analysis)
# ═══════════════════════════════════════════════════════════════════════════════

def summarize_book(book: dict) -> dict:
    """Create a compact summary of an orderbook for the tick log."""
    bids = book.get("bids", [])
    asks = book.get("asks", [])

    best_bid = bids[0]["p"] if bids else 0
    best_ask = asks[0]["p"] if asks else 0
    best_bid_sz = bids[0]["s"] if bids else 0
    best_ask_sz = asks[0]["s"] if asks else 0

    total_bid_liq = sum(b["p"] * b["s"] for b in bids[:10])
    total_ask_liq = sum(a["p"] * a["s"] for a in asks[:10])

    return {
        "best_bid": best_bid,
        "best_ask": best_ask,
        "best_bid_sz": round(best_bid_sz, 1),
        "best_ask_sz": round(best_ask_sz, 1),
        "spread": round(best_ask - best_bid, 4) if best_ask and best_bid else 0,
        "depth_bids": len(bids),
        "depth_asks": len(asks),
        "liq_bid_10": round(total_bid_liq, 2),
        "liq_ask_10": round(total_ask_liq, 2),
    }


# ═══════════════════════════════════════════════════════════════════════════════
#  Auto-Compression (saves ~90% disk space)
# ═══════════════════════════════════════════════════════════════════════════════

def compress_old_files(data_dir: str):
    """Compress .jsonl files > 100 MB to save volume space.
    
    Renames active file with timestamp, gzips it in chunks, deletes original.
    The recorder will create a fresh file on next write.
    """
    SIZE_LIMIT = 100 * 1024 * 1024  # 100 MB
    CHUNK_SIZE = 8 * 1024 * 1024     # 8 MB chunks (avoid OOM)
    compressed_count = 0

    for jsonl_file in glob.glob(os.path.join(data_dir, "*.jsonl")):
        try:
            file_size = os.path.getsize(jsonl_file)
        except OSError:
            continue
        if file_size < SIZE_LIMIT:
            continue

        # Rotate: books.jsonl → books_20260330_143022.jsonl.gz
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        base = os.path.basename(jsonl_file).replace(".jsonl", "")
        archive = os.path.join(data_dir, f"{base}_{ts}.jsonl")
        try:
            os.rename(jsonl_file, archive)
        except OSError:
            continue

        gz_file = archive + ".gz"
        try:
            orig_mb = os.path.getsize(archive) / (1024 * 1024)
            with open(archive, "rb") as f_in:
                with gzip.open(gz_file, "wb", compresslevel=6) as f_out:
                    while True:
                        chunk = f_in.read(CHUNK_SIZE)
                        if not chunk:
                            break
                        f_out.write(chunk)
            comp_mb = os.path.getsize(gz_file) / (1024 * 1024)
            os.remove(archive)
            compressed_count += 1
            print(f"  📦 Compressed {base}: {orig_mb:.1f} MB → {comp_mb:.1f} MB")
        except Exception as e:
            print(f"  ⚠️ Compression failed for {base}: {e}")
            # Clean up partial gz if it exists
            if os.path.exists(gz_file):
                try:
                    os.remove(gz_file)
                except OSError:
                    pass
            # Restore original file
            if os.path.exists(archive) and not os.path.exists(jsonl_file):
                try:
                    os.rename(archive, jsonl_file)
                except OSError:
                    pass

    if compressed_count > 0:
        print(f"  📦 Compressed {compressed_count} files")


def cleanup_old_dated_files(data_dir: str):
    """Remove stale date-based files (e.g. books_20260330.jsonl).
    
    These are leftovers from before the single-file format.
    Only remove if the main file (books.jsonl) already exists.
    """
    import re
    date_pattern = re.compile(r"^(books|ticks|windows)_\d{8}\.jsonl$")
    removed = 0
    for f in os.listdir(data_dir):
        if date_pattern.match(f):
            path = os.path.join(data_dir, f)
            try:
                os.remove(path)
                removed += 1
                print(f"  🗑️ Removed old file: {f}")
            except OSError:
                pass
    if removed:
        print(f"  🗑️ Cleaned up {removed} old date-based files")


# ═══════════════════════════════════════════════════════════════════════════════
#  Main Recorder
# ═══════════════════════════════════════════════════════════════════════════════

def main():
    parser = argparse.ArgumentParser(
        description="₿ Polymarket BTC 5-Min Tick Recorder"
    )
    parser.add_argument("--interval", type=int, default=3,
                        help="Tick interval in seconds (default: 3)")
    parser.add_argument("--dir", type=str, default=DATA_DIR,
                        help=f"Data directory (default: {DATA_DIR})")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PORT", 8080)),
                        help="HTTP server port (default: 8080 or $PORT)")
    args = parser.parse_args()

    os.makedirs(args.dir, exist_ok=True)

    # Start HTTP file server for data downloads
    start_http_server(args.port)

    # Clean up old date-based files from previous format
    cleanup_old_dated_files(args.dir)

    # Compress old data files (saves ~90% space)
    compress_old_files(args.dir)

    # Auto-compress every 30 minutes in background
    def compression_loop(data_dir):
        while True:
            time.sleep(1800)  # Every 30 min
            compress_old_files(data_dir)
    compress_thread = threading.Thread(target=compression_loop, args=(args.dir,), daemon=True)
    compress_thread.start()

    # Single file per data type — compression rotates when > 100 MB
    tick_file = os.path.join(args.dir, "ticks.jsonl")
    book_file = os.path.join(args.dir, "books.jsonl")
    window_file = os.path.join(args.dir, "windows.jsonl")

    # Count existing data
    existing_ticks = 0
    if os.path.exists(tick_file):
        with open(tick_file) as f:
            existing_ticks = sum(1 for _ in f)

    print()
    print("═" * 65)
    print("  ₿  POLYMARKET TICK RECORDER")
    print("═" * 65)
    print(f"  Tick interval:  {args.interval}s")
    print(f"  Tick file:      {tick_file}")
    print(f"  Book file:      {book_file}")
    print(f"  Window file:    {window_file}")
    print(f"  Existing ticks: {existing_ticks}")
    print(f"  ────────────────────────────────────────────")
    print(f"  Records per tick:")
    print(f"    • Binance spot price (real-time)")
    print(f"    • Binance 5m/1m candle OHLCV")
    print(f"    • Polymarket UP+DOWN midpoint prices")
    print(f"    • FULL orderbook (both tokens, all levels)")
    print(f"    • RSI, EMA, BB, ATR, VWAP, funding rate")
    print(f"    • Window state + resolution tracking")
    print(f"  ────────────────────────────────────────────")
    print(f"  Target: 1000+ windows (~3.5 days at 288/day)")
    print(f"  Press Ctrl+C to stop")
    print("═" * 65)
    print()

    running = True
    def handler(sig, frame):
        nonlocal running
        running = False
    signal.signal(signal.SIGINT, handler)

    tick_count = existing_ticks
    window_count = 0
    resolved = {}  # slug → resolution
    window_records = {}  # slug → {first_seen, ticks, ...}

    # Slow-changing data caches
    technicals_cache = {"data": {}, "ts": 0}
    funding_cache = {"val": 0.0, "ts": 0}
    taker_cache = {"val": 1.0, "ts": 0}
    chainlink_cache = {"val": 0.0, "ts": 0}

    cycle = 0
    while running:
        t0 = time.time()
        cycle += 1

        try:
            now = datetime.now(timezone.utc)
            now_iso = now.isoformat()

            # ── Fast data (every tick) ─────────────────────────────────
            spot = fetch_spot_price()
            candle = fetch_candle_snapshot()

            # ── Slow data (cached, refresh periodically) ───────────────
            if time.time() - technicals_cache["ts"] > 30:
                technicals_cache["data"] = fetch_technicals()
                technicals_cache["ts"] = time.time()

            if time.time() - funding_cache["ts"] > 300:
                funding_cache["val"] = fetch_funding_rate()
                funding_cache["ts"] = time.time()

            if time.time() - taker_cache["ts"] > 60:
                taker_cache["val"] = fetch_taker_ratio()
                taker_cache["ts"] = time.time()

            if time.time() - chainlink_cache["ts"] > 10:
                chainlink_cache["val"] = fetch_chainlink_btc_price()
                chainlink_cache["ts"] = time.time()

            # ── Discover market windows ────────────────────────────────
            windows = discover_windows(look_ahead_minutes=15, look_behind=1)

            # ── Record each window ─────────────────────────────────────
            for w in windows:
                slug = w["slug"]

                # Track window lifecycle
                if slug not in window_records:
                    window_records[slug] = {
                        "slug": slug,
                        "condition_id": w["condition_id"],
                        "question": w["question"],
                        "start_ts": w["start_ts"],
                        "end_ts": w["end_ts"],
                        "first_seen": now_iso,
                        "first_spot": spot,
                        "tick_count": 0,
                    }

                wr = window_records[slug]
                wr["tick_count"] += 1
                wr["last_seen"] = now_iso
                wr["last_spot"] = spot

                # Fetch FULL orderbooks for BOTH tokens
                up_book = fetch_full_book(w["up_token"]) if w["up_token"] else {"bids": [], "asks": []}
                down_book = fetch_full_book(w["down_token"]) if w["down_token"] else {"bids": [], "asks": []}

                # Midpoints
                up_mid = fetch_midpoint(w["up_token"]) if w["up_token"] else 0.5
                down_mid = round(1.0 - up_mid, 4)

                # Summarize books for tick log
                up_summary = summarize_book(up_book)
                down_summary = summarize_book(down_book)

                # ── Write TICK record ──────────────────────────────────
                tick = {
                    "ts": now_iso,
                    "tick": tick_count,
                    # Window identity
                    "slug": slug,
                    "secs_left": w["seconds_left"],
                    "state": w["state"],
                    # Polymarket prices
                    "up_mid": up_mid,
                    "down_mid": down_mid,
                    "gamma_up": w["gamma_up"],
                    "gamma_down": w["gamma_down"],
                    # UP token book summary
                    "up_bid": up_summary["best_bid"],
                    "up_ask": up_summary["best_ask"],
                    "up_bid_sz": up_summary["best_bid_sz"],
                    "up_ask_sz": up_summary["best_ask_sz"],
                    "up_spread": up_summary["spread"],
                    "up_depth_b": up_summary["depth_bids"],
                    "up_depth_a": up_summary["depth_asks"],
                    "up_liq_b10": up_summary["liq_bid_10"],
                    "up_liq_a10": up_summary["liq_ask_10"],
                    # DOWN token book summary
                    "dn_bid": down_summary["best_bid"],
                    "dn_ask": down_summary["best_ask"],
                    "dn_bid_sz": down_summary["best_bid_sz"],
                    "dn_ask_sz": down_summary["best_ask_sz"],
                    "dn_spread": down_summary["spread"],
                    "dn_depth_b": down_summary["depth_bids"],
                    "dn_depth_a": down_summary["depth_asks"],
                    "dn_liq_b10": down_summary["liq_bid_10"],
                    "dn_liq_a10": down_summary["liq_ask_10"],
                    # Binance
                    "spot": spot,
                    **candle,
                    # Technicals (slower refresh)
                    **technicals_cache["data"],
                    "funding_rate": funding_cache["val"],
                    "taker_ratio": taker_cache["val"],
                    "chainlink_btc": chainlink_cache["val"],
                    # Resolution
                    "resolution": w["resolution"],
                    "volume": w["volume"],
                }

                with open(tick_file, "a") as f:
                    f.write(json.dumps(tick) + "\n")
                tick_count += 1

                # ── Write FULL BOOK snapshot (separate file, larger) ───
                book_record = {
                    "ts": now_iso,
                    "slug": slug,
                    "secs_left": w["seconds_left"],
                    "spot": spot,
                    "up_book": up_book,
                    "down_book": down_book,
                }
                with open(book_file, "a") as f:
                    f.write(json.dumps(book_record) + "\n")

                # ── Track resolution ───────────────────────────────────
                if w["resolution"] and slug not in resolved:
                    resolved[slug] = w["resolution"]
                    wr["resolution"] = w["resolution"]
                    wr["resolved_at"] = now_iso
                    wr["resolved_spot"] = spot

                    # Write window summary
                    with open(window_file, "a") as f:
                        f.write(json.dumps(wr) + "\n")
                    window_count += 1

            # ── Status line ────────────────────────────────────────────
            elapsed = time.time() - t0
            active = sum(1 for w in windows if w["state"] == "ACTIVE")
            upcoming = sum(1 for w in windows if w["state"] == "UPCOMING")
            ts = now.strftime("%H:%M:%S")

            candle_open = candle.get("candle_5m_open", 0)
            delta_pct = ((spot - candle_open) / candle_open * 100) if candle_open else 0
            arrow = "📈" if delta_pct >= 0 else "📉"

            print(
                f"  {ts} │ {arrow} ${spot:,.0f} Δ{delta_pct:+.3f}% │ "
                f"{active}act {upcoming}up │ "
                f"{tick_count}ticks {window_count}win │ "
                f"{elapsed:.1f}s"
            )

        except Exception as e:
            print(f"  ❌ Error: {e}")
            import traceback
            traceback.print_exc()


        # Sleep for remaining interval
        elapsed = time.time() - t0
        sleep_time = max(0.1, args.interval - elapsed)
        time.sleep(sleep_time)

    # ── Shutdown summary ───────────────────────────────────────────────
    print()
    print("═" * 65)
    print(f"  ₿ Recorder stopped")
    print(f"  Total ticks:     {tick_count}")
    print(f"  Windows resolved: {window_count}")
    print(f"  Tick file:       {tick_file}")
    print(f"  Book file:       {book_file}")
    print(f"  Window file:     {window_file}")

    # File sizes
    for path in [tick_file, book_file, window_file]:
        if os.path.exists(path):
            size_mb = os.path.getsize(path) / (1024 * 1024)
            print(f"    {os.path.basename(path)}: {size_mb:.1f} MB")

    print("═" * 65)
    print()


if __name__ == "__main__":
    main()
