import csv
import io
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import quote

import requests
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="AllStocksPortfolio")
BASE_DIR = Path(__file__).resolve().parent

NSE_HOME_URL = "https://www.nseindia.com"
NSE_QUOTE_URL = "https://www.nseindia.com/api/quote-equity"
YAHOO_CHART_URL_TEMPLATE = "https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
NSE_EQUITY_MASTER_URL = "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv"
NSE_SME_MASTER_URL = "https://nsearchives.nseindia.com/content/equities/SME_EQUITY_L.csv"
NSE_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.nseindia.com/",
    "Origin": "https://www.nseindia.com",
}
SYMBOL_PATTERN = re.compile(r"[^A-Z0-9&-]")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # allow all (for now)
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def normalize_symbol(value: str) -> str:
    cleaned = SYMBOL_PATTERN.sub("", str(value or "").strip().upper())
    return cleaned


def create_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(NSE_HEADERS)

    try:
        session.get(NSE_HOME_URL, timeout=10)
    except requests.RequestException:
        # The quote endpoint often works without the warm-up request, so do not fail here.
        pass

    return session


def fetch_single_yahoo_quote(symbol: str) -> dict:
    ticker = quote(f"{symbol}.NS", safe=".-")
    response = requests.get(
        YAHOO_CHART_URL_TEMPLATE.format(ticker=ticker),
        params={"interval": "1d", "range": "1d"},
        timeout=20,
        headers={"User-Agent": NSE_HEADERS["User-Agent"]},
    )
    response.raise_for_status()
    payload = response.json()
    result = ((payload.get("chart") or {}).get("result") or [None])[0] or {}
    meta = result.get("meta") or {}
    indicators = result.get("indicators") or {}
    quote_rows = (indicators.get("quote") or [None])[0] or {}
    closes = quote_rows.get("close") or []
    last_price = meta.get("regularMarketPrice")

    if last_price in (None, ""):
        last_price = next((price for price in reversed(closes) if price not in (None, "")), None)

    if last_price in (None, ""):
        raise ValueError("Yahoo quote is missing regular market price")

    last_update_time = meta.get("regularMarketTime")
    last_update_iso = ""
    if isinstance(last_update_time, (int, float)) and last_update_time > 0:
        last_update_iso = datetime.fromtimestamp(last_update_time, tz=timezone.utc).isoformat()

    return {
        "symbol": symbol,
        "name": meta.get("longName") or meta.get("shortName") or symbol,
        "currentPrice": last_price,
        "change": None,
        "percentChange": None,
        "lastUpdateTime": last_update_iso,
        "source": "yahoo-finance",
    }


def fetch_single_nse_quote(symbol: str) -> dict:
    last_error = None

    for _ in range(2):
        session = create_nse_session()
        try:
            response = session.get(
                NSE_QUOTE_URL,
                params={"symbol": symbol},
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            price_info = payload.get("priceInfo") or {}
            metadata = payload.get("metadata") or {}
            info = payload.get("info") or {}

            last_price = price_info.get("lastPrice")
            if last_price in (None, ""):
                last_error = ValueError("NSE quote is missing last price")
                continue

            return {
                "symbol": metadata.get("symbol") or info.get("symbol") or symbol,
                "name": info.get("companyName") or metadata.get("symbol") or symbol,
                "currentPrice": last_price,
                "change": price_info.get("change"),
                "percentChange": price_info.get("pChange"),
                "lastUpdateTime": metadata.get("lastUpdateTime") or "",
                "source": "nseindia.com",
            }
        except Exception as exc:
            last_error = exc
        finally:
            session.close()

    try:
        return fetch_single_yahoo_quote(symbol)
    except Exception as yahoo_error:
        raise yahoo_error from last_error


def fetch_nse_quotes(symbols: list[str]) -> tuple[dict, dict]:
    normalized_symbols = []
    seen_symbols = set()

    for raw_symbol in symbols:
        symbol = normalize_symbol(raw_symbol)
        if symbol and symbol not in seen_symbols:
            normalized_symbols.append(symbol)
            seen_symbols.add(symbol)

    if not normalized_symbols:
        return {}, {}

    quotes = {}
    errors = {}
    max_workers = min(5, len(normalized_symbols))

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_map = {
            executor.submit(fetch_single_nse_quote, symbol): symbol
            for symbol in normalized_symbols
        }

        for future in as_completed(future_map):
            symbol = future_map[future]
            try:
                quotes[symbol] = future.result()
            except Exception as exc:  # pragma: no cover - kept broad for resilient API responses
                errors[symbol] = str(exc)

    return quotes, errors


def fetch_nse_master_rows(url: str, segment: str) -> list[dict]:
    response = requests.get(
        url,
        headers={
            "User-Agent": NSE_HEADERS["User-Agent"],
            "Accept": "text/csv, text/plain, */*",
            "Referer": f"{NSE_HOME_URL}/market-data/securities-available-for-trading",
        },
        timeout=20,
    )
    response.raise_for_status()

    csv_text = response.text.lstrip("\ufeff")
    reader = csv.DictReader(io.StringIO(csv_text), skipinitialspace=True)
    rows = []

    for raw_row in reader:
        symbol = normalize_symbol(raw_row.get("SYMBOL"))
        company_name = str(raw_row.get("NAME OF COMPANY") or "").strip()
        series = str(raw_row.get("SERIES") or "").strip().upper()

        if not symbol or not company_name or symbol.startswith("THISFILE"):
            continue

        rows.append({
            "symbol": symbol,
            "name": company_name,
            "company_name": company_name,
            "series": series,
            "segment": segment,
        })

    return rows


def fetch_nse_master_data() -> list[dict]:
    listings_by_symbol = {}
    errors = []

    for url, segment in (
        (NSE_EQUITY_MASTER_URL, "EQUITY"),
        (NSE_SME_MASTER_URL, "SME"),
    ):
        try:
            for row in fetch_nse_master_rows(url, segment):
                # Prefer the main equity listing when a symbol exists in both files.
                listings_by_symbol.setdefault(row["symbol"], row)
        except requests.RequestException as exc:
            errors.append(f"{segment}: {exc}")

    if not listings_by_symbol:
        detail = "Unable to fetch the NSE stock master right now."
        if errors:
            detail = f"{detail} {'; '.join(errors)}"
        raise HTTPException(status_code=502, detail=detail)

    return list(listings_by_symbol.values())


if (BASE_DIR / "assets").exists():
    app.mount("/assets", StaticFiles(directory=BASE_DIR / "assets"), name="assets")


@app.get("/")
def get_index():
    return FileResponse(BASE_DIR / "index.html")


@app.get("/data")
def get_data():
    return fetch_nse_master_data()


@app.get("/api/nse/quotes")
def get_nse_quotes(symbols: str = Query(..., description="Comma-separated NSE equity symbols")):
    requested_symbols = [part for part in symbols.split(",") if part.strip()]
    quotes, errors = fetch_nse_quotes(requested_symbols)

    if not quotes:
        raise HTTPException(
            status_code=502,
            detail="Unable to fetch NSE quotes for the requested symbols right now.",
        )

    return {
        "source": "nseindia.com",
        "fetchedAt": datetime.now(timezone.utc).isoformat(),
        "quotes": quotes,
        "errors": errors,
    }


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8000)
