import csv
import io

import requests

NSE_MASTER_URLS = (
    "https://nsearchives.nseindia.com/content/equities/EQUITY_L.csv",
    "https://nsearchives.nseindia.com/content/equities/SME_EQUITY_L.csv",
)


def fetch_rows(url: str) -> list[dict]:
    response = requests.get(
        url,
        headers={"User-Agent": "Mozilla/5.0", "Accept": "text/csv, text/plain, */*"},
        timeout=20,
    )
    response.raise_for_status()

    reader = csv.DictReader(io.StringIO(response.text.lstrip("\ufeff")), skipinitialspace=True)
    result = []

    for row in reader:
        symbol = str(row.get("SYMBOL") or "").strip().upper()
        company_name = str(row.get("NAME OF COMPANY") or "").strip()

        if not symbol or not company_name or symbol.startswith("**"):
            continue

        result.append({
            "symbol": symbol,
            "company_name": company_name,
            "series": str(row.get("SERIES") or "").strip().upper(),
        })

    return result


all_stocks = {}

for url in NSE_MASTER_URLS:
    for stock in fetch_rows(url):
        all_stocks.setdefault(stock["symbol"], stock)

print(list(all_stocks.values()))
