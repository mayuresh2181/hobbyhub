import requests
import datetime as dt
import pandas as pd
import io
import time
import os
import concurrent.futures

from openpyxl import load_workbook
from openpyxl.styles import Font

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


# =========================================================
# CONFIGURATION
# =========================================================

BASE_FOLDER = "data/nse/delivery"

LOG_FOLDER = os.path.join(BASE_FOLDER, "daily_logs")
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, "daily_output")

TRADING_DAYS_LOOKBACK = 30

MIN_DELIVERY_RATIO = 3

MAX_WORKERS = 5

INDEX_FILES = [
    "ind_nifty500list.csv",
    "ind_niftymidcap250list.csv"
]

os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# =========================================================
# CREATE NSE SESSION
# =========================================================

def create_session():

    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[429, 500, 502, 503, 504]
    )

    adapter = HTTPAdapter(max_retries=retry_strategy)

    session.mount("https://", adapter)
    session.mount("http://", adapter)

    headers = {
        "User-Agent": (
            "Mozilla/5.0 "
            "(Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 "
            "(KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/"
    }

    session.headers.update(headers)

    # Warmup
    session.get(
        "https://www.nseindia.com",
        timeout=20
    )

    return session


# =========================================================
# GET SYMBOLS
# =========================================================

def get_symbols(session):

    all_symbols = set()

    for file_name in INDEX_FILES:

        print(f"Fetching index file: {file_name}")

        url = (
            "https://archives.nseindia.com/content/indices/"
            + file_name
        )

        try:

            response = session.get(
                url,
                timeout=30
            )

            if response.status_code != 200:
                print(f"Failed: {file_name}")
                continue

            df = pd.read_csv(
                io.StringIO(response.text)
            )

            df.columns = (
                df.columns
                .str.strip()
                .str.upper()
            )

            df['SYMBOL'] = (
                df['SYMBOL']
                .astype(str)
                .str.strip()
            )

            all_symbols.update(
                df['SYMBOL'].tolist()
            )

        except Exception as e:

            print(f"Error: {file_name} -> {e}")

    return sorted(list(all_symbols))


# =========================================================
# GET LAST TRADING DAYS
# =========================================================

def get_last_trading_days(n):

    today = dt.date.today()

    days = []

    current = today - dt.timedelta(days=1)

    while len(days) < n:

        if current.weekday() < 5:
            days.append(current)

        current -= dt.timedelta(days=1)

    return sorted(days)


# =========================================================
# GET DELIVERY DATA
# =========================================================

def get_delivery_data(date, session):

    file_path = os.path.join(
        LOG_FOLDER,
        f"{date.strftime('%Y%m%d')}.csv"
    )

    # -------------------------------------
    # LOAD FROM CACHE
    # -------------------------------------

    if os.path.exists(file_path):

        print(f"Loading cached: {date}")

        try:

            df = pd.read_csv(file_path)

            df['DATE'] = (
                pd.to_datetime(df['DATE'])
                .dt.date
            )

            return df

        except Exception as e:

            print(f"Corrupt cache {date}: {e}")

    # -------------------------------------
    # DOWNLOAD
    # -------------------------------------

    print(f"Downloading: {date}")

    url = (
        "https://archives.nseindia.com/products/content/"
        f"sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"
    )

    try:

        response = session.get(
            url,
            timeout=30
        )

        if response.status_code != 200:

            print(f"Failed: {date}")

            return None

        # NSE holiday / invalid response
        if "SYMBOL" not in response.text:

            print(f"No data for: {date}")

            return None

        df = pd.read_csv(
            io.StringIO(response.text)
        )

        # ---------------------------------
        # CLEAN COLUMN NAMES
        # ---------------------------------

        df.columns = (
            df.columns
            .str.strip()
            .str.upper()
            .str.replace(" ", "_")
        )

        # ---------------------------------
        # CLEAN SYMBOL
        # ---------------------------------

        df['SYMBOL'] = (
            df['SYMBOL']
            .astype(str)
            .str.strip()
        )

        # ---------------------------------
        # NUMERIC CONVERSION
        # ---------------------------------

        numeric_cols = [
            'DELIV_QTY',
            'TTL_TRD_QNTY'
        ]

        for col in numeric_cols:

            if col in df.columns:

                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(",", "")
                    .str.strip()
                )

                df[col] = pd.to_numeric(
                    df[col],
                    errors='coerce'
                )

        df['DATE'] = date

        # ---------------------------------
        # SAVE CACHE
        # ---------------------------------

        df.to_csv(
            file_path,
            index=False
        )

        return df

    except Exception as e:

        print(f"Error for {date}: {e}")

        return None


# =========================================================
# MULTITHREADED DOWNLOAD
# =========================================================

def download_all_data(trading_days, session):

    all_data = []

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=MAX_WORKERS
    ) as executor:

        future_map = {
            executor.submit(
                get_delivery_data,
                d,
                session
            ): d for d in trading_days
        }

        for future in concurrent.futures.as_completed(
            future_map
        ):

            df = future.result()

            if df is not None:
                all_data.append(df)

    return all_data


# =========================================================
# FORMAT EXCEL
# =========================================================

def format_excel(output_file):

    wb = load_workbook(output_file)

    ws = wb.active

    # Freeze header
    ws.freeze_panes = "A2"

    # Bold headers
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # Auto width
    for column_cells in ws.columns:

        length = max(
            len(str(cell.value))
            if cell.value else 0
            for cell in column_cells
        )

        adjusted_width = min(length + 5, 40)

        ws.column_dimensions[
            column_cells[0].column_letter
        ].width = adjusted_width

    wb.save(output_file)


# =========================================================
# MAIN
# =========================================================

def main():

    print("\n========================================")
    print("NSE DELIVERY BREAKOUT SCANNER")
    print("========================================\n")

    session = create_session()

    # -------------------------------------
    # SYMBOLS
    # -------------------------------------

    symbols = get_symbols(session)

    print(f"\nTotal unique symbols: {len(symbols)}")

    # -------------------------------------
    # TRADING DAYS
    # -------------------------------------

    trading_days = get_last_trading_days(
        TRADING_DAYS_LOOKBACK
    )

    print(
        f"Trading days considered: "
        f"{len(trading_days)}"
    )

    # -------------------------------------
    # DOWNLOAD DATA
    # -------------------------------------

    all_data = download_all_data(
        trading_days,
        session
    )

    if not all_data:

        print("No delivery data found.")
        return

    # -------------------------------------
    # MERGE DATA
    # -------------------------------------

    final_df = pd.concat(
        all_data,
        ignore_index=True
    )

    # -------------------------------------
    # FILTER SYMBOLS
    # -------------------------------------

    final_df = final_df[
        final_df['SYMBOL'].isin(symbols)
    ]

    # -------------------------------------
    # LATEST DAY
    # -------------------------------------

    available_dates = sorted(
        final_df['DATE'].unique()
    )

    latest_date = available_dates[-1]

    print(f"\nLatest trading day: {latest_date}")

    latest_df = final_df[
        final_df['DATE'] == latest_date
    ].copy()

    historical_df = final_df[
        final_df['DATE'] < latest_date
    ].copy()

    # -------------------------------------
    # AVERAGE DELIVERY
    # -------------------------------------

    avg_delivery = (
        historical_df
        .groupby('SYMBOL')['DELIV_QTY']
        .mean()
        .reset_index()
        .rename(
            columns={
                'DELIV_QTY':
                'AVG_30_DELIV_QTY'
            }
        )
    )

    # -------------------------------------
    # MERGE
    # -------------------------------------

    merged = pd.merge(
        latest_df,
        avg_delivery,
        on='SYMBOL',
        how='inner'
    )

    # -------------------------------------
    # DELIVERY RATIO
    # -------------------------------------

    merged['DELIVERY_RATIO'] = (
        merged['DELIV_QTY']
        / merged['AVG_30_DELIV_QTY']
    ).round(2)

    # -------------------------------------
    # FILTER
    # -------------------------------------

    result = merged[
        merged['DELIVERY_RATIO']
        >= MIN_DELIVERY_RATIO
    ].copy()

    if result.empty:

        print(
            "\nNo stocks found with "
            "delivery breakout."
        )

        return

    # -------------------------------------
    # SORT
    # -------------------------------------

    result = result.sort_values(
        by='DELIVERY_RATIO',
        ascending=False
    )

    # -------------------------------------
    # TRADINGVIEW LINKS
    # -------------------------------------

    result['SYMBOL_LINK'] = result[
        'SYMBOL'
    ].apply(
        lambda x:
        f'=HYPERLINK("https://www.tradingview.com/chart/?symbol=NSE:{x}","{x}")'
    )

    # -------------------------------------
    # FINAL OUTPUT
    # -------------------------------------

    output_df = result[
        [
            'SYMBOL_LINK',
            'DELIV_QTY',
            'AVG_30_DELIV_QTY',
            'DELIVERY_RATIO'
        ]
    ].copy()

    output_df.columns = [
        'SYMBOL',
        'DELIV_QTY',
        'AVG_30_DELIV_QTY',
        'DELIVERY_RATIO'
    ]

    # -------------------------------------
    # SAVE EXCEL
    # -------------------------------------

    output_file = os.path.join(
        OUTPUT_FOLDER,
        f"delivery_breakout_"
        f"{latest_date.strftime('%Y%m%d')}.xlsx"
    )

    output_df.to_excel(
        output_file,
        index=False,
        engine='openpyxl'
    )

    format_excel(output_file)

    # -------------------------------------
    # SUMMARY
    # -------------------------------------

    print("\n========================================")
    print("SCAN COMPLETE")
    print("========================================")

    print(f"\nStocks Found: {len(output_df)}")

    print(f"\nExcel Report:")
    print(output_file)

    print("\nTop Results:\n")

    print(
        output_df.head(10)
    )


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    start_time = time.time()

    main()

    end_time = time.time()

    print(
        f"\nExecution Time: "
        f"{round(end_time - start_time, 2)} seconds"
    )
