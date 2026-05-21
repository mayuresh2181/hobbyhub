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
MIN_DELIVERY_PERCENT = 50
MIN_VOLUME_RATIO = 2
MIN_PRICE_CHANGE = 2

MAX_WORKERS = 5

os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# =========================================================
# NSE SESSION
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
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0 Safari/537.36"
        ),
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/"
    }

    session.headers.update(headers)

    # Warmup request
    session.get("https://www.nseindia.com", timeout=20)

    return session


# =========================================================
# GET NIFTY 500 SYMBOLS
# =========================================================

def get_nifty500_symbols(session):

    print("Fetching NIFTY 500 list...")

    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

    response = session.get(url, timeout=30)

    if response.status_code != 200:
        raise Exception("Unable to fetch NIFTY 500 list")

    df = pd.read_csv(io.StringIO(response.text))

    df.columns = df.columns.str.strip().str.upper()

    df['SYMBOL'] = df['SYMBOL'].str.strip()

    return df['SYMBOL'].unique().tolist()


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
# DOWNLOAD DELIVERY DATA
# =========================================================

def get_delivery_data(date, session):

    file_path = os.path.join(
        LOG_FOLDER,
        f"{date.strftime('%Y%m%d')}.csv"
    )

    # -----------------------------------
    # LOAD FROM DISK
    # -----------------------------------

    if os.path.exists(file_path):

        print(f"Loading from disk: {date}")

        try:
            df = pd.read_csv(file_path)

            df['DATE'] = pd.to_datetime(df['DATE']).dt.date

            return df

        except Exception as e:
            print(f"Corrupt cached file for {date}: {e}")

    # -----------------------------------
    # DOWNLOAD
    # -----------------------------------

    print(f"Downloading: {date}")

    url = (
        "https://archives.nseindia.com/products/content/"
        f"sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"
    )

    try:

        response = session.get(url, timeout=30)

        if response.status_code != 200:
            print(f"Failed for {date}")
            return None

        # NSE holiday / invalid file
        if "SYMBOL" not in response.text:
            print(f"No market data for {date}")
            return None

        df = pd.read_csv(io.StringIO(response.text))

        df.columns = (
            df.columns
            .str.strip()
            .str.upper()
            .str.replace(" ", "_")
        )

        # Clean symbol
        df['SYMBOL'] = df['SYMBOL'].astype(str).str.strip()

        # Convert numeric columns
        numeric_cols = [
            'DELIV_QTY',
            'TTL_TRD_QNTY',
            'CLOSE_PRICE',
            'PREV_CLOSE'
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

        # Save cache
        df.to_csv(file_path, index=False)

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

        future_to_date = {
            executor.submit(
                get_delivery_data,
                d,
                session
            ): d for d in trading_days
        }

        for future in concurrent.futures.as_completed(future_to_date):

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

    # Bold header
    for cell in ws[1]:
        cell.font = Font(bold=True)

    # Auto width
    for column_cells in ws.columns:

        length = max(
            len(str(cell.value)) if cell.value else 0
            for cell in column_cells
        )

        ws.column_dimensions[
            column_cells[0].column_letter
        ].width = min(length + 5, 40)

    wb.save(output_file)


# =========================================================
# MAIN EXECUTION
# =========================================================

def main():

    print("\n======================================")
    print("RUNNING NSE DELIVERY BREAKOUT SCANNER")
    print("======================================\n")

    session = create_session()

    # -----------------------------------
    # SYMBOL LIST
    # -----------------------------------

    nifty500_symbols = get_nifty500_symbols(session)

    print(f"NIFTY 500 symbols loaded: {len(nifty500_symbols)}")

    # -----------------------------------
    # TRADING DAYS
    # -----------------------------------

    trading_days = get_last_trading_days(
        TRADING_DAYS_LOOKBACK
    )

    print(f"Trading days fetched: {len(trading_days)}")

    # -----------------------------------
    # DOWNLOAD DATA
    # -----------------------------------

    all_data = download_all_data(
        trading_days,
        session
    )

    if not all_data:

        print("No delivery data available.")
        return

    # -----------------------------------
    # COMBINE
    # -----------------------------------

    final_df = pd.concat(
        all_data,
        ignore_index=True
    )

    # NIFTY 500 only
    final_df = final_df[
        final_df['SYMBOL'].isin(nifty500_symbols)
    ]

    # -----------------------------------
    # YESTERDAY
    # -----------------------------------

    available_dates = sorted(
        final_df['DATE'].unique()
    )

    yesterday = available_dates[-1]

    print(f"\nLatest trading day found: {yesterday}")

    yesterday_df = final_df[
        final_df['DATE'] == yesterday
    ].copy()

    historical_df = final_df[
        final_df['DATE'] < yesterday
    ].copy()

    # -----------------------------------
    # HISTORICAL AVERAGES
    # -----------------------------------

    avg_delivery = (
        historical_df
        .groupby('SYMBOL')['DELIV_QTY']
        .mean()
        .reset_index()
        .rename(
            columns={
                'DELIV_QTY': 'AVG_30_DELIV_QTY'
            }
        )
    )

    avg_volume = (
        historical_df
        .groupby('SYMBOL')['TTL_TRD_QNTY']
        .mean()
        .reset_index()
        .rename(
            columns={
                'TTL_TRD_QNTY': 'AVG_30_VOLUME'
            }
        )
    )

    # -----------------------------------
    # MERGE
    # -----------------------------------

    merged = pd.merge(
        yesterday_df,
        avg_delivery,
        on='SYMBOL',
        how='inner'
    )

    merged = pd.merge(
        merged,
        avg_volume,
        on='SYMBOL',
        how='inner'
    )

    # -----------------------------------
    # DELIVERY %
    # -----------------------------------

    merged['DELIV_PER'] = (
        merged['DELIV_QTY']
        / merged['TTL_TRD_QNTY']
        * 100
    ).round(2)

    # -----------------------------------
    # RATIOS
    # -----------------------------------

    merged['DELIVERY_RATIO'] = (
        merged['DELIV_QTY']
        / merged['AVG_30_DELIV_QTY']
    ).round(2)

    merged['VOLUME_RATIO'] = (
        merged['TTL_TRD_QNTY']
        / merged['AVG_30_VOLUME']
    ).round(2)

    # -----------------------------------
    # PRICE CHANGE
    # -----------------------------------

    merged['PRICE_CHANGE'] = (
        (
            merged['CLOSE_PRICE']
            - merged['PREV_CLOSE']
        )
        / merged['PREV_CLOSE']
        * 100
    ).round(2)

    # -----------------------------------
    # FINAL FILTER
    # -----------------------------------

    result = merged[
        (merged['DELIVERY_RATIO'] >= MIN_DELIVERY_RATIO)
        &
        (merged['DELIV_PER'] >= MIN_DELIVERY_PERCENT)
        &
        (merged['VOLUME_RATIO'] >= MIN_VOLUME_RATIO)
        &
        (merged['PRICE_CHANGE'] >= MIN_PRICE_CHANGE)
    ].copy()

    # -----------------------------------
    # SORT
    # -----------------------------------

    if result.empty:

        print("\nNo breakout stocks found.")
        return

    result = result.sort_values(
        by='DELIVERY_RATIO',
        ascending=False
    )

    # -----------------------------------
    # TRADINGVIEW LINKS
    # -----------------------------------

    result['SYMBOL_LINK'] = result['SYMBOL'].apply(
        lambda x:
        f'=HYPERLINK("https://www.tradingview.com/chart/?symbol=NSE:{x}","{x}")'
    )

    # -----------------------------------
    # FINAL COLUMNS
    # -----------------------------------

    final_output = result[
        [
            'SYMBOL_LINK',
            'DELIV_QTY',
            'AVG_30_DELIV_QTY',
            'DELIVERY_RATIO',
            'DELIV_PER',
            'TTL_TRD_QNTY',
            'AVG_30_VOLUME',
            'VOLUME_RATIO',
            'CLOSE_PRICE',
            'PRICE_CHANGE'
        ]
    ].copy()

    final_output.columns = [
        'SYMBOL',
        'DELIV_QTY',
        'AVG_30_DELIV_QTY',
        'DELIVERY_RATIO',
        'DELIV_PER',
        'TTL_TRD_QNTY',
        'AVG_30_VOLUME',
        'VOLUME_RATIO',
        'CLOSE_PRICE',
        'PRICE_CHANGE'
    ]

    # -----------------------------------
    # SAVE OUTPUT
    # -----------------------------------

    output_file = os.path.join(
        OUTPUT_FOLDER,
        f"delivery_breakout_{yesterday.strftime('%Y%m%d')}.xlsx"
    )

    final_output.to_excel(
        output_file,
        index=False,
        engine='openpyxl'
    )

    # Excel formatting
    format_excel(output_file)

    # -----------------------------------
    # SUMMARY
    # -----------------------------------

    print("\n======================================")
    print("SCAN COMPLETE")
    print("======================================")

    print(f"\nStocks Found: {len(final_output)}")

    print(f"\nReport Saved:")
    print(output_file)

    print("\nTop Results:\n")

    print(
        final_output[
            [
                'SYMBOL',
                'DELIVERY_RATIO',
                'DELIV_PER',
                'VOLUME_RATIO',
                'PRICE_CHANGE'
            ]
        ].head(10)
    )


# =========================================================
# ENTRY POINT
# =========================================================

if __name__ == "__main__":

    start = time.time()

    main()

    end = time.time()

    print(f"\nExecution Time: {round(end - start, 2)} seconds")
