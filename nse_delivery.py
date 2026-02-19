import requests
import datetime as dt
import pandas as pd
import io
import time
import os


# =========================================
# CONFIGURATION
# =========================================

BASE_FOLDER = "data/nse/delivery"
LOG_FOLDER = os.path.join(BASE_FOLDER, "daily_logs")
OUTPUT_FOLDER = os.path.join(BASE_FOLDER, "daily_output")

TRADING_DAYS_LOOKBACK = 30

# Create folders if not exist
os.makedirs(LOG_FOLDER, exist_ok=True)
os.makedirs(OUTPUT_FOLDER, exist_ok=True)


# =========================================
# GET NIFTY 500 LIST
# =========================================

def get_nifty500_symbols():

    url = "https://archives.nseindia.com/content/indices/ind_nifty500list.csv"

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com/"
    }

    session = requests.Session()
    session.headers.update(headers)
    session.get("https://www.nseindia.com")
    time.sleep(1)

    response = session.get(url)

    df = pd.read_csv(io.StringIO(response.text))
    df.columns = df.columns.str.strip().str.upper()

    return df['SYMBOL'].tolist()


# =========================================
# GET LAST N TRADING DAYS
# =========================================

def get_last_trading_days(n):
    today = dt.date.today()
    days = []
    current = today - dt.timedelta(days=1)

    while len(days) < n:
        if current.weekday() < 5:
            days.append(current)
        current -= dt.timedelta(days=1)

    return sorted(days)


# =========================================
# DOWNLOAD OR LOAD DELIVERY DATA
# =========================================

def get_delivery_data(date):

    file_path = os.path.join(LOG_FOLDER, f"{date.strftime('%Y%m%d')}.csv")

    if os.path.exists(file_path):
        print(f"Loading from disk: {date}")
        df = pd.read_csv(file_path)
        df['DATE'] = pd.to_datetime(df['DATE']).dt.date
        return df

    print(f"Downloading: {date}")

    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Referer": "https://www.nseindia.com/"
    }
    session.headers.update(headers)

    session.get("https://www.nseindia.com")
    time.sleep(1)

    url = f"https://archives.nseindia.com/products/content/sec_bhavdata_full_{date.strftime('%d%m%Y')}.csv"

    response = session.get(url)

    if response.status_code != 200:
        print(f"Failed for {date}")
        return None

    df = pd.read_csv(io.StringIO(response.text))

    df.columns = (
        df.columns
        .str.strip()
        .str.upper()
        .str.replace(" ", "_")
    )

    df['DELIV_QTY'] = pd.to_numeric(df['DELIV_QTY'], errors='coerce')
    df['TTL_TRD_QNTY'] = pd.to_numeric(df['TTL_TRD_QNTY'], errors='coerce')
    df['DATE'] = date

    df.to_csv(file_path, index=False)

    return df


# =========================================
# MAIN EXECUTION
# =========================================

print("\nRunning Delivery Breakout Scanner (NIFTY 500)...\n")

nifty500_symbols = get_nifty500_symbols()
trading_days = get_last_trading_days(TRADING_DAYS_LOOKBACK)

all_data = []

for d in trading_days:
    df = get_delivery_data(d)
    if df is not None:
        all_data.append(df)
        time.sleep(1)

if not all_data:
    print("No delivery data available.")
    exit()

final_df = pd.concat(all_data, ignore_index=True)

# Filter NIFTY 500 only
final_df = final_df[final_df['SYMBOL'].isin(nifty500_symbols)]

yesterday = trading_days[-1]

yesterday_df = final_df[final_df['DATE'] == yesterday]

# 30-day average
avg_delivery = (
    final_df
    .groupby('SYMBOL')['DELIV_QTY']
    .mean()
    .reset_index()
    .rename(columns={'DELIV_QTY': 'AVG_30_DELIV_QTY'})
)

merged = pd.merge(
    yesterday_df[['SYMBOL', 'DELIV_QTY']],
    avg_delivery,
    on='SYMBOL',
    how='inner'
)

result = merged[
    merged['DELIV_QTY'] > 3 * merged['AVG_30_DELIV_QTY']
].copy()

if not result.empty:

    result['RATIO'] = (result['DELIV_QTY'] / result['AVG_30_DELIV_QTY']).round(2)
    result = result.sort_values(by='RATIO', ascending=False)

    # Make SYMBOL clickable (TradingView)
    result['SYMBOL'] = result['SYMBOL'].apply(
        lambda x: f'=HYPERLINK("https://www.tradingview.com/chart/?symbol=NSE:{x}", "{x}")'
    )

    output_file = os.path.join(
        OUTPUT_FOLDER,
        f"del_breakout_{yesterday.strftime('%Y%m%d')}.xlsx"
    )

    result[['SYMBOL', 'DELIV_QTY', 'AVG_30_DELIV_QTY', 'RATIO']].to_excel(
        output_file,
        index=False,
        engine='openpyxl'
    )

    print("\nReport saved at:", output_file)

else:
    print("\nNo NIFTY 500 stocks found with 3x delivery spike.")
