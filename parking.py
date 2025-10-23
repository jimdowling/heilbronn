import requests
import time
from datetime import datetime, timedelta, timezone
import pytz

import pandas as pd


def get_parking_last_hour(api_key, hours: int) -> pd.DataFrame:
    API_BASE_URL = "https://apis.smartcity.hn/bildungscampus/iotplatform/parkinglot/v1"
    API_KEY = api_key
    AUTH_GROUP = "parkinglot_assets"
    ENTITY_ID = "65e6efa0-83a5-11ee-be51-afb66b2180af"
    KEY = "current"

    # === Step 1: Fetch a broad window to find latest timestamp ===
    berlin_tz = pytz.timezone("Europe/Berlin")
    now_berlin = datetime.now(berlin_tz)
    broad_start = int((now_berlin - timedelta(hours=hours)).timestamp() * 1000)
    broad_end = int(now_berlin.timestamp() * 1000)
    
    print(f"Time now is: {now_berlin}")
    print(f"Unix time now is: {now_berlin.timestamp()}")
        
#     broad_start = int((datetime.utcnow() - timedelta(hours=hours)).timestamp() * 1000)
#     broad_end = int(time.time() * 1000)

    url = f"{API_BASE_URL}/authGroup/{AUTH_GROUP}/entityId/{ENTITY_ID}/valueType/timeseries"
    params = {
        "keys": KEY,
        "startTs": broad_start,
        "endTs": broad_end,
        "x-apikey": API_KEY
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"❌ Request failed ({response.status_code})")
        print(response.text)
        raise SystemExit

    data = response.json()
    timeseries = data.get("timeseries", {}).get(KEY, [])
    if not timeseries:
        print("No data returned.")
        raise SystemExit

    # === Step 2: Determine latest timestamp and query the last hour ===
    latest_ts = max(e["ts"] for e in timeseries)
    start_ts = latest_ts - hours * 60 * 60 * 1000  # one hour earlier

    print(f"API unix timestamp returned is: {latest_ts/1000}")
    dt_berlin = datetime.fromtimestamp(latest_ts/1000, berlin_tz)
    print(f"API latest timestamp returned is: {dt_berlin}")
    
    
    params = {
        "keys": KEY,
        "startTs": start_ts,
        "endTs": latest_ts,
        "x-apikey": API_KEY
    }

    response = requests.get(url, params=params)
    if response.status_code != 200:
        print(f"❌ Second query failed ({response.status_code})")
        print(response.text)
        raise SystemExit

    data = response.json()
    timeseries = data.get("timeseries", {}).get(KEY, [])

    # === Step 3: Convert to DataFrame (local time → bigint) ===
    df = pd.DataFrame([
        {
            "carpark_id": "mitte",
            "carpark_name": "Parkhaus Mitte",
            "unix_time": int(e["ts"]),  # ✅ Cast timestamp to BIGINT (epoch ms)
            "y": float(e["value"]),
            "vendor": "MSR"
        }
        for e in timeseries
    ])

    df["ds"] = (
        pd.to_datetime(df["unix_time"], unit="ms", utc=True)
        .dt.tz_convert("Europe/Berlin")
    )   
    
    df = df.sort_values("ds").reset_index(drop=True)
    return df



if __name__ == "__main__":
    
    get_parking_last_hour(3)
