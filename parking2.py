import hopsworks
import parking
import sys
import requests
import time
from datetime import datetime, timedelta, timezone
import pytz

import pandas as pd

def get_parking_last_hour(api_key: str, hours: int) -> pd.DataFrame:
    API_BASE_URL = "https://apis.smartcity.hn/bildungscampus/iotplatform/parkinglot/v1"
    API_KEY = api_key
    AUTH_GROUP = "parkinglot_assets"
    KEY = "current"

    berlin_tz = pytz.timezone("Europe/Berlin")
    now_berlin = datetime.now(berlin_tz)
    broad_start = int((now_berlin - timedelta(hours=hours)).timestamp() * 1000)
    broad_end = int(now_berlin.timestamp() * 1000)

    print(f"Time now: {now_berlin} ({now_berlin.timestamp()})")

    # === Step 1: Get all entity IDs with pagination ===
    page = 0
    all_entities = []

    while True:
        url = f"{API_BASE_URL}/authGroup/{AUTH_GROUP}/entityId"
        params = {
            "page": page,
            "x-apikey": API_KEY
        }

        resp = requests.get(url, params=params)
        if resp.status_code != 200:
            print(f"❌ Entity list request failed ({resp.status_code})")
            print(resp.text)
            raise SystemExit

        data = resp.json()
        entities = data.get("entities", [])
        all_entities.extend(entities)

        print(f"Fetched page {page} with {len(entities)} entities")

        if not data.get("hasNext", False):
            break

        page += 1
        time.sleep(0.2)  # be polite to API

    print(f"✅ Total entities fetched: {len(all_entities)}")

    # === Step 2: Download time series for each entity ===
    all_records = []

    for entity in all_entities:
        entity_id = entity["entityId"]["id"]
        carpark_name = entity["ENTITY_FIELD"].get("name", "Unknown")

        url = f"{API_BASE_URL}/authGroup/{AUTH_GROUP}/entityId/{entity_id}/valueType/timeseries"
        params = {
            "keys": KEY,
            "startTs": broad_start,
            "endTs": broad_end,
            "x-apikey": API_KEY
        }

        response = requests.get(url, params=params)
        if response.status_code != 200:
            print(f"⚠️ Skipping {entity_id} (status {response.status_code})")
            continue

        data = response.json()
        timeseries = data.get("timeseries", {}).get(KEY, [])

        for e in timeseries:
            all_records.append({
                "carpark_id": entity_id,
                "carpark_name": carpark_name,
                "unix_time": int(e["ts"]),
                "y": float(e["value"]),
                "vendor": "MSR"
            })

    if not all_records:
        print("No data returned from any entity.")
        raise SystemExit

    df = pd.DataFrame(all_records)
    df["ds"] = pd.to_datetime(df["unix_time"], unit="ms", utc=True).dt.tz_convert("Europe/Berlin")
    df = df.sort_values(["carpark_name", "ds"]).reset_index(drop=True)

    return df



if __name__ == "__main__":
    print("All arguments:", sys.argv)
    print("First argument:", sys.argv[1])
    print("2nd argument:", sys.argv[2])

    api_key=""
    df = get_parking_last_hour(api_key=sys.argv[1], hours=int(sys.argv[2]))
    print(df.tail(10))
    proj = hopsworks.login()
    fs = proj.get_feature_store()
    fg = fs.get_feature_group("parking")
    fg.insert(df)