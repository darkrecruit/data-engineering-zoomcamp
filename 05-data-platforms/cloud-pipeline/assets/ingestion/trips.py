"""@bruin

name: ingestion.trips

type: python

image: python:3.11

connection: gcp-default

materialization:
  type: table
  strategy: append

@bruin"""

import json
import os
from datetime import datetime

import pandas as pd
import requests
from dateutil.relativedelta import relativedelta


def materialize():
    """
    Fetch NYC Taxi trip data from the TLC public endpoint.

    Uses BRUIN_START_DATE and BRUIN_END_DATE to determine which months to fetch.
    Uses BRUIN_VARS to get the list of taxi types to ingest.

    Returns a DataFrame with raw trip data (no transformations).
    """

    # Get date range from Bruin environment variables
    start_date_str = os.getenv("BRUIN_START_DATE", "2022-01-01")
    end_date_str = os.getenv("BRUIN_END_DATE", "2022-12-31")

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Get taxi types from pipeline variables
    bruin_vars_str = os.getenv("BRUIN_VARS", '{"taxi_types": ["yellow"]}')
    bruin_vars = json.loads(bruin_vars_str)
    taxi_types = bruin_vars.get("taxi_types", ["yellow"])

    # TLC data source base URL
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    # Generate list of files to fetch: one per taxi_type per month in the date range
    files_to_fetch = []
    current_date = start_date

    while current_date <= end_date:
        year = current_date.strftime("%Y")
        month = current_date.strftime("%m")

        for taxi_type in taxi_types:
            filename = f"{taxi_type}_tripdata_{year}-{month}.parquet"
            url = base_url + filename
            files_to_fetch.append((url, taxi_type, year, month))

        # Move to next month
        current_date = current_date + relativedelta(months=1)

    # Fetch and concatenate all data
    all_data = []
    extracted_at = datetime.utcnow()

    for url, taxi_type, year, month in files_to_fetch:
        try:
            print(f"Fetching {url}...")
            response = requests.get(url, timeout=30)

            if response.status_code == 200:
                # Write to temporary file and read with pandas
                temp_file = f"/tmp/{taxi_type}_tripdata_{year}-{month}.parquet"
                with open(temp_file, "wb") as f:
                    f.write(response.content)

                df = pd.read_parquet(temp_file)
                df["extracted_at"] = extracted_at
                df["taxi_type"] = taxi_type
                all_data.append(df)
                print(f"✓ Successfully fetched {len(df)} rows from {year}-{month}")

            else:
                print(f"✗ Failed to fetch {url} (status: {response.status_code})")

        except Exception as e:
            print(f"✗ Error fetching {url}: {str(e)}")

    # Combine all data
    if all_data:
        result_df = pd.concat(all_data, ignore_index=True)
        print(f"\nTotal rows fetched: {len(result_df)}")
        return result_df
    else:
        print("No data fetched. Returning empty DataFrame.")
        return pd.DataFrame()
