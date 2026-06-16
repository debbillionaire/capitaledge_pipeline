# Capitaledge Stock Price Pipeline
# ETL script to extract stock data from Alpha Vantage API, transform it into a structured format, and load it into a PostgreSQL database.

# import libraries
import requests
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from dotenv import load_dotenv  
import os
import logging
import time


# Set up logging
# There are 4 main levels: DEBUG, INFO, WARNING, ERROR. Will be using INFO for general messages about the pipeline's progress.

logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s  %(levelname)s  %(message)s",
    handlers=[
        logging.FileHandler("capitaledge_pipeline.log"),  # Log to a file
        logging.StreamHandler()  # Also log to console
    ]

)

logger = logging.getLogger(__name__)

# DATABASE CONFIGURATION

load_dotenv()
API_KEY = os.getenv('API_KEY')
DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = os.getenv('DB_PORT')
DB_NAME = os.getenv('DB_NAME')

symbols = ['IBM', 'TSCO.LON', 'SHOP.TRT', 'GPV.TRV']

def extract(symbols):

    all_records = []

    for symbol in symbols:

        url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={API_KEY}"

        response = requests.get(url)
        response.raise_for_status()

        data = response.json()
        time.sleep(12)  # Sleep for 12 seconds to respect API rate limits (5 calls per minute)

        if "Time Series (Daily)" not in data:

            logger.error(f"API response error for {symbol}: {data}")

            continue

        time_series = data["Time Series (Daily)"]

        for date, values in time_series.items():

            all_records.append({
                "symbol": symbol,
                "date": date,
                "open": values["1. open"],
                "high": values["2. high"],
                "low": values["3. low"],
                "close": values["4. close"],
                "volume": values["5. volume"]
            })

        logger.info(
            f"Extracted data for {symbol}: {len(time_series)} rows"
        )

    logger.info(
        f"Total records extracted: {len(all_records)} rows"
    )

    return all_records


# TRANSFORM FUNCTION

def transform_data(records):

    df = pd.DataFrame(records)

    # Convert date column
    df['date'] = pd.to_datetime(df['date'])

    # Convert numeric columns
    df = df.astype({
        'open': float,
        'high': float,
        'low': float,
        'close': float,
        'volume': int
    })

    # Remove duplicates
    df = df.drop_duplicates()

    # Sort by date
    df = df.sort_values('date')

    logger.info(
        f"Transformation completed. Rows in transformed dataframe: {len(df)}"
    )

    return df



# LOAD FUNCTION
def load(df):
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

    # creating engine using sqlalchemy
    engine = create_engine(db_url)

    #load dataframe to sql database
    df.to_sql('stockprices_data', engine, if_exists='append', index=False)
    logger.info(f"Data loaded successfully into database: {len(df)} rows")


def run_pipeline():
    logger.info("Pipeline started.")
    records = extract(symbols)
    df = transform_data(records)
    load(df)

if __name__ == "__main__":
    run_pipeline()






        