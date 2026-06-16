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

from sqlalchemy.util import symbol

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
        response.raise_for_status()  # Check if the request was successful
        data = response.json()

        for record in data["values"]:
            record["symbol"] = symbol  # Add the symbol to each record

        all_records.extend(data["values"])
        logger.info(f"Extracted data for symbol: {symbol}, Records: {len(data['values'])} rows")

    logger.info(f"Total records extracted: {len(all_records)} rows")      #coming out of the loop, we can log the total number of records extracted across all symbols
    return all_records


# TRANSFORM FUNCTION

def transform_data(records):

    # converting the dictionary timeseries data into a dataframe
    df = pd.DataFrame.from_dict(records, orient='index')

    # Resetting the index to make the date a column
    df.reset_index(inplace=True)

    # rename index column to "date"
    df.rename(columns={'index': 'date'}, inplace=True)

    #convert the date column to datetime format 
    df['date'] = pd.to_datetime(df['date'])

    # Renaming  the columns for simplicity, removing the numbers and dots
    df.rename(columns={
        '1. open': 'open',
        '2. high': 'high',
        '3. low': 'low',
        '4. close': 'close',
        '5. volume': 'volume'
    }, inplace=True)


    # converting the numericcolumns to coreect data types
    df = df.astype({
        'open': 'float',
        'high': 'float',
        'low': 'float',
        'close': 'float',
        'volume': 'int'
    })

    # sort by date (important for time series data)
    df = df.sort_values('date').reset_index(drop=True)

    # remove duplicates if any
    df = df.drop_duplicates()

    return df


        