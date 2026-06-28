import time
import requests
from config import logger, url, headers


def Connect_to_api():

    stock_symbol = ['TSLA', 'MSFT', 'GOOGL']

    json_response = []

    for symbol in stock_symbol:
        querystring = {
            "function" : "TIME_SERIES_INTRADAY", 
            "symbol" : symbol,
            "interval" : "5min",
            "outputsize" : "compact",
            "datatype" : "json",
        }

        try:
            response = requests.get(url, headers=headers, params=querystring)

            response.raise_for_status()

            data = response.json()

            logger.info(f"Successfully connected to API for {symbol}")

            json_response.append(data)

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")
            continue

        time.sleep(1)

    if not json_response:
        raise requests.exceptions.RequestException("No stock data retrieved from API")

    return json_response

def extract_json_response(response):
    records = []

    for data in response:

        symbol = data['Meta Data']['2. Symbol']

        for date_str, metrics in data['Time Series (5min)'].items():
            record = {
                "symbol" : symbol,
                "date" : date_str,
                "open" : metrics["1. open"],
                "close" : metrics["4. close"],
                "high" : metrics["2. high"],
                "low" : metrics["3. low"]
            }

            records.append(record)
    
    return records
