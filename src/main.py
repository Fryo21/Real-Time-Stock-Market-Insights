import requests
from extract import Connect_to_api, extract_json_response
from config import logger



def main():


    try:
        response = Connect_to_api()
        
        data = extract_json_response(response)

    except requests.exceptions.RequestException as error:
        logger.error(f"Could not retrieve stock data: {error}")
        return

    except (ValueError, TypeError, KeyError) as error:
        logger.error(f"Could not process stock data: {error}")
        return


    for stock in data:
        results = {
            'date' : stock['date'],
            'symbol' : stock['symbol'],
            'open' : stock['open'],
            'high' : stock['high'],
            'low' : stock['low'],
            'close' : stock['close']
        }

        print(results)

    return None


if __name__ == '__main__':
    main()

else:
    print("Not main")
