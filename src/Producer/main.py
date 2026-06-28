import requests
import time
from kafka.errors import KafkaError
from extract import Connect_to_api, extract_json_response
from config import logger
from producer_setup import init_producer, topic


def main():

    print("Running producer...")
    try:
        response = Connect_to_api()
        data = extract_json_response(response)

    except requests.exceptions.RequestException as error:
        logger.error(f"Could not retrieve stock data: {error}")
        print(f"API error: {error}")
        return

    except (ValueError, TypeError, KeyError) as error:
        logger.error(f"Could not process stock data: {error}")
        print(f"Data processing error: {error}")
        return

    if not data:
        print("No records to publish.")
        return

    try:
        producer = init_producer()
    except KafkaError as error:
        logger.error(f"Could not connect to Kafka: {error}")
        print(f"Kafka connection error: {error}")
        return

    sent = 0
    for stock in data:
        results = {
            'date': stock['date'],
            'symbol': stock['symbol'],
            'open': stock['open'],
            'high': stock['high'],
            'low': stock['low'],
            'close': stock['close'],
        }

        try:
            producer.send(topic, results).get(timeout=10)
            sent += 1
            print(f"Sent record {sent}/{len(data)} to '{topic}'")
        except KafkaError as error:
            logger.error(f"Failed to send record for {stock['symbol']}: {error}")
            print(f"Kafka send error: {error}")
            break

        time.sleep(1)

    producer.flush()
    producer.close()
    print(f"Done. Published {sent} record(s) to topic '{topic}'.")


if __name__ == '__main__':
    main()
