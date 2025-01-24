# kafka_producer_case.py

# Produce real-time stock price updates to a Kafka topic.
import os
import sys
import time
import random
from dotenv import load_dotenv
from utils.utils_producer import (
    verify_services,
    create_kafka_producer,
    create_kafka_topic,
)
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################
load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################

def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("KAFKA_TOPIC", "stock_price_updates")
    logger.info(f"Kafka topic: {topic}")
    return topic

def get_message_interval() -> int:
    """Fetch message interval from environment or use default."""
    interval = int(os.getenv("MESSAGE_INTERVAL_SECONDS", 5))
    logger.info(f"Message interval: {interval} seconds")
    return interval

def get_stock_symbols() -> list:
    """Fetch list of stock symbols to simulate, or use default."""
    symbols = os.getenv("STOCK_SYMBOLS", "AAPL,GOOG,AMZN,TSLA").split(",")
    logger.info(f"Stock symbols: {symbols}")
    return symbols

#####################################
# Message Generator
#####################################

def generate_stock_price_messages(producer, topic, interval_secs, symbols):
    """
    Generate and send real-time stock price updates to a Kafka topic.

    Args:
        producer (KafkaProducer): The Kafka producer instance.
        topic (str): The Kafka topic to send messages to.
        interval_secs (int): Time in seconds between sending messages.
        symbols (list): List of stock symbols to simulate.
    """
    try:
        while True:
            for symbol in symbols:
                # Simulate a stock price with a random range
                price = round(random.uniform(100, 1500), 2)
                message = f"{symbol} price: ${price}"
                logger.info(f"Generated stock price update: {message}")
                producer.send(topic, value=message)
                logger.info(f"Sent message to topic '{topic}': {message}")
                time.sleep(interval_secs)
    except KeyboardInterrupt:
        logger.warning("Producer interrupted by user.")
    except Exception as e:
        logger.error(f"Error in stock price message generation: {e}")
    finally:
        producer.close()
        logger.info("Kafka producer closed.")

#####################################
# Main Function
#####################################

def main():
    """
    Main entry point for this producer.

    - Ensures the Kafka topic exists.
    - Creates a Kafka producer using the `create_kafka_producer` utility.
    - Streams simulated stock price updates to the Kafka topic.
    """
    logger.info("START producer.")
    verify_services()

    # fetch .env content
    topic = get_kafka_topic()
    interval_secs = get_message_interval()
    symbols = get_stock_symbols()

    # Create the Kafka producer
    producer = create_kafka_producer()
    if not producer:
        logger.error("Failed to create Kafka producer. Exiting...")
        sys.exit(3)

    # Create topic if it doesn't exist
    try:
        create_kafka_topic(topic)
        logger.info(f"Kafka topic '{topic}' is ready.")
    except Exception as e:
        logger.error(f"Failed to create or verify topic '{topic}': {e}")
        sys.exit(1)

    # Generate and send stock price messages
    logger.info(f"Starting stock price message production to topic '{topic}'...")
    generate_stock_price_messages(producer, topic, interval_secs, symbols)

    logger.info("END producer.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
