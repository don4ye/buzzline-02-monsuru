# kafka_consumer_prince.py

# Consume financial stock price update messages from a Kafka topic and process them.
import os
import csv
from dotenv import load_dotenv
from utils.utils_consumer import create_kafka_consumer
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

def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id = os.getenv("KAFKA_CONSUMER_GROUP_ID", "stock_consumer_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id

def get_csv_file_path() -> str:
    """Fetch file path for saving stock price updates."""
    file_path = os.getenv("CSV_FILE_PATH", "financial_stock_prices.csv")
    logger.info(f"CSV file path: {file_path}")
    return file_path

#####################################
# Define a function to process a single message
#####################################

def process_message(message: str, file_path: str) -> None:
    """
    Process a single financial stock price update message.

    The function logs the message and saves stock prices to a CSV file.

    Args:
        message (str): The message to process (stock price update).
        file_path (str): Path to the CSV file where stock data will be saved.
    """
    logger.info(f"Processing message: {message}")

    # Parse the message to extract stock symbol and price
    try:
        # Assuming message format is: 'symbol price: $price_value'
        symbol, price_str = message.split(" price: $")
        price = float(price_str.strip())  # Convert price to float

        # Log the message to a CSV file
        with open(file_path, mode='a', newline='') as file:
            writer = csv.writer(file)
            writer.writerow([symbol, price])
        
        logger.info(f"Stock data saved to CSV: {symbol}, {price}")
    except Exception as e:
        logger.error(f"Failed to process message: {e}")

#####################################
# Define main function for this module
#####################################

def main() -> None:
    """
    Main entry point for the consumer.

    - Reads Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Processes stock price update messages from the Kafka topic.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    file_path = get_csv_file_path()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}', saving to '{file_path}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # Check the type of the message to determine whether decoding is needed
            message_str = message.value
            if isinstance(message_str, bytes):
                message_str = message_str.decode('utf-8')  # Decode if it's in bytes
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str, file_path)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")

#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":
    main()
