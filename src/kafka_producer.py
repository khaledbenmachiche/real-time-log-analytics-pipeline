"""
Kafka Producer for Web Access Logs
This script reads web access logs and publishes RAW log lines to a Kafka topic.
NO PARSING - sends raw lines for Spark to process.
"""

import time
from confluent_kafka import Producer
import logging
import sys
import os

# Add config directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from config import KAFKA_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WebLogKafkaProducer:
    """
    Kafka Producer for streaming RAW web access logs (no parsing)
    """
    
    def __init__(self, kafka_config=None):
        """
        Initialize Kafka producer with configuration
        
        Args:
            kafka_config (dict): Kafka configuration parameters
        """
        self.config = kafka_config or KAFKA_CONFIG
        self.producer = None
        self.topic = self.config['topic_name']
    
    def connect(self):
        """
        Establish connection to Kafka broker
        """
        try:
            # Handle both string and list formats for bootstrap_servers
            bootstrap_servers = self.config['bootstrap_servers']
            if isinstance(bootstrap_servers, list):
                servers = ','.join(bootstrap_servers)
            else:
                servers = bootstrap_servers
                
            producer_config = {
                'bootstrap.servers': servers,
                'retries': 3,
                'acks': 'all',
                'batch.size': 16384,
                'linger.ms': 10
            }
            self.producer = Producer(producer_config)
            logger.info(f"Connected to Kafka broker: {servers}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def send_raw_log(self, log_line, key=None):
        """
        Send raw log line to Kafka topic (no parsing)
        
        Args:
            log_line (str): Raw log line
            key (str): Optional message key
        """
        try:
            # Extract IP from raw line for partitioning (simple regex)
            import re
            ip_match = re.search(r'^(\d+\.\d+\.\d+\.\d+)', log_line.strip())
            message_key = key or (ip_match.group(1) if ip_match else 'unknown')
            
            # Delivery callback function
            def delivery_report(err, msg):
                if err is not None:
                    logger.error(f"Message delivery failed: {err}")
                else:
                    logger.debug(f"Message delivered to {msg.topic()} [{msg.partition()}]")
            
            self.producer.produce(
                self.topic,
                value=log_line.strip().encode('utf-8'),
                key=message_key.encode('utf-8'),
                callback=delivery_report
            )
            
            # Trigger delivery reports (non-blocking)
            self.producer.poll(0)
            
        except Exception as e:
            logger.error(f"Failed to send message to Kafka: {e}")
    
    def stream_from_file(self, file_path, delay=0.1, max_lines=None):
        """
        Stream logs from a file to Kafka
        
        Args:
            file_path (str): Path to log file
            delay (float): Delay between messages in seconds
            max_lines (int): Maximum number of lines to process (None for all)
        """
        if not self.producer:
            logger.error("Producer not connected. Call connect() first.")
            return
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as file:
                lines_processed = 0
                
                logger.info(f"Starting to stream logs from {file_path}")
                
                for line in file:
                    if max_lines and lines_processed >= max_lines:
                        break
                    
                    # Send raw log line (no parsing)
                    if line.strip():  # Skip empty lines
                        self.send_raw_log(line)
                        lines_processed += 1
                        
                        if lines_processed % 100 == 0:
                            logger.info(f"Processed {lines_processed} log entries")
                    
                    # Add delay to simulate real-time streaming
                    if delay > 0:
                        time.sleep(delay)
                
                logger.info(f"Finished streaming {lines_processed} log entries")
                
        except FileNotFoundError:
            logger.error(f"Log file not found: {file_path}")
        except Exception as e:
            logger.error(f"Error streaming from file: {e}")
    
    def close(self):
        """
        Close Kafka producer connection
        """
        if self.producer:
            self.producer.flush()
            logger.info("Kafka producer connection closed")


def main():
    """
    Main function to run the Kafka producer
    """
    # Initialize producer
    producer = WebLogKafkaProducer()
    
    try:
        # Connect to Kafka
        if not producer.connect():
            logger.error("Failed to connect to Kafka. Exiting.")
            return
        
        # Path to NASA log file
        log_file_path = os.path.join(
            os.path.dirname(__file__), 
            '..', 
            'NASA_access_log_Jul95'
        )
        
        # Check if log file exists
        if not os.path.exists(log_file_path):
            logger.error(f"Log file not found: {log_file_path}")
            return
        
        # Stream logs to Kafka
        # Adjust delay and max_lines for testing
        producer.stream_from_file(
            file_path=log_file_path,
            delay=0.05,  # 50ms delay between messages
        )
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        producer.close()


if __name__ == "__main__":
    main()
