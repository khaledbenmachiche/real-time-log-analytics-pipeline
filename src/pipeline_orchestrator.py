"""
Pipeline Orchestrator for Web Log Analysis
This script coordinates the entire pipeline: Kafka producer, Spark consumer, and Elasticsearch indexing
"""

import os
import sys
import time
import logging
import subprocess
import threading
from datetime import datetime

# Add config directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from config import KAFKA_CONFIG, SPARK_CONFIG, ELASTICSEARCH_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class PipelineOrchestrator:
    """
    Orchestrates the entire web log analysis pipeline
    """
    
    def __init__(self):
        self.processes = {}
        self.running = False
    
    def check_kafka_status(self):
        """
        Check if Kafka is running
        """
        try:
            # Simple way to check if Kafka is accessible
            import socket
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            result = sock.connect_ex(('localhost', 9092))
            sock.close()
            return result == 0
        except Exception:
            return False
    
    def check_elasticsearch_status(self):
        """
        Check if Elasticsearch is running
        """
        try:
            import requests
            response = requests.get('http://localhost:9200', timeout=5)
            return response.status_code == 200
        except Exception:
            return False
    
    def start_kafka_producer(self):
        """
        Start Kafka producer in a separate process
        """
        try:
            producer_script = os.path.join(os.path.dirname(__file__), 'kafka_producer.py')
            
            # Start producer process
            process = subprocess.Popen(
                [sys.executable, producer_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.processes['kafka_producer'] = process
            logger.info("Kafka producer started")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start Kafka producer: {e}")
            return False
    
    def start_spark_consumer(self):
        """
        Start Spark consumer in a separate process
        """
        try:
            consumer_script = os.path.join(os.path.dirname(__file__), 'spark_consumer.py')
            
            # Start consumer process
            process = subprocess.Popen(
                [sys.executable, consumer_script],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            
            self.processes['spark_consumer'] = process
            logger.info("Spark consumer started")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start Spark consumer: {e}")
            return False
    
    def start_elasticsearch_indexing(self):
        """
        Start Elasticsearch indexing process
        """
        try:
            from elasticsearch_indexer import ElasticsearchIndexer, process_spark_output_to_elasticsearch
            
            def indexing_worker():
                """
                Worker function for continuous indexing
                """
                es_indexer = ElasticsearchIndexer()
                es_indexer.setup_indices()
                
                spark_output_dir = "/tmp/spark-output"
                
                while self.running:
                    try:
                        if os.path.exists(spark_output_dir):
                            process_spark_output_to_elasticsearch(spark_output_dir, es_indexer)
                        time.sleep(30)  # Check every 30 seconds
                    except Exception as e:
                        logger.error(f"Error in indexing worker: {e}")
                        time.sleep(60)  # Wait longer on error
            
            # Start indexing in a separate thread
            indexing_thread = threading.Thread(target=indexing_worker, daemon=True)
            indexing_thread.start()
            
            self.processes['elasticsearch_indexer'] = indexing_thread
            logger.info("Elasticsearch indexing started")
            return True
            
        except Exception as e:
            logger.error(f"Failed to start Elasticsearch indexing: {e}")
            return False
    
    def monitor_processes(self):
        """
        Monitor running processes and restart if needed
        """
        while self.running:
            try:
                for name, process in self.processes.items():
                    if hasattr(process, 'poll') and process.poll() is not None:
                        logger.warning(f"Process {name} has stopped unexpectedly")
                
                time.sleep(10)  # Check every 10 seconds
                
            except Exception as e:
                logger.error(f"Error monitoring processes: {e}")
                time.sleep(30)
    
    def start_pipeline(self):
        """
        Start the complete pipeline
        """
        logger.info("Starting Web Log Analysis Pipeline...")
        
        # Check prerequisites
        if not self.check_kafka_status():
            logger.error("Kafka is not running. Please start Kafka first.")
            return False
        
        if not self.check_elasticsearch_status():
            logger.error("Elasticsearch is not running. Please start Elasticsearch first.")
            return False
        
        self.running = True
        
        # Start components in order
        logger.info("Starting Spark consumer...")
        if not self.start_spark_consumer():
            return False
        
        # Wait a bit for Spark to initialize
        time.sleep(10)
        
        logger.info("Starting Elasticsearch indexing...")
        if not self.start_elasticsearch_indexing():
            return False
        
        # Wait a bit more
        time.sleep(5)
        
        logger.info("Starting Kafka producer...")
        if not self.start_kafka_producer():
            return False
        
        logger.info("Pipeline started successfully!")
        
        # Start monitoring
        monitor_thread = threading.Thread(target=self.monitor_processes, daemon=True)
        monitor_thread.start()
        
        return True
    
    def stop_pipeline(self):
        """
        Stop all pipeline components
        """
        logger.info("Stopping Web Log Analysis Pipeline...")
        
        self.running = False
        
        # Stop all processes
        for name, process in self.processes.items():
            try:
                if hasattr(process, 'terminate'):
                    process.terminate()
                    process.wait(timeout=10)
                logger.info(f"Stopped {name}")
            except Exception as e:
                logger.error(f"Error stopping {name}: {e}")
        
        self.processes.clear()
        logger.info("Pipeline stopped")
    
    def get_pipeline_status(self):
        """
        Get status of all pipeline components
        """
        status = {
            'kafka_running': self.check_kafka_status(),
            'elasticsearch_running': self.check_elasticsearch_status(),
            'processes': {}
        }
        
        for name, process in self.processes.items():
            if hasattr(process, 'poll'):
                status['processes'][name] = 'running' if process.poll() is None else 'stopped'
            else:
                status['processes'][name] = 'running'
        
        return status


def print_usage():
    """
    Print usage instructions
    """
    print("""
Web Log Analysis Pipeline Orchestrator

Usage:
    python pipeline_orchestrator.py [command]

Commands:
    start    - Start the complete pipeline
    stop     - Stop all pipeline components
    status   - Show pipeline status
    help     - Show this help message

Prerequisites:
    - Kafka server running on localhost:9092
    - Elasticsearch running on localhost:9200
    - Required Python packages installed (see requirements.txt)

Example:
    python pipeline_orchestrator.py start
    """)


def main():
    """
    Main function
    """
    if len(sys.argv) < 2:
        print_usage()
        return
    
    command = sys.argv[1].lower()
    orchestrator = PipelineOrchestrator()
    
    try:
        if command == 'start':
            if orchestrator.start_pipeline():
                logger.info("Pipeline is running. Press Ctrl+C to stop.")
                try:
                    while True:
                        time.sleep(1)
                except KeyboardInterrupt:
                    logger.info("Received interrupt signal")
            else:
                logger.error("Failed to start pipeline")
        
        elif command == 'stop':
            orchestrator.stop_pipeline()
        
        elif command == 'status':
            status = orchestrator.get_pipeline_status()
            print(f"Kafka: {'✓' if status['kafka_running'] else '✗'}")
            print(f"Elasticsearch: {'✓' if status['elasticsearch_running'] else '✗'}")
            print("Processes:")
            for name, state in status['processes'].items():
                print(f"  {name}: {state}")
        
        elif command == 'help':
            print_usage()
        
        else:
            print(f"Unknown command: {command}")
            print_usage()
    
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        orchestrator.stop_pipeline()


if __name__ == "__main__":
    main()
