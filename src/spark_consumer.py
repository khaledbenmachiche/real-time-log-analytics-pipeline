"""
Spark Structured Streaming Consumer for Web Access Logs
This script consumes web access logs from Kafka, processes them in real-time,
and sends the results to Elasticsearch for visualization.
"""

import json
import sys
import os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, from_json, window, count, desc, asc, 
    regexp_extract, regexp_replace, when, isnan, isnull, 
    to_timestamp, date_format, expr, lit
)
from pyspark.sql.types import (
    StructType, StructField, StringType,
    IntegerType, TimestampType, LongType
)
import logging

# Add config directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from config import KAFKA_CONFIG, SPARK_CONFIG, ELASTICSEARCH_CONFIG, LOG_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class WebLogSparkConsumer:
    """
    Spark Structured Streaming consumer for processing web access logs
    """
    
    def __init__(self, kafka_config=None, spark_config=None, es_config=None):
        """
        Initialize Spark consumer with configurations
        
        Args:
            kafka_config (dict): Kafka configuration
            spark_config (dict): Spark configuration  
            es_config (dict): Elasticsearch configuration
        """
        self.kafka_config = kafka_config or KAFKA_CONFIG
        self.spark_config = spark_config or SPARK_CONFIG
        self.es_config = es_config or ELASTICSEARCH_CONFIG
        self.spark = None
    
    def create_spark_session(self):
        """
        Create and configure Spark session
        """
        try:
            self.spark = SparkSession.builder \
                .appName(self.spark_config['app_name']) \
                .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,org.elasticsearch:elasticsearch-spark-30_2.12:8.12.1") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
                .getOrCreate()
            
            # Set log level to reduce noise
            self.spark.sparkContext.setLogLevel("WARN")
            
            logger.info("Spark session created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create Spark session: {e}")
            return False
    
    def read_from_kafka(self):
        """
        Read streaming data from Kafka topic
        
        Returns:
            DataFrame: Spark streaming DataFrame
        """
        try:
            kafka_df = self.spark \
                .readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.kafka_config['bootstrap_servers']) \
                .option("subscribe", self.kafka_config['topic_name']) \
                .option("startingOffsets", "latest") \
                .option("failOnDataLoss", "false") \
                .load()
            
            logger.info(f"Connected to Kafka topic: {self.kafka_config['topic_name']}")
            return kafka_df
            
        except Exception as e:
            logger.error(f"Failed to read from Kafka: {e}")
            raise
    
    def parse_kafka_messages(self, kafka_df):
        """
        Parse raw log lines from Kafka and extract all fields
        
        Args:
            kafka_df (DataFrame): Raw Kafka DataFrame
            
        Returns:
            DataFrame: Parsed log DataFrame
        """
        try:
            # First, get the raw log lines from Kafka value
            raw_logs_df = kafka_df.select(
                col("value").cast("string").alias("raw_log"),
                col("timestamp").alias("kafka_timestamp")
            )
            
            # Define Apache Common Log Format regex pattern
            log_pattern = r'(\S+)\s+(\S+)\s+(\S+)\s+\[([^\]]+)\]\s+"([^"]+)"\s+(\d+)\s+(\S+)'
            
            # Extract fields using regex
            parsed_df = raw_logs_df.select(
                regexp_extract(col("raw_log"), log_pattern, 1).alias("ip"),
                regexp_extract(col("raw_log"), log_pattern, 2).alias("remote_log_name"),
                regexp_extract(col("raw_log"), log_pattern, 3).alias("remote_user"),
                regexp_extract(col("raw_log"), log_pattern, 4).alias("timestamp_str"),
                regexp_extract(col("raw_log"), log_pattern, 5).alias("request"),
                regexp_extract(col("raw_log"), log_pattern, 6).cast("int").alias("status_code"),
                regexp_extract(col("raw_log"), log_pattern, 7).alias("content_size_str"),
                col("kafka_timestamp"),
                col("raw_log")
            ).filter(col("ip") != "")  # Filter out lines that didn't match the pattern
            
            # Extract method and URL from request field
            parsed_df = parsed_df.withColumn(
                "method", 
                when(col("request").rlike(r'^(\S+)'), 
                     regexp_extract(col("request"), r'^(\S+)', 1))
                .otherwise("")
            ).withColumn(
                "url",
                when(col("request").rlike(r'^\S+\s+(\S+)'),
                     regexp_extract(col("request"), r'^\S+\s+(\S+)', 1))
                .otherwise("")
            ).withColumn(
                "protocol",
                when(col("request").rlike(r'^\S+\s+\S+\s+(\S+)'),
                     regexp_extract(col("request"), r'^\S+\s+\S+\s+(\S+)', 1))
                .otherwise("")
            )
            
            # Convert content_size, handling "-" values
            parsed_df = parsed_df.withColumn(
                "content_size",
                when(col("content_size_str") == "-", 0)
                .otherwise(col("content_size_str").cast("int"))
            )
            
            # Parse timestamp (Apache format: 01/Jul/1995:00:00:12 -0400)
            # For now, we'll keep it as string and convert to proper timestamp
            parsed_df = parsed_df.withColumn(
                "timestamp",
                # Try to convert Apache timestamp to standard format
                # This is a simplified conversion - you might need to adjust for your specific format
                to_timestamp(
                    regexp_replace(col("timestamp_str"), r"(\d{2})/(\w{3})/(\d{4}):(\d{2}):(\d{2}):(\d{2}) ([+-]\d{4})", 
                                 "$3-$2-$1 $4:$5:$6"), 
                    "yyyy-MMM-dd HH:mm:ss"
                )
            )
            
            # If timestamp conversion fails, use current timestamp
            parsed_df = parsed_df.withColumn(
                "timestamp",
                when(col("timestamp").isNull(), expr("current_timestamp()"))
                .otherwise(col("timestamp"))
            )
            
            # Add hour and minute columns for aggregations
            parsed_df = parsed_df.withColumn(
                "hour", date_format(col("timestamp"), "yyyy-MM-dd HH:00:00")
            ).withColumn(
                "minute", date_format(col("timestamp"), "yyyy-MM-dd HH:mm:00")
            )
            
            # Select final columns
            final_df = parsed_df.select(
                "ip", "timestamp", "method", "url", "protocol", 
                "status_code", "content_size", "hour", "minute", 
                "kafka_timestamp", "raw_log"
            )
            
            logger.info("Raw log lines parsed and fields extracted successfully")
            return final_df
            
        except Exception as e:
            logger.error(f"Failed to parse raw log lines: {e}")
            raise
    
    def process_error_logs(self, df):
        """
        Filter and process error logs (404, 500 status codes)
        
        Args:
            df (DataFrame): Parsed log DataFrame
            
        Returns:
            DataFrame: Error logs DataFrame
        """
        error_df = df.filter(
            (col("status_code") == 404) | 
            (col("status_code") == 500) |
            (col("status_code") >= 400)
        ).select(
            col("ip"),
            col("timestamp"),
            col("url"),
            col("status_code"),
            col("method"),
            lit("error").alias("log_type")
        )
        
        return error_df
    
    def process_traffic_by_minute(self, df):
        """
        Count requests per minute
        
        Args:
            df (DataFrame): Parsed log DataFrame
            
        Returns:
            DataFrame: Traffic count by minute
        """
        traffic_df = df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "1 minute").alias("time_window")
            ) \
            .agg(
                count("*").alias("request_count")
            ) \
            .select(
                col("time_window.start").alias("minute"),
                col("request_count"),
                lit("traffic").alias("log_type")
            )
        
        return traffic_df
    
    def process_top_ips(self, df):
        """
        Identify most active IP addresses
        
        Args:
            df (DataFrame): Parsed log DataFrame
            
        Returns:
            DataFrame: Top active IPs
        """
        ip_df = df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "2 minutes").alias("time_window"),
                col("ip")
            ) \
            .agg(
                count("*").alias("request_count")
            ) \
            .select(
                col("time_window.start").alias("window_start"),
                col("ip"),
                col("request_count"),
                lit("top_ips").alias("log_type")
            )
        
        return ip_df
    
    def process_url_analysis(self, df):
        """
        Analyze most requested URLs
        
        Args:
            df (DataFrame): Parsed log DataFrame
            
        Returns:
            DataFrame: URL analysis DataFrame
        """
        url_df = df \
            .withWatermark("timestamp", "2 minutes") \
            .groupBy(
                window(col("timestamp"), "2 minutes").alias("time_window"),
                col("url")
            ) \
            .agg(
                count("*").alias("request_count")
            ) \
            .select(
                col("time_window.start").alias("window_start"),
                col("url"),
                col("request_count"),
                lit("url_analysis").alias("log_type")
            )
        
        return url_df
    
    def write_to_console(self, df, query_name):
        """
        Write streaming results to console for debugging
        
        Args:
            df (DataFrame): DataFrame to write
            query_name (str): Name for the query
        """
        query = df.writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .option("numRows", 20) \
            .queryName(query_name) \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def write_to_elasticsearch(self, df, query_name, index_suffix=""):
        """
        Write streaming results to Elasticsearch
        
        Args:
            df (DataFrame): DataFrame to write
            query_name (str): Name for the query
            index_suffix (str): Suffix for the index name
        """
        def write_to_es(batch_df, epoch_id):
            """
            Function to write each batch to Elasticsearch
            """
            try:
                if batch_df.count() > 0:
                    # Determine index name based on query type
                    if "error" in query_name:
                        index_name = "web-logs-errors"
                    elif "traffic" in query_name:
                        index_name = "web-logs-traffic"
                    elif "ip" in query_name or "top_ips" in query_name:
                        index_name = "web-logs-ip-analysis"
                    elif "url" in query_name:
                        index_name = "web-logs-url-analysis"
                    else:
                        index_name = f"web-logs-{query_name}"
                    
                    # Add timestamp for document ID and indexing
                    batch_with_ts = batch_df.withColumn("@timestamp", expr("current_timestamp()"))
                    
                    # Write to Elasticsearch using elasticsearch-spark connector
                    batch_with_ts.write \
                        .format("org.elasticsearch.spark.sql") \
                        .option("es.nodes", self.es_config['hosts'][0].replace('http://', '')) \
                        .option("es.port", "9200") \
                        .option("es.resource", index_name) \
                        .option("es.nodes.wan.only", "true") \
                        .option("es.index.auto.create", "true") \
                        .option("es.mapping.id", "@timestamp") \
                        .mode("append") \
                        .save()
                    
                    logger.info(f"Batch {epoch_id} written to Elasticsearch index '{index_name}' with {batch_df.count()} records")
                else:
                    logger.info(f"Batch {epoch_id} for {query_name} is empty, skipping Elasticsearch write")
                    
            except Exception as e:
                logger.error(f"Error writing batch {epoch_id} to Elasticsearch {query_name}: {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")

        # Use different output modes for different query types
        if "error" in query_name:
            output_mode = "append"
        else:
            output_mode = "update"  # For windowed aggregations
        
        query = df.writeStream \
            .outputMode(output_mode) \
            .foreachBatch(write_to_es) \
            .queryName(query_name) \
            .option("checkpointLocation", f"{LOG_CONFIG['checkpoint_location']}/{query_name}") \
            .trigger(processingTime="30 seconds") \
            .start()
        
        return query
    
    def start_streaming(self):
        """
        Start the streaming application
        """
        try:
            # Read from Kafka
            kafka_df = self.read_from_kafka()
            
            # Parse messages
            parsed_df = self.parse_kafka_messages(kafka_df)
            
            # Process different types of analysis
            error_logs = self.process_error_logs(parsed_df)
            traffic_by_minute = self.process_traffic_by_minute(parsed_df)
            top_ips = self.process_top_ips(parsed_df)
            url_analysis = self.process_url_analysis(parsed_df)
            
            # Start streaming queries
            queries = []
            
            # Console output for debugging
            """
            queries.append(self.write_to_console(error_logs, "error_logs_console"))
            queries.append(self.write_to_console(traffic_by_minute, "traffic_console"))
            queries.append(self.write_to_console(top_ips, "top_ips_console"))
            queries.append(self.write_to_console(url_analysis, "url_analysis_console"))
            """

            
            # Elasticsearch
            queries.append(self.write_to_elasticsearch(error_logs, "error_logs"))
            queries.append(self.write_to_elasticsearch(traffic_by_minute, "traffic_by_minute"))
            queries.append(self.write_to_elasticsearch(top_ips, "top_ips"))
            queries.append(self.write_to_elasticsearch(url_analysis, "url_analysis"))
            
            logger.info("All streaming queries started successfully")
            
            # Wait for termination
            for query in queries:
                query.awaitTermination()
                
        except Exception as e:
            logger.error(f"Error in streaming application: {e}")
            raise
    
    def stop(self):
        """
        Stop Spark session
        """
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """
    Main function to run the Spark consumer
    """
    consumer = WebLogSparkConsumer()
    
    try:
        # Create Spark session
        if not consumer.create_spark_session():
            logger.error("Failed to create Spark session. Exiting.")
            return
        
        # Start streaming
        logger.info("Starting Spark Structured Streaming application...")
        consumer.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
    finally:
        consumer.stop()


if __name__ == "__main__":
    main()
