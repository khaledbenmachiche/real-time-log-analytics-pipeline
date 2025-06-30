# Real-time Web Access Log Analysis Pipeline

## Project Objective

This project implements a complete Big Data pipeline for real-time web access log analysis using modern streaming technologies. The system processes Apache Common Log Format access logs and provides real-time analytics and monitoring capabilities.

**Key Features:**
1. **Real-time Log Ingestion** - Stream web access logs through Apache Kafka
2. **Stream Processing** - Process and analyze logs using Apache Spark Structured Streaming
3. **Data Storage & Indexing** - Store processed data in Elasticsearch for fast search and aggregation
4. **Visualization** - Create dashboards and visualizations using Kibana
5. **Log Generation** - Generate realistic test data for pipeline testing

## Technologies Used

- **Apache Kafka** - Distributed streaming platform for log ingestion
- **Apache Spark** - Unified analytics engine with Spark Structured Streaming
- **Elasticsearch** - Search and analytics engine for data storage and indexing
- **Kibana** - Data visualization and exploration tool
- **Python** - Core implementation language with libraries:
  - `confluent-kafka` - Kafka Python client
  - `pyspark` - Spark Python API
  - `elasticsearch` - Elasticsearch Python client
  - `faker` - Generate realistic test data
- **Docker & Docker Compose** - Containerized infrastructure orchestration

## System Architecture

```
NASA Access Logs / Generated Logs → Kafka Producer → Apache Kafka Topic → 
Spark Structured Streaming → Real-time Processing → Elasticsearch → Kibana Dashboards
```

## Project Structure

```
├── docker-compose.yaml          # Docker infrastructure configuration
├── requirements.txt             # Python dependencies
├── NASA_access_log_Jul95        # Sample NASA access log data (July 1995)
├── .env                         # Environment variables (empty)
├── .gitignore                   # Git ignore rules
├── README.md                    # Project documentation
├── config/
│   └── config.py               # Configuration settings for all components
└── src/
    ├── kafka_producer.py       # Kafka producer for streaming raw log lines
    ├── spark_consumer.py       # Spark Structured Streaming consumer & processor
    ├── elasticsearch_indexer.py # Elasticsearch integration and indexing
    ├── log_generator.py        # Realistic web log generator for testing
    └── pipeline_orchestrator.py # Pipeline coordination and management
```

## Core Components

### 1. Kafka Producer (`kafka_producer.py`)
- Reads raw web access log files (like NASA_access_log_Jul95)
- Streams **raw log lines** to Kafka topic (no parsing at this stage)
- Configurable rate limiting and batch processing
- Supports both file-based and real-time log streaming

### 2. Spark Consumer (`spark_consumer.py`)
- Consumes raw log lines from Kafka using Spark Structured Streaming
- Parses Apache Common Log Format using regex patterns
- Extracts: IP, timestamp, HTTP method, URL, response code, response size, user agent
- Performs real-time analytics and aggregations
- Sends processed data to Elasticsearch

### 3. Elasticsearch Indexer (`elasticsearch_indexer.py`)
- Manages Elasticsearch connections and index creation
- Defines proper mappings for web log data
- Handles bulk indexing operations
- Creates time-based indices for efficient data management

### 4. Log Generator (`log_generator.py`)
- Generates realistic Apache access logs for testing
- Uses Faker library for realistic IP addresses, user agents, URLs
- Weighted distribution of HTTP response codes
- Simulates traffic patterns and user behavior

### 5. Pipeline Orchestrator (`pipeline_orchestrator.py`)
- Coordinates the entire pipeline execution
- Health checks for Kafka and Elasticsearch
- Process management and monitoring
- Error handling and recovery mechanisms

## Installation and Setup

### Prerequisites

- **Docker and Docker Compose** - For infrastructure services
- **Python 3.8+** - For running the pipeline components
- **At least 4GB RAM** - For running all services

### 1. Clone the repository

```bash
git clone <repository-url>
cd <dir>
```

### 2. Install Python dependencies

```bash
# Install all required packages
pip install -r requirements.txt

# Install PySpark separately (recommended)
pip install pyspark==3.5.0
```

### 3. Start the infrastructure services

```bash
# Start all services in background
docker-compose up -d

# Check services status
docker-compose ps
```

This command starts:
- **Zookeeper** (port 2181) - Kafka coordination
- **Kafka** (port 9092) - Message streaming platform  
- **Elasticsearch** (port 9200) - Data storage and search
- **Kibana** (port 5601) - Data visualization interface

### 4. Verify services are running

```bash
# Check Kafka connectivity
docker-compose logs kafka

# Verify Elasticsearch is healthy
curl http://localhost:9200/_cluster/health

# Access Kibana web interface
# Open http://localhost:5601 in your browser
```

### 5. Run the pipeline

#### Option A: Using the orchestrator (recommended)
```bash
cd src
python pipeline_orchestrator.py
```

#### Option B: Run components individually
```bash
# Terminal 1: Start Spark consumer
cd src
python spark_consumer.py

# Terminal 2: Start Kafka producer (with real NASA logs)
cd src  
python kafka_producer.py --file ../NASA_access_log_Jul95

# Terminal 3: Generate test logs (alternative to NASA logs)
cd src
python log_generator.py --output-mode kafka --rate 10
```

## Features & Analytics

### Data Extraction & Parsing
The pipeline extracts and processes the following information from Apache Common Log Format:
- **Client IP Address** - Source of the request
- **Request Timestamp** - When the request occurred (with timezone handling)
- **HTTP Method** - GET, POST, PUT, DELETE, etc.
- **Requested URL/Path** - The resource being accessed
- **HTTP Response Code** - 200, 404, 500, etc.
- **Response Size** - Size of the response in bytes
- **User Agent** - Browser/client information
- **Referrer** - Previous page (if available)

### Real-time Stream Processing
- **Log Parsing** - Regex-based parsing of raw log lines
- **Data Cleaning** - Handle malformed entries and missing fields
- **Time-based Windowing** - Aggregate metrics over time windows
- **Error Detection** - Filter and count 4xx/5xx HTTP responses
- **Traffic Analysis** - Request counting and rate calculation
- **IP Activity Tracking** - Most active clients identification
- **Anomaly Detection** - Traffic spike and unusual pattern detection
- **Response Time Analysis** - Performance monitoring

### Data Storage & Indexing
- **Time-series Indexing** - Efficient storage with timestamp-based indices
- **Full-text Search** - Search across URLs, user agents, and other text fields
- **Aggregations** - Pre-computed metrics for fast dashboard queries
- **Data Retention** - Configurable retention policies for log data

### Kibana Dashboards & Visualizations
- **Real-time Traffic Histogram** - Requests per minute/hour
- **Top Active IPs Table** - Most frequent visitors
- **HTTP Error Monitoring** - 404/500 error trends over time
- **Response Code Distribution** - Pie charts of status codes
- **Popular Pages Ranking** - Most visited URLs
- **Geographic IP Analysis** - Visitor location mapping (if GeoIP enabled)
- **User Agent Analysis** - Browser and device statistics
- **Traffic Heat Maps** - Activity patterns by time of day/week

## Configuration

The project uses a centralized configuration approach in `config/config.py`:

### Kafka Configuration
```python
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9092',
    'topic_name': 'web-access-logs',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'log-processing-group'
}
```

### Spark Configuration  
```python
SPARK_CONFIG = {
    'app_name': 'WebLogAnalysis',
    'master': 'spark://localhost:7077',
    'executor_memory': '1g',
    'driver_memory': '1g'
}
```

### Elasticsearch Configuration
```python
ELASTICSEARCH_CONFIG = {
    'hosts': ['http://localhost:9200'],
    'index_name': 'web-logs',
    'doc_type': '_doc'
}
```

## Usage Examples

### Processing Real NASA Log Data
```bash
# Use the historical NASA access logs
cd src
python kafka_producer.py --file ../NASA_access_log_Jul95 --rate 100
```

### Generating Test Data
```bash
# Generate realistic test logs at 50 logs/second
cd src  
python log_generator.py --rate 50 --duration 3600 --output-mode kafka
```

## Technical Details

### Data Flow Architecture
1. **Log Source** → Raw log lines (NASA logs or generated logs)
2. **Kafka Producer** → Streams raw lines to `web-access-logs` topic
3. **Spark Streaming** → Consumes, parses, and processes logs in real-time
4. **Elasticsearch** → Stores processed data with proper indexing
5. **Kibana** → Provides visualization and dashboard capabilities

### Log Format Support
The pipeline supports **Apache Common Log Format**:
```
127.0.0.1 - - [25/Dec/1995:10:39:25 +0000] "GET /index.html HTTP/1.0" 200 1024
```

### Spark Processing Pipeline
- **Stream Reading** → Kafka source with automatic offset management
- **Schema Definition** → Structured parsing using regex patterns  
- **Data Validation** → Handle malformed logs and missing fields
- **Windowed Aggregations** → Time-based metrics calculation
- **Output Sinks** → Multiple outputs (Elasticsearch, console, files)

## Documentation & Resources

### Project Documentation
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Elasticsearch Reference](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana User Guide](https://www.elastic.co/guide/en/kibana/current/index.html)

### Key Libraries Used
- **confluent-kafka**: High-performance Kafka Python client
- **pyspark**: Apache Spark Python API
- **elasticsearch**: Official Elasticsearch Python client
- **faker**: Generate realistic test data

## Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/Feature`)
3. Commit your changes (`git commit -m 'Add some Feature'`)
4. Push to the branch (`git push origin feature/Feature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- **Apache Software Foundation** for Kafka and Spark
- **Elastic N.V.** for Elasticsearch and Kibana
- **NASA** for providing real access log data
- **Open Source Community** for the amazing Python libraries