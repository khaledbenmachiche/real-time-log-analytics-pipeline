# Real-time Log Analysis with Apache Kafka, Spark and Elasticsearch

## Project Objective

This project implements a complete Big Data pipeline for real-time web access log analysis. It allows to:

1. **Ingest** web access logs via Apache Kafka
2. **Process** data in real-time with Apache Spark Streaming
3. **Index** and **visualize** results in Elasticsearch/Kibana

## Technologies Used

- **Apache Kafka** - Real-time data stream ingestion
- **Apache Spark** - Data processing with Spark Structured Streaming
- **Elasticsearch** - Storage and indexing of results
- **Kibana** - Graphical data visualization
- **Python** - Log generation and processing scripts
- **Docker** - Infrastructure orchestration

## System Architecture

```
Access Logs → Kafka Producer → Apache Kafka → Spark Streaming → Elasticsearch → Kibana Dashboard
```

## Project Structure

```
├── docker-compose.yaml          # Docker configuration for infrastructure
├── requirements.txt             # Python dependencies
├── src/
├── config/
├── data/
├── kibana/
│   └── dashboards/             # Dashboard configuration
└── docs/
```

## Installation and Setup

### Prerequisites

- Docker and Docker Compose
- Python 3.8+
- Java 8+ (for Spark)

### 1. Clone the repository

```bash
git clone <repository-url>
cd optional
```

### 2. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3. Start infrastructure with Docker

```bash
docker-compose up -d
```

This command starts:
- Zookeeper (port 2181)
- Kafka (port 9092)
- Elasticsearch (port 9200)
- Kibana (port 5601)

### 4. Verify services are running

```bash
# Check Kafka
docker-compose ps

# Check Elasticsearch
curl http://localhost:9200

# Access Kibana
# Open http://localhost:5601 in browser
```

## Features

### Data Extraction
The pipeline extracts the following information from logs:
- Client **IP Address**
- Request **Timestamp**
- Requested **URL**
- **HTTP Response Code**
- **User Agent**
- **Response Size**

### Real-time Processing
- **Error filtering** (404, 500 codes)
- **Request counting per minute**
- **Most active IP identification**
- **Traffic spike detection**
- **Access pattern analysis**

### Kibana Dashboards
- **Traffic histogram** per minute
- **Most active IPs table**
- **404/500 errors curve** over time
- **Response code distribution**
- **Top visited pages**

## Troubleshooting

### Common Issues

1. **Kafka won't start**
   ```bash
   # Check logs
   docker-compose logs kafka
   
   # Clean data
   docker-compose down -v
   docker-compose up -d
   ```

2. **Spark can't connect to Kafka**
   ```bash
   # Check network connectivity
   docker network ls
   docker network inspect optional_default
   ```

3. **Elasticsearch refuses connections**
   ```bash
   # Increase virtual memory
   sudo sysctl -w vm.max_map_count=262144
   ```

## Documentation

- [Apache Kafka Guide](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming](https://spark.apache.org/docs/latest/structured-streaming-programming-guide.html)
- [Elasticsearch Guide](https://www.elastic.co/guide/en/elasticsearch/reference/current/index.html)
- [Kibana User Guide](https://www.elastic.co/guide/en/kibana/current/index.html)

## Contributing

1. Fork the project
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## License

This project is licensed under the MIT License. See the `LICENSE` file for details.

## Acknowledgments

- Apache Software Foundation for Big Data tools
- Elastic for Elasticsearch and Kibana
- The open source community for contributions