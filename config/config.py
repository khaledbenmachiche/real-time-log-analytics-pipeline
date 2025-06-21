# Configuration file for Big Data Log Pipeline

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': 'localhost:9092',
    'topic_name': 'web-access-logs',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True,
    'group_id': 'log-processing-group'
}

# Spark Configuration
SPARK_CONFIG = {
    'app_name': 'WebLogAnalysis',
    'master': 'spark://localhost:7077',
    'executor_memory': '1g',
    'driver_memory': '1g'
}

# Elasticsearch Configuration
ELASTICSEARCH_CONFIG = {
    'hosts': ['http://localhost:9200'],
    'index_name': 'web-logs',
    'doc_type': '_doc'
}

# Log Processing Configuration
LOG_CONFIG = {
    'batch_duration': 10,  # seconds
    'checkpoint_location': '/tmp/spark-checkpoint',
    'output_mode': 'append'
}
