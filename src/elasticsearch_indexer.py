"""
Elasticsearch Integration for Web Log Analysis
This script handles the connection and data indexing to Elasticsearch
"""

import json
import logging
from datetime import datetime
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
import sys
import os

# Add config directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'config'))
from config import ELASTICSEARCH_CONFIG

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ElasticsearchIndexer:
    """
    Elasticsearch indexer for web log analysis results
    """
    
    def __init__(self, es_config=None):
        """
        Initialize Elasticsearch client
        
        Args:
            es_config (dict): Elasticsearch configuration
        """
        self.config = es_config or ELASTICSEARCH_CONFIG
        self.es = None
        self.connect()
    
    def connect(self):
        """
        Connect to Elasticsearch cluster
        """
        try:
            self.es = Elasticsearch(
                hosts=self.config['hosts'],
                verify_certs=False,
                http_auth=None,  # Add authentication if needed
                timeout=30,
                max_retries=3,
                retry_on_timeout=True
            )
            
            # Test connection
            if self.es.ping():
                logger.info("Successfully connected to Elasticsearch")
                return True
            else:
                logger.error("Failed to connect to Elasticsearch")
                return False
                
        except Exception as e:
            logger.error(f"Error connecting to Elasticsearch: {e}")
            return False
    
    def create_index_template(self, index_name, mapping):
        """
        Create index template with mapping
        
        Args:
            index_name (str): Name of the index
            mapping (dict): Elasticsearch mapping
        """
        try:
            if not self.es.indices.exists(index=index_name):
                self.es.indices.create(
                    index=index_name,
                    body={"mappings": mapping}
                )
                logger.info(f"Created index: {index_name}")
            else:
                logger.info(f"Index {index_name} already exists")
                
        except Exception as e:
            logger.error(f"Error creating index {index_name}: {e}")
    
    def get_error_logs_mapping(self):
        """
        Get mapping for error logs index
        """
        return {
            "properties": {
                "ip": {"type": "ip"},
                "timestamp": {"type": "date"},
                "url": {"type": "keyword"},
                "status_code": {"type": "integer"},
                "method": {"type": "keyword"},
                "log_type": {"type": "keyword"},
                "processed_at": {"type": "date"}
            }
        }
    
    def get_traffic_mapping(self):
        """
        Get mapping for traffic analysis index
        """
        return {
            "properties": {
                "minute": {"type": "date"},
                "request_count": {"type": "long"},
                "log_type": {"type": "keyword"},
                "processed_at": {"type": "date"}
            }
        }
    
    def get_ip_analysis_mapping(self):
        """
        Get mapping for IP analysis index
        """
        return {
            "properties": {
                "window_start": {"type": "date"},
                "ip": {"type": "ip"},
                "request_count": {"type": "long"},
                "log_type": {"type": "keyword"},
                "processed_at": {"type": "date"}
            }
        }
    
    def get_url_analysis_mapping(self):
        """
        Get mapping for URL analysis index
        """
        return {
            "properties": {
                "window_start": {"type": "date"},
                "url": {"type": "keyword"},
                "request_count": {"type": "long"},
                "log_type": {"type": "keyword"},
                "processed_at": {"type": "date"}
            }
        }
    
    def setup_indices(self):
        """
        Setup all required indices with proper mappings
        """
        indices_config = {
            "web-logs-errors": self.get_error_logs_mapping(),
            "web-logs-traffic": self.get_traffic_mapping(),
            "web-logs-ip-analysis": self.get_ip_analysis_mapping(),
            "web-logs-url-analysis": self.get_url_analysis_mapping()
        }
        
        for index_name, mapping in indices_config.items():
            self.create_index_template(index_name, mapping)
    
    def index_document(self, index_name, doc_id, document):
        """
        Index a single document
        
        Args:
            index_name (str): Name of the index
            doc_id (str): Document ID
            document (dict): Document to index
        """
        try:
            document['processed_at'] = datetime.now().isoformat()
            
            response = self.es.index(
                index=index_name,
                id=doc_id,
                body=document
            )
            
            return response['result'] == 'created' or response['result'] == 'updated'
            
        except Exception as e:
            logger.error(f"Error indexing document to {index_name}: {e}")
            return False
    
    def bulk_index_documents(self, index_name, documents):
        """
        Bulk index multiple documents
        
        Args:
            index_name (str): Name of the index
            documents (list): List of documents to index
        """
        try:
            # Prepare documents for bulk indexing
            actions = []
            for doc in documents:
                doc['processed_at'] = datetime.now().isoformat()
                action = {
                    "_index": index_name,
                    "_source": doc
                }
                actions.append(action)
            
            # Perform bulk indexing
            success, failed = bulk(self.es, actions, chunk_size=1000)
            
            logger.info(f"Bulk indexed {success} documents to {index_name}")
            if failed:
                logger.warning(f"Failed to index {len(failed)} documents")
            
            return success, failed
            
        except Exception as e:
            logger.error(f"Error in bulk indexing to {index_name}: {e}")
            return 0, []
    
    def search_documents(self, index_name, query, size=100):
        """
        Search documents in an index
        
        Args:
            index_name (str): Name of the index
            query (dict): Elasticsearch query
            size (int): Number of results to return
        """
        try:
            response = self.es.search(
                index=index_name,
                body=query,
                size=size
            )
            
            return response['hits']['hits']
            
        except Exception as e:
            logger.error(f"Error searching in {index_name}: {e}")
            return []
    
    def get_index_stats(self, index_name):
        """
        Get statistics for an index
        
        Args:
            index_name (str): Name of the index
        """
        try:
            stats = self.es.indices.stats(index=index_name)
            return stats['indices'][index_name]
            
        except Exception as e:
            logger.error(f"Error getting stats for {index_name}: {e}")
            return None
    
    def delete_index(self, index_name):
        """
        Delete an index
        
        Args:
            index_name (str): Name of the index to delete
        """
        try:
            if self.es.indices.exists(index=index_name):
                self.es.indices.delete(index=index_name)
                logger.info(f"Deleted index: {index_name}")
                return True
            else:
                logger.info(f"Index {index_name} does not exist")
                return False
                
        except Exception as e:
            logger.error(f"Error deleting index {index_name}: {e}")
            return False


def process_spark_output_to_elasticsearch(spark_output_dir, es_indexer):
    """
    Process Spark output files and send to Elasticsearch
    
    Args:
        spark_output_dir (str): Directory containing Spark output files
        es_indexer (ElasticsearchIndexer): Elasticsearch indexer instance
    """
    import glob
    
    try:
        # Process different types of output
        output_types = {
            "error_logs": "web-logs-errors",
            "traffic_by_minute": "web-logs-traffic",
            "top_ips": "web-logs-ip-analysis",
            "url_analysis": "web-logs-url-analysis"
        }
        
        for output_type, index_name in output_types.items():
            output_path = os.path.join(spark_output_dir, output_type)
            
            if os.path.exists(output_path):
                # Find JSON files in the output directory
                json_files = glob.glob(os.path.join(output_path, "*.json"))
                
                for json_file in json_files:
                    documents = []
                    
                    with open(json_file, 'r') as f:
                        for line in f:
                            try:
                                doc = json.loads(line.strip())
                                documents.append(doc)
                            except json.JSONDecodeError:
                                continue
                    
                    if documents:
                        success, failed = es_indexer.bulk_index_documents(index_name, documents)
                        logger.info(f"Processed {len(documents)} documents from {json_file}")
                
            else:
                logger.warning(f"Output directory not found: {output_path}")
                
    except Exception as e:
        logger.error(f"Error processing Spark output: {e}")


def main():
    """
    Main function for testing Elasticsearch integration
    """
    # Initialize Elasticsearch indexer
    es_indexer = ElasticsearchIndexer()
    
    # Setup indices
    es_indexer.setup_indices()
    
    # Test with sample data
    sample_error_log = {
        "ip": "192.168.1.1",
        "timestamp": "1995-07-01T00:00:12",
        "url": "/missing-page.html",
        "status_code": 404,
        "method": "GET",
        "log_type": "error"
    }
    
    # Index sample document
    success = es_indexer.index_document(
        "web-logs-errors",
        "test-1",
        sample_error_log
    )
    
    if success:
        logger.info("Successfully indexed test document")
    else:
        logger.error("Failed to index test document")
    
    # Get index stats
    stats = es_indexer.get_index_stats("web-logs-errors")
    if stats:
        logger.info(f"Index stats: {stats['total']['docs']['count']} documents")


if __name__ == "__main__":
    main()
