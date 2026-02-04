"""
Kafka Producer with Source IP Partitioning

Sends network flow records to Kafka, partitioning by Source IP for stateful processing.
"""

import sys
import json
import logging
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Callable
from kafka import KafkaProducer as KP
from kafka.errors import KafkaError

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class NetworkFlowProducer:
    """
    Kafka producer for network flow data with Source IP-based partitioning.
    
    Benefits of Source IP partitioning:
    - All flows from same IP go to same partition
    - Enables stateful aggregations (e.g., connections per IP)
    - Better locality for ML feature engineering
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize Kafka producer.
        
        Args:
            config_path: Path to config file (optional)
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        kafka_config = self.config_loader.get_config().get('kafka', {})
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'cicids-network-flows')
        
        producer_config = kafka_config.get('producer', {})
        self.partition_by_field = producer_config.get('partition_by', 'Source_IP')
        
        # Producer settings
        self.producer_settings = {
            'bootstrap_servers': self.bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'key_serializer': lambda k: k.encode('utf-8') if k else None,
            'acks': producer_config.get('acks', 'all'),
            'retries': producer_config.get('retries', 3),
            'batch_size': producer_config.get('batch_size', 16384),
            'linger_ms': producer_config.get('linger_ms', 10),
            'compression_type': producer_config.get('compression_type', 'snappy'),
            'buffer_memory': producer_config.get('buffer_memory', 33554432),
        }
        
        self.producer = None
        self.stats = {
            'messages_sent': 0,
            'messages_failed': 0,
            'bytes_sent': 0
        }
        
        logger.info("NetworkFlowProducer initialized")
        logger.info(f"Bootstrap servers: {self.bootstrap_servers}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Partition by: {self.partition_by_field}")
    
    def connect(self):
        """Initialize Kafka connection."""
        try:
            self.producer = KP(**self.producer_settings)
            logger.info("Connected to Kafka successfully")
            return True
        except KafkaError as e:
            logger.error(f"Failed to connect to Kafka: {e}")
            return False
    
    def _get_partition_key(self, record: Dict[str, Any]) -> str:
        """
        Get partition key from record (Source IP).
        
        Args:
            record: Data record
            
        Returns:
            Partition key (Source IP)
        """
        # Get Source IP, handling different possible column names
        source_ip = record.get(self.partition_by_field) or \
                   record.get('Source IP') or \
                   record.get('source_ip') or \
                   'unknown'
        
        return str(source_ip)
    
    def send_record(self, record: Dict[str, Any], callback: Optional[Callable] = None):
        """
        Send a single record to Kafka.
        
        Args:
            record: Data record as dictionary
            callback: Optional callback for async confirmation
        """
        if not self.producer:
            raise RuntimeError("Producer not connected. Call connect() first.")
        
        try:
            # Get partition key (Source IP)
            partition_key = self._get_partition_key(record)
            
            # Send to Kafka
            future = self.producer.send(
                self.topic,
                key=partition_key,
                value=record
            )
            
            # Add callback if provided, otherwise use default
            if callback:
                future.add_callback(callback)
            else:
                future.add_callback(self._on_send_success)
            
            future.add_errback(self._on_send_error)
            
        except Exception as e:
            logger.error(f"Error sending record: {e}")
            self.stats['messages_failed'] += 1
    
    def send_batch(self, records: list, flush: bool = True):
        """
        Send a batch of records to Kafka.
        
        Args:
            records: List of data records
            flush: Whether to flush after batch
        """
        for record in records:
            self.send_record(record)
        
        if flush:
            self.flush()
    
    def _on_send_success(self, record_metadata):
        """Callback for successful send."""
        self.stats['messages_sent'] += 1
        self.stats['bytes_sent'] += record_metadata.serialized_value_size
        
        if self.stats['messages_sent'] % 1000 == 0:
            logger.info(f"Sent {self.stats['messages_sent']:,} messages")
    
    def _on_send_error(self, exc):
        """Callback for send error."""
        logger.error(f"Failed to send message: {exc}")
        self.stats['messages_failed'] += 1
    
    def flush(self):
        """Flush any pending messages."""
        if self.producer:
            self.producer.flush()
            logger.debug("Flushed pending messages")
    
    def close(self):
        """Close the producer connection."""
        if self.producer:
            self.flush()
            self.producer.close()
            logger.info("Producer closed")
            logger.info(f"Final stats: {self.stats}")
    
    def get_stats(self) -> Dict[str, int]:
        """Get producer statistics."""
        return self.stats.copy()


def main():
    """Test the producer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Test Kafka producer')
    parser.add_argument('--config', type=str, help='Path to config file')
    args = parser.parse_args()
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    )
    
    try:
        producer = NetworkFlowProducer(config_path=args.config)
        
        if producer.connect():
            # Send a test record
            test_record = {
                'Source_IP': '192.168.1.100',
                'Destination_IP': '10.0.0.1',
                'Flow_Duration': 1000,
                'Label': 'BENIGN'
            }
            
            logger.info("Sending test record...")
            producer.send_record(test_record)
            producer.flush()
            
            logger.info(f"Test successful! Stats: {producer.get_stats()}")
            producer.close()
        else:
            logger.error("Failed to connect to Kafka")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Test failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
