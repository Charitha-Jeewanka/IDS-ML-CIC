"""
End-to-End Pipeline Validation

Tests the complete pipeline: Producer → Kafka → Consumer → Parquet
"""

import sys
import time
import logging
from pathlib import Path
import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader
from src.KafkaProducer.producer import NetworkFlowProducer

logger = logging.getLogger(__name__)


class PipelineValidator:
    """Validate end-to-end Kafka pipeline."""
    
    def __init__(self, config_path: str = None):
        """Initialize validator."""
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        kafka_config = self.config_loader.get_config().get('kafka', {})
        self.bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        self.topic = kafka_config.get('topic', 'cicids-network-flows')
        
        self.results = {
            'kafka_connection': False,
            'topic_exists': False,
            'producer_send': False,
            'consumer_receive': False,
            'parquet_output': False
        }
    
    def test_kafka_connection(self) -> bool:
        """Test Kafka broker connection."""
        logger.info("Testing Kafka connection...")
        
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers,
                request_timeout_ms=5000
            )
            consumer.close()
            
            logger.info("  SUCCESS: Connected to Kafka")
            self.results['kafka_connection'] = True
            return True
            
        except KafkaError as e:
            logger.error(f"  FAILED: Cannot connect to Kafka: {e}")
            logger.error(f"  Check if Kafka is running on {self.bootstrap_servers}")
            return False
    
    def test_topic_exists(self) -> bool:
        """Test if topic exists."""
        logger.info(f"Checking if topic '{self.topic}' exists...")
        
        try:
            consumer = KafkaConsumer(
                bootstrap_servers=self.bootstrap_servers
            )
            topics = consumer.topics()
            consumer.close()
            
            if self.topic in topics:
                logger.info(f"  SUCCESS: Topic '{self.topic}' exists")
                self.results['topic_exists'] = True
                return True
            else:
                logger.error(f"  FAILED: Topic '{self.topic}' not found")
                logger.error(f"  Available topics: {topics}")
                logger.error(f"  Run: make setup-kafka")
                return False
                
        except Exception as e:
            logger.error(f"  FAILED: Error checking topic: {e}")
            return False
    
    def test_producer(self) -> bool:
        """Test sending a message via producer."""
        logger.info("Testing producer...")
        
        try:
            producer = NetworkFlowProducer()
            
            if not producer.connect():
                logger.error("  FAILED: Producer cannot connect")
                return False
            
            # Send test record
            test_record = {
                'Source_IP': '192.168.1.100',
                'Destination_IP': '10.0.0.1',
                'Flow_Duration': 1000.0,
                'Label': 'TEST-VALIDATION'
            }
            
            producer.send_record(test_record)
            producer.flush()
            
            stats = producer.get_stats()
            if stats['messages_sent'] > 0:
                logger.info(f"  SUCCESS: Sent {stats['messages_sent']} test message(s)")
                self.results['producer_send'] = True
                producer.close()
                return True
            else:
                logger.error("  FAILED: No messages sent")
                producer.close()
                return False
                
        except Exception as e:
            logger.error(f"  FAILED: Producer error: {e}")
            return False
    
    def test_consumer(self) -> bool:
        """Test receiving a message via consumer."""
        logger.info("Testing consumer...")
        
        try:
            consumer = KafkaConsumer(
                self.topic,
                bootstrap_servers=self.bootstrap_servers,
                auto_offset_reset='latest',
                consumer_timeout_ms=5000,
                value_deserializer=lambda m: json.loads(m.decode('utf-8'))
            )
            
            logger.info("  Waiting for test message (5 second timeout)...")
            
            message_received = False
            for message in consumer:
                logger.info(f"  SUCCESS: Received message from partition {message.partition}")
                logger.info(f"    Offset: {message.offset}")
                logger.info(f"    Key: {message.key}")
                logger.info(f"    Preview: {str(message.value)[:100]}...")
                message_received = True
                break
            
            consumer.close()
            
            if message_received:
                self.results['consumer_receive'] = True
                return True
            else:
                logger.warning("  WARNING: No messages received (timeout)")
                logger.warning("  This may be normal if topic is empty")
                return False
                
        except Exception as e:
            logger.error(f"  FAILED: Consumer error: {e}")
            return False
    
    def test_parquet_output(self) -> bool:
        """Test if Parquet files exist."""
        logger.info("Checking Parquet output...")
        
        try:
            parquet_dir = Path(self.config_loader.resolve_path('data/parquet'))
            parquet_files = list(parquet_dir.glob("*.parquet"))
            
            if parquet_files:
                logger.info(f"  SUCCESS: Found {len(parquet_files)} Parquet file(s)")
                self.results['parquet_output'] = True
                return True
            else:
                logger.warning("  WARNING: No Parquet files found")
                logger.warning("  Run: make csv-to-parquet")
                return False
                
        except Exception as e:
            logger.error(f"  FAILED: Error checking Parquet files: {e}")
            return False
    
    def run_all_tests(self) -> bool:
        """Run all validation tests."""
        logger.info("\n" + "="*70)
        logger.info("KAFKA PIPELINE VALIDATION")
        logger.info("="*70 + "\n")
        
        # Run tests in order
        tests = [
            ("Kafka Connection", self.test_kafka_connection),
            ("Topic Exists", self.test_topic_exists),
            ("Producer Send", self.test_producer),
            ("Consumer Receive", self.test_consumer),
            ("Parquet Output", self.test_parquet_output),
        ]
        
        for test_name, test_func in tests:
            result = test_func()
            print()  # Blank line between tests
            
            if not result and test_name in ["Kafka Connection", "Topic Exists"]:
                logger.error(f"Critical test failed: {test_name}")
                logger.error("Skipping remaining tests")
                break
        
        # Print summary
        self.print_summary()
        
        # Pass if critical tests passed
        return self.results['kafka_connection'] and self.results['topic_exists']
    
    def print_summary(self):
        """Print validation summary."""
        logger.info("="*70)
        logger.info("VALIDATION SUMMARY")
        logger.info("="*70)
        
        for test_name, result in self.results.items():
            status = "PASS" if result else "FAIL"
            logger.info(f"  {test_name:.<40} {status}")
        
        logger.info("="*70 + "\n")
        
        if all([self.results['kafka_connection'], self.results['topic_exists']]):
            logger.info("Pipeline is ready for streaming!")
        else:
            logger.error("Pipeline validation failed. Check errors above.")


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Validate Kafka pipeline')
    parser.add_argument('--config', type=str, help='Path to config file')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    )
    
    try:
        validator = PipelineValidator(config_path=args.config)
        success = validator.run_all_tests()
        sys.exit(0 if success else 1)
        
    except Exception as e:
        logger.error(f"Validation failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
