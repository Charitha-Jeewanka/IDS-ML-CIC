"""
Kafka Topic Setup Script

Creates the CICIDS network flows topic with proper configuration.
"""

import sys
import logging
from pathlib import Path
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError, KafkaError

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


def setup_kafka_topic(config_path: str = None) -> bool:
    """
    Create Kafka topic for network flow data.
    
    Args:
        config_path: Path to config file (optional)
        
    Returns:
        True if successful, False otherwise
    """
    try:
        # Load configuration
        config_loader = ConfigLoader(config_path)
        config_loader.load_config()
        
        kafka_config = config_loader.get_config().get('kafka', {})
        bootstrap_servers = kafka_config.get('bootstrap_servers', ['localhost:9092'])
        topic_name = kafka_config.get('topic', 'cicids-network-flows')
        num_partitions = kafka_config.get('partitions', 8)
        replication_factor = kafka_config.get('replication_factor', 1)
        
        logger.info("="*70)
        logger.info("KAFKA TOPIC SETUP")
        logger.info("="*70)
        logger.info(f"Bootstrap servers: {bootstrap_servers}")
        logger.info(f"Topic name: {topic_name}")
        logger.info(f"Partitions: {num_partitions}")
        logger.info(f"Replication factor: {replication_factor}")
        
        # Create admin client
        logger.info("\nConnecting to Kafka...")
        admin_client = KafkaAdminClient(
            bootstrap_servers=bootstrap_servers,
            client_id='cicids-topic-setup'
        )
        
        # Create topic
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
            topic_configs={
                'compression.type': 'snappy',
                'retention.ms': '604800000',  # 7 days
                'segment.bytes': '1073741824',  # 1 GB
            }
        )
        
        logger.info(f"\nCreating topic '{topic_name}'...")
        admin_client.create_topics([topic], validate_only=False)
        
        logger.info("Topic created successfully!")
        
        # Verify topic
        logger.info("\nVerifying topic...")
        topics = admin_client.list_topics()
        
        if topic_name in topics:
            logger.info(f"Verified: Topic '{topic_name}' exists")
            
            # Get topic details
            topic_metadata = admin_client.describe_topics([topic_name])
            for topic_info in topic_metadata:
                logger.info(f"\nTopic Details:")
                logger.info(f"  Name: {topic_info['topic']}")
                logger.info(f"  Partitions: {len(topic_info['partitions'])}")
                for partition in topic_info['partitions']:
                    logger.info(f"    Partition {partition['partition']}: Leader={partition['leader']}")
        else:
            logger.error(f"Topic '{topic_name}' not found after creation")
            return False
        
        admin_client.close()
        
        logger.info("\n" + "="*70)
        logger.info("SETUP COMPLETE")
        logger.info("="*70)
        
        return True
        
    except TopicAlreadyExistsError:
        logger.warning(f"Topic '{topic_name}' already exists")
        logger.info("Skipping creation. Topic is ready to use.")
        return True
        
    except KafkaError as e:
        logger.error(f"Kafka error during setup: {e}")
        logger.error("  - Is Kafka running?")
        logger.error(f"  - Can you reach {bootstrap_servers}?")
        return False
        
    except Exception as e:
        logger.error(f"Failed to setup Kafka topic: {e}", exc_info=True)
        return False


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Setup Kafka topic for CICIDS data')
    parser.add_argument('--config', type=str, help='Path to config file')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    )
    
    success = setup_kafka_topic(config_path=args.config)
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
