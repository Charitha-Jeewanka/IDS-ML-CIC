"""
PySpark Consumer Runner

CLI for running the Kafka to Parquet streaming consumer.
"""

import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.ColdPath.spark_consumer import KafkaToParquetConsumer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/kafka_consumer.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Run PySpark consumer."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='PySpark Structured Streaming: Kafka → Parquet',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
This consumer reads network flow records from Kafka and writes them to
Parquet files with compression. It uses Spark Structured Streaming for
fault-tolerant, exactly-once processing.

The consumer will run continuously until stopped with Ctrl+C.

Output:
  - Parquet files: data/parquet/
  - Checkpoints: artifacts/checkpoints/

Examples:
  # Start consumer with default config
  python run_consumer.py
  
  # Use custom config
  python run_consumer.py --config /path/to/config.yaml
        """
    )
    
    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom config file'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("Starting PySpark Kafka Consumer")
        logger.info("="*70)
        
        consumer = KafkaToParquetConsumer(config_path=args.config)
        consumer.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("\nConsumer stopped by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Consumer failed:{e}", exc_info=True)
        logger.error("\nTroubleshooting:")
        logger.error("  1. Is Kafka running?")
        logger.error("  2. Does the topic exist? Run: make setup-kafka")
        logger.error("  3. Are there messages in the topic?")
        logger.error("  4. Check logs/kafka_consumer.log for details")
        sys.exit(1)


if __name__ == "__main__":
    main()
