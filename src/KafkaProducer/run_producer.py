"""
Kafka Producer Runner

CLI for streaming processed CSV files to Kafka.
"""

import sys
import logging
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.KafkaProducer.csv_streamer import CSVStreamer

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
    handlers=[
        logging.FileHandler('logs/kafka_producer.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)


def main():
    """Run Kafka producer."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description='Stream processed CSV files to Kafka',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Stream all processed CSV files
  python run_producer.py
  
  # Stream a single file
  python run_producer.py --file Monday_WorkingHours_ISCX.csv
  
  # Use custom chunk size
  python run_producer.py --chunk-size 5000
  
  # Disable progress bars
  python run_producer.py --no-progress
        """
    )
    
    parser.add_argument(
        '--file',
        type=str,
        help='Single CSV file to stream (from data/processed/)'
    )
    parser.add_argument(
        '--chunk-size',
        type=int,
        default=1000,
        help='Number of records per batch (default: 1000)'
    )
    parser.add_argument(
        '--no-progress',
        action='store_true',
        help='Disable progress bars'
    )
    parser.add_argument(
        '--config',
        type=str,
        help='Path to custom config file'
    )
    
    args = parser.parse_args()
    
    try:
        logger.info("Starting Kafka Producer")
        logger.info("="*70)
        
        # Create streamer
        streamer = CSVStreamer(
            config_path=args.config,
            chunk_size=args.chunk_size
        )
        
        # Stream files
        if args.file:
            # Single file mode
            csv_path = Path(args.file)
            if not csv_path.exists():
                csv_path = streamer.processed_dir / args.file
            
            if not csv_path.exists():
                logger.error(f"File not found: {args.file}")
                sys.exit(1)
            
            if not streamer.producer.connect():
                logger.error("Failed to connect to Kafka")
                logger.error("  - Is Kafka running?")
                logger.error("  - Check bootstrap servers in config.yaml")
                sys.exit(1)
            
            success = streamer.stream_file(csv_path, show_progress=not args.no_progress)
            streamer.producer.close()
            
            if success:
                logger.info("Producer completed successfully")
                sys.exit(0)
            else:
                logger.error("Producer failed")
                sys.exit(1)
        else:
            # Batch mode - all files
            stats = streamer.stream_all(show_progress=not args.no_progress)
            
            if stats['files_failed'] == 0:
                logger.info("Producer completed successfully")
                sys.exit(0)
            else:
                logger.error(f"Producer completed with {stats['files_failed']} failures")
                sys.exit(1)
                
    except KeyboardInterrupt:
        logger.info("\nProducer interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Producer failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
