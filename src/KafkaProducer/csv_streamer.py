"""
CSV to Kafka Streamer

Streams processed CSV files to Kafka with Source IP partitioning.
"""

import sys
import logging
from pathlib import Path
from typing import Dict, Any, Optional
import pandas as pd
from tqdm import tqdm

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader
from src.KafkaProducer.producer import NetworkFlowProducer

logger = logging.getLogger(__name__)


class CSVStreamer:
    """
    Stream CSV files to Kafka with Source IP-based partitioning.
    
    Handles memory-efficient chunked reading and progress tracking.
    """
    
    def __init__(self, config_path: str = None, chunk_size: int = 1000):
        """
        Initialize CSV streamer.
        
        Args:
            config_path: Path to config file (optional)
            chunk_size: Number of records per batch
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        self.data_paths = self.config_loader.get_data_paths()
        self.processed_dir = Path(self.config_loader.resolve_path(self.data_paths['processed']))
        
        self.chunk_size = chunk_size
        self.producer = NetworkFlowProducer(config_path)
        
        self.stats = {
            'files_streamed': 0,
            'files_failed': 0,
            'total_records': 0
        }
        
        logger.info("CSVStreamer initialized")
        logger.info(f"Input directory: {self.processed_dir}")
        logger.info(f"Chunk size: {chunk_size:,}")
    
    def stream_file(self, csv_path: Path, show_progress: bool = True) -> bool:
        """
        Stream a single CSV file to Kafka.
        
        Args:
            csv_path: Path to CSV file
            show_progress: Show progress bar
            
        Returns:
            True if successful, False otherwise
        """
        filename = csv_path.name
        logger.info(f"Streaming {filename} to Kafka...")
        
        try:
            # Get total rows for progress bar
            total_rows = sum(1 for _ in open(csv_path)) - 1  # Exclude header
            logger.info(f"  Total rows: {total_rows:,}")
            
            # Stream in chunks
            records_sent = 0
            
            # Use tqdm for progress bar
            with tqdm(total=total_rows, desc=f"Streaming {filename}", 
                     disable=not show_progress, unit="records") as pbar:
                
                # Read CSV in chunks
                for chunk in pd.read_csv(csv_path, chunksize=self.chunk_size):
                    # Convert chunk to list of dictionaries
                    records = chunk.to_dict('records')
                    
                    # Send batch to Kafka
                    self.producer.send_batch(records, flush=False)
                    
                    records_sent += len(records)
                    pbar.update(len(records))
            
            # Final flush
            self.producer.flush()
            
            logger.info(f"  Streamed {records_sent:,} records successfully")
            
            self.stats['files_streamed'] += 1
            self.stats['total_records'] += records_sent
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to stream {filename}: {e}", exc_info=True)
            self.stats['files_failed'] += 1
            return False
    
    def stream_all(self, show_progress: bool = True) -> Dict[str, Any]:
        """
        Stream all CSV files in processed directory to Kafka.
        
        Args:
            show_progress: Show progress bars
            
        Returns:
            Statistics dictionary
        """
        logger.info("\n" + "="*70)
        logger.info("CSV TO KAFKA STREAMING")
        logger.info("="*70)
        
        # Connect to Kafka
        if not self.producer.connect():
            logger.error("Failed to connect to Kafka")
            return self.stats
        
        # Get all CSV files
        csv_files = sorted(self.processed_dir.glob("*.csv"))
        
        if not csv_files:
            logger.error("No CSV files found in processed directory")
            return self.stats
        
        logger.info(f"Found {len(csv_files)} CSV files to stream\n")
        
        # Stream each file
        for i, csv_file in enumerate(csv_files, 1):
            logger.info(f"[{i}/{len(csv_files)}] {csv_file.name}")
            self.stream_file(csv_file, show_progress=show_progress)
            print()  # Blank line between files
        
        # Print summary
        self.print_summary()
        
        # Close producer
        self.producer.close()
        
        return self.stats
    
    def print_summary(self):
        """Print streaming summary."""
        logger.info("\n" + "="*70)
        logger.info("STREAMING SUMMARY")
        logger.info("="*70)
        
        print(f"\nFiles Streamed: {self.stats['files_streamed']}")
        print(f"Files Failed: {self.stats['files_failed']}")
        print(f"Total Records Sent: {self.stats['total_records']:,}")
        
        producer_stats = self.producer.get_stats()
        print(f"\nKafka Stats:")
        print(f"  Messages Sent: {producer_stats['messages_sent']:,}")
        print(f"  Messages Failed: {producer_stats['messages_failed']:,}")
        print(f"  Bytes Sent: {producer_stats['bytes_sent']:,} ({producer_stats['bytes_sent']/(1024*1024):.2f} MB)")
        
        logger.info("="*70 + "\n")


def main():
    """Main entry point for CSV to Kafka streaming."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Stream CSV files to Kafka')
    parser.add_argument('--file', type=str, help='Single CSV file to stream')
    parser.add_argument('--chunk-size', type=int, default=1000, help='Records per batch')
    parser.add_argument('--no-progress', action='store_true', help='Disable progress bars')
    parser.add_argument('--config', type=str, help='Path to config file')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s'
    )
    
    try:
        streamer = CSVStreamer(
            config_path=args.config,
            chunk_size=args.chunk_size
        )
        
        # Connect to Kafka
        if not streamer.producer.connect():
            logger.error("Failed to connect to Kafka. Is Kafka running?")
            sys.exit(1)
        
        if args.file:
            # Stream single file
            csv_path = Path(args.file)
            if not csv_path.exists():
                # Try in processed directory
                csv_path = streamer.processed_dir / args.file
            
            if not csv_path.exists():
                logger.error(f"File not found: {args.file}")
                sys.exit(1)
            
            success = streamer.stream_file(csv_path, show_progress=not args.no_progress)
            streamer.producer.close()
            sys.exit(0 if success else 1)
        else:
            # Stream all files
            stats = streamer.stream_all(show_progress=not args.no_progress)
            sys.exit(0 if stats['files_failed'] == 0 else 1)
            
    except KeyboardInterrupt:
        logger.info("\nStreaming interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Streaming failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
