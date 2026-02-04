"""
PySpark Structured Streaming Consumer

Consumes network flow data from Kafka and writes to Parquet files.
"""

import sys
import logging
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from utils.config_loader import ConfigLoader

logger = logging.getLogger(__name__)


class KafkaToParquetConsumer:
    """
    PySpark Structured Streaming consumer for Kafka to Parquet.
    
    Reads JSON messages from Kafka topic and writes to Parquet with checkpointing.
    """
    
    def __init__(self, config_path: str = None):
        """
        Initialize consumer.
        
        Args:
            config_path: Path to config file (optional)
        """
        # Load configuration
        self.config_loader = ConfigLoader(config_path)
        self.config_loader.load_config()
        
        kafka_config = self.config_loader.get_config().get('kafka', {})
        parquet_config = self.config_loader.get_config().get('parquet', {})
        
        # Kafka settings
        self.bootstrap_servers = ','.join(kafka_config.get('bootstrap_servers', ['localhost:9092']))
        self.topic = kafka_config.get('topic', 'cicids-network-flows')
        consumer_config = kafka_config.get('consumer', {})
        self.group_id = consumer_config.get('group_id', 'cicids-parquet-writer')
        
        # Parquet settings
        self.output_path = self.config_loader.resolve_path(parquet_config.get('output_path', 'data/parquet'))
        self.checkpoint_path = self.config_loader.resolve_path(parquet_config.get('checkpoint_location', 'artifacts/checkpoints'))
        self.compression = parquet_config.get('compression', 'snappy')
        
        # Spark settings
        spark_config = parquet_config.get('spark', {})
        self.app_name = spark_config.get('app_name', 'CICIDS-Kafka-Consumer')
        self.trigger_interval = spark_config.get('trigger_interval', '10 seconds')
        
        # Create paths
        Path(self.output_path).mkdir(parents=True, exist_ok=True)
        Path(self.checkpoint_path).mkdir(parents=True, exist_ok=True)
        
        self.spark = None
        
        logger.info("KafkaToParquetConsumer initialized")
        logger.info(f"Kafka: {self.bootstrap_servers}")
        logger.info(f"Topic: {self.topic}")
        logger.info(f"Output: {self.output_path}")
        logger.info(f"Checkpoint: {self.checkpoint_path}")
    
    def create_spark_session(self) -> SparkSession:
        """Create and configure Spark session."""
        logger.info("Creating Spark session...")
        
        self.spark = SparkSession.builder \
            .appName(self.app_name) \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .config("spark.sql.streaming.checkpointLocation", self.checkpoint_path) \
            .getOrCreate()
        
        # Set log level
        self.spark.sparkContext.setLogLevel("WARN")
        
        logger.info(f"Spark session created: {self.spark.version}")
        return self.spark
    
    def get_schema(self) -> StructType:
        """
        Define schema for network flow records.
        
        Note: This is a simplified schema. Adjust based on your actual data.
        """
        # This schema should match your cleaned CSV columns
        # For demo, using key fields. You'll need to expand this.
        return StructType([
            StructField("Source_IP", StringType(), True),
            StructField("Source_Port", IntegerType(), True),
            StructField("Destination_IP", StringType(), True),
            StructField("Destination_Port", IntegerType(), True),
            StructField("Protocol", IntegerType(), True),
            StructField("Flow_Duration", FloatType(), True),
            StructField("Total_Fwd_Packets", IntegerType(), True),
            StructField("Total_Backward_Packets", IntegerType(), True),
            StructField("Flow_Bytes_per_s", FloatType(), True),
            StructField("Flow_Packets_per_s", FloatType(), True),
            StructField("Label", StringType(), True),
            # Add more fields as needed from your data
        ])
    
    def start_streaming(self):
        """Start Kafka to Parquet streaming."""
        if not self.spark:
            self.create_spark_session()
        
        logger.info("="*70)
        logger.info("STARTING KAFKA TO PARQUET STREAMING")
        logger.info("="*70)
        
        try:
            # Read from Kafka
            logger.info(f"Subscribing to topic: {self.topic}")
            df = self.spark.readStream \
                .format("kafka") \
                .option("kafka.bootstrap.servers", self.bootstrap_servers) \
                .option("subscribe", self.topic) \
                .option("startingOffsets", "earliest") \
                .option("kafka.group.id", self.group_id) \
                .load()
            
            logger.info("Connected to Kafka stream")
            
            # Parse JSON value
            schema = self.get_schema()
            parsed_df = df.select(
                from_json(col("value").cast("string"), schema).alias("data")
            ).select("data.*")
            
            logger.info("Configured JSON parsing")
            
            # Write to Parquet
            logger.info(f"Writing to Parquet: {self.output_path}")
            query = parsed_df.writeStream \
                .format("parquet") \
                .option("path", self.output_path) \
                .option("checkpointLocation", self.checkpoint_path) \
                .option("compression", self.compression) \
                .trigger(processingTime=self.trigger_interval) \
                .start()
            
            logger.info("Streaming started successfully")
            logger.info(f"Trigger interval: {self.trigger_interval}")
            logger.info("\nPress Ctrl+C to stop...")
            
            # Wait for termination
            query.awaitTermination()
            
        except KeyboardInterrupt:
            logger.info("\nStopping stream...")
            query.stop()
            logger.info("Stream stopped")
        except Exception as e:
            logger.error(f"Streaming error: {e}", exc_info=True)
            raise
    
    def stop(self):
        """Stop Spark session."""
        if self.spark:
            self.spark.stop()
            logger.info("Spark session stopped")


def main():
    """Main entry point for PySpark consumer."""
    import argparse
    
    parser = argparse.ArgumentParser(description='Kafka to Parquet streaming consumer')
    parser.add_argument('--config', type=str, help='Path to config file')
    
    args = parser.parse_args()
    
    # Setup logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s | %(name)s | %(levelname)s | %(message)s',
        handlers=[
            logging.FileHandler('logs/kafka_consumer.log'),
            logging.StreamHandler()
        ]
    )
    
    try:
        consumer = KafkaToParquetConsumer(config_path=args.config)
        consumer.start_streaming()
        
    except KeyboardInterrupt:
        logger.info("\nConsumer interrupted by user")
        sys.exit(0)
    except Exception as e:
        logger.error(f"Consumer failed: {e}", exc_info=True)
        sys.exit(1)
    finally:
        if'consumer' in locals():
            consumer.stop()


if __name__ == "__main__":
    main()
