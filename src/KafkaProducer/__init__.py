"""
KafkaProducer Package

Components for streaming network flow data to Kafka with Source IP partitioning.
"""

__version__ = "0.1.0"

from .producer import NetworkFlowProducer
from .csv_streamer import CSVStreamer

__all__ = ["NetworkFlowProducer", "CSVStreamer"]
