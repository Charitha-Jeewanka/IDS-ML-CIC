#!/bin/bash
#
# Kafka KRaft Mode Setup Script
# Sets up and starts Kafka in KRaft mode (without Zookeeper)
#

set -e

echo "=========================================="
echo "Kafka KRaft Mode Setup"
echo "=========================================="
echo ""

# Check KAFKA_HOME
if [ -z "$KAFKA_HOME" ]; then
    echo "ERROR: KAFKA_HOME is not set"
    echo "Please set it to your Kafka installation directory"
    echo "Example: export KAFKA_HOME=/opt/kafka"
    exit 1
fi

echo "KAFKA_HOME: $KAFKA_HOME"
echo ""

# Configuration directory
KAFKA_CONFIG_DIR="$KAFKA_HOME/config"
KRAFT_CONFIG="$KAFKA_CONFIG_DIR/kraft/server.properties"

# Check if KRaft config exists
if [ ! -f "$KRAFT_CONFIG" ]; then
    echo "ERROR: KRaft config not found at $KRAFT_CONFIG"
    echo "Using default server.properties instead"
    KRAFT_CONFIG="$KAFKA_CONFIG_DIR/server.properties"
fi

# Log directory
LOG_DIR="/tmp/kraft-combined-logs"

echo "Step 1: Generating Cluster UUID..."
CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
echo "Cluster UUID: $CLUSTER_ID"
echo ""

echo "Step 2: Formatting log directory..."
$KAFKA_HOME/bin/kafka-storage.sh format \
    --standalone \
    -t $CLUSTER_ID \
    -c $KRAFT_CONFIG
echo "Log directory formatted"
echo ""

echo "Step 3: Starting Kafka in KRaft mode..."
echo "Press Ctrl+C to stop Kafka"
echo ""

# Start Kafka
$KAFKA_HOME/bin/kafka-server-start.sh $KRAFT_CONFIG
