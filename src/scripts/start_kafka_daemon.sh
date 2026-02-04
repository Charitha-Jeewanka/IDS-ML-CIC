#!/bin/bash
#
# Start Kafka in KRaft mode (background daemon)
#

set -e

if [ -z "$KAFKA_HOME" ]; then
    echo "ERROR: KAFKA_HOME is not set"
    exit 1
fi

KAFKA_CONFIG="$KAFKA_HOME/config/server.properties"
LOG_DIR="/tmp/kraft-combined-logs"

echo "Using config: $KAFKA_CONFIG"

# Check if storage needs formatting
if [ ! -d "$LOG_DIR" ] || [ ! -f "$LOG_DIR/meta.properties" ]; then
    echo "Formatting Kafka storage (first time setup)..."
    
    # Generate UUID
    CLUSTER_ID=$($KAFKA_HOME/bin/kafka-storage.sh random-uuid)
    echo "Generated Cluster ID: $CLUSTER_ID"
    
    # Format storage
    $KAFKA_HOME/bin/kafka-storage.sh format \
        --standalone \
        -t $CLUSTER_ID \
        -c $KAFKA_CONFIG
    
    echo "Storage formatted successfully"
else
    echo "Storage already formatted, skipping format step"
fi

# Start Kafka in daemon mode
echo "Starting Kafka in KRaft mode (daemon)..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_CONFIG

echo "Waiting for Kafka to start..."

# Wait up to 45 seconds for Kafka to start
MAX_WAIT=45
COUNTER=0
while [ $COUNTER -lt $MAX_WAIT ]; do
    if ss -tuln 2>/dev/null | grep -q ":9092"; then
        echo "Kafka is running on port 9092"
        echo "Startup took approximately $COUNTER seconds"
        exit 0
    fi
    sleep 1
    COUNTER=$((COUNTER + 1))
    if [ $((COUNTER % 10)) -eq 0 ]; then
        echo "Still waiting... ($COUNTER seconds)"
    fi
done

# Timeout - Kafka didn't start in time
echo "Kafka did not start after ${MAX_WAIT} seconds"
echo ""
echo "Check logs at: $KAFKA_HOME/logs/server.log"
echo "Last 30 lines:"
tail -30 $KAFKA_HOME/logs/server.log 2>/dev/null || echo "Could not read log file"
exit 1

