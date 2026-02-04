#!/bin/bash
#
# Stop Kafka (works for both KRaft and Zookeeper modes)
#

if [ -z "$KAFKA_HOME" ]; then
    echo "ERROR: KAFKA_HOME is not set"
    exit 1
fi

echo "Stopping Kafka..."
$KAFKA_HOME/bin/kafka-server-stop.sh

echo "Kafka stopped"
