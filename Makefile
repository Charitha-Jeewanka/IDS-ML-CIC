.PHONY: help install check-columns validate run-etl run-etl-file clean test

# Default target
help:
	@echo "Available commands:"
	@echo ""
	@echo "Phase 1: ETL Pipeline"
	@echo "  make install          - Install Python dependencies"
	@echo "  make check-columns    - Check if all CSV files have same columns"
	@echo "  make validate         - Validate ETL pipeline setup"
	@echo "  make run-etl          - Run full ETL pipeline on all files"
	@echo "  make run-etl-file FILE=<name> - Run ETL on single file"
	@echo ""
	@echo "Kafka Management (KRaft Mode)"
	@echo "  make kafka-start-daemon - Start Kafka in background"
	@echo "  make kafka-stop         - Stop Kafka"
	@echo "  make kafka-status       - Check if Kafka is running"
	@echo ""
	@echo "Phase 2 & 3: Kafka and Parquet"
	@echo "  make csv-to-parquet   - Convert processed CSVs to Parquet"
	@echo "  make validate-parquet - Validate Parquet files"
	@echo "  make setup-kafka      - Setup Kafka topic"
	@echo "  make run-producer     - Stream processed CSVs to Kafka"
	@echo "  make run-consumer     - Start PySpark consumer (Kafka -> Parquet)"
	@echo "  make test-pipeline    - Test end-to-end pipeline"
	@echo ""
	@echo "Utilities"
	@echo "  make view-report      - View latest execution report"
	@echo "  make view-logs        - View pipeline logs"
	@echo "  make stats            - Show quick statistics"
	@echo "  make test             - Run validation tests"
	@echo "  make clean            - Clean generated files"
	@echo ""
	@echo "Example usage:"
	@echo "  make csv-to-parquet"
	@echo "  make run-etl-file FILE=Monday_WorkingHours_ISCX.csv"

# Install dependencies
install:
	@echo "Installing Python dependencies..."
	uv pip install -r requirements.txt

# Kafka Management (KRaft mode)
.PHONY: kafka-start kafka-start-daemon kafka-stop kafka-status

# Start Kafka in KRaft mode (foreground)
kafka-start:
	@echo "Starting Kafka in KRaft mode (foreground)..."
	@./src/scripts/start_kafka_kraft.sh

# Start Kafka in KRaft mode (background daemon)
kafka-start-daemon:
	@echo "Starting Kafka daemon..."
	@./src/scripts/start_kafka_daemon.sh

# Stop Kafka
kafka-stop:
	@echo "Stopping Kafka..."
	@./src/scripts/stop_kafka.sh

# Check Kafka status
kafka-status:
	@echo "Checking Kafka status..."
	@if ss -tuln 2>/dev/null | grep -q ":9092"; then \
		echo "Kafka is running on port 9092"; \
	else \
		echo "Kafka is not running"; \
	fi

# Check column consistency
check-columns:
	@echo "Checking column consistency across CSV files..."
	python3 utils/check_columns.py

# Validate ETL setup
validate:
	@echo "Validating ETL pipeline setup..."
	python3 src/DataPipeline/validate_setup.py

# Run full ETL pipeline
run-etl:
	@echo "Running ETL pipeline on all CSV files..."
	python3 src/DataPipeline/run_etl.py

# Run ETL on single file
run-etl-file:
ifndef FILE
	@echo "Error: FILE parameter is required"
	@echo "Usage: make run-etl-file FILE=filename.csv"
	@exit 1
endif
	@echo "Running ETL pipeline on $(FILE)..."
	python3 src/DataPipeline/run_etl.py --file $(FILE)

# Run ETL with debug logging
run-etl-debug:
	@echo "Running ETL pipeline with debug logging..."
	python3 src/DataPipeline/run_etl.py --debug

# Run tests
test: validate
	@echo "Running tests..."

# Clean generated files
clean:
	@echo "Cleaning generated files..."
	rm -rf data/processed/*
	rm -rf data/parquet/*
	rm -rf artifacts/reports/*
	rm -rf artifacts/checkpoints/*
	rm -rf logs/*
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	@echo "Clean complete!"

# View latest report
view-report:
	@echo "Viewing latest ETL report..."
	@latest=$$(ls -t artifacts/reports/etl_report_*.json 2>/dev/null | head -1); \
	if [ -n "$$latest" ]; then \
		cat "$$latest" | python3 -m json.tool; \
	else \
		echo "No reports found in artifacts/reports/"; \
	fi

# View logs
view-logs:
	@echo "Viewing ETL logs..."
	@if [ -f logs/etl_pipeline.log ]; then \
		tail -100 logs/etl_pipeline.log; \
	else \
		echo "No log file found at logs/etl_pipeline.log"; \
	fi

# Follow logs in real-time
tail-logs:
	@echo "Following ETL logs (Ctrl+C to stop)..."
	@if [ -f logs/etl_pipeline.log ]; then \
		tail -f logs/etl_pipeline.log; \
	else \
		echo "No log file found at logs/etl_pipeline.log"; \
	fi

# Check processed files
list-processed:
	@echo "Processed files:"
	@ls -lh data/processed/ 2>/dev/null || echo "No processed files found"

# Quick stats
stats:
	@echo "ETL Pipeline Statistics:"
	@echo "------------------------"
	@echo -n "Raw CSV files: "
	@ls data/raw/*.csv 2>/dev/null | wc -l
	@echo -n "Processed files: "
	@ls data/processed/*.csv 2>/dev/null | wc -l
	@echo -n "Parquet files: "
	@ls data/parquet/*.parquet 2>/dev/null | wc -l
	@echo -n "Reports generated: "
	@ls artifacts/reports/*.json 2>/dev/null | wc -l

# Phase 2 & 3: Kafka and Parquet
.PHONY: csv-to-parquet validate-parquet setup-kafka run-producer run-consumer test-pipeline

# Convert processed CSVs to Parquet
csv-to-parquet:
	@echo "Converting CSV files to Parquet..."
	python3 src/ColdPath/csv_to_parquet.py

# Validate Parquet files
validate-parquet:
	@echo "Validating Parquet files..."
	python3 src/ColdPath/csv_to_parquet.py --validate

# Setup Kafka topic
setup-kafka:
	@echo "Setting up Kafka topic..."
	python3 src/scripts/setup_kafka_topic.py

# Run Kafka producer
run-producer:
	@echo "Running Kafka producer..."
	python3 src/KafkaProducer/run_producer.py

# Run Kafka producer for single file
run-producer-file:
ifndef FILE
	@echo "Error: FILE parameter is required"
	@echo "Usage: make run-producer-file FILE=filename.csv"
	@exit 1
endif
	@echo "Streaming $(FILE) to Kafka..."
	python3 src/KafkaProducer/run_producer.py --file $(FILE)

# Run PySpark consumer
run-consumer:
	@echo "Starting PySpark consumer..."
	python3 src/ColdPath/run_consumer.py

# Full pipeline test
test-pipeline:
	@echo "Running end-to-end pipeline test..."
	python3 src/scripts/validate_pipeline.py

