.PHONY: help install check-columns validate run-etl run-etl-file clean test

# Default target
help:
	@echo "Available commands:"
	@echo "  make install          - Install Python dependencies"
	@echo "  make check-columns    - Check if all CSV files have same columns"
	@echo "  make validate         - Validate ETL pipeline setup"
	@echo "  make run-etl          - Run full ETL pipeline on all files"
	@echo "  make run-etl-file FILE=filename.csv - Run ETL on single file"
	@echo "  make test             - Run validation tests"
	@echo "  make clean            - Clean generated files and directories"
	@echo ""
	@echo "Example usage:"
	@echo "  make run-etl-file FILE=Monday_WorkingHours_ISCX.csv"

# Install dependencies
install:
	@echo "Installing Python dependencies..."
	uv pip install -r requirements.txt

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
	rm -rf artifacts/reports/*
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
	@echo -n "Reports generated: "
	@ls artifacts/reports/*.json 2>/dev/null | wc -l
