#!/bin/bash

echo "==================================================="
echo "  INITIATING EVENT-DRIVEN PIPELINE SENSOR"
echo "==================================================="

# 1. Calculate the target month dynamically (Current Month - 1)
# Linux 'date' command allows us to easily grab "last month"
TARGET_MONTH=$(date -d "last month" '+%Y_%b' | tr '[:upper:]' '[:lower:]')
echo "[INFO] Target Month calculated as: $TARGET_MONTH"

# 2. Define expected S3 file paths
SALES_FILE="sales/sales_source_${TARGET_MONTH}.csv"
PAYMENT_FILE="payment/payment_gateway_${TARGET_MONTH}.csv"

# 3. The "Sensor" - File Existence Check
echo "[INFO] Checking S3 landing zones for required files..."

if [ ! -f "$SALES_FILE" ]; then
    echo "[CRITICAL ERROR] Sales file $SALES_FILE is missing!"
    echo "Pipeline aborted to prevent partial data loads."
    exit 1
fi

if [ ! -f "$PAYMENT_FILE" ]; then
    echo "[CRITICAL ERROR] Payment file $PAYMENT_FILE is missing!"
    echo "Pipeline aborted to prevent partial data loads."
    exit 1
fi

echo "[SUCCESS] Both files found. Proceeding with pipeline execution."
echo "==================================================="

# 4. Run the Pipeline using the dynamic paths
echo "[1/2] Running ETL Ingestion..."
python3 ingestion.py "$SALES_FILE" "$PAYMENT_FILE"
if [ $? -ne 0 ]; then echo "Ingestion failed"; exit 1; fi

echo "[2/2] Executing SQL Reconciliation Engine..."
docker exec -i enterprise_reconciliation_db psql -U admin -d financial_data -c "CALL sp_reconcile_revenue();"

echo "==================================================="
echo "  PIPELINE COMPLETE. FETCHING AUDIT REPORT:"
echo "==================================================="

# Output the findings for the live demo
docker exec -i enterprise_reconciliation_db psql -U admin -d financial_data -c "SELECT transaction_id, issue_type FROM audit_reconciliation_log;"