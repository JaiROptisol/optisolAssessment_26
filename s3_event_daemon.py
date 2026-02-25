import time
import os
import subprocess
import logging
import sys
import psycopg2
from minio import Minio
from datetime import datetime
from dateutil.relativedelta import relativedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - SENSOR - %(message)s')
logger = logging.getLogger(__name__)

# Connect to Local S3 (MinIO)
s3_client = Minio("localhost:9090", access_key="admin_s3", secret_key="password123", secure=False)
BUCKET_NAME = "finance-landing-zone"

# Database Connection Info
DB_CONFIG = {
    "dbname": "financial_data", "user": "admin", 
    "password": "password123", "host": "localhost", "port": "5433"
}

def ensure_bucket_exists():
    if not s3_client.bucket_exists(BUCKET_NAME):
        s3_client.make_bucket(BUCKET_NAME)
        logger.info(f"Created S3 Bucket: {BUCKET_NAME}")

def update_execution_status(sales_key, payment_key, status):
    """Inserts or updates the execution log state in the database."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    if status == 'STARTED':
        cur.execute("""
            INSERT INTO pipeline_execution_log (sales_file_name, payment_file_name, status)
            VALUES (%s, %s, %s)
        """, (sales_key, payment_key, status))
    elif status == 'FINISHED':
        cur.execute("""
            UPDATE pipeline_execution_log 
            SET status = %s, end_time = CURRENT_TIMESTAMP
            WHERE sales_file_name = %s AND payment_file_name = %s
        """, (status, sales_key, payment_key))
        
    conn.commit()
    cur.close()
    conn.close()

def is_already_processed(sales_key, payment_key):
    """Checks the database to see if these files were already handled."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT status FROM pipeline_execution_log 
        WHERE sales_file_name = %s AND payment_file_name = %s
    """, (sales_key, payment_key))
    result = cur.fetchone()
    cur.close()
    conn.close()
    
    if result and result[0] in ('STARTED', 'FINISHED'):
        return True
    return False

def check_files_and_trigger():
    # 1. Get ALL objects currently in the S3 bucket
    objects = [obj.object_name for obj in s3_client.list_objects(BUCKET_NAME, recursive=True)]
    
    # 2. Filter out only the sales files
    sales_files = [obj for obj in objects if obj.startswith('sales/sales_source_') and obj.endswith('.csv')]
    
    # 3. Dynamically loop through every sales file found
    for sales_key in sales_files:
        # Extract the dynamic month string (e.g., '2025_dec' or '2026_jan')
        # sales_key looks like: 'sales/sales_source_2025_dec.csv'
        month_str = sales_key.replace('sales/sales_source_', '').replace('.csv', '')
        
        # 4. Construct the expected payment file name for this specific month
        payment_key = f"payment/payment_gateway_{month_str}.csv"
        
        # 5. Check if the matching payment file exists in the bucket
        # 5. Check if the matching payment file exists in the bucket
        if payment_key in objects:
            
            # STATE MANAGEMENT: Check if we already processed this specific pair
            if is_already_processed(sales_key, payment_key):
                logger.info(f"Skipping {month_str} files: DB shows already processed.")
                continue 

            logger.info(f"💥 EVENT TRIGGERED: New Unprocessed Files for {month_str.upper()} Detected!")
            
            # Log STARTED state so the daemon doesn't trigger it twice
            update_execution_status(sales_key, payment_key, 'STARTED')
            
            # THE ENTERPRISE FIX: Hand off execution to Apache Airflow!
            logger.info(f"Commanding Apache Airflow to orchestrate the pipeline...")
            subprocess.run([
                "docker", "exec", "enterprise_airflow", 
                "airflow", "dags", "trigger", "enterprise_financial_reconciliation"
            ])
            
            logger.info(f"✅ Handoff Complete. Airflow is now running the DAG in the background.\n")
            
if __name__ == "__main__":
    logger.info("Starting Event-Driven S3 Daemon with DB State Management...")
    ensure_bucket_exists()
    while True:
        try:
            check_files_and_trigger()
            time.sleep(5) 
        except Exception as e:
            logger.error(f"Daemon Error: {e}")
            time.sleep(5)