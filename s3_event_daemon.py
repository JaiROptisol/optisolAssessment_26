import time
import os
import subprocess
import logging
import psycopg2
from minio import Minio
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(asctime)s - SENSOR - %(message)s')
logger = logging.getLogger(__name__)

# Force the script to read the .env file in the same folder
load_dotenv()

BUCKET_NAME = os.getenv("S3_BUCKET_NAME")

# Connect to Local S3 (MinIO)
s3_client = Minio(
    "localhost:9090", 
    access_key=os.getenv("MINIO_ROOT_USER"), 
    secret_key=os.getenv("MINIO_ROOT_PASSWORD"), 
    secure=False
)

# Database Connection Info
DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB", "financial_data"),
    "user": os.getenv("POSTGRES_USER", "admin"),
    "password": os.getenv("POSTGRES_PASSWORD", "password123"),
    "host": "localhost", 
    "port": "5433"      
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
    
    if result is not None:
        return True
    return False

def check_files_and_trigger():
    # 1. Get ALL objects currently in the S3 bucket
    objects = [obj.object_name for obj in s3_client.list_objects(BUCKET_NAME, recursive=True)]
    
    # 2. Filter out only the sales files
    sales_files = [obj for obj in objects if obj.startswith('sales/sales_source_') and obj.endswith('.csv')]
    
    # 3. Dynamically loop through every sales file found
    for sales_key in sales_files:
        month_str = sales_key.replace('sales/sales_source_', '').replace('.csv', '')
        
        # 4. Construct the expected payment file name
        payment_key = f"payment/payment_gateway_{month_str}.csv"
        
        # 5. Check if the matching payment file exists in the bucket
        if payment_key in objects:
            
            # STATE MANAGEMENT: Check if we already processed this specific pair
            if is_already_processed(sales_key, payment_key):
                # Using debug level or just passing so it doesn't spam the console every 5 seconds
                continue 

            logger.info(f"💥 EVENT TRIGGERED: New Unprocessed Files for {month_str.upper()} Detected!")
            
            # Log STARTED state so the daemon doesn't trigger it twice
            update_execution_status(sales_key, payment_key, 'STARTED')
            
            # Hand off execution to Apache Airflow
            logger.info(f"Commanding Apache Airflow to orchestrate the pipeline...")
            subprocess.run([
                "docker", "exec", "enterprise_airflow", 
                "airflow", "dags", "trigger", "enterprise_financial_reconciliation"
            ])
            
            logger.info(f"✅ Handoff Complete. Airflow is now running the DAG in the background.\n")
            
if __name__ == "__main__":
    logger.info("Starting Event-Driven S3 Daemon with Strict DB State Management...")
    ensure_bucket_exists()
    while True:
        try:
            check_files_and_trigger()
            time.sleep(5) 
        except Exception as e:
            logger.error(f"Daemon Error: {e}")
            time.sleep(5)