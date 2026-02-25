import psycopg2
import csv
import socket
import os
from minio import Minio
from datetime import datetime

# Connect to the Airflow internal Docker network
DB_CONFIG = {
    "dbname": "financial_data", "user": "admin", 
    "password": "password123", "host": "postgres_db", "port": "5432"
}

def generate_and_upload_reports():
    print("Connecting to database...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    # Create dynamic filenames with today's date
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    clean_report_name = f"clean_revenue_report_{timestamp}.csv"
    audit_report_name = f"ai_audit_report_{timestamp}.csv"
    
    # Write to Airflow's /tmp/ directory to avoid permission issues
    clean_report_path = f"/tmp/{clean_report_name}"
    audit_report_path = f"/tmp/{audit_report_name}"

    # 1. Generate Clean Revenue Report (Production Schema)
    print("Generating Clean Revenue Report...")
    cur.execute("SELECT * FROM production.fact_revenue;")
    clean_rows = cur.fetchall()
    clean_colnames = [desc[0] for desc in cur.description]
    
    with open(clean_report_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(clean_colnames)
        writer.writerows(clean_rows)

    # 2. Generate AI Audit Report (Staging Schema)
    print("Generating AI Audit Report...")
    cur.execute("SELECT log_id, transaction_id, issue_type, description, ai_root_cause_analysis FROM audit_reconciliation_log;")
    audit_rows = cur.fetchall()
    audit_colnames = [desc[0] for desc in cur.description]
    
    with open(audit_report_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(audit_colnames)
        writer.writerows(audit_rows)
        
    cur.close()
    conn.close()

    # 3. Upload to MinIO S3
    print("Uploading reports to S3 (finance-reports bucket)...")
    minio_ip = socket.gethostbyname("minio_s3")
    client = Minio(f"{minio_ip}:9000", access_key="admin_s3", secret_key="password123", secure=False)
    
    # Ensure the reporting bucket exists
    if not client.bucket_exists("finance-reports"):
        client.make_bucket("finance-reports")
        print("Created new bucket: finance-reports")
        
    # Push the files
    client.fput_object("finance-reports", clean_report_name, clean_report_path)
    client.fput_object("finance-reports", audit_report_name, audit_report_path)
    
    # Clean up local temp files
    os.remove(clean_report_path)
    os.remove(audit_report_path)
    
    print(f"✅ Successfully uploaded {clean_report_name} and {audit_report_name} to S3!")

if __name__ == "__main__":
    generate_and_upload_reports()