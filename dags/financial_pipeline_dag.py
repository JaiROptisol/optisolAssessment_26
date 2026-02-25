from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from minio import Minio
import socket
import psycopg2

# 1. DEFINE THE FUNCTION FIRST
def mark_db_state_failed(context):
    """Fires automatically if the DAG completely fails, resetting the lock."""
    print("DAG FAILED! Releasing database lock...")
    conn = psycopg2.connect(
        dbname="financial_data", user="admin", 
        password="password123", host="postgres_db", port="5432"
    )
    cur = conn.cursor()
    # Change STARTED to FAILED so the S3 daemon can try again later
    cur.execute("UPDATE pipeline_execution_log SET status = 'FAILED' WHERE status = 'STARTED';")
    conn.commit()
    cur.close()
    conn.close()

# 2. THEN REFERENCE IT IN DEFAULT_ARGS
default_args = {
    'owner': 'data_engineering',
    'depends_on_past': False,
    'email_on_failure': True,             
    'email': ['data-team@company.com'],
    'retries': 3,                         
    'retry_delay': timedelta(minutes=1),  
    'on_failure_callback': mark_db_state_failed, # <-- Python now knows what this is!
}

def fetch_s3_data():
    minio_ip = socket.gethostbyname("minio_s3")
    client = Minio(f"{minio_ip}:9000", access_key="admin_s3", secret_key="password123", secure=False)
    
    today = datetime.today()
    last_month = today - relativedelta(months=1)
    target_month_str = last_month.strftime("%Y_%b").lower()
    
    sales_key = f"sales/sales_source_{target_month_str}.csv"
    payment_key = f"payment/payment_gateway_{target_month_str}.csv"
    
    print(f"Fetching {sales_key} and {payment_key} from S3...")
    client.fget_object("finance-landing-zone", sales_key, "/tmp/temp_sales.csv")
    client.fget_object("finance-landing-zone", payment_key, "/tmp/temp_payment.csv")
    print("Download complete!")

with DAG(
    'enterprise_financial_reconciliation',
    default_args=default_args,
    description='End-to-end financial data ingestion, audit, and AI analysis',
    schedule_interval='@daily',
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['finance', 'tier-1'],
) as dag:

    download_data = PythonOperator(
        task_id='download_s3_data',
        python_callable=fetch_s3_data
    )

    run_ingestion = BashOperator(
        task_id='run_python_ingestion',
        bash_command='cd /opt/airflow/project && python3 ingestion.py /tmp/temp_sales.csv /tmp/temp_payment.csv',
    )

    run_audit_sp = PostgresOperator(
        task_id='execute_audit_rules',
        postgres_conn_id='FINANCIAL_DB',
        sql='CALL sp_reconcile_revenue();',
    )

    run_production_load_sp = PostgresOperator(
        task_id='load_clean_data_to_production',
        postgres_conn_id='FINANCIAL_DB',
        sql='CALL sp_load_production_fact();',
    )

    run_ai_auditor = BashOperator(
        task_id='trigger_ai_rca_agent',
        bash_command='cd /opt/airflow/project && python3 ai_auditor.py',
    )

    mark_pipeline_finished = PostgresOperator(
        task_id='mark_db_state_finished',
        postgres_conn_id='FINANCIAL_DB',
        sql="UPDATE pipeline_execution_log SET status = 'FINISHED', end_time = CURRENT_TIMESTAMP WHERE status = 'STARTED';",
    )

    generate_final_reports = BashOperator(
        task_id='generate_and_upload_reports',
        bash_command='cd /opt/airflow/project && python3 report_generator.py',
    )

    # UPDATED GRAPH
    download_data >> run_ingestion >> run_audit_sp >> run_production_load_sp >> run_ai_auditor >> mark_pipeline_finished >> generate_final_reports