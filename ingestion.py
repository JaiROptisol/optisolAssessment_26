import pandas as pd
from sqlalchemy import create_engine, text
import logging
import sys
import os
# 1. ENTERPRISE REFINEMENT: Robust Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


# 2. Secure Database Connection Details
DB_USER = os.getenv('POSTGRES_USER')
DB_PASS = os.getenv('POSTGRES_PASSWORD')
DB_HOST = 'postgres_db'
DB_PORT = '5432'
DB_NAME = os.getenv('POSTGRES_DB')

DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"

DATABASE_URI = f"postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(DATABASE_URI)

def validate_and_load(file_path, table_name, primary_key, rename_columns=None):
    """Reads CSV, validates, maps schema, and loads to PostgreSQL."""
    try:
        logger.info(f"Starting ingestion for {file_path} into {table_name}...")
        
        # Ingestion: Read data
        df = pd.read_csv(file_path)
        initial_count = len(df)
        
        # Transformation: Schema Mapping (Fixing the mismatch)
        if rename_columns:
            df = df.rename(columns=rename_columns)
            logger.info(f"Mapped schema columns: {rename_columns}")
        
        # Validation: Check for missing Primary Keys
        df_clean = df.dropna(subset=[primary_key])
        dropped_count = initial_count - len(df_clean)
        
        if dropped_count > 0:
            logger.warning(f"Data Quality Alert: Dropped {dropped_count} rows due to missing {primary_key}.")
            
        # Idempotency: Clear staging table before loading
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {table_name};"))
            logger.info(f"Truncated staging table: {table_name}")
            
        # Storage: Load data into Database
        df_clean.to_sql(table_name, engine, if_exists='append', index=False)
        logger.info(f"Successfully loaded {len(df_clean)} records into {table_name}.\n")

    except Exception as e:
        logger.error(f"CRITICAL FAILURE during {table_name} ingestion: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    import sys
    
    # Enterprise Pattern: Parameterized Execution
    if len(sys.argv) != 3:
        logger.error("Usage: python ingestion.py <sales_file_path> <payment_file_path>")
        sys.exit(1)
        
    sales_file_path = sys.argv[1]
    payment_file_path = sys.argv[2]
    
    logger.info("--- Starting Daily Financial Ingestion Pipeline ---")
    
    # Process Sales Data
    validate_and_load(
        file_path=sales_file_path,
        table_name='stg_sales',
        primary_key='transaction_id',
        rename_columns={'date': 'transaction_date'}
    )
    
    # Process Payments Data
    validate_and_load(
        file_path=payment_file_path,
        table_name='stg_payments',
        primary_key='transaction_id'
    )
    
    logger.info("--- Ingestion Pipeline Completed Successfully ---")