import os
import psycopg2
import requests
import logging
import time
from elsai_prompts.prompt_manager import PromptManager 


# Enterprise Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - AI AUDITOR - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "dbname": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
    "host": "postgres_db", 
    "port": "5432"
}
OLLAMA_API_URL = "http://ollama_llm:11434/api/generate"
MODEL_NAME = "phi3"

# 1. Initialize the PromptManager (v1.1.0)
prompt_manager = PromptManager(
    api_key=os.getenv("ELSAI_API_KEY"),
    project_id=os.getenv("ELSAI_PROJECT_ID"),
    base_url="https://managed-services.elsaifoundry.ai"
)

def generate_ai_rca(issue_type, description, payment_method, raw_template=None):
    """Formats the prompt and calls the local Ollama LLM."""
    if raw_template:
        try:
            prompt = raw_template.format(
                issue_type=issue_type, 
                description=description, 
                payment_method=payment_method if payment_method else 'Unknown'
            )
        except Exception as e:
            logger.error(f"Template Formatting Failed: {e}")
            prompt = f"Technical Root Cause for {issue_type}: {description}. System: {payment_method}. Output 1 sentence:"
    else:
        # Local Fallback Prompt from .env
        fallback_template = os.getenv("FALLBACK_AI_PROMPT")
        if fallback_template:
            prompt = fallback_template.format(
                issue_type=issue_type, 
                description=description, 
                payment_method=payment_method if payment_method else 'Unknown'
            )
        else:
            prompt = f"Technical Root Cause for {issue_type}: {description}. System: {payment_method if payment_method else 'Unknown'}. Output 1 sentence:"

    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "options": {"temperature": 0.4}
    }
    
    logger.info(f"Using fallback prompt. Generating RCA for Issue: {issue_type}")

    try:
        response = requests.post(OLLAMA_API_URL, json=payload)
        response.raise_for_status()
        raw_text = response.json().get("response", "").strip()
        
        # CLEANING: Ensure it's a single clean sentence
        clean_text = raw_text.replace('"', '').replace('\n', ' ').strip()
        if '.' in clean_text:
            clean_text = clean_text.split('.')[0] + '.'
            
        return clean_text
        
    except Exception as e:
        logger.error(f"LLM API Call Failed: {e}")
        return "Internal AI processing timeout."
    
def run_ai_audit():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT a.log_id, a.transaction_id, a.issue_type, a.description, p.payment_method
        FROM audit_reconciliation_log a
        LEFT JOIN stg_payments p ON a.transaction_id = p.transaction_id
        WHERE a.ai_root_cause_analysis IS NULL;
    """)
    anomalies = cur.fetchall()
    
    if anomalies:
        try:
            raw_template = prompt_manager.get_active_prompt_version(prompt_name="aiAudit")
        except Exception as e:
            logger.warning("Elsai AI Prompt not found. Using structured local fallback prompt.")
            raw_template = None
    
        for row in anomalies:
            log_id, txn_id, issue_type, description, payment_method = row
            ai_analysis = generate_ai_rca(issue_type, description, payment_method, raw_template)
                
            cur.execute("""
                UPDATE audit_reconciliation_log
                SET ai_root_cause_analysis = %s
                WHERE log_id = %s;
            """, (ai_analysis, log_id))
            conn.commit()
            time.sleep(1) # Small delay for local LLM stability

    cur.close()
    conn.close()

if __name__ == "__main__":
    run_ai_audit()