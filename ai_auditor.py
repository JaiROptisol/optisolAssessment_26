import psycopg2
import requests
import logging
import sys
import time

# Enterprise Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - AI AUDITOR - %(message)s')
logger = logging.getLogger(__name__)

DB_CONFIG = {
    "dbname": "financial_data", "user": "admin", 
    "password": "password123", "host": "postgres_db", "port": "5432"
}

OLLAMA_API_URL = "http://ollama_llm:11434/api/generate"
MODEL_NAME = "phi3"

def generate_ai_rca(issue_type, description, payment_method):
    """Calls the local Ollama LLM to generate a Root Cause Hypothesis."""
    
    # AGGRESSIVE PROMPT: Forcing the LLM to behave like a strict function
    prompt = f"""You are a strict technical diagnostic API. Your ONLY job is to output a single-sentence root cause for a data discrepancy.
DO NOT include any greetings. DO NOT say "Based on". DO NOT repeat the prompt. DO NOT use formatting.

Error Type: {issue_type}
Log Details: {description}
Payment System: {payment_method if payment_method else 'Unknown'}

Output ONLY the technical reason:"""

    payload = {
        "model": MODEL_NAME,
        "prompt": prompt,
        "stream": False,
        "options": {
            "temperature": 0.4 # Raised slightly to prevent repeating "API timeout"
        }
    }

    try:
        response = requests.post(OLLAMA_API_URL, json=payload)
        response.raise_for_status()
        raw_text = response.json().get("response", "").strip()
        
        # --- PYTHON SANITIZER (Forcefully cleaning the AI's output) ---
        # 1. Strip out common chatty prefixes the AI might ignore our prompt to use
        bad_prefixes = ["Based on the provided", "The technical reason", "Mismatch Type:", "System Log:", "**Short, Highly Technical Reason:**"]
        for prefix in bad_prefixes:
            if prefix in raw_text:
                raw_text = raw_text.split(prefix)[-1]
                
        # 2. Remove quotes and newlines
        clean_text = raw_text.replace('"', '').replace('\n', ' ').strip()
        
        # 3. Force it to be just one sentence (cut off anything after the first period)
        if '.' in clean_text:
            clean_text = clean_text.split('.')[0] + '.'
            
        return clean_text.strip()
        
    except Exception as e:
        logger.error(f"LLM API Call Failed: {e}")
        return "API timeout during AI inference."
    
    
def run_ai_audit():
    logger.info("Connecting to operational database to fetch anomalies...")
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    
    cur.execute("""
        SELECT a.log_id, a.transaction_id, a.issue_type, a.description, p.payment_method
        FROM audit_reconciliation_log a
        LEFT JOIN stg_payments p ON a.transaction_id = p.transaction_id
        WHERE a.ai_root_cause_analysis IS NULL;
    """)
    anomalies = cur.fetchall()
    
    if not anomalies:
        logger.info("No new anomalies require AI analysis.")
        cur.close()
        conn.close()
        return

    logger.info(f"Found {len(anomalies)} anomalies. Booting local AI Agent for RCA...")
    
    for row in anomalies:
        log_id, txn_id, issue_type, description, payment_method = row
        logger.info(f"Analyzing {txn_id}...")
        
        ai_analysis = generate_ai_rca(issue_type, description, payment_method)
        
        # Fallback if the AI drops the connection
        if not ai_analysis:
            ai_analysis = "API timeout during AI inference. Requires manual review."
            
        cur.execute("""
            UPDATE audit_reconciliation_log
            SET ai_root_cause_analysis = %s
            WHERE log_id = %s;
        """, (ai_analysis, log_id))
        conn.commit()
        
        # RATE LIMITER: Give the local LLM 2 seconds to breathe before the next prompt
        time.sleep(2)

    logger.info("✅ AI Auditing Complete. RCA hypotheses saved to database.")
    cur.close()
    conn.close()

if __name__ == "__main__":
    run_ai_audit()