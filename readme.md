# Enterprise Financial Reconciliation & AI Audit Pipeline

## 1. Problem Understanding & Approach
**The Problem:** Financial organizations frequently struggle to reconcile internal sales records against external payment gateway settlements. Discrepancies (missing records, amount mismatches, duplicates) require manual investigation, which is slow, error-prone, and lacks transparent auditability.

**The Approach:** I designed an automated, event-driven **Medallion (ELT) Data Architecture**. 
* Instead of processing heavy data transformations in Python memory (which scales poorly), I utilized **Push-Down Compute**, using PostgreSQL Stored Procedures to perform complex anti-joins and merge logic.
* I implemented a **Strict Quarantine** data quality rule: perfectly matched records are loaded into a clean `production` schema, while discrepancies are blocked and logged in an `audit` ledger.
* To accelerate resolution, I integrated a **Local Agentic AI (Microsoft Phi-3)** to act as an automated L1 Support Engineer, dynamically reading the audit logs and writing human-readable Root Cause Analyses (RCA) directly into the database.
* The entire pipeline is orchestrated via **Apache Airflow** to ensure dependency management, idempotency, and automated retries.

---

## 2. Architecture & Flow Diagram

The pipeline utilizes MinIO (S3) for object storage, Apache Airflow for orchestration, PostgreSQL for processing/storage, and Ollama (Phi-3) for local AI generation.

```mermaid
graph TD
    subgraph "Source & Landing (MinIO)"
        A[Sales CSV] --> C(finance-landing-zone)
        B[Payment CSV] --> C
    end

    subgraph "Orchestration (Apache Airflow)"
        D[Event Daemon Sensor] -->|Triggers| E{Airflow DAG}
        E -->|Task 1: Extract| F[Python Ingestion]
        E -->|Task 2: Transform| G[SQL ELT Audit Rules]
        E -->|Task 3: Load| L[SQL Prod Load]
        E -->|Task 4: AI Agent| H[Local AI RCA Agent]
        E -->|Task 5: Report| I[Python Report Generator]
    end

    subgraph "Processing & Storage (PostgreSQL)"
        F --> J[(stg_sales & stg_payments)]
        J -->|Push-Down Compute| K{Reconciliation Rules}
        K -->|Pass| M[(production.fact_revenue)]
        K -->|Fail: Quarantined| N[(audit_reconciliation_log)]
        H -.->|Reads Context & Writes RCA| N
    end

    subgraph "Consumption"
        I -.->|Extracts Clean Data| M
        I -.->|Extracts Audit Log| N
        I --> O(finance-reports S3 Bucket)
        O --> P[Business / BI Tools]
    end