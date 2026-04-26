import time
import json
import logging
import os
import psycopg

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(message)s')

def log_job(action, job_id, task_type, duration_ms, status):
    log_data = {
        "timestamp": time.time(),
        "action": action,
        "job_id": str(job_id),
        "task_type": task_type,
        "duration_ms": duration_ms,
        "status": status
    }
    logging.info(json.dumps(log_data))

def process_background_jobs():
    db_url = os.environ.get("SUPABASE_DB_URL") or os.environ.get("DATABASE_URL")
    if not db_url:
        logging.error('{"error": "No database URL configured for worker"}')
        return

    logging.info('{"message": "Background job worker started"}')
    
    while True:
        try:
            with psycopg.connect(db_url) as conn:
                with conn.cursor() as cur:
                    # FOR UPDATE SKIP LOCKED ensures multiple worker instances don't grab the same job
                    cur.execute("""
                        SELECT id, task_type, payload 
                        FROM background_jobs 
                        WHERE status = 'PENDING' 
                        ORDER BY created_at ASC 
                        FOR UPDATE SKIP LOCKED LIMIT 1;
                    """)
                    job = cur.fetchone()
                    
                    if job:
                        job_id, task_type, payload = job
                        start_time = time.time()
                        
                        try:
                            # SIMULATE PROCESSING (Email/WhatsApp)
                            if task_type == 'EMAIL':
                                # e.g., send_email(payload)
                                time.sleep(1) 
                            elif task_type == 'WHATSAPP':
                                # e.g., send_whatsapp(payload)
                                time.sleep(1)
                                
                            cur.execute("UPDATE background_jobs SET status = 'COMPLETED', processed_at = now() WHERE id = %s", (job_id,))
                            conn.commit()
                            
                            duration_ms = round((time.time() - start_time) * 1000, 2)
                            log_job("job_completed", job_id, task_type, duration_ms, "success")
                            
                        except Exception as e:
                            conn.rollback()
                            cur.execute("UPDATE background_jobs SET status = 'FAILED' WHERE id = %s", (job_id,))
                            conn.commit()
                            duration_ms = round((time.time() - start_time) * 1000, 2)
                            log_job("job_failed", job_id, task_type, duration_ms, "failed")
                    else:
                        time.sleep(2) # Backoff if no jobs
        except Exception as e:
            logging.error(json.dumps({"error": f"Worker connection failed: {str(e)}"}))
            time.sleep(5)

if __name__ == "__main__":
    process_background_jobs()
