# queuectl/database.py

import sqlite3
import os
from datetime import datetime, timezone, timedelta
import sys

DATABASE_FILE = 'queue.db'


def _robust_convert_timestamp(ts_bytes):
    """Converts an ISO-formatted timestamp from bytes to a datetime object."""
    try:
        # Decode bytes to string
        ts_str = ts_bytes.decode('utf-8')
        
        # Handle the 'Z' (Zulu/UTC) suffix if present
        if ts_str.endswith('Z'):
            ts_str = ts_str[:-1] + '+00:00'
        
        # Handle the space separator that SQLite sometimes uses (e.g. 'YYYY-MM-DD HH:MM:SS')
        if ' ' in ts_str:
            ts_str = ts_str.replace(' ', 'T', 1)
        
        return datetime.fromisoformat(ts_str)
    except (ValueError, TypeError) as e:
        print(f"Warning: Could not parse timestamp {ts_bytes}: {e}")
        return None


# Register our new, robust function to handle the "timestamp" type
sqlite3.register_converter("timestamp", _robust_convert_timestamp)


def get_db_connection():
    """Establishes a connection to the SQLite database."""
    try:
        conn = sqlite3.connect(
            DATABASE_FILE,
            timeout=10.0,
            # We still need this to *trigger* our registered converter
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        return conn
    except sqlite3.Error as e:
        print(f"FATAL: Could not connect to database at {DATABASE_FILE}: {e}")
        print("Run 'queuectl init' to create the database.")
        exit(1)


def init_db():
    """Initializes the database and creates tables."""
    if os.path.exists(DATABASE_FILE):
        print(f"Database file '{DATABASE_FILE}' already exists.")
    else:
        print(f"Creating new database at '{DATABASE_FILE}'...")
    
    conn = sqlite3.connect(DATABASE_FILE)
    cursor = conn.cursor()
    
    # --- Jobs Table ---
    # state: pending | processing | completed | failed | dead
    # run_at: Used for exponential backoff scheduling
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS jobs
                   (
                       id           TEXT PRIMARY KEY,
                       command      TEXT      NOT NULL,
                       state        TEXT      NOT NULL DEFAULT 'pending',
                       attempts     INTEGER   NOT NULL DEFAULT 0,
                       max_retries  INTEGER   NOT NULL DEFAULT 3,

                       priority     INTEGER   NOT NULL DEFAULT 0,
                       timeout      INTEGER            DEFAULT 300,

                       run_at       TIMESTAMP,
                       created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                       updated_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                       started_at   TIMESTAMP,
                       completed_at TIMESTAMP
                   )
                   """)
    
    # --- Config Table ---
    # A simple key-value store for system settings
    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS config
                   (
                       key   TEXT PRIMARY KEY,
                       value TEXT NOT NULL
                   )
                   """)
    
    # Set default values in a portable way (ignore if already present)
    try:
        cursor.execute("INSERT INTO config (key, value) VALUES (?, ?)", ('max_retries', '3'))
    except sqlite3.IntegrityError:
        pass
    try:
        cursor.execute("INSERT INTO config (key, value) VALUES (?, ?)", ('backoff_base', '2'))
    except sqlite3.IntegrityError:
        pass
    
    conn.commit()
    conn.close()
    print("Database initialized successfully.")


def set_config(key, value):
    """Sets a configuration value in the config table."""
    conn = get_db_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO config (key, value) VALUES (?, ?)",
            (key, value)
        )
        conn.commit()
        print(f"Config set: {key} = {value}")
    except sqlite3.Error as e:
        print(f"Database error setting config: {e}")
    finally:
        conn.close()


def get_config(key, default=None):
    """Gets a configuration value from the config table."""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT value FROM config WHERE key = ?", (key,))
        row = cursor.fetchone()
        return row['value'] if row else default
    except sqlite3.Error as e:
        print(f"Database error getting config: {e}")
        return default
    finally:
        conn.close()


def create_job(job_id, command, max_retries_override=None, run_at_str=None, priority=0, timeout=None):
    """Adds a new or scheduled job to the queue."""
    conn = get_db_connection()
    
    try:
        if max_retries_override is None:
            default_retries = get_config('max_retries', 3)
            max_retries = int(default_retries)
        else:
            max_retries = int(max_retries_override)
        
        if timeout is None:
            timeout = 300  # Default timeout
        
        now = datetime.now(timezone.utc)
        job_state = 'pending'
        run_at_dt = None
        
        # --- MODIFIED: Correct Scheduling Logic ---
        if run_at_str:
            try:
                run_at_dt_aware = datetime.fromisoformat(run_at_str)
                
                # 1. Check if user provided a "naive" time (no timezone)
                if run_at_dt_aware.tzinfo is None:
                    # If naive, assume it's the user's LOCAL time.
                    # Stamp it with the system's local timezone.
                    run_at_dt_aware = run_at_dt_aware.replace(tzinfo=datetime.now().astimezone().tzinfo)
                
                # 2. Now that the datetime is "aware", convert it to UTC
                #    for consistent database storage.
                run_at_dt_utc = run_at_dt_aware.astimezone(timezone.utc)
                
                if run_at_dt_utc > now:
                    job_state = 'scheduled'
                    run_at_dt = run_at_dt_utc  # This is the UTC time to save
                    print(f"Job {job_id} is scheduled to run at {run_at_dt}")
                else:
                    # Time is in the past, run it now
                    job_state = 'pending'
                    run_at_dt = None
            
            except ValueError:
                print(
                    f"Error: Invalid run_at format '{run_at_str}'. Must be ISO 8601 (e.g., YYYY-MM-DDTHH:MM:SS+HH:MM).")
                return
        # --- END MODIFIED LOGIC ---
        
        conn.execute(
            """
            INSERT INTO jobs (id, command, max_retries, priority, timeout,
                              created_at, updated_at, state, attempts, run_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, ?)
            """,
            (job_id, command, max_retries, priority, timeout,
             now, now, job_state, run_at_dt)
        )
        conn.commit()
        if job_state == 'pending':
            print(f"Successfully enqueued job: {job_id}")
    
    except sqlite3.IntegrityError:
        print(f"Error: Job with ID '{job_id}' already exists.")
    except Exception as e:
        print(f"Error enqueuing job: {e}")
    finally:
        conn.close()


def fetch_and_lock_job():
    """
    Atomically fetches the next available job (by priority)
    and marks it as 'processing'.
    """
    conn = get_db_connection()
    try:
        conn.execute("BEGIN IMMEDIATE")
        
        now = datetime.now(timezone.utc)
        
        # --- MODIFIED: WHERE and ORDER BY ---
        # 1. Look for 'pending' OR 'failed'/'scheduled' that are ready to run
        # 2. Order by priority (highest first), then by creation time (oldest first)
        cursor = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE (state = 'pending')
               OR (state IN ('failed', 'scheduled') AND run_at <= ?)
            ORDER BY priority DESC, created_at ASC
            LIMIT 1
            """,
            (now,)
        )
        job = cursor.fetchone()
        
        if job:
            conn.execute(
                """
                UPDATE jobs
                SET state      = 'processing',
                    updated_at = ?
                WHERE id = ?
                """,
                (now, job['id'])
            )
            conn.commit()
            return dict(job)
        else:
            conn.commit()
            return None
    
    except sqlite3.Error as e:
        print(f"Database error fetching job: {e}")
        conn.rollback()
        return None
    finally:
        conn.close()


def finalize_job(job_id, success):
    """
    Finalizes a job by marking it 'completed' or handling failure
    with exponential backoff and DLQ logic.
    """
    conn = get_db_connection()
    now = datetime.now(timezone.utc)
    
    try:
        if success:
            # --- Happy Path ---
            conn.execute(
                """
                UPDATE jobs
                SET state        = 'completed',
                    updated_at   = ?,
                    completed_at = ?
                WHERE id = ?
                """,
                (now, now, job_id)
            )
            conn.commit()
        else:
            # --- Unhappy Path (Retry/DLQ Logic) ---
            conn.execute("BEGIN IMMEDIATE")  # Lock for read-modify-write
            
            # 1. Get current job state
            cursor = conn.execute(
                "SELECT attempts, max_retries FROM jobs WHERE id = ?", (job_id,)
            )
            job = cursor.fetchone()
            
            if not job:
                print(f"Error finalizing: Job {job_id} not found.")
                conn.rollback()
                return
            
            new_attempts = job['attempts'] + 1
            
            if new_attempts >= job['max_retries']:
                # 2. Move to Dead Letter Queue (DLQ)
                print(f"Job {job_id} failed. Max retries ({job['max_retries']}) reached. Moving to DLQ.")
                conn.execute(
                    """
                    UPDATE jobs
                    SET state      = 'dead',
                        attempts   = ?,
                        updated_at = ?
                    WHERE id = ?
                    """,
                    (new_attempts, now, job_id)
                )
            else:
                # 3. Schedule for Retry with Exponential Backoff
                
                # Get backoff base from config (default to 2)
                base_str = get_config('backoff_base', '2')
                try:
                    backoff_base = int(base_str)
                except ValueError:
                    print(f"Warning: Invalid backoff_base '{base_str}', using 2.")
                    backoff_base = 2
                
                # delay = base ^ attempts
                delay_seconds = backoff_base ** new_attempts
                
                retry_run_at = now + timedelta(seconds=delay_seconds)
                
                print(
                    f"Job {job_id} failed. Attempt {new_attempts}/{job['max_retries']}. Retrying in {delay_seconds}s.")
                
                conn.execute(
                    """
                    UPDATE jobs
                    SET state      = 'failed',
                        attempts   = ?,
                        updated_at = ?,
                        run_at     = ?
                    WHERE id = ?
                    """,
                    (new_attempts, now, retry_run_at, job_id)
                )
            conn.commit()  # Commit the read-modify-write transaction
    
    except sqlite3.Error as e:
        print(f"Database error finalizing job {job_id}: {e}")
        if not success:  # Only rollback if we were in the 'else' block's transaction
            conn.rollback()
    finally:
        conn.close()


def get_jobs_by_state(state):
    """Fetches all jobs matching a specific state."""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT * FROM jobs WHERE state = ?", (state,))
        jobs = cursor.fetchall()
        # Convert list of sqlite3.Row to list of dict
        return [dict(job) for job in jobs]
    except sqlite3.Error as e:
        print(f"Database error getting jobs by state: {e}")
        return []
    finally:
        conn.close()


def retry_dlq_job(job_id):
    """Moves a 'dead' job back to 'pending' to be retried."""
    conn = get_db_connection()
    now = datetime.now(timezone.utc)
    try:
        # Reset attempts, state, and run_at
        cursor = conn.execute(
            """
            UPDATE jobs
            SET state      = 'pending',
                attempts   = 0,
                updated_at = ?,
                run_at     = NULL
            WHERE id = ?
              AND state = 'dead'
            """,
            (now, job_id)
        )
        
        if cursor.rowcount > 0:
            conn.commit()
            print(f"Job {job_id} has been re-queued from the DLQ.")
        else:
            conn.rollback()
            print(f"Error: Job {job_id} not found in DLQ (state='dead').")
    
    except sqlite3.Error as e:
        print(f"Database error retrying job {job_id}: {e}")
    finally:
        conn.close()


def release_job(job_id):
    """Resets a 'processing' job back to 'pending' on graceful shutdown."""
    conn = get_db_connection()
    now = datetime.now(timezone.utc)
    try:
        conn.execute(
            """
            UPDATE jobs
            SET state      = 'pending',
                updated_at = ?
            WHERE id = ?
              AND state = 'processing'
            """,
            (now, job_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error releasing job {job_id}: {e}")
    finally:
        conn.close()


def get_job_status_summary():
    """Gets a count of jobs grouped by state."""
    conn = get_db_connection()
    try:
        cursor = conn.execute(
            "SELECT state, COUNT(*) as count FROM jobs GROUP BY state"
        )
        rows = cursor.fetchall()
        # Return as a simple dict: {'pending': 5, 'completed': 10}
        return {row['state']: row['count'] for row in rows}
    except sqlite3.Error as e:
        print(f"Database error getting job summary: {e}")
        return {}
    finally:
        conn.close()


def mark_job_started(job_id):
    """Sets the started_at timestamp for a job."""
    conn = get_db_connection()
    now = datetime.now(timezone.utc)
    try:
        conn.execute(
            "UPDATE jobs SET started_at = ? WHERE id = ?", (now, job_id)
        )
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error marking job started: {e}")
    finally:
        conn.close()


def get_all_config():
    """Gets all key-value pairs from the config table."""
    conn = get_db_connection()
    try:
        cursor = conn.execute("SELECT key, value FROM config")
        return {row['key']: row['value'] for row in cursor.fetchall()}
    except sqlite3.Error as e:
        print(f"Database error getting all config: {e}")
        return {}
    finally:
        conn.close()


def delete_job(job_id):
    """Permanently deletes a job from the queue."""
    conn = get_db_connection()
    try:
        conn.execute("DELETE FROM jobs WHERE id = ?", (job_id,))
        conn.commit()
        print(f"DB: Deleted job {job_id}")
    except sqlite3.Error as e:
        print(f"Database error deleting job {job_id}: {e}")
    finally:
        conn.close()


def requeue_job(job_id):
    """Moves any 'failed' or 'dead' job back to 'pending'."""
    conn = get_db_connection()
    now = datetime.now(timezone.utc)
    try:
        # This is a more general-purpose "retry"
        cursor = conn.execute(
            """
            UPDATE jobs
            SET state      = 'pending',
                attempts   = 0,
                updated_at = ?,
                run_at     = NULL
            WHERE id = ?
              AND state IN ('dead', 'failed')
            """,
            (now, job_id)
        )
        if cursor.rowcount > 0:
            conn.commit()
            print(f"DB: Re-queued job {job_id}")
        else:
            conn.rollback()
            print(f"DB: Job {job_id} not found or not in a re-queueable state.")
    
    except sqlite3.Error as e:
        print(f"Database error re-queuing job {job_id}: {e}")
    finally:
        conn.close()
