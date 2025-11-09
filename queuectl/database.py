# queuectl/database.py

import sqlite3
import os
from datetime import datetime, timezone

DATABASE_FILE = 'queue.db'


def get_db_connection():
    """Establishes a connection to the SQLite database."""
    # Ensure the DB file exists if we're trying to connect
    if not os.path.exists(DATABASE_FILE):
        if 'init' not in os.sys.argv:  # Avoid loop during init
            print(f"Error: Database file '{DATABASE_FILE}' not found.")
            print("Please run 'queuectl init' first.")
            exit(1)
    
    conn = sqlite3.connect(DATABASE_FILE, detect_types=sqlite3.PARSE_DECLTYPES)
    conn.row_factory = sqlite3.Row  # Access columns by name
    # Enable WAL mode for better concurrency
    conn.execute("PRAGMA journal_mode=WAL;")
    return conn


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
                       id          TEXT PRIMARY KEY,
                       command     TEXT      NOT NULL,
                       state       TEXT      NOT NULL DEFAULT 'pending',
                       attempts    INTEGER   NOT NULL DEFAULT 0,
                       max_retries INTEGER   NOT NULL DEFAULT 3,
                       run_at      TIMESTAMP,
                       created_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                       updated_at  TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
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


def create_job(job_id, command, max_retries_override=None):
    """Adds a new job to the queue."""
    conn = get_db_connection()
    
    try:
        # Use override or get default from config
        if max_retries_override is None:
            default_retries = get_config('max_retries', 3)
            max_retries = int(default_retries)
        else:
            max_retries = int(max_retries_override)
        
        now = datetime.now(timezone.utc)
        
        conn.execute(
            """
            INSERT INTO jobs (id, command, max_retries, created_at, updated_at, state, attempts)
            VALUES (?, ?, ?, ?, ?, 'pending', 0)
            """,
            (job_id, command, max_retries, now, now)
        )
        conn.commit()
        print(f"Successfully enqueued job: {job_id}")
    
    except sqlite3.IntegrityError:
        print(f"Error: Job with ID '{job_id}' already exists.")
    except Exception as e:
        print(f"Error enqueuing job: {e}")
    finally:
        conn.close()


# queuectl/database.py
# ... (add these functions to your existing file) ...

def fetch_and_lock_job():
    """
    Atomically fetches the next available job and marks it as 'processing'.

    This function uses 'BEGIN IMMEDIATE' to acquire a database lock
    to prevent multiple workers from grabbing the same job.
    """
    conn = get_db_connection()
    try:
        # 'BEGIN IMMEDIATE' acquires a RESERVED lock immediately,
        # which is upgraded to EXCLUSIVE on the first write (the UPDATE).
        # This blocks other writers, ensuring atomicity.
        conn.execute("BEGIN IMMEDIATE")
        
        now = datetime.now(timezone.utc)
        
        # Fetch a job that is 'pending' OR 'failed' and ready for retry
        cursor = conn.execute(
            """
            SELECT *
            FROM jobs
            WHERE (state = 'pending' OR (state = 'failed' AND run_at <= ?))
            ORDER BY created_at ASC
            LIMIT 1
            """,
            (now,)
        )
        job = cursor.fetchone()
        
        if job:
            # We found a job, lock it by setting its state
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
            return dict(job)  # Return as a standard dict
        else:
            # No job found, just commit the empty transaction
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
    Finalizes a job by marking it 'completed' or handling failure.

    NOTE: In Stage 1, we only handle the 'completed' state.
    Stage 2 will add the 'failed' and 'dead' logic.
    """
    conn = get_db_connection()
    now = datetime.now(timezone.utc)
    
    try:
        if success:
            conn.execute(
                """
                UPDATE jobs
                SET state      = 'completed',
                    updated_at = ?
                WHERE id = ?
                """,
                (now, job_id)
            )
        else:
            # --- STAGE 2 PREVIEW ---
            # This is where retry/DLQ logic will go.
            # For now, we'll just log it.
            print(f"Job {job_id} failed. (Retry logic not yet implemented)")
            # In a real (but simple) Stage 1, we could just mark it 'failed'
            # But we will build the full logic in the next stage.
            # For now, we will just set it to 'failed' temporarily.
            # This will be overwritten by Stage 2.
            conn.execute(
                """
                UPDATE jobs
                SET state      = 'failed',
                    updated_at = ?
                WHERE id = ?
                """,
                (now, job_id)
            )
        
        conn.commit()
    except sqlite3.Error as e:
        print(f"Database error finalizing job {job_id}: {e}")
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
