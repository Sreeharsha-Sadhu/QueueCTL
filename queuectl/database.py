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
    
    # Set default values
    cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('max_retries', '3')")
    cursor.execute("INSERT OR IGNORE INTO config (key, value) VALUES ('backoff_base', '2')")
    
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
