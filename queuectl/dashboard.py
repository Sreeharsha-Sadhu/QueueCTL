import json
import os
import subprocess
from flask import Flask, render_template, redirect, url_for, request, jsonify
from . import database
from .config import PID_FILE, LOG_DIR

app = Flask(__name__)


def get_db():
    """Helper to get a DB connection for the web request."""
    db = database.get_db_connection()
    return db


def get_worker_status():
    """Checks the .pid file to see if workers are active."""
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                pids = [pid for pid in f.read().splitlines() if pid.strip()]
            if pids:
                return f"Active: Main process (PID: {', '.join(pids)})"
            else:
                return "Inactive: PID file is empty."
        except Exception as e:
            return f"Error: {e}"
    return "Inactive: No PID file."


@app.route("/api/worker-status")
def api_worker_status():
    """Returns the worker status as JSON for live polling."""
    status = get_worker_status()
    return jsonify({"status": status, "is_active": status.startswith("Active")})


@app.route("/")
def index():
    """Main dashboard page."""
    db = get_db()
    
    # Get summary
    summary_rows = db.execute(
        "SELECT state, COUNT(*) as count FROM jobs GROUP BY state"
    ).fetchall()
    summary = {row['state']: row['count'] for row in summary_rows}
    
    # Get Config and Worker Status
    config = database.get_all_config()
    worker_status = get_worker_status()
    
    # Get 25 most recent jobs from DLQ
    dlq_jobs = db.execute(
        "SELECT * FROM jobs WHERE state = 'dead' ORDER BY updated_at DESC LIMIT 25"
    ).fetchall()
    
    # Get 25 most recent 'in-flight' jobs
    inflight_jobs = db.execute(
        "SELECT * FROM jobs WHERE state IN ('processing', 'failed', 'scheduled') ORDER BY updated_at DESC LIMIT 25"
    ).fetchall()
    
    # Get 25 most recent completed jobs
    completed_jobs = db.execute(
        "SELECT * FROM jobs WHERE state = 'completed' ORDER BY completed_at DESC LIMIT 25"
    ).fetchall()
    
    db.close()
    
    return render_template(
        'dashboard.html',
        summary=summary,
        config=config,
        worker_status=worker_status,  # Pass initial status
        dlq_jobs=dlq_jobs,
        inflight_jobs=inflight_jobs,
        completed_jobs=completed_jobs
    )


# --- Worker Control Routes ---
@app.route("/worker/start", methods=['POST'])
def start_workers():
    """
    Launches the 'queuectl worker start' command as a
    separate background process.
    """
    if not get_worker_status().startswith("Active"):
        count = request.form.get('count', '2')
        try:
            print(f"WEB: Launching 'queuectl worker start --count {count}'...")
            subprocess.Popen(['queuectl', 'worker', 'start', '--count', count])
        except Exception as e:
            print(f"WEB ERROR: Failed to start workers: {e}")
    
    return redirect(url_for('index'))


@app.route("/worker/stop", methods=['POST'])
def stop_workers():
    """
    Runs the 'queuectl worker stop' command to gracefully
    shut down the background workers.
    """
    try:
        print(f"WEB: Launching 'queuectl worker stop'...")
        subprocess.run(['queuectl', 'worker', 'stop'])
    except Exception as e:
        print(f"WEB ERROR: Failed to stop workers: {e}")
    
    return redirect(url_for('index'))


# --- Job & Config Routes ---
@app.route("/enqueue", methods=['POST'])
def enqueue_job():
    """Endpoint to enqueue a new job from a form."""
    job_json_str = request.form.get('job_json')
    try:
        job_data = json.loads(job_json_str)
        
        job_id = job_data.get('id')
        command = job_data.get('command')
        if not job_id or not command:
            raise ValueError("JSON must include 'id' and 'command'")
        
        database.create_job(
            job_id=job_id,
            command=command,
            max_retries_override=job_data.get('max_retries'),
            run_at_str=job_data.get('run_at'),
            priority=job_data.get('priority', 0),
            timeout=job_data.get('timeout')
        )
    except Exception as e:
        print(f"WEB ERROR: Failed to enqueue job: {e}")
    
    return redirect(url_for('index'))


@app.route("/config", methods=['POST'])
def update_config():
    """Endpoint to update config values."""
    try:
        retries = request.form.get('max_retries')
        backoff = request.form.get('backoff_base')
        
        if retries:
            database.set_config('max_retries', retries)
        if backoff:
            database.set_config('backoff_base', backoff)
    
    except Exception as e:
        print(f"WEB ERROR: Failed to update config: {e}")
    
    return redirect(url_for('index'))


@app.route("/job/requeue/<job_id>")
def requeue_job_route(job_id):
    """API endpoint to re-queue a 'failed' or 'dead' job."""
    print(f"WEB: Re-queuing job {job_id}...")
    database.requeue_job(job_id)
    return redirect(url_for('index'))


@app.route("/job/delete/<job_id>")
def delete_job_route(job_id):
    """API endpoint to delete any job."""
    print(f"WEB: Deleting job {job_id}...")
    database.delete_job(job_id)
    return redirect(url_for('index'))


@app.route("/job/logs/<job_id>")
def view_logs(job_id):
    """Page to display stdout and stderr for a job."""
    stdout_log = os.path.join(LOG_DIR, f"{job_id}.out.log")
    stderr_log = os.path.join(LOG_DIR, f"{job_id}.err.log")
    
    stdout_content = "Log not found or empty."
    stderr_content = "Log not found or empty."
    
    try:
        if os.path.exists(stdout_log):
            with open(stdout_log, 'r') as f:
                stdout_content = f.read()
        if os.path.exists(stderr_log):
            with open(stderr_log, 'r') as f:
                stderr_content = f.read()
    except Exception as e:
        stdout_content = f"Error reading log: {e}"
        stderr_content = f"Error reading log: {e}"
    
    return render_template(
        'logs.html',
        job_id=job_id,
        stdout=stdout_content,
        stderr=stderr_content
    )


def run_web_server():
    """Starts the Flask web server."""
    print("Starting Flask web server on http://127.0.0.1:5000")
    print("Press Ctrl+C to stop the web server.")
    app.run(debug=True, port=5000)
