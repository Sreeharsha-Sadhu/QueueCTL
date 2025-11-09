# queuectl/dashboard.py

import json
from flask import Flask, render_template, redirect, url_for
from . import database

# Create a 'templates' folder in your project root
# and add a file 'dashboard.html' (see step 3)

app = Flask(__name__)


def get_db():
    """Helper to get a DB connection for the web request."""
    # We're just reading, so a new connection per request is fine.
    db = database.get_db_connection()
    return db


@app.route("/")
def index():
    """Main dashboard page."""
    db = get_db()
    
    # Get summary
    summary_rows = db.execute(
        "SELECT state, COUNT(*) as count FROM jobs GROUP BY state"
    ).fetchall()
    summary = {row['state']: row['count'] for row in summary_rows}
    
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
        dlq_jobs=dlq_jobs,
        inflight_jobs=inflight_jobs,
        completed_jobs=completed_jobs
    )


@app.route("/job/retry/<job_id>")
def retry_job(job_id):
    """API endpoint to retry a job from the DLQ."""
    print(f"WEB: Retrying job {job_id} from DLQ...")
    database.retry_dlq_job(job_id)
    return redirect(url_for('index'))


def run_web_server():
    """Starts the Flask web server."""
    print("Starting Flask web server on http://127.0.0.1:5000")
    print("Press Ctrl+C to stop the web server.")
    app.run(debug=True, port=5000)
