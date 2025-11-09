# QueueCTL: A CLI & Web-Based Background Job Queue

`queuectl` is a minimal, production-grade background job queue system built in Python. It is designed to manage background jobs with multiple worker processes, handle retries using exponential backoff, and maintain a Dead Letter Queue (DLQ) for permanently failed jobs.

It features a full-featured CLI for system administration and a live-monitoring web dashboard to manage the queue from a browser.

This project was built as part of a backend developer internship assignment.

[Link to your CLI Demo Video]

## Core Features

  * **CLI Interface:** All operations are managed via a `click`-based Command Line Interface.
  * **Live Web Dashboard:** A `Flask`-based dashboard provides an interface to monitor, enqueue, re-queue, and delete jobs. It includes live-updating worker status and start/stop controls.
  * **Persistent Storage:** Uses **SQLite** for robust, serverless job persistence with WAL (Write-Ahead Logging) mode enabled for high concurrency.
  * **Concurrent Workers:** Runs multiple worker processes using Python's `multiprocessing` module.
  * **Race-Condition Safe:** Employs atomic database transactions (`BEGIN IMMEDIATE`) to ensure a job is only ever picked up by one worker.
  * **Retry & Backoff:** Automatically retries failed jobs with configurable exponential backoff (`delay = base ^ attempts`).
  * **Dead Letter Queue (DLQ):** Moves jobs to a `dead` state after exhausting retries, where they can be manually inspected and retried.
  * **Job Scheduling:** Enqueue jobs to run at a specific time in the future using an `run_at` ISO 8601 timestamp.
  * **Priority Queues:** Higher-priority jobs are processed before lower-priority jobs.
  * **Job Timeouts:** Automatically fails jobs that run longer than a specified `timeout`.
  * **Persistent Logging:** `stdout` and `stderr` for all jobs are saved to the `logs/` directory for debugging.
  * **Graceful Shutdown:** Workers can be stopped gracefully (`queuectl worker stop` or `Ctrl+C`), allowing them to finish their current job before exiting.

-----

## Architecture Overview

### 1\. Job Lifecycle

A job moves through a simple, well-defined state machine:

1.  **`scheduled`**: (Optional) The job has a `run_at` timestamp in the future.
2.  **`pending`**: The job is waiting to be picked up by a worker.
3.  **`processing`**: A worker has locked the job and is executing its command.
4.  **`completed`**: The job's command exited with code `0`.
5.  **`failed`**: The command exited with a non-zero code. The worker calculates the next retry time and sets the job's `run_at` timestamp.
6.  **`dead`**: The job has failed `max_retries` times and is moved to the DLQ.

### 2\. Persistence

  * An embedded **SQLite** database (`queue.db`) is used for all persistence.
  * **Rationale:** SQLite is serverless, file-based, requires zero setup, and provides robust ACID-compliant transactions (with a `timeout` for locking), which are essential for a job queue.
  * The DB is set to `WAL` (Write-Ahead Logging) mode to improve concurrency.
  * **Tables:**
      * `jobs`: Stores the job specification and state (including `priority`, `timeout`, `run_at`, etc.).
      * `config`: A simple key-value store for system settings.

### 3\. Worker Logic

  * **Concurrency:** The `worker start --count <N>` command spawns `N` independent Python processes.
  * **Job Fetching:** The `fetch_and_lock_job` function atomically selects the next available job, ordered by `priority DESC, created_at ASC`.
  * **Execution:** Jobs are run using `subprocess.Popen` in `shell=True` mode. `stdout` and `stderr` are redirected to files in the `logs/` directory.
  * **State Machine:** The worker runs as a non-blocking state machine, polling `subprocess.poll()` and checking for job timeouts, allowing it to respond to shutdown signals instantly.

### 4\. Web Dashboard

  * A lightweight `Flask` server provides a web UI. It reads from and writes to the *same* `queue.db` file, allowing real-time monitoring and management.
  * It features an API endpoint (`/api/worker-status`) that polls the `.queuectl.pids` file, enabling live status updates on the dashboard.
  * It can start/stop workers by launching `queuectl worker start/stop` commands as detached subprocesses.

-----

## Setup & Installation

1.  **Clone the repository:**

    ```sh
    git clone https://github.com/Sreeharsha-Sadhu/QueueCTL.git
    cd QueueCTL
    ```

2.  **Create and activate a virtual environment:**

    ```sh
    python -m venv venv
    # On macOS/Linux:
    source venv/bin/activate
    # On Windows (PowerShell):
    .\venv\Scripts\Activate.ps1
    ```

3.  **Install the package in editable mode:**
    This installs dependencies (like `click` and `flask`) and creates the `queuectl` command in your path. (Ensure `flask` is in your `setup.py` or `requirements.txt`).

    ```sh
    pip install -e .
    # If Flask isn't in setup.py, install it manually:
    pip install flask
    ```

4.  **Install `sqlite3` CLI (for Testing)**
    The Python application uses a built-in `sqlite3` module, but the **`test_scenarios.sh`** script (and any manual database checks) requires the `sqlite3` command-line tool.

      * **macOS:** `brew install sqlite3`
      * **Linux (Debian/Ubuntu):** `sudo apt-get update && sudo apt-get install sqlite3`
      * **Windows:** `scoop install sqlite` or `choco install sqlite`

5.  **Initialize the database:**
    This creates the `queue.db` file and its tables.

    ```sh
    queuectl init
    ```

6.  **Create Log Directory:**
    This step is required for the workers to save job output.

    ```sh
    mkdir logs
    ```

-----

## Usage Examples

### 1\. Running the Web Dashboard (Recommended)

This is the easiest way to use the system.

```sh
queuectl web
```

Then open `http://127.0.0.1:5000` in your browser. From here, you can start/stop workers, enqueue jobs, and manage the DLQ.

### 2\. Running Workers via CLI

```sh
# Start 4 workers in the foreground
queuectl worker start --count 4

# Stop all running workers (from another terminal)
queuectl worker stop
```

### 3\. Enqueueing Jobs via CLI

*Note: Quoting is shell-specific. These examples are for PowerShell.*

**Basic Job:**

```sh
queuectl enqueue "{\`"id\`": \`"job1\`", \`"command\`": \`"echo Hello World\`"}"
```

**Advanced Job (Scheduled, High-Priority, Timeout):**

```powershell
# This job has priority 10, will run after the specified time,
# and will be killed if it takes longer than 60 seconds.
$run_at = [System.DateTime]::UtcNow.AddMinutes(5).ToString("o")
queuectl enqueue "{\`"id\`": \`"job-adv\`", \`"command\`": \`"ping -n 30 127.0.0.1 > NUL\`", \`"priority\`": 10, \`"timeout\`": 60, \`"run_at\`": \`"$run_at\`"}"
```

### 4\. Viewing Logs via CLI

```sh
# View the stdout for 'job1'
queuectl logs job1

# View the stderr for 'job1'
queuectl logs job1 --stderr
```

### 5\. Other CLI Commands

```sh
# Set the default max retries to 5
queuectl config set max_retries 5

# Get a live summary of all jobs and workers
queuectl status

# List all pending jobs
queuectl list --state pending

# List jobs in the DLQ
queuectl dlq list

# Retry a failed job from the DLQ
queuectl dlq retry bad-job
```

-----

## Testing

A comprehensive, cross-platform test script is provided to validate all core functionality.

1.  Make sure your database is initialized (`queuectl init`).
2.  Make sure no workers are running (the script handles this, but `queuectl worker stop` is a good manual first step).
3.  Run the test script:
    ```sh
    # On macOS/Linux:
    chmod +x test_scenarios.sh
    ./test_scenarios.sh

    # On Windows (must be run from Git Bash):
    bash test_scenarios.sh
    ```

## Assumptions & Trade-offs

  * **`shell=True`**: This is a potential security risk if the command string can be injected by a malicious user. For this assignment, it is assumed the enqueuer is a trusted source.
  * **SQLite Concurrency**: While `WAL` mode and a `timeout=10.0` make SQLite robust, a dedicated queue (like RabbitMQ or Redis) would scale better under extreme write load.
  * **Web Server Security**: The Flask server runs in `debug=True` mode, which is not suitable for an internet-facing production environment. It is designed for local monitoring.
  * **Windows Shutdown**: On Windows, a "graceful" shutdown is not always possible. The `stop` command uses `taskkill /T /F` (force kill) to ensure the process tree is reliably cleaned up, which is a trade-off for robustness. This behavior is accounted for in the test script.
