# QueueCTL - CLI Background Job Queue

`queuectl` is a minimal, production-grade background job queue system built in Python. It is designed to manage background jobs with multiple worker processes, handle retries using exponential backoff, and maintain a Dead Letter Queue (DLQ) for permanently failed jobs.

This project was built as part of a backend developer internship assignment.

[Link to your CLI Demo Video]

## ✨ Core Features

* **CLI Interface:** All operations managed via a `click`-based CLI.
* **Persistent Storage:** Uses **SQLite** for robust, serverless job persistence.
* **Concurrent Workers:** Runs multiple worker processes using Python's `multiprocessing` module.
* **Race-Condition Safe:** Uses atomic database transactions (`BEGIN IMMEDIATE`) to ensure a job is only ever picked up by one worker.
* **Retry & Backoff:** Automatically retries failed jobs with configurable exponential backoff (`delay = base ^ attempts`).
* **Dead Letter Queue (DLQ):** Moves jobs to a `dead` state after exhausting retries, where they can be manually inspected and retried.
* **Graceful Shutdown:** Workers can be stopped gracefully (`queuectl worker stop` or `Ctrl+C`), allowing them to finish their current job before exiting.

---

## 🏛️ Architecture Overview

### 1. Job Lifecycle

A job moves through a simple state machine:

1.  **`pending`**: The initial state. A job waits in the queue to be picked up.
2.  **`processing`**: A worker has locked the job and is executing its command.
3.  **`completed`**: The job's command exited with code `0`.
4.  **`failed`**: The command exited with a non-zero code. The worker calculates the next retry time and sets the job's `run_at` timestamp.
5.  **`dead`**: The job has failed `max_retries` times and is moved to the DLQ.

### 2. Persistence

* An embedded **SQLite** database (`queue.db`) is used for all persistence.
* **Why SQLite?** It's serverless, file-based, requires zero setup, and provides robust ACID-compliant transactions and locking, which are essential for a job queue.
* The DB is set to `WAL` (Write-Ahead Logging) mode to improve concurrency.
* **Tables:**
    * `jobs`: Stores the job specification and state.
    * `config`: A simple key-value store for settings like `max_retries`.

### 3. Worker Logic

* **Concurrency:** The `worker start --count <N>` command spawns `N` independent Python processes.
* **Job Fetching:** The *most critical* piece of logic is the `fetch_and_lock_job` function. It works as follows:
    1.  `BEGIN IMMEDIATE TRANSACTION;` - This immediately acquires a database lock.
    2.  `SELECT` the next available job (`state='pending'` or `state='failed'` and `run_at <= NOW()`).
    3.  `UPDATE` the job's state to `processing`.
    4.  `COMMIT;`
    * Because this operation is wrapped in a transaction, it is **atomic**. No two workers can ever get the same job.
* **Execution:** Jobs are run using `subprocess.Popen` in `shell=True` mode, allowing them to execute shell commands.
* **Graceful Shutdown:** A `multiprocessing.Event` is shared between the main `start` command and all child workers. When `Ctrl+C` is pressed (or `worker stop` is run), the event is set.
    * **Idle workers** stop immediately.
    * **Busy workers** are in a non-blocking `poll()` loop, which allows them to finish their current job before exiting.

---

## 🚀 Setup & Installation

1.  **Clone the repository:**

    ```sh
    git clone [Your-Repo-URL]
    cd queuectl_project
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
    This installs dependencies (like `click`) and creates the `queuectl` command in your path.

    ```sh
    pip install -e .
    ```

4.  **Install `sqlite3` CLI (for Testing)**
    The Python application uses a built-in `sqlite3` module, but the **`test_scenarios.sh`** script (and any manual database checks) requires the `sqlite3` command-line tool.

      * **macOS:**
        `sqlite3` is typically pre-installed. If not, use Homebrew:

        ```sh
        brew install sqlite3
        ```

      * **Linux (Debian/Ubuntu):**

        ```sh
        sudo apt-get update && sudo apt-get install sqlite3
        ```

      * **Linux (Fedora/RHEL):**

        ```sh
        sudo dnf install sqlite3
        ```

      * **Windows:**
        Windows does not include `sqlite3` by default. The easiest way is to use a package manager:

        ```sh
        # If you use Scoop
        scoop install sqlite

        # If you use Chocolatey
        choco install sqlite
        ```

        **Manual Method (Windows):**

        1.  Go to the [SQLite Download Page](https://www.sqlite.org/download.html).
        2.  Download the **`sqlite-tools-win32-*.zip`** file.
        3.  Extract `sqlite3.exe` from the zip.
        4.  Place `sqlite3.exe` somewhere in your system's `PATH` (e.g., `C:\Windows\System32` or your Git `usr/bin` folder).

5.  **Initialize the database:**
    This creates the `queue.db` file and its tables.

    ```sh
    queuectl init
    ```
---

## ⚙️ Usage Examples

### 1. Configuration

Set the default max retries to 5.
```sh
queuectl config set max_retries 5
```

### 2. Enqueueing Jobs

*Note: PowerShell users must escape inner quotes with a backtick (`` ` ``). macOS/Linux users should use single/double quotes normally.*

```sh
# PowerShell
queuectl enqueue "{\`"id\`": \`"job1\`", \`"command\`": \`"echo Hello World\`"}"

# Windows (cmd.exe) - use 'timeout' for sleep
queuectl enqueue "{\"id\": \"job-win-sleep\", \"command\": \"timeout /t 5 /nobreak\"}"

# macOS/Linux - use 'sleep'
queuectl enqueue '{"id": "job-nix-sleep", "command": "sleep 5"}'
```

### 3. Running Workers

```sh
# Start 4 workers in the foreground
queuectl worker start --count 4

# Stop all running workers (from another terminal)
queuectl worker stop
```

### 4. Checking Status

```sh
# Get a live summary of all jobs and workers
queuectl status
```
**Example Output:**
```
--- Job Status ---
- Pending     : 2
- Processing  : 4
- Completed   : 12
- Dead        : 1
- Total       : 19

--- Worker Status ---
Active: 4 worker(s) running (PIDs: 123, 124, 125, 126)
```

### 5. Managing the DLQ

```sh
# Enqueue a job that will fail
queuectl enqueue "{\`"id\`": \`"bad-job\`", \`"command\`": \`"invalid-command\`"}"

# (Wait for it to fail 3 times and move to DLQ...)

# List jobs in the DLQ
queuectl dlq list

# Retry the failed job
queuectl dlq retry bad-job
```

---

## 🧪 Testing

A test script is provided to validate all core functionality. It simulates a "happy path," a "failure path," and a "concurrency path."

1.  Make sure your database is initialized (`queuectl init`).
2.  Make sure no workers are running (`queuectl worker stop`).
3.  Run the test script:

    ```sh
    # On macOS/Linux:
    chmod +x test_scenarios.sh
    ./test_scenarios.sh

    # On Windows:
    # You may need to run this in Git Bash or WSL, as it's a .sh file.
    # Alternatively, manually run the commands inside the script.
    bash test_scenarios.sh
    ```

## 🧠 Assumptions & Trade-offs

* **`shell=True`**: This is a potential security risk if the command string can be injected by a malicious user. For this assignment, it's assumed the enqueuer is a trusted source.
* **SQLite Concurrency**: While `WAL` mode and atomic transactions are robust, a dedicated queue (like RabbitMQ or Redis) would scale better under extreme write load. SQLite is perfect for a self-contained application.
* **Worker Management**: The PID file is a simple and effective way to manage worker processes. A more complex system might use a dedicated daemon or service manager.
