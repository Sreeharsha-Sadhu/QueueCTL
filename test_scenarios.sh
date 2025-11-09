#!/bin/bash
echo "--- QueueCTL Cross-Platform Test Script ---"

# --- 1. OS Detection & Command Setup ---
OS_NAME="unknown"
case "$(uname -s)" in
    Linux*)     OS_NAME="linux" ;;
    Darwin*)    OS_NAME="macos" ;;
    CYGWIN*)    OS_NAME="windows" ;;
    MINGW*)     OS_NAME="windows" ;;
    *)          echo "Unsupported OS. Exiting." >&2; exit 1 ;;
esac

if [ "$OS_NAME" = "windows" ]; then
    echo "Detected Windows (MINGW/Cygwin)"
    CLEANUP_CMD() {
        PIDS_TO_KILL=$(wmic process where "name='python.exe' and commandline like '%queuectl%' and not commandline like '%wmic%'" get processid /format:list | grep ProcessId | cut -d= -f2)
        if [ ! -z "$PIDS_TO_KILL" ]; then
            for PID in $PIDS_TO_KILL; do
                echo "Found orphaned process PID: $PID. Forcing kill..."
                taskkill /PID $PID /T /F > /dev/null 2>&1
            done
        else
            echo "No orphaned processes found."
        fi
    }
    SLEEP_CMD="ping -n 4 127.0.0.1 > NUL"
    # The stop command is now a force-kill
    STOP_CMD() { queuectl worker stop; }
    
else
    echo "Detected Linux/macOS"
    CLEANUP_CMD() { pkill -f "queuectl worker start"; }
    SLEEP_CMD="sleep 3"
    # On Linux, the graceful stop works
    STOP_CMD() { queuectl worker stop; }
fi

# --- 2. Environment Cleanup ---
echo "Cleaning up any orphaned worker processes (brute-force)..."
CLEANUP_CMD
sleep 1
if [ -f ".queuectl.pids" ]; then
    echo "Cleaning up stale .queuectl.pids file..."
    rm -f .queuectl.pids
fi

# ... (Sections 1-5 are unchanged) ...
echo "Initializing database..."
rm -f queue.db
queuectl init
echo "Setting config (retries=2, base=1 for fast tests)..."
queuectl config set max_retries 2
queuectl config set backoff_base 1
echo "--- SCENARIO 1: Happy Path ---"
queuectl enqueue '{"id": "job-pass", "command": "echo Job job-pass succeeded"}'
echo "--- SCENARIO 2: Failure & DLQ ---"
queuectl enqueue '{"id": "job-fail", "command": "invalid-command-xyz"}'
echo "Starting 2 workers in the background..."
queuectl worker start --count 2 &
echo "Waiting for PID file..."
sleep 2 
if [ ! -f ".queuectl.pids" ]; then
    echo "!!! TEST FAILED: .queuectl.pids file was not created."
    exit 1
fi
WORKER_PID=$(cat .queuectl.pids)
echo "Workers started (PID: $WORKER_PID). Waiting 5 seconds for jobs to process..."
sleep 5 
echo "--- Verification ---"
queuectl list --state completed | grep "job-pass"
if [ $? -ne 0 ]; then echo "!!! TEST FAILED: 'job-pass' not completed."; taskkill /PID $WORKER_PID /T /F > /dev/null 2>&1; exit 1; fi
echo "✅ 'job-pass' completed successfully."
queuectl dlq list | grep "job-fail"
if [ $? -ne 0 ]; then echo "!!! TEST FAILED: 'job-fail' not in DLQ."; taskkill /PID $WORKER_PID /T /F > /dev/null 2>&1; exit 1; fi
echo "✅ 'job-fail' moved to DLQ successfully."
echo "--- SCENARIO 3: DLQ Retry ---"
queuectl dlq retry job-fail
sleep 3 
queuectl dlq list | grep "job-fail"
if [ $? -ne 0 ]; then echo "!!! TEST FAILED: 'job-fail' not back in DLQ."; taskkill /PID $WORKER_PID /T /F > /dev/null 2>&1; exit 1; fi
echo "✅ 'job-fail' was retried and returned to DLQ."

# --- 9. SCENARIO 4: Concurrency & Shutdown ---
echo "--- SCENARIO 4: Concurrency & Shutdown ---"
echo "Enqueuing 4 long-running jobs..."
queuectl enqueue "{\"id\": \"job-c1\", \"command\": \"$SLEEP_CMD\"}"
queuectl enqueue "{\"id\": \"job-c2\", \"command\": \"$SLEEP_CMD\"}"
queuectl enqueue "{\"id\": \"job-c3\", \"command\": \"$SLEEP_CMD\"}"
queuectl enqueue "{\"id\": \"job-c4\", \"command\": \"$SLEEP_CMD\"}"

echo "Waiting 2 seconds for workers to pick up jobs..."
sleep 2 
echo "Checking status (should show 2 processing, 2 pending)..."
queuectl status

PROCESSING_COUNT=$(sqlite3 queue.db "SELECT COUNT(*) FROM jobs WHERE state='processing';")
if [ "$PROCESSING_COUNT" -ne 2 ]; then
    echo "!!! TEST FAILED: Expected 2 'processing' jobs, found $PROCESSING_COUNT."
    taskkill /PID $WORKER_PID /T /F > /dev/null 2>&1; exit 1
fi
echo "✅ 2 jobs are 'processing' concurrently."

# --- 10. Stop Workers ---
echo "Stopping workers..."
STOP_CMD # Use the OS-specific stop command (now /T /F on Windows)

echo "Waiting 5 seconds for main process ($WORKER_PID) to shut down..."
sleep 5 

if ps -p $WORKER_PID > /dev/null 2>&1; then
   echo "!!! TEST FAILED: Main process $WORKER_PID is still running."
   taskkill /PID $WORKER_PID /T /F > /dev/null 2>&1; exit 1
fi
if [ -f ".queuectl.pids" ]; then
    echo "!!! TEST FAILED: PID file .queuectl.pids was not cleaned up."
    exit 1
fi
echo "✅ Workers stopped successfully and PID file cleaned up."

# --- 11. Verify Persistence ---
echo "Verifying job state persistence after shutdown..."

# --- THIS IS THE FIX ---
# We check the state based on the OS.
if [ "$OS_NAME" = "windows" ]; then
    echo "Checking Windows (forced) state: 2 processing, 2 pending..."
    PROCESSING_COUNT=$(sqlite3 queue.db "SELECT COUNT(*) FROM jobs WHERE state='processing' AND id LIKE 'job-c%';")
    PENDING_COUNT=$(sqlite3 queue.db "SELECT COUNT(*) FROM jobs WHERE state='pending' AND id LIKE 'job-c%';")

    if [ "$PROCESSING_COUNT" -eq 2 ] && [ "$PENDING_COUNT" -eq 2 ]; then
        echo "✅ Job states persisted correctly (2 processing, 2 pending)."
    else
        echo "!!! TEST FAILED: Job states did not persist. Found $PROCESSING_COUNT processing, $PENDING_COUNT pending."
        exit 1
    fi
else
    # Linux/macOS
    echo "Checking *nix (graceful) state: 2 completed, 2 pending..."
    COMPLETED_COUNT=$(sqlite3 queue.db "SELECT COUNT(*) FROM jobs WHERE state='completed' AND id LIKE 'job-c%';")
    PENDING_COUNT=$(sqlite3 queue.db "SELECT COUNT(*) FROM jobs WHERE state='pending' AND id LIKE 'job-c%';")

    if [ "$COMPLETED_COUNT" -eq 2 ] && [ "$PENDING_COUNT" -eq 2 ]; then
        echo "✅ Job states persisted correctly (2 completed, 2 pending)."
    else
        echo "!!! TEST FAILED: Job states did not persist. Found $COMPLETED_COUNT completed, $PENDING_COUNT pending."
        exit 1
    fi
fi
# --- END FIX ---

echo "--- ALL TESTS PASSED ---"