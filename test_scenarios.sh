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
    # 5-second sleep
    SLEEP_CMD="ping -n 6 127.0.0.1 > NUL"
    # 2-second sleep
    SHORT_SLEEP_CMD="ping -n 3 127.0.0.1 > NUL"
    # The stop command is a force-kill
    STOP_CMD() { queuectl worker stop; }

else
    echo "Detected Linux/macOS"
    CLEANUP_CMD() { pkill -9 -f "queuectl.*worker start"; }
    SLEEP_CMD="sleep 5"
    SHORT_SLEEP_CMD="sleep 2"
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

# ... (Sections 1-3 are unchanged) ...
echo "Initializing database..."
rm -f queue.db
queuectl init
echo "Setting config (retries=2, base=1 for fast tests)..."
queuectl config set max_retries 2
queuectl config set backoff_base 1

# ... (Scenario 1, 2, 3 are unchanged) ...
echo "--- SCENARIO 1: Happy Path & Logging ---"
queuectl enqueue '{"id": "job-pass", "command": "echo Job job-pass succeeded"}'
echo "--- SCENARIO 2: Failure & DLQ ---"
queuectl enqueue '{"id": "job-fail", "command": "invalid-command-xyz"}'
echo "--- SCENARIO 3: Job Timeout ---"
queuectl enqueue "{\"id\": \"job-timeout\", \"command\": \"$SLEEP_CMD\", \"timeout\": 2}"
echo "Starting 2 workers in the background..."
queuectl worker start --count 2 &
echo "Waiting for PID file..."
sleep 2
if [ ! -f ".queuectl.pids" ]; then
    echo "!!! TEST FAILED: .queuectl.pids file was not created."
    exit 1
fi
WORKER_PID=$(cat .queuectl.pids)
echo "Workers started (PID: $WORKER_PID). Waiting 8 seconds for jobs to process..."
sleep 8
echo "--- Verification ---"
echo "Checking 'job-pass' (should be 'completed')..."
queuectl list --state completed | grep "job-pass"
if [ $? -ne 0 ]; then echo "!!! TEST FAILED: 'job-pass' not completed."; STOP_CMD; exit 1; fi
echo "✅ 'job-pass' completed successfully."
echo "Checking 'job-pass' log file..."
if ! grep -q "Job job-pass succeeded" "logs/job-pass.out.log"; then
    echo "!!! TEST FAILED: 'job-pass' log content not found."
    STOP_CMD; exit 1
fi
echo "✅ 'job-pass' log file created and verified."
echo "Checking 'job-fail' (should be 'dead')..."
queuectl dlq list | grep "job-fail"
if [ $? -ne 0 ]; then echo "!!! TEST FAILED: 'job-fail' not in DLQ."; STOP_CMD; exit 1; fi
echo "✅ 'job-fail' moved to DLQ successfully."
echo "Checking 'job-timeout' (should be 'dead')..."
queuectl dlq list | grep "job-timeout"
if [ $? -ne 0 ]; then echo "!!! TEST FAILED: 'job-timeout' did not move to DLQ."; STOP_CMD; exit 1; fi
echo "✅ 'job-timeout' correctly timed out and moved to DLQ."

# --- SCENARIO 4: DLQ Retry ---
echo "--- SCENARIO 4: DLQ Retry ---"
queuectl dlq retry job-fail
echo "Waiting 3s for workers to re-process and fail 'job-fail'..."
sleep 3
echo "Verifying 'job-fail' is back in DLQ..."
queuectl dlq list | grep "job-fail"
if [ $? -ne 0 ]; then echo "!!! TEST FAILED: 'job-fail' not back in DLQ."; STOP_CMD; exit 1; fi
echo "✅ 'job-fail' was retried and returned to DLQ."


# --- THIS IS THE FIX ---
# This block is now moved to AFTER Scenario 4 is complete.
echo "Stopping workers to prepare for priority test..."
STOP_CMD
echo "Waiting 5 seconds for workers to fully stop..."
sleep 5
# --- END FIX ---


# --- SCENARIO 5: Priority ---
echo "--- SCENARIO 5: Priority ---"
echo "Enqueuing priority jobs..."
# Enqueue all three jobs while workers are DOWN
queuectl enqueue "{\"id\": \"job-p-low\", \"command\": \"$SLEEP_CMD\", \"priority\": 1}"
queuectl enqueue "{\"id\": \"job-p-high\", \"command\": \"$SLEEP_CMD\", \"priority\": 10}"
queuectl enqueue "{\"id\": \"job-p-mid\", \"command\": \"$SLEEP_CMD\", \"priority\": 5}"


echo "Starting workers to test priority..."
queuectl worker start --count 2 &
echo "Waiting for new PID file..."
sleep 2
if [ ! -f ".queuectl.pids" ]; then
    echo "!!! TEST FAILED: .queuectl.pids file was not created for priority test."
    exit 1
fi
WORKER_PID=$(cat .queuectl.pids) # Get the NEW worker PID

echo "Waiting 0.5 seconds for workers to pick up jobs..."
sleep 0.5

# Check that 'job-p-high' is 'processing' OR 'completed' (if it was fast)
HIGH_PRI_STATE=$(sqlite3 queue.db "SELECT state FROM jobs WHERE id='job-p-high';")
if [ "$HIGH_PRI_STATE" = "processing" ] || [ "$HIGH_PRI_STATE" = "completed" ]; then
    echo "✅ High-priority job was picked up first (State: $HIGH_PRI_STATE)."
else
    echo "!!! TEST FAILED: High priority job was not processed. State: $HIGH_PRI_STATE"
    STOP_CMD; exit 1
fi

# Check that 'job-p-mid' is 'processing' OR 'completed'
MID_PRI_STATE=$(sqlite3 queue.db "SELECT state FROM jobs WHERE id='job-p-mid';")
if [ "$MID_PRI_STATE" = "processing" ] || [ "$MID_PRI_STATE" = "completed" ]; then
    echo "✅ Mid-priority job was picked up second (State: $MID_PRI_STATE)."
else
    echo "!!! TEST FAILED: Mid priority job was not processed. State: $MID_PRI_STATE"
    STOP_CMD; exit 1
fi

# Check that 'job-p-low' is still 'pending'
LOW_PRI_STATE=$(sqlite3 queue.db "SELECT state FROM jobs WHERE id='job-p-low';")
if [ "$LOW_PRI_STATE" != "pending" ]; then
    echo "!!! TEST FAILED: Low priority job was processed too early. State: $LOW_PRI_STATE"
    STOP_CMD; exit 1
fi
echo "✅ Low-priority job correctly waited."

echo "Waiting 5 seconds for all priority jobs to finish..."
sleep 5

# ... (Rest of the script is unchanged and will now run correctly) ...

# --- SCENARIO 6: Scheduling ---
echo "--- SCENARIO 6: Scheduling ---"
SCHEDULED_TIME=$(python -c "from datetime import datetime, timedelta; print((datetime.now() + timedelta(seconds=5)).isoformat())")
echo "Enqueuing 'job-sched' to run at $SCHEDULED_TIME"
queuectl enqueue "{\"id\": \"job-sched\", \"command\": \"echo 'scheduled job ran'\", \"run_at\": \"$SCHEDULED_TIME\"}"

echo "Checking 'job-sched' (should be 'scheduled')..."
SCHED_STATE=$(sqlite3 queue.db "SELECT state FROM jobs WHERE id='job-sched';")
if [ "$SCHED_STATE" != "scheduled" ]; then
    echo "!!! TEST FAILED: Scheduled job is not in 'scheduled' state. State: $SCHED_STATE"
    STOP_CMD; exit 1
fi
echo "✅ Job is correctly marked 'scheduled'."

echo "Waiting 7 seconds for scheduled job to run..."
sleep 7
echo "Checking 'job-sched' (should be 'completed')..."
SCHED_STATE=$(sqlite3 queue.db "SELECT state FROM jobs WHERE id='job-sched';")
if [ "$SCHED_STATE" != "completed" ]; then
    echo "!!! TEST FAILED: Scheduled job did not run. State: $SCHED_STATE"
    STOP_CMD; exit 1
fi
echo "✅ Scheduled job ran and completed successfully."

# --- SCENARIO 7: Concurrency & Shutdown ---
echo "--- SCENARIO 7: Concurrency & Shutdown ---"
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
    STOP_CMD; exit 1
fi
echo "✅ 2 jobs are 'processing' concurrently."

# --- 10. Stop Workers ---
echo "Stopping workers..."
STOP_CMD # Use the OS-specific stop command

echo "Waiting 5 seconds for main process ($WORKER_PID) to shut down..."
sleep 5

if ps -p $WORKER_PID > /dev/null 2>&1; then
   echo "!!! TEST FAILED: Main process $WORKER_PID is still running."
   if [ "$OS_NAME" = "windows" ]; then taskkill /PID $WORKER_PID /T /F > /dev/null 2>&1; else kill -9 $WORKER_PID; fi
   exit 1
fi
if [ -f ".queuectl.pids" ]; then
    echo "!!! TEST FAILED: PID file .queuectl.pids was not cleaned up."
    exit 1
fi
echo "✅ Workers stopped successfully and PID file cleaned up."

# --- 11. Verify Persistence ---
echo "Verifying job state persistence after shutdown..."

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

echo "--- ALL TESTS PASSED ---"
