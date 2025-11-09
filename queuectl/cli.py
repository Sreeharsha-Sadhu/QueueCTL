# queuectl/cli.py
import os
import subprocess

import click
import json
import signal
import multiprocessing
import time
from . import database
from . import worker as worker_module
from . import dashboard
from .config import PID_FILE, LOG_DIR


@click.group()
def cli():
    """
    queuectl - A CLI for managing background jobs.
    """
    pass


@cli.command()
def init():
    """
    Initializes the queue database and tables.
    """
    database.init_db()


@cli.command()
def status():
    """
    Show a summary of all job states and active workers.
    """
    click.echo("--- Job Status ---")
    try:
        summary = database.get_job_status_summary()
        if not summary:
            click.echo("No jobs in the queue.")
        else:
            total = 0
            for state, count in summary.items():
                click.echo(f"- {state.capitalize():<12}: {count}")
                total += count
            click.echo(f"- {'Total':<12}: {total}")
    
    except Exception as e:
        click.echo(f"Error getting job status: {e}")
    
    click.echo("\n--- Worker Status ---")
    if os.path.exists(PID_FILE):
        try:
            with open(PID_FILE, 'r') as f:
                pids = [pid for pid in f.read().splitlines() if pid.strip()]
            if pids:
                click.echo(f"Active: {len(pids)} worker(s) running (PIDs: {', '.join(pids)})")
            else:
                click.echo("Inactive: PID file is empty.")
        except Exception as e:
            click.echo(f"Error reading PID file: {e}")
    else:
        click.echo("Inactive: No PID file found.")


# --- Config Commands ---

@cli.group()
def config():
    """
    Manage system configuration (max-retries, backoff_base).
    """
    pass


@config.command('set')
@click.argument('key')
@click.argument('value')
def set_config(key, value):
    """
    Set a configuration value (e.g., max_retries, backoff_base).
    """
    if key not in ('max_retries', 'backoff_base'):
        click.echo(f"Error: Unknown config key '{key}'. Allowed: max_retries, backoff_base")
        return
    database.set_config(key, value)


# --- Enqueue Command ---

@cli.command()
@click.argument('job_json_string')
def enqueue(job_json_string):
    """
    Add a new job to the queue.

    Example:
    queuectl enqueue '{"id":"job1","command":"sleep 2"}'

    With optional fields:
    queuectl enqueue '{"id":"job2", "command":"echo high", "priority": 10}'
    queuectl enqueue '{"id":"job3", "command":"echo later", "run_at": "2025-11-10T10:00:00Z"}'
    queuectl enqueue '{"id":"job4", "command":"/bin/false", "max_retries": 5}'
    """
    try:
        job_data = json.loads(job_json_string)
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON string provided.")
        return
    
    job_id = job_data.get('id')
    command = job_data.get('command')
    
    if not job_id or not command:
        click.echo("Error: Job data must include 'id' and 'command'.")
        return
    
    # --- NEW: Get optional fields ---
    max_retries = job_data.get('max_retries')  # Can be None
    run_at_str = job_data.get('run_at')  # Can be None
    priority = job_data.get('priority', 0)  # Default 0
    timeout = job_data.get('timeout')  # Can be None
    
    database.create_job(job_id, command, max_retries, run_at_str, priority, timeout)


# --- Logs Command ---
@cli.command()
@click.argument('job_id')
@click.option('--stderr', 'log_type', flag_value='err', help="Show stderr log.")
@click.option('--stdout', 'log_type', flag_value='out', default=True, help="Show stdout log (default).")
def logs(job_id, log_type):
    """
    Show the stdout or stderr logs for a job.
    Logs are stored in the 'logs/' directory.
    """
    # log_dir = 'logs' <-- REMOVE
    log_suffix = 'out.log' if log_type == 'out' else 'err.log'
    # Use the new constant
    log_file_path = os.path.join(LOG_DIR, f"{job_id}.{log_suffix}")
    
    if not os.path.exists(log_file_path):
        click.echo(f"Log file not found: {log_file_path}")
        click.echo(f"Ensure the 'logs/' directory exists and the job has run.")
        return
    
    try:
        with open(log_file_path, 'r') as f:
            click.echo(f.read())
    except Exception as e:
        click.echo(f"Error reading log file: {e}")


# --- Worker Commands ---

@cli.group()
def worker():
    """
    Manage worker processes.
    """
    pass


@worker.command('start')
@click.option('--count', default=1, type=int, help='Number of workers to start.')
def start(count):
    """
    Start one or more worker processes in the foreground.
    Manages a .pid file for 'worker stop'.
    Handles Ctrl+C for graceful shutdown.
    """
    if os.path.exists(PID_FILE):
        click.echo(f"Error: PID file '{PID_FILE}' already exists. Workers may already be running.")
        click.echo("Run 'queuectl worker stop' to clear it.")
        return
    
    main_pid = str(os.getpid())
    shutdown_event = multiprocessing.Event()
    processes = []
    
    # This handler will now ONLY handle SIGTERM (from 'worker stop')
    def handle_parent_shutdown(sig, frame):
        if not shutdown_event.is_set():
            # Updated message to be specific
            click.echo("\n'worker stop' command (SIGTERM) received. Sending shutdown signal...")
            shutdown_event.set()
    
    # This line was causing the conflict.
    
    # KEEP: This line handles the 'worker stop' command.
    signal.signal(signal.SIGTERM, handle_parent_shutdown)
    # --- END FIX ---
    
    for _ in range(count):
        proc = multiprocessing.Process(
            target=worker_module.run_worker_loop,
            args=(shutdown_event,)
        )
        proc.start()
        processes.append(proc)
    
    child_pids_str = ", ".join([str(p.pid) for p in processes])
    click.echo(f"Started main process (PID: {main_pid}) with {count} worker(s): {child_pids_str}")
    click.echo("Workers running in foreground. Press Ctrl+C to shut down.")
    
    try:
        with open(PID_FILE, 'w') as f:
            f.write(main_pid)
        
        while not shutdown_event.is_set():
            try:
                # This sleep is interruptible by Ctrl+C
                time.sleep(0.5)
            except KeyboardInterrupt:
                # This is now the ONLY place Ctrl+C is handled
                # in the parent process.
                if not shutdown_event.is_set():
                    click.echo("\nCtrl+C received. Sending shutdown signal...")
                    shutdown_event.set()
                pass  # Let the loop re-check the shutdown_event
    
    except KeyboardInterrupt:
        # Fallback in case interrupt happens outside the loop
        if not shutdown_event.is_set():
            click.echo("\nCtrl+C (fallback). Forcing shutdown...")
            shutdown_event.set()
        pass
    
    finally:
        click.echo(f"\nMain process {main_pid} received shutdown. Waiting for workers...")
        for p in processes:
            p.join()  # This will wait for workers to exit
        
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
        click.echo(f"All workers shut down. Main process {main_pid} exiting.")


@worker.command('stop')
def stop():
    """
    Stops all running workers by reading the .pid file
    and FORCIBLY killing the main process AND ITS CHILDREN.
    """
    if not os.path.exists(PID_FILE):
        click.echo("No PID file found. Are workers running?")
        return
    
    click.echo(f"Reading main PID from file: {PID_FILE}")
    main_pid = None
    try:
        with open(PID_FILE, 'r') as f:
            pids = [int(pid) for pid in f.read().splitlines() if pid.strip()]
        
        if not pids:
            click.echo("PID file is empty.")
            return
        
        main_pid = pids[0]
        click.echo(f"Sending shutdown signal to process tree (PID: {main_pid})...")
        
        try:
            if os.name == 'nt':
                click.echo("Windows detected. Using 'taskkill /T /F' (forceful)...")
                # /T - Kills the process AND any child processes.
                # /F - Forcefully terminates the process.
                subprocess.run(
                    ['taskkill', '/PID', str(main_pid), '/T', '/F'],
                    check=True,
                    capture_output=True,
                    text=True,
                    # Prevent new console window from flashing
                    creationflags=0x08000000
                )
            else:
                click.echo("Linux/macOS detected. Using 'os.kill'...")
                os.kill(main_pid, signal.SIGTERM)
            
            click.echo(f"Successfully sent signal to {main_pid}.")
        
        except Exception as e:
            # This is expected if the process already died.
            click.echo(f"Failed to stop process {main_pid} (it may already be stopped): {e}")
            if hasattr(e, 'stderr') and e.stderr:
                click.echo(f"Stderr: {e.stderr.strip()}")
    
    except Exception as e:
        click.echo(f"Error reading PID file: {e}")
    finally:
        if os.path.exists(PID_FILE):
            os.remove(PID_FILE)
            click.echo("Cleaned up PID file.")
        
        click.echo("Stop command finished.")


# --- List Command ---


@cli.command('list')
@click.option('--state',
              type=click.Choice(['pending', 'processing', 'completed', 'failed', 'dead'], case_sensitive=False),
              default='pending',
              help='The state of jobs to list.')
def list_jobs(state):
    """
    List jobs by their state.
    """
    jobs = database.get_jobs_by_state(state)
    if not jobs:
        click.echo(f"No jobs found with state: {state}")
        return
    
    click.echo(f"--- Jobs ({state}) ---")
    for job in jobs:
        click.echo(f"ID: {job['id']}")
        click.echo(f"  Command:   {job['command']}")
        click.echo(f"  State:     {job['state']}")
        click.echo(f"  Attempts:  {job['attempts']}/{job['max_retries']}")
        if job['run_at']:
            click.echo(f"  Next Run:  {job['run_at']}")
        click.echo(f"  Created:   {job['created_at']}")
        click.echo("-" * 20)


# --- DLQ Commands ---

@cli.group()
def dlq():
    """
    Manage the Dead Letter Queue (DLQ).
    """
    pass


@dlq.command('list')
def dlq_list():
    """
    List all jobs in the Dead Letter Queue (state='dead').
    """
    jobs = database.get_jobs_by_state('dead')
    if not jobs:
        click.echo("Dead Letter Queue is empty.")
        return
    
    click.echo("--- Dead Letter Queue Jobs ---")
    for job in jobs:
        click.echo(f"ID: {job['id']}")
        click.echo(f"  Command:   {job['command']}")
        click.echo(f"  Attempts:  {job['attempts']}/{job['max_retries']}")
        click.echo(f"  Failed At: {job['updated_at']}")
        click.echo("-" * 20)


@dlq.command('retry')
@click.argument('job_id')
def dlq_retry(job_id):
    """
    Move a specific job from the DLQ back to 'pending'.
    """
    database.retry_dlq_job(job_id)


@cli.command()
def web():
    """
    Start a local web dashboard to monitor the queue.
    """
    try:
        dashboard.run_web_server()
    except ImportError:
        click.echo("Error: 'flask' is not installed.")
        click.echo("Please run 'pip install flask' to use the web dashboard.")
    except Exception as e:
        click.echo(f"Failed to start web server: {e}")


if __name__ == '__main__':
    cli()
