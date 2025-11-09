# queuectl/worker.py

import subprocess
import time
import os
import signal
import multiprocessing
from . import database

LOG_DIR = 'logs'


def execute_job(job):
    """
    Executes the job's command in a NON-BLOCKING subprocess,
    redirecting stdout/stderr to log files.

    Returns a tuple of: (Popen_object, stdout_file, stderr_file)
    """
    command = job['command']
    job_id = job['id']
    
    # Ensure log directory exists
    os.makedirs(LOG_DIR, exist_ok=True)
    
    log_out_path = os.path.join(LOG_DIR, f"{job_id}.out.log")
    log_err_path = os.path.join(LOG_DIR, f"{job_id}.err.log")
    
    print(f"Worker {os.getpid()}: Starting job {job_id}: {command}")
    print(f"Worker {os.getpid()}: Stdout log: {log_out_path}")
    
    try:
        # --- MODIFIED: Redirect stdout/stderr to files ---
        # Open file handles for Popen to write to
        f_out = open(log_out_path, 'w')
        f_err = open(log_err_path, 'w')
        
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=f_out,
            stderr=f_err,
            text=True
        )
        return process, f_out, f_err
    except Exception as e:
        print(f"Worker {os.getpid()}: Job {job_id} failed to start: {e}")
        # Close handles if Popen failed
        if 'f_out' in locals(): f_out.close()
        if 'f_err' in locals(): f_err.close()
        return None, None, None


def run_worker_loop(shutdown_event: multiprocessing.Event):
    """
    The main worker loop, rewritten as a non-blocking state machine.
    """
    
    def handle_signal(sig, frame):
        if not shutdown_event.is_set():
            print(f"\nWorker {os.getpid()}: Shutdown signal received...")
            shutdown_event.set()
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # State variables
    current_process: subprocess.Popen = None
    current_job = None
    current_job_start_time = None
    stdout_file = None
    stderr_file = None
    
    while True:
        
        if current_process:
            # --- STATE 1: We are currently processing a job ---
            return_code = current_process.poll()
            
            # --- NEW: Timeout Logic ---
            job_timeout = current_job.get('timeout', 300)
            if job_timeout and (time.time() - current_job_start_time) > job_timeout:
                print(f"Worker {os.getpid()}: Job {current_job['id']} TIMED OUT (>{job_timeout}s). Terminating...")
                current_process.terminate()  # Send SIGTERM
                time.sleep(1)  # Give it a second to die
                current_process.kill()  # Send SIGKILL
                return_code = -9  # Custom timeout code
            # --- End Timeout Logic ---
            
            if return_code is not None:
                # --- Job just finished ---
                print(f"Worker {os.getpid()}: Job {current_job['id']} finished with code {return_code}.")
                
                # Close log file handles
                if stdout_file: stdout_file.close()
                if stderr_file: stderr_file.close()
                
                # Finalize the job in the DB
                database.finalize_job(current_job['id'], success=(return_code == 0))
                
                # Reset state to idle
                current_process = None
                current_job = None
                current_job_start_time = None
            
            else:
                # --- Job is still running ---
                shutdown_event.wait(timeout=0.1)
        
        elif not shutdown_event.is_set():
            # --- STATE 2: We are idle and not shutting down ---
            job_dict = database.fetch_and_lock_job()
            
            if job_dict:
                # --- Found a job, start it ---
                current_job = job_dict  # Save the full job dict
                database.mark_job_started(current_job['id'])  # Mark start time
                
                current_process, stdout_file, stderr_file = execute_job(current_job)
                current_job_start_time = time.time()
                
                if current_process is None:
                    # Job failed to even start, finalize it immediately
                    database.finalize_job(current_job['id'], success=False)
                    current_job = None
                    current_job_start_time = None
            else:
                # --- No job found, sleep ---
                shutdown_event.wait(timeout=1.0)
        
        else:
            # --- STATE 3: We are idle AND shutting down ---
            break
    
    print(f"Worker {os.getpid()}: Exiting.")
