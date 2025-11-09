# queuectl/worker.py

import subprocess
import time
import os
import signal
import multiprocessing
from multiprocessing.synchronize import Event as SyncEvent
from . import database


def execute_job(job):
    """
    Executes the job's command in a NON-BLOCKING subprocess.
    Returns the subprocess.Popen object.
    """
    command = job['command']
    job_id = job['id']
    
    print(f"Worker {os.getpid()}: Starting job {job_id}: {command}")
    
    try:
        # --- MODIFIED: Use Popen for non-blocking execution ---
        process = subprocess.Popen(
            command,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        return process
    except Exception as e:
        print(f"Worker {os.getpid()}: Job {job_id} failed to start: {e}")
        return None  # Will be treated as an immediate failure


def run_worker_loop(shutdown_event: SyncEvent):
    """
    The main worker loop, rewritten as a non-blocking state machine.
    """
    
    # --- NEW: Signal handler for the child process ---
    def handle_signal(sig, frame):
        # This handler just sets the event.
        # The main loop will detect it and shut down.
        if not shutdown_event.is_set():
            print(f"\nWorker {os.getpid()}: Shutdown signal received...")
            shutdown_event.set()
    
    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)
    
    # --- State variables ---
    current_process: subprocess.Popen = None
    current_job_id = None
    
    # --- Main Worker Loop ---
    while True:
        
        if current_process:
            # --- STATE 1: We are currently processing a job ---
            return_code = current_process.poll()  # Check if job is done (non-blocking)
            
            if return_code is not None:
                # --- Job just finished ---
                stdout, stderr = current_process.communicate()  # Get final output
                
                print(f"Worker {os.getpid()}: Job {current_job_id} finished with code {return_code}.")
                
                if return_code == 0:
                    if stdout: print(f"Worker {os.getpid()}: Stdout: {stdout.strip()}")
                else:
                    if stderr: print(f"Worker {os.getpid()}: Stderr: {stderr.strip()}")
                
                # Finalize the job in the DB
                database.finalize_job(current_job_id, success=(return_code == 0))
                
                # Reset state to idle
                current_process = None
                current_job_id = None
            
            else:
                # --- Job is still running ---
                # Wait a very short time before checking again.
                # This check allows us to exit quickly if the event is set.
                shutdown_event.wait(timeout=0.1)
        
        elif not shutdown_event.is_set():
            # --- STATE 2: We are idle and not shutting down ---
            job = database.fetch_and_lock_job()
            
            if job:
                # --- Found a job, start it ---
                current_job_id = job['id']
                current_process = execute_job(job)
                
                if current_process is None:
                    # Job failed to even start, finalize it immediately
                    database.finalize_job(current_job_id, success=False)
                    current_job_id = None
            else:
                # --- No job found, sleep for a second ---
                # This wait is interruptible by the shutdown_event
                shutdown_event.wait(timeout=0.1)
        
        else:
            # --- STATE 3: We are idle AND shutting down ---
            # The shutdown event is set and we are not in a job.
            # Time to exit.
            break
    
    print(f"Worker {os.getpid()}: Exiting.")
