# queuectl/worker.py

import subprocess
import time
import os
from . import database


def execute_job(job):
    """
    Executes the job's command in a subprocess.
    Returns the subprocess return code (0 for success, non-zero for failure).
    """
    command = job['command']
    job_id = job['id']
    
    print(f"Worker {os.getpid()}: Executing job {job_id}: {command}")
    
    try:
        # shell=True is required to interpret commands like 'echo Hello'
        result = subprocess.run(
            command,
            shell=True,
            capture_output=True,
            text=True,
            timeout=300  # 5-minute timeout
        )
        
        if result.returncode == 0:
            print(f"Worker {os.getpid()}: Job {job_id} completed successfully.")
        else:
            print(f"Worker {os.getpid()}: Job {job_id} failed with code {result.returncode}.")
            print(f"Worker {os.getpid()}: Stderr: {result.stderr.strip()}")
        
        return result.returncode
    
    except subprocess.TimeoutExpired:
        print(f"Worker {os.getpid()}: Job {job_id} timed out.")
        return -1  # Use a custom code for timeout
    except Exception as e:
        print(f"Worker {os.getpid()}: Job {job_id} failed with exception: {e}")
        return -1  # Generic failure


def run_worker_loop():
    """
    The main worker loop.
    Fetches, executes, and finalizes jobs continuously.
    """
    while True:
        job = None
        try:
            job = database.fetch_and_lock_job()
            
            if job:
                # 1. Execute the job
                return_code = execute_job(job)
                
                # 2. Finalize the job
                success = (return_code == 0)
                database.finalize_job(job['id'], success)
            
            else:
                # No jobs found, sleep to prevent busy-waiting
                time.sleep(1)
        
        except KeyboardInterrupt:
            print(f"\nWorker {os.getpid()}: Shutting down gracefully...")
            if job:
                # If interrupted, release the job back to 'pending'
                print(f"Worker {os.getpid()}: Releasing job {job['id']}...")
                database.release_job(job['id'])
            break
        except Exception as e:
            print(f"Worker {os.getpid()}: Error in worker loop: {e}")
            if job:
                # If something unexpected happens, release the job
                database.release_job(job['id'])
            time.sleep(5)  # Wait a bit longer after an error
