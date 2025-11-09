# queuectl/cli.py

import click
import json
from . import database


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


# --- Config Commands ---

@cli.group()
def config():
    """
    Manage system configuration (max-retries, backoff-base).
    """
    pass


@config.command('set')
@click.argument('key')
@click.argument('value')
def set_config(key, value):
    """
    Set a configuration value (e.g., max-retries, backoff_base).
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

    With custom retries:
    queuectl enqueue '{"id":"job2","command":"/bin/false","max_retries":5}'
    """
    try:
        job_data = json.loads(job_json_string)
    except json.JSONDecodeError:
        click.echo("Error: Invalid JSON string provided.")
        return
    
    job_id = job_data.get('id')
    command = job_data.get('command')
    max_retries = job_data.get('max_retries')  # Can be None
    
    if not job_id or not command:
        click.echo("Error: Job data must include 'id' and 'command'.")
        return
    
    database.create_job(job_id, command, max_retries)


if __name__ == '__main__':
    cli()
