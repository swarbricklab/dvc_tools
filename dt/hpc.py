"""HPC utilities for distributed DVC operations via qxub.

Provides common functionality for submitting and monitoring batch jobs
on HPC clusters using qxub (https://github.com/swarbricklab/qxub).

Also provides shared infrastructure for partitioning work across parallel
workers, including manifest storage and hash-based partitioning.
"""

import json
import shutil
import subprocess
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from . import config as cfg
from . import utils
from .errors import HPCError


def check_qxub() -> bool:
    """Check if qxub is available in PATH.
    
    Returns:
        True if qxub command is available.
    """
    return shutil.which('qxub') is not None


def require_qxub() -> None:
    """Ensure qxub is available, raising an error if not.
    
    Raises:
        HPCError: If qxub is not found.
    """
    if not check_qxub():
        raise HPCError(
            "qxub not found. Install from https://github.com/swarbricklab/qxub"
        )


def get_qxub_config() -> dict:
    """Get qxub configuration from dt config.
    
    Returns:
        Dictionary with qxub settings.
    """
    return {
        'env': cfg.get_value('qxub.env', 'dt'),
        'queue': cfg.get_value('qxub.queue', 'copyq'),
        'walltime': cfg.get_value('qxub.walltime', '10:00:00'),
        'mem': cfg.get_value('qxub.mem', '4GB'),
    }


def build_qxub_command(
    job_name: str,
    worker_command: List[str],
    qxub_args: Optional[List[str]] = None,
) -> List[str]:
    """Build a qxub exec command.
    
    Args:
        job_name: Name for the PBS job.
        worker_command: The command to run in the job.
        qxub_args: Additional arguments for qxub exec.
        
    Returns:
        Complete command list for subprocess.
    """
    config = get_qxub_config()
    
    cmd = [
        'qxub', 'exec', '--terse',
        '--env', config['env'],
        '--queue', config['queue'],
        '--time', config['walltime'],
        '--mem', config['mem'],
    ]
    
    if qxub_args:
        cmd.extend(qxub_args)
    
    cmd.extend(['-N', job_name])
    cmd.append('--')
    cmd.extend(worker_command)
    
    return cmd


def submit_workers(
    manifest_dir: Path,
    num_workers: int,
    operation: str,
    qxub_args: Optional[List[str]] = None,
    verbose: bool = False,
) -> List[str]:
    """Submit worker jobs via qxub.
    
    Args:
        manifest_dir: Path to manifest directory containing worker_N.json files.
        num_workers: Number of workers to potentially submit.
        operation: The dt operation to run ('push' or 'pull').
        qxub_args: Additional arguments for qxub exec.
        verbose: Print progress.
        
    Returns:
        List of job IDs.
        
    Raises:
        HPCError: If qxub is not available.
    """
    require_qxub()
    
    job_ids = []
    repo_root = Path.cwd()
    
    for worker_id in range(num_workers):
        # Check if this worker has any files
        worker_file = manifest_dir / f'worker_{worker_id}.json'
        if not worker_file.exists():
            continue
            
        with open(worker_file) as f:
            partition = json.load(f)
        if not partition.get('files'):
            if verbose:
                print(f"Skipping worker {worker_id}: no files")
            continue
        
        # Build the worker command
        worker_cmd = [
            'dt', operation,
            '--worker', str(worker_id),
            '--manifest', str(manifest_dir),
            '--verbose',
        ]
        
        job_name = f'dt-{operation}-{manifest_dir.name}-w{worker_id}'
        cmd = build_qxub_command(job_name, worker_cmd, qxub_args)
        
        if verbose:
            print(f"Submitting worker {worker_id}...")
            print(f"  Command: {' '.join(cmd)}")
        
        try:
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                cwd=repo_root,
            )
            
            if result.returncode == 0:
                # qxub --terse returns job ID on first line
                job_id = result.stdout.strip().split('\n')[0]
                job_ids.append(job_id)
                if verbose:
                    print(f"  Job ID: {job_id}")
            else:
                print(f"Warning: Failed to submit worker {worker_id}: {result.stderr}")
        except Exception as e:
            print(f"Warning: Failed to submit worker {worker_id}: {e}")
    
    return job_ids


def monitor_jobs(job_ids: List[str], verbose: bool = False) -> bool:
    """Monitor jobs until completion using qxub monitor.
    
    Args:
        job_ids: List of job IDs to monitor.
        verbose: Print progress.
        
    Returns:
        True if all jobs succeeded.
        
    Raises:
        HPCError: If qxub is not available.
    """
    if not job_ids:
        return True
    
    require_qxub()
    
    cmd = ['qxub', 'monitor', '--summary'] + job_ids
    
    if verbose:
        print(f"Monitoring {len(job_ids)} job(s):")
        for job_id in job_ids:
            print(f"  {job_id}")
        print(f"Command: {' '.join(cmd)}")
    
    try:
        result = subprocess.run(cmd)
        return result.returncode == 0
    except Exception as e:
        print(f"Error monitoring jobs: {e}")
        return False


# =============================================================================
# Parallel transfer infrastructure
# =============================================================================

def get_transfer_dir(operation: str) -> Path:
    """Get the .dt/tmp/{operation} directory for manifest storage.
    
    The .dt directory is always at the project root (alongside .git/.dvc),
    regardless of the current working directory.
    
    Args:
        operation: Transfer operation name ('push' or 'pull')
        
    Returns:
        Path to the transfer directory (created if needed)
    """
    transfer_dir = utils.get_dt_dir() / 'tmp' / operation
    transfer_dir.mkdir(parents=True, exist_ok=True)
    return transfer_dir


def get_prefixes_for_worker(worker_id: int, num_workers: int) -> Set[str]:
    """Get hash prefixes assigned to a worker.
    
    Partitions the 256 possible hash prefixes (00-ff) across workers.
    
    Args:
        worker_id: Worker index (0 to num_workers-1)
        num_workers: Total number of workers
        
    Returns:
        Set of 2-character hex prefixes for this worker.
    """
    prefixes = set()
    for i in range(256):
        if i % num_workers == worker_id:
            prefixes.add(f"{i:02x}")
    return prefixes


def save_manifest(
    manifest: Dict[str, Any],
    partitions: Dict[int, List[str]],
    job_id: str,
    operation: str,
) -> Path:
    """Save manifest and partitions to disk.
    
    Args:
        manifest: Original manifest with metadata
        partitions: Worker partitions (worker_id -> list of file hashes)
        job_id: Unique job identifier
        operation: Transfer operation name ('push' or 'pull')
        
    Returns:
        Path to the manifest directory
    """
    manifest_dir = get_transfer_dir(operation) / job_id
    manifest_dir.mkdir(parents=True, exist_ok=True)
    
    # Save metadata
    with open(manifest_dir / 'manifest.json', 'w') as f:
        json.dump({
            'remote': manifest.get('remote'),
            'repo_root': manifest.get('repo_root'),
            'total_files': len(manifest.get('files', [])),
            'num_workers': len(partitions),
        }, f, indent=2)
    
    # Save each worker's partition
    for worker_id, files in partitions.items():
        with open(manifest_dir / f'worker_{worker_id}.json', 'w') as f:
            json.dump({'files': files}, f)
    
    return manifest_dir


def load_worker_partition(manifest_dir: Path, worker_id: int) -> Tuple[Dict, List[str]]:
    """Load manifest metadata and worker's file partition.
    
    Args:
        manifest_dir: Path to manifest directory
        worker_id: Worker index
        
    Returns:
        Tuple of (metadata dict, list of file hashes)
    """
    with open(manifest_dir / 'manifest.json') as f:
        metadata = json.load(f)
    
    worker_file = manifest_dir / f'worker_{worker_id}.json'
    if not worker_file.exists():
        return metadata, []
    
    with open(worker_file) as f:
        partition = json.load(f)
    
    return metadata, partition.get('files', [])
