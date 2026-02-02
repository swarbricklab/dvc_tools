"""Command-line interface for DVC Tools."""

import click

from . import config as cfg
from . import clone as clone_mod
from . import init as init_mod
from . import cache as cache_mod
from . import remote as remote_mod
from . import doctor as doctor_mod
from . import push as push_mod
from . import add as add_mod
from . import checkout as checkout_mod
from . import tmp as tmp_mod
from . import import_data as import_mod
from . import pull as pull_mod
from . import offline as offline_mod


@click.group()
@click.version_option(package_name='dvc-tools')
def cli():
    """DVC Tools - Convenient tools for working with DVC in HPC environments.
    
    This package provides the `dt` command with subcommands for working with
    DVC projects in HPC environments with shared external caches and SSH remotes.
    """
    pass


@cli.command()
@click.option('--name', help='Override the project name (defaults to current directory name)')
@click.option('--owner', help='Override the GitHub owner (user or organization)')
@click.option('--cache-root', help='Override the cache root directory')
@click.option('--remote-root', help='Override the remote root directory')
@click.option('--no-git', is_flag=True, help='Skip git initialization')
@click.option('--no-dvc', is_flag=True, help='Skip DVC initialization')
@click.option('--no-cache', is_flag=True, help='Skip cache setup')
@click.option('--no-remote', is_flag=True, help='Skip remote setup')
def init(name, owner, cache_root, remote_root, no_git, no_dvc, no_cache, no_remote):
    """Initialize a new DVC project with proper cache and remote setup.
    
    This command creates a complete DVC project with git, DVC, external cache,
    and remote storage properly configured for HPC environments.
    """
    try:
        init_mod.init_project(
            name=name,
            owner=owner,
            cache_root=cache_root,
            remote_root=remote_root,
            no_git=no_git,
            no_dvc=no_dvc,
            no_cache=no_cache,
            no_remote=no_remote,
        )
    except init_mod.InitError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument('repository', metavar='REPOSITORY')
@click.argument('path', required=False)
@click.option('--owner', help='Override the GitHub owner for short names')
@click.option('--no-init', is_flag=True, help='Skip running dt init after cloning')
@click.option('--no-submodules', is_flag=True, help='Skip cloning git submodules')
@click.option('--cache-name', help='Override cache directory name')
@click.option('--remote-name', help='Override remote directory name')
@click.option('--shallow', is_flag=True, help='Perform a shallow clone')
def clone(repository, path, owner, no_init, no_submodules, cache_name, remote_name, shallow):
    """Clone an existing DVC project from GitHub.
    
    REPOSITORY can be either:
    
    \b
    - A full URL: git@github.com:owner/repo.git
    - A short name: repo (requires owner to be configured)
    
    When owner is configured, you can use short names:
    
    \b
        dt clone myproject
    
    is equivalent to:
    
    \b
        dt clone git@github.com:myorg/myproject.git
    
    This command clones a repository and configures it for the local platform
    with proper cache and remote setup.
    """
    try:
        clone_mod.clone_repository(
            repository=repository,
            path=path,
            owner=owner,
            no_submodules=no_submodules,
            cache_name=cache_name,
            remote_name=remote_name,
            shallow=shallow,
        )
    except clone_mod.CloneError as e:
        raise click.ClickException(str(e))


@cli.group(invoke_without_command=True)
@click.pass_context
def config(ctx):
    """View and modify configuration settings.
    
    Configuration follows a hierarchical scope system:
    local > project > user > system
    
    Run without arguments to list all effective configuration.
    """
    if ctx.invoked_subcommand is None:
        # Default behavior: list all config with sources
        config_values = cfg.list_config_with_sources()
        if config_values:
            for key, value, scope in config_values:
                click.echo(f"{key}={value}  ({scope})")
        else:
            click.echo("No configuration set.")


def _get_scope(local: bool, project: bool, user: bool, system: bool) -> str:
    """Determine which scope to use from flags."""
    if local:
        return 'local'
    elif user:
        return 'user'
    elif system:
        return 'system'
    else:
        return 'project'  # default


def _count_scope_flags(local: bool, project: bool, user: bool, system: bool) -> int:
    """Count how many scope flags are set."""
    return sum([local, project, user, system])


@config.command('list')
@click.option('--local', is_flag=True, help='List local configuration')
@click.option('--project', is_flag=True, help='List project configuration')
@click.option('--user', is_flag=True, help='List user configuration')
@click.option('--system', is_flag=True, help='List system configuration')
@click.option('--show-origin', is_flag=True, help='Show which scope each value comes from')
def config_list(local, project, user, system, show_origin):
    """List configuration values."""
    if _count_scope_flags(local, project, user, system) > 1:
        raise click.UsageError("Only one scope flag can be specified.")
    
    if any([local, project, user, system]):
        scope = _get_scope(local, project, user, system)
        config_values = cfg.list_config(scope)
        paths = cfg.get_config_paths()
        click.echo(f"# {scope}: {paths[scope]}")
        if config_values:
            for key, value in sorted(config_values.items()):
                click.echo(f"{key}={value}")
        else:
            click.echo("No configuration in this scope.")
    elif show_origin:
        config_values = cfg.list_config_with_sources()
        if config_values:
            for key, value, scope in config_values:
                click.echo(f"{scope}\t{key}={value}")
        else:
            click.echo("No configuration set.")
    else:
        config_values = cfg.list_config()
        if config_values:
            for key, value in sorted(config_values.items()):
                click.echo(f"{key}={value}")
        else:
            click.echo("No configuration set.")


@config.command('get')
@click.argument('key')
def config_get(key):
    """Get a configuration value."""
    value = cfg.get_value(key)
    if value is not None:
        click.echo(value)
    else:
        raise click.ClickException(f"Key '{key}' not found in configuration.")


@config.command('set')
@click.argument('key')
@click.argument('value')
@click.option('--local', is_flag=True, help='Set in local scope')
@click.option('--project', is_flag=True, help='Set in project scope (default)')
@click.option('--user', is_flag=True, help='Set in user scope')
@click.option('--system', is_flag=True, help='Set in system scope')
def config_set(key, value, local, project, user, system):
    """Set a configuration value."""
    if _count_scope_flags(local, project, user, system) > 1:
        raise click.UsageError("Only one scope flag can be specified.")
    
    scope = _get_scope(local, project, user, system)
    cfg.set_value(key, value, scope)
    click.echo(f"Set {key}={value} in {scope} config.")


@config.command('unset')
@click.argument('key')
@click.option('--local', is_flag=True, help='Unset in local scope')
@click.option('--project', is_flag=True, help='Unset in project scope (default)')
@click.option('--user', is_flag=True, help='Unset in user scope')
@click.option('--system', is_flag=True, help='Unset in system scope')
def config_unset(key, local, project, user, system):
    """Unset a configuration value."""
    if _count_scope_flags(local, project, user, system) > 1:
        raise click.UsageError("Only one scope flag can be specified.")
    
    scope = _get_scope(local, project, user, system)
    if cfg.unset_value(key, scope):
        click.echo(f"Unset {key} from {scope} config.")
    else:
        raise click.ClickException(f"Key '{key}' not found in {scope} configuration.")


@config.command('path')
@click.option('--local', is_flag=True, help='Show local config path')
@click.option('--project', is_flag=True, help='Show project config path')
@click.option('--user', is_flag=True, help='Show user config path')
@click.option('--system', is_flag=True, help='Show system config path')
def config_path(local, project, user, system):
    """Show configuration file paths."""
    paths = cfg.get_config_paths()
    
    if _count_scope_flags(local, project, user, system) > 1:
        raise click.UsageError("Only one scope flag can be specified.")
    
    if any([local, project, user, system]):
        scope = _get_scope(local, project, user, system)
        path = paths[scope]
        exists = "✓" if path.exists() else "✗"
        click.echo(f"{path} [{exists}]")
    else:
        for scope in cfg.SCOPES:
            path = paths[scope]
            exists = "✓" if path.exists() else "✗"
            click.echo(f"{scope}: {path} [{exists}]")


@cli.group()
def cache():
    """Manage external shared caches."""
    pass


@cache.command('list')
def cache_list():
    """List the primary DVC cache and all alternate caches.
    
    Shows the primary cache configured for DVC, plus any alternate
    caches configured for multi-cache checkout.
    """
    import subprocess
    
    # Get primary cache from DVC
    try:
        result = subprocess.run(
            ['dvc', 'cache', 'dir'],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0 and result.stdout.strip():
            primary = result.stdout.strip()
        else:
            primary = "(not configured)"
    except Exception:
        primary = "(dvc not available)"
    
    click.echo(f"Primary: {primary}")
    
    # Get alternate caches from dt config
    alt_caches = cfg.get_list_value('cache.alt')
    
    if alt_caches:
        click.echo()
        click.echo("Alternate caches:")
        for path, scope in alt_caches:
            click.echo(f"  {path}  ({scope})")
    else:
        click.echo()
        click.echo("No alternate caches configured.")


@cache.command('add')
@click.argument('path')
@click.option('--local', 'scope', flag_value='local', default=True, help='Add to local config (default)')
@click.option('--project', 'scope', flag_value='project', help='Add to project config')
@click.option('--user', 'scope', flag_value='user', help='Add to user config')
@click.option('--system', 'scope', flag_value='system', help='Add to system config')
def cache_add(path, scope):
    """Add an alternate cache path for multi-cache checkout.
    
    Alternate caches are searched during `dt checkout` to find
    cached files from other projects or remotes.
    """
    from pathlib import Path
    
    # Resolve to absolute path
    abs_path = str(Path(path).resolve())
    
    if cfg.add_list_value('cache.alt', abs_path, scope):
        click.echo(f"Added {abs_path} to {scope} config.")
    else:
        click.echo(f"Path already exists in {scope} config.")


@cache.command('remove')
@click.argument('path')
@click.option('--local', 'scope', flag_value='local', default=True, help='Remove from local config (default)')
@click.option('--project', 'scope', flag_value='project', help='Remove from project config')
@click.option('--user', 'scope', flag_value='user', help='Remove from user config')
@click.option('--system', 'scope', flag_value='system', help='Remove from system config')
def cache_remove(path, scope):
    """Remove an alternate cache path.
    """
    from pathlib import Path
    
    # Try both as given and resolved
    abs_path = str(Path(path).resolve())
    
    if cfg.remove_list_value('cache.alt', abs_path, scope):
        click.echo(f"Removed {abs_path} from {scope} config.")
    elif cfg.remove_list_value('cache.alt', path, scope):
        click.echo(f"Removed {path} from {scope} config.")
    else:
        raise click.ClickException(f"Path not found in {scope} config.")


@cache.command('init')
@click.argument('project_name', required=False)
@click.option('--name', help='Override project name')
@click.option('--cache-root', help='Override cache root directory')
@click.option('--cache-path', help='Override complete cache path')
def cache_init(project_name, name, cache_root, cache_path):
    """Set up an external shared cache with proper permissions.
    
    Creates the cache directory structure with group write permissions
    and configures DVC to use it.
    """
    # Use project_name as name if --name not specified
    effective_name = name or project_name
    
    try:
        cache_dir = cache_mod.init_cache(
            name=effective_name,
            cache_root=cache_root,
            cache_path=cache_path,
        )
        click.echo(f"Cache initialized at {cache_dir}")
    except cache_mod.CacheError as e:
        raise click.ClickException(str(e))


@cache.command('add-from')
@click.argument('repository')
@click.option('--owner', help='Override the GitHub owner for short names')
@click.option('--local', 'scope', flag_value='local', default=True, help='Add to local config (default)')
@click.option('--project', 'scope', flag_value='project', help='Add to project config')
@click.option('--user', 'scope', flag_value='user', help='Add to user config')
@click.option('--system', 'scope', flag_value='system', help='Add to system config')
def cache_add_from(repository, owner, scope):
    """Add a repository's remote as an alternate cache.
    
    Discovers the locally-accessible remote from another repository
    and adds it as an alternate cache for multi-cache checkout.
    
    This is useful for importing data from another project - adding
    their remote as an alternate cache allows `dt checkout` to find
    files without copying them to your local cache first.
    
    \b
    Examples:
        dt cache add-from otherproject
        dt cache add-from git@github.com:myorg/otherproject.git
    """
    result = remote_mod.find_local_remote_from_repo(repository, owner=owner)
    
    if not result:
        raise click.ClickException(
            f"No locally-accessible remote found for '{repository}'.\n"
            f"Use 'dt remote list {repository}' to see available remotes."
        )
    
    remote_name, local_path = result
    
    if cfg.add_list_value('cache.alt', local_path, scope):
        click.echo(f"Added {local_path} to {scope} config.")
        click.echo(f"  (from remote '{remote_name}')")
    else:
        click.echo(f"Path already exists in {scope} config: {local_path}")


@cli.group()
def remote():
    """Manage remote storage."""
    pass


@remote.command('init')
@click.argument('project_name', required=False)
@click.option('--name', help='Override project name')
@click.option('--remote-root', help='Override remote root directory')
@click.option('--remote-path', help='Override complete remote path')
def remote_init(project_name, name, remote_root, remote_path):
    """Set up remote storage with SSH and local access.
    
    Creates the remote directory structure with proper permissions
    and configures DVC with SSH and local remotes.
    """
    # Use project_name as name if --name not specified
    effective_name = name or project_name
    
    try:
        remote_dir = remote_mod.init_remote(
            name=effective_name,
            remote_root=remote_root,
            remote_path=remote_path,
        )
        click.echo(f"Remote initialized at {remote_dir}")
    except remote_mod.RemoteError as e:
        raise click.ClickException(str(e))


@remote.command('list')
@click.argument('repository', required=False)
@click.option('--owner', help='Override the GitHub owner for short names')
def remote_list(repository, owner):
    """List DVC remotes for a repository.
    
    Without arguments, lists remotes for the current repository.
    With a repository argument, lists remotes for that remote repository.
    
    \b
    Examples:
        dt remote list                    # List remotes for current repo
        dt remote list otherproject       # List remotes for another repo
        dt remote list myorg/otherproject # List with explicit owner
    """
    if repository:
        # List remotes from a remote repository
        try:
            remotes = remote_mod.list_remotes_from_repo(repository, owner=owner)
        except Exception as e:
            raise click.ClickException(str(e))
    else:
        # List remotes for current repository
        remotes = remote_mod.list_remotes()
    
    if not remotes:
        click.echo("No remotes configured.")
        return
    
    for name, url, is_default in remotes:
        default_marker = " (default)" if is_default else ""
        click.echo(f"{name}{default_marker}: {url}")


@cli.command()
@click.option('-v', '--verbose', is_flag=True, help='Show detailed output including dvc doctor and config')
def doctor(verbose):
    """Diagnose common setup issues and verify environment configuration.
    
    Checks for:
    - Git and DVC installation
    - GitHub CLI availability
    - SSH key setup and GitHub authentication
    - Cache and remote root configuration
    
    Use -v for verbose output including dvc doctor results.
    """
    # Print dt version header
    dt_version = doctor_mod.get_dt_version()
    click.echo(f"DVC Tools version: {dt_version}")
    click.echo()
    
    # Run diagnostics
    results = doctor_mod.run_diagnostics(verbose=verbose)
    
    passed = 0
    failed = 0
    
    for result in results:
        click.echo(str(result))
        if result.passed:
            passed += 1
        else:
            failed += 1
    
    click.echo()
    if failed == 0:
        click.echo(f"All {passed} checks passed.")
    else:
        click.echo(f"{passed} passed, {failed} failed.")
    
    # Verbose output
    if verbose:
        click.echo()
        click.echo("--- Configuration (with sources) ---")
        config_values = doctor_mod.get_config_with_sources()
        if config_values:
            for key, value, scope in config_values:
                click.echo(f"{scope}\t{key}={value}")
        else:
            click.echo("No configuration set.")
        
        click.echo()
        click.echo("--- DVC Doctor ---")
        dvc_output = doctor_mod.run_dvc_doctor()
        click.echo(dvc_output)


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option('--workers', '-w', type=int, default=None,
              help='Number of parallel workers for distributed push via qxub.')
@click.option('--worker', type=int, default=None,
              help='Worker ID (internal, used by submitted jobs).')
@click.option('--manifest', type=click.Path(exists=True), default=None,
              help='Manifest directory (internal, used by submitted jobs).')
@click.option('--remote', '-r', default=None,
              help='Push to specific remote instead of all project remotes.')
@click.option('--no-wait', is_flag=True,
              help='Submit jobs and exit without waiting for completion.')
@click.option('--dry', '--dry-run', is_flag=True,
              help='Show what would be pushed without actually pushing.')
@click.option('--verbose', '-v', is_flag=True,
              help='Print detailed progress.')
@click.pass_context
def push(ctx, workers, worker, manifest, remote, no_wait, dry, verbose):
    """Push DVC-tracked files to remotes.
    
    Without --workers: pushes to all project-configured remotes sequentially.
    With --workers N: distributes push across N compute nodes via qxub.
    
    The parallel mode partitions files by hash prefix, ensuring no conflicts
    between workers. Each worker calls DVC's internal push directly.
    
    \b
    Examples:
        dt push                              # Push to all project remotes
        dt push data/processed.csv.dvc       # Push specific targets
        dt push --dry                        # Show what would be pushed
        dt push --dry -v                     # List all files to push
        dt push --jobs 8                     # DVC parallel uploads (single node)
        dt push --workers 16                 # Distributed via 16 qxub jobs
        dt push -w 8 -r myremote data.dvc    # Push target to remote using 8 jobs
        dt push -w 16 --no-wait              # Submit jobs and exit
    
    \b
    Additional DVC options (passed through):
        --jobs N    Number of parallel upload threads per worker
        --force     Force push even if remote has newer versions
    
    Run `dvc push --help` for additional DVC options.
    """
    from pathlib import Path
    
    try:
        # Dry run mode: show what would be pushed
        if dry:
            targets = [arg for arg in ctx.args if not arg.startswith('-')]
            manifest = push_mod.build_manifest(
                targets=targets if targets else None,
                remote=remote,
                verbose=False,
            )
            files = manifest.get('files', [])
            paths = manifest.get('paths', {})
            
            if not files:
                click.echo("Nothing to push.")
                return
            
            # Calculate total size if possible
            total_size = push_mod.get_files_size(files)
            
            if verbose:
                click.echo(f"Files to push ({len(files)} files, {push_mod.format_size(total_size)}):")
                for file_hash in sorted(files):
                    # Show path if available, with full hash in parentheses
                    path = paths.get(file_hash)
                    if path:
                        click.echo(f"  {path}  ({file_hash})")
                    else:
                        click.echo(f"  {file_hash}")
            else:
                click.echo(f"Would push {len(files)} file(s), {push_mod.format_size(total_size)}")
                if workers:
                    partitions = push_mod.partition_manifest(manifest, workers)
                    click.echo(f"\nWith {workers} workers:")
                    for worker_id, worker_files in partitions.items():
                        if worker_files:
                            click.echo(f"  Worker {worker_id}: {len(worker_files)} file(s)")
            return
        
        # Worker mode: called by submitted jobs
        if worker is not None and manifest is not None:
            manifest_path = Path(manifest)
            pushed, failed = push_mod.worker_push(
                manifest_dir=manifest_path,
                worker_id=worker,
                verbose=verbose,
            )
            if verbose:
                click.echo(f"Pushed: {pushed}, Failed: {failed}")
            if failed > 0:
                raise SystemExit(1)
            return
        
        # Parallel mode: submit qxub jobs
        if workers is not None:
            # Extract targets from extra args (non-option args)
            targets = [arg for arg in ctx.args if not arg.startswith('-')]
            
            # Extract qxub-relevant options if any (could extend later)
            qxub_args = None
            
            job_ids, manifest_dir = push_mod.parallel_push(
                targets=targets if targets else None,
                remote=remote,
                num_workers=workers,
                qxub_args=qxub_args,
                wait=not no_wait,
                verbose=verbose,
            )
            
            if no_wait and job_ids:
                click.echo(f"Submitted {len(job_ids)} job(s):")
                for job_id in job_ids:
                    click.echo(f"  {job_id}")
                if manifest_dir:
                    click.echo(f"Manifest: {manifest_dir}")
                click.echo(f"\nMonitor with: qxub monitor {' '.join(job_ids)}")
            elif job_ids:
                click.echo(f"Completed {len(job_ids)} job(s)")
            return
        
        # Simple mode: push to all project remotes
        # If remote specified, push to just that one
        if remote:
            success, output = push_mod.push_to_remote(remote, ctx.args)
            status = "✓" if success else "✗"
            click.echo(f"{status} {remote}")
            if output:
                for line in output.split('\n'):
                    if line.strip():
                        click.echo(f"  {line}")
            if not success:
                raise SystemExit(1)
            return
        
        # Default: push to all project remotes
        results = push_mod.push_all(ctx.args)
        
        all_success = True
        for remote_name, success, output in results:
            status = "✓" if success else "✗"
            click.echo(f"{status} {remote_name}")
            if output:
                for line in output.split('\n'):
                    if line.strip():
                        click.echo(f"  {line}")
            if not success:
                all_success = False
        
        if not all_success:
            raise SystemExit(1)
            
    except push_mod.PushError as e:
        raise click.ClickException(str(e))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.argument('targets', nargs=-1, type=click.Path(exists=True), required=True)
@click.option('-t', '--threads', type=int, default=None,
              help='Number of threads for checksum computation (default: 192).')
@click.option('--no-wait', is_flag=True,
              help='Submit job and exit without waiting for completion.')
@click.option('-v', '--verbose', is_flag=True,
              help='Show detailed progress.')
@click.option('--worker', is_flag=True, hidden=True,
              help='Internal: run dvc add directly (used by compute node).')
@click.pass_context
def add(ctx, targets, threads, no_wait, verbose, worker):
    """Add files or directories to DVC tracking via compute node.
    
    Submits `dvc add` to a compute node via qxub with parallel checksum
    computation. For local execution, use `dvc add` directly.
    
    \b
    Options:
        --threads/-t N    Use N threads for checksums (default: 48)
        --no-wait         Don't wait for job to complete
    
    \b
    Examples:
        dt add data/                      # Add directory (48 threads)
        dt add -t 24 large_file.csv       # Use 24 threads
        dt add --no-wait data/            # Submit and exit immediately
    
    \b
    Configuration:
        add.max_threads     Maximum threads allowed (default: 48)
        add.mem_per_thread  GB of RAM per thread (default: 4)
    
    All other options are passed through to `dvc add`.
    Run `dvc add --help` for additional options.
    """
    try:
        # Extract dvc_args from extra args (options only)
        dvc_args = [arg for arg in ctx.args if arg.startswith('-')]
        
        if worker:
            # Running on compute node - execute dvc add directly
            success = add_mod.add(
                targets=list(targets),
                threads=threads,
                dvc_args=dvc_args if dvc_args else None,
                verbose=verbose,
            )
            if not success:
                raise click.ClickException("dvc add failed")
        else:
            # Submit to compute node (single job for all targets)
            job_ids = add_mod.add_via_qxub(
                targets=list(targets),
                threads=threads,
                dvc_args=dvc_args if dvc_args else None,
                verbose=verbose,
                wait=not no_wait,
            )
            
            if no_wait and job_ids:
                click.echo(f"Submitted job: {job_ids[0]}")
                click.echo(f"Monitor with: qxub monitor {job_ids[0]}")
                
    except add_mod.AddError as e:
        raise click.ClickException(str(e))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.argument('targets', nargs=-1, type=click.Path())
@click.option('-v', '--verbose', is_flag=True, help='Show which cache is being checked')
@click.option('-c', '--cache', 'cache_name', help='Use only this cache (by name or path)')
@click.option('--no-refresh', is_flag=True, help='Skip refreshing temp clones (for offline use)')
@click.pass_context
def checkout(ctx, targets, verbose, cache_name, no_refresh):
    """Checkout DVC-tracked files, searching across multiple caches.
    
    Runs `dvc checkout` but searches for cached files across:
    
    \b
    1. The primary DVC cache (from .dvc/config or .dvc/config.local)
    2. All alternate caches configured via `dt cache add`
    
    This enables checking out files that exist in another project's cache
    or remote storage without copying them to the local cache first.
    
    For import .dvc files (created by `dvc import`), automatically clones
    the source repository to find a locally-accessible cache.
    
    Use --cache to checkout from a specific cache only. In this mode,
    checkout will fail if files are not found (no --allow-missing).
    
    All other options are passed through to `dvc checkout`.
    Run `dvc checkout --help` for additional options.
    
    \b
    Examples:
        dt checkout                        # Checkout all tracked files
        dt checkout data/processed.dvc     # Checkout specific targets
        dt checkout --force                # Force checkout (overwrite modified)
        dt checkout -v                     # Show cache search progress
        dt checkout --cache neochemo       # Checkout from specific cache only
        dt checkout --no-refresh           # Skip refreshing temp clones
    """
    try:
        results = checkout_mod.smart_checkout(
            targets=list(targets),
            extra_args=ctx.args,
            verbose=verbose,
            cache=cache_name,
            refresh=not no_refresh,
        )
        
        any_success = False
        any_failure = False
        
        for cache_path, success, output in results:
            if verbose:
                status = "✓" if success else "✗"
                click.echo(f"{status} {cache_path}")
            
            if success:
                any_success = True
                # Show output only for successful checkouts with content
                if output and 'M ' in output or 'A ' in output:
                    for line in output.split('\n'):
                        if line.strip():
                            click.echo(line)
            else:
                any_failure = True
                # Show errors only in verbose mode since --allow-missing
                # is expected to produce some "errors"
                if verbose and output:
                    for line in output.split('\n'):
                        if line.strip():
                            click.echo(f"  {line}")
        
        # Only fail if all caches failed
        if not any_success and any_failure:
            raise SystemExit(1)
            
    except checkout_mod.CheckoutError as e:
        raise click.ClickException(str(e))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.option('--workers', '-w', type=int, default=None,
              help='Number of parallel workers for distributed pull via qxub.')
@click.option('--worker', type=int, default=None,
              help='Worker ID (internal, used by submitted jobs).')
@click.option('--manifest', type=click.Path(exists=True), default=None,
              help='Manifest directory (internal, used by submitted jobs).')
@click.option('--remote', '-r', default=None,
              help='Pull from specific remote.')
@click.option('--no-wait', is_flag=True,
              help='Submit jobs and exit without waiting for completion.')
@click.option('--dry', '--dry-run', is_flag=True,
              help='Show what would be pulled without actually pulling.')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
@click.option('--no-refresh', is_flag=True, help='Skip refreshing temp clones (for offline use)')
@click.pass_context
def pull(ctx, workers, worker, manifest, remote, no_wait, dry, verbose, no_refresh):
    """Pull DVC-tracked files, handling imports automatically.
    
    For targets tracked by import .dvc files (those with deps.repo),
    uses dt checkout to fetch from the source repository's cache.
    For other targets, uses regular dvc pull.
    
    Without --workers: pulls using regular dvc pull.
    With --workers N: distributes pull across N compute nodes via qxub.
    
    The parallel mode partitions files by hash prefix, ensuring no conflicts
    between workers. Each worker calls DVC's internal transfer directly.
    
    \b
    Target resolution:
      - data.dvc        → check if data.dvc is an import
      - data/           → resolve to data.dvc if it exists
      - data/file.txt   → resolve to parent .dvc file if any
    
    All other options are passed through to `dvc pull`.
    Run `dvc pull --help` for additional options.
    
    \b
    Examples:
        dt pull                            # Pull all tracked files
        dt pull data/                      # Pull specific target
        dt pull --dry                      # Show what would be pulled
        dt pull --dry -v                   # List all files to pull
        dt pull -v                         # Show detailed progress
        dt pull --jobs 4                   # Parallel pull (passed to dvc)
        dt pull --workers 16               # Distributed via 16 qxub jobs
        dt pull -w 8 -r myremote data.dvc  # Pull target from remote using 8 jobs
        dt pull -w 16 --no-wait            # Submit jobs and exit
        dt pull --no-refresh               # Skip refreshing temp clones
    """
    from pathlib import Path
    
    try:
        # Worker mode: called by submitted jobs (no import handling needed)
        if worker is not None and manifest is not None:
            manifest_path = Path(manifest)
            pulled, failed = pull_mod.worker_pull(
                manifest_dir=manifest_path,
                worker_id=worker,
                verbose=verbose,
            )
            if verbose:
                click.echo(f"Pulled: {pulled}, Failed: {failed}")
            if failed > 0:
                raise SystemExit(1)
            return
        
        # --- Step 1: Discover and separate targets ---
        # Extract targets from extra args (non-option args)
        targets = [arg for arg in ctx.args if not arg.startswith('-')]
        
        # If no targets specified, find all .dvc files
        if not targets:
            if verbose:
                click.echo("Discovering .dvc files...")
            all_dvc_files = pull_mod.find_all_dvc_files()
            targets = [str(f) for f in all_dvc_files]
            if verbose:
                click.echo(f"  Found {len(targets)} .dvc files")
        
        if not targets:
            click.echo("No .dvc files found")
            return
        
        # Separate import targets from regular targets
        if verbose:
            click.echo("Resolving targets...")
        import_targets, regular_targets = pull_mod.separate_targets(targets, verbose)
        
        # --- Step 2: Handle imports first ---
        if import_targets:
            if dry:
                # Dry run: just report imports
                click.echo(f"Imports to checkout ({len(import_targets)}):")
                for target in import_targets:
                    dvc_file = pull_mod.resolve_to_dvc_file(target)
                    click.echo(f"  {target} → dt checkout {dvc_file}")
            else:
                # Actually checkout imports
                if verbose:
                    click.echo(f"\nHandling {len(import_targets)} import target(s)...")
                
                for target in import_targets:
                    dvc_file = pull_mod.resolve_to_dvc_file(target)
                    if dvc_file:
                        if verbose:
                            click.echo(f"  dt checkout {dvc_file}")
                        pull_mod.smart_checkout(
                            targets=[str(dvc_file)],
                            cache=None,
                            verbose=verbose,
                            refresh=not no_refresh,
                        )
        
        # --- Step 3: Handle regular targets (dry-run, parallel, or standard) ---
        if not regular_targets:
            if verbose:
                click.echo("\nNo regular targets to pull")
            return
        
        if dry:
            # Dry run mode for regular targets
            if verbose and import_targets:
                click.echo()  # Blank line after imports
            
            manifest_data = pull_mod.build_pull_manifest(
                targets=regular_targets,
                remote=remote,
                verbose=False,
            )
            files = manifest_data.get('files', [])
            paths = manifest_data.get('paths', {})
            
            if not files:
                click.echo("No regular files to pull.")
                return
            
            # Calculate total size if possible
            total_size = pull_mod.get_remote_files_size(files, remote=remote)
            
            if verbose:
                click.echo(f"Regular files to pull ({len(files)} files, {pull_mod.format_size(total_size)}):")
                for file_hash in sorted(files):
                    path = paths.get(file_hash)
                    if path:
                        click.echo(f"  {path}  ({file_hash})")
                    else:
                        click.echo(f"  {file_hash}")
            else:
                click.echo(f"Would pull {len(files)} regular file(s), {pull_mod.format_size(total_size)}")
                if workers:
                    partitions = pull_mod.partition_manifest(manifest_data, workers)
                    click.echo(f"\nWith {workers} workers:")
                    for worker_id, worker_files in partitions.items():
                        if worker_files:
                            click.echo(f"  Worker {worker_id}: {len(worker_files)} file(s)")
            return
        
        if workers is not None:
            # Parallel mode for regular targets only
            if verbose:
                click.echo(f"\nPulling {len(regular_targets)} regular target(s) with {workers} workers...")
            
            job_ids, manifest_dir = pull_mod.parallel_pull(
                targets=regular_targets,
                remote=remote,
                num_workers=workers,
                qxub_args=None,
                wait=not no_wait,
                verbose=verbose,
            )
            
            if no_wait and job_ids:
                click.echo(f"Submitted {len(job_ids)} job(s):")
                for job_id in job_ids:
                    click.echo(f"  {job_id}")
                click.echo(f"\nManifest: {manifest_dir}")
                click.echo("Monitor with: qxub monitor --summary " + " ".join(job_ids))
            return
        
        # Standard pull mode for regular targets
        if verbose:
            click.echo(f"\nPulling {len(regular_targets)} regular target(s)...")
        
        # Build dvc_args from remaining ctx.args (options only)
        dvc_args = [arg for arg in ctx.args if arg.startswith('-')]
        
        cmd = ['dvc', 'pull']
        if dvc_args:
            cmd.extend(dvc_args)
        cmd.extend(regular_targets)
        
        if verbose:
            click.echo(f"  Running: {' '.join(cmd)}")
        
        import subprocess
        result = subprocess.run(cmd)
        if result.returncode != 0:
            raise SystemExit(1)
            
    except pull_mod.CheckoutError as e:
        raise click.ClickException(str(e))
    except pull_mod.PullError as e:
        raise click.ClickException(str(e))


@cli.group()
def tmp():
    """Manage temporary repository clones.
    
    Temporary clones are stored in .dt/tmp/clones/ and used to access
    DVC configuration from remote repositories without full checkout.
    """
    pass


@tmp.command('clone')
@click.argument('repository')
@click.option('--owner', help='Override the GitHub owner for short names')
@click.option('--no-refresh', is_flag=True, help='Use cached clone without refreshing')
def tmp_clone(repository, owner, no_refresh):
    """Clone a repository into .dt/tmp/clones/.
    
    Creates a sparse clone with only .dvc/ directory checked out.
    If the clone already exists, it is refreshed by default.
    
    \b
    Examples:
        dt tmp clone otherproject
        dt tmp clone git@github.com:myorg/otherproject.git
        dt tmp clone otherproject --no-refresh
    """
    try:
        repo_path = tmp_mod.clone_repo(
            repo_spec=repository,
            owner=owner,
            refresh=not no_refresh,
            verbose=True,
        )
    except tmp_mod.TmpError as e:
        raise click.ClickException(str(e))


@tmp.command('list')
def tmp_list():
    """List cached repository clones.
    
    Shows all repositories currently cached in .dt/tmp/clones/.
    """
    repos = tmp_mod.list_repos()
    
    if not repos:
        click.echo("No cached repositories in .dt/tmp/clones/")
        return
    
    click.echo(f"Cached repositories in .dt/tmp/clones/:")
    for repo_id, path in repos:
        click.echo(f"  {repo_id}")


@tmp.command('clean')
@click.argument('repository', required=False)
@click.option('--owner', help='Override the GitHub owner for short names')
@click.option('--all', 'clean_all', is_flag=True, help='Remove all cached clones')
def tmp_clean(repository, owner, clean_all):
    """Remove cached repository clones.
    
    Without arguments, shows help. Use --all to remove all clones,
    or specify a repository to remove just that one.
    
    \b
    Examples:
        dt tmp clean neochemo     # Remove specific repo
        dt tmp clean --all        # Remove all cached repos
    """
    if not repository and not clean_all:
        raise click.UsageError("Specify a repository or use --all to clean all.")
    
    try:
        if clean_all:
            removed = tmp_mod.clean_repos()
        else:
            removed = tmp_mod.clean_repos(repo_spec=repository, owner=owner)
        
        if removed:
            for repo_id in removed:
                click.echo(f"Removed {repo_id}")
        else:
            click.echo("No cached repositories to remove.")
    except tmp_mod.TmpError as e:
        raise click.ClickException(str(e))


@cli.command('import')
@click.argument('repository')
@click.argument('path')
@click.option('-o', '--out', help='Destination path to download files to')
@click.option('--owner', help='Override the GitHub owner for short names')
@click.option('--no-checkout', is_flag=True, help='Skip checkout after import')
@click.option('--no-refresh', is_flag=True, help='Skip refreshing temp clone (for offline use)')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def import_cmd(repository, path, out, owner, no_checkout, no_refresh, verbose):
    """Import DVC-tracked data from another repository.
    
    Creates a .dvc file pointing to data in REPOSITORY at PATH,
    then checks out the data from locally-accessible caches.
    
    Unlike `dvc import`, this does not require network access to
    the remote storage. Instead, it uses cache paths discovered
    via `dt cache add-from`.
    
    \b
    Examples:
        dt import neochemo data/processed
        dt import neochemo data/samples.h5ad -o my_samples.h5ad
        dt import git@github.com:lab/project.git results/model
        dt import neochemo data/large --no-checkout
        dt import neochemo data/file --no-refresh
    """
    try:
        dvc_file, cache_path = import_mod.import_data(
            repository=repository,
            path=path,
            out=out,
            owner=owner,
            checkout=not no_checkout,
            verbose=verbose,
            refresh=not no_refresh,
        )
        
        click.echo(f"Created {dvc_file}")
        if cache_path:
            click.echo(f"Using cache: {cache_path}")
            
    except import_mod.ImportError as e:
        raise click.ClickException(str(e))


@cli.group()
def offline():
    """Manage offline mode for compute nodes without internet.
    
    Offline mode redirects Git URL lookups to local temporary clones,
    enabling DVC operations on compute nodes without internet access.
    
    \b
    Typical workflow:
        # On login node (has internet)
        dt tmp clone source-repo
        dt offline enable
        
        # Submit job to compute node - git operations use local clones
        
        # Later, refresh clones if needed
        dt offline disable
        dt tmp refresh --all
        dt offline enable
    """
    pass


@offline.command('enable')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def offline_enable(verbose):
    """Enable offline mode for Git repos and DVC remotes.
    
    Sets up:
    - Git config to redirect remote repository URLs to local temp clones
    - DVC config to use local paths for SSH-based remotes
    
    After enabling, DVC operations that would normally require
    internet access will use local clones and paths.
    
    \b
    Examples:
        dt offline enable
        dt offline enable -v
    """
    try:
        enabled_repos, enabled_remotes = offline_mod.enable(verbose=verbose)
        
        total = len(enabled_repos) + len(enabled_remotes)
        
        if total > 0:
            click.echo(f"Offline mode enabled")
            if enabled_repos:
                click.echo(f"  Git redirects: {len(enabled_repos)} repo(s)")
                if not verbose:
                    for repo_id in enabled_repos:
                        click.echo(f"    {repo_id}")
            if enabled_remotes:
                click.echo(f"  DVC remotes: {len(enabled_remotes)} remote(s)")
                if not verbose:
                    for name in enabled_remotes:
                        click.echo(f"    {name}")
        else:
            click.echo("No repos or remotes to enable")
            
    except offline_mod.OfflineError as e:
        raise click.ClickException(str(e))


@offline.command('disable')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def offline_disable(verbose):
    """Disable offline mode for Git repos and DVC remotes.
    
    Removes:
    - Git config entries that redirect URLs to local clones
    - DVC config entries that override SSH remotes with local paths
    
    After disabling, Git and DVC operations will use original remote URLs.
    
    \b
    Examples:
        dt offline disable
        dt offline disable -v
    """
    try:
        disabled_repos, disabled_remotes = offline_mod.disable(verbose=verbose)
        
        total = len(disabled_repos) + len(disabled_remotes)
        
        if total > 0:
            click.echo(f"Offline mode disabled")
            if disabled_repos:
                click.echo(f"  Git redirects: {len(disabled_repos)} repo(s)")
            if disabled_remotes:
                click.echo(f"  DVC remotes: {len(disabled_remotes)} remote(s)")
        else:
            click.echo("Offline mode was not enabled")
            
    except offline_mod.OfflineError as e:
        raise click.ClickException(str(e))


@offline.command('status')
def offline_status():
    """Show offline mode status.
    
    Displays:
    - Temporary clones available and their redirect status
    - DVC remotes that can use local paths
    
    \b
    Examples:
        dt offline status
    """
    try:
        info = offline_mod.status()
        
        if info['enabled']:
            click.echo("Offline mode: ENABLED")
        else:
            click.echo("Offline mode: DISABLED")
        
        # Git clone section
        click.echo()
        if info['clones']:
            click.echo("Git redirects (temp clones):")
            for repo_id in info['clones']:
                if repo_id in info['active']:
                    click.echo(f"  ✓ {repo_id}")
                else:
                    click.echo(f"  ○ {repo_id}")
        else:
            click.echo("Git redirects: none available")
            click.echo("  Use 'dt tmp clone <repo>' to create temp clones.")
        
        # DVC remote section
        click.echo()
        remote_info = info.get('remotes', {})
        available = remote_info.get('available', [])
        active = remote_info.get('active', [])
        
        if available:
            click.echo("DVC remotes (SSH → local):")
            for name in available:
                if name in active:
                    click.echo(f"  ✓ {name}")
                else:
                    click.echo(f"  ○ {name}")
        else:
            click.echo("DVC remotes: no SSH remotes with local paths")
        
        # Hints
        if info['missing'] or (available and set(available) - set(active)):
            click.echo()
            click.echo("Run 'dt offline enable' to enable all available.")
            
    except offline_mod.OfflineError as e:
        raise click.ClickException(str(e))


if __name__ == '__main__':
    cli()
