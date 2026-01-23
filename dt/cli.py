"""Command-line interface for DVC Tools."""

import click

from . import config as cfg
from . import clone as clone_mod
from . import init as init_mod
from . import cache as cache_mod
from . import remote as remote_mod
from . import doctor as doctor_mod
from . import push as push_mod
from . import checkout as checkout_mod
from . import tmp as tmp_mod


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
        dt clone neochemo
    
    is equivalent to:
    
    \b
        dt clone git@github.com:swarbricklab/neochemo.git
    
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
@click.pass_context
def push(ctx):
    """Push DVC-tracked files to all project-configured remotes.
    
    Runs `dvc push` for each remote configured at project or local scope,
    skipping remotes inherited from user or system config.
    
    All options and arguments are passed through to `dvc push`.\n
    Run `dvc push --help` for additional options than can be passed through.
    
    \b
    Examples:
        dt push                          # Push to all project remotes
        dt push data/processed.csv.dvc   # Push specific targets
        dt push --jobs 8                 # Push using 8 parallel jobs
    """
    try:
        results = push_mod.push_all(ctx.args)
        
        all_success = True
        for remote, success, output in results:
            status = "✓" if success else "✗"
            click.echo(f"{status} {remote}")
            if output:
                # Indent output lines
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
@click.argument('targets', nargs=-1, type=click.Path())
@click.option('-v', '--verbose', is_flag=True, help='Show which cache is being checked')
@click.pass_context
def checkout(ctx, targets, verbose):
    """Checkout DVC-tracked files, searching across multiple caches.
    
    Runs `dvc checkout` but searches for cached files across:
    
    \b
    1. The primary DVC cache (from .dvc/config or .dvc/config.local)
    2. All alternate caches configured via `dt cache add`
    
    This enables checking out files that exist in another project's cache
    or remote storage without copying them to the local cache first.
    
    All other options are passed through to `dvc checkout`.
    Run `dvc checkout --help` for additional options.
    
    \b
    Examples:
        dt checkout                        # Checkout all tracked files
        dt checkout data/processed.dvc     # Checkout specific targets
        dt checkout --force                # Force checkout (overwrite modified)
        dt checkout -v                     # Show cache search progress
    """
    try:
        results = checkout_mod.checkout(
            targets=list(targets),
            extra_args=ctx.args,
            verbose=verbose,
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


@cli.group()
def tmp():
    """Manage temporary repository clones.
    
    Temporary clones are stored in .dt/tmp/ and used to access
    DVC configuration from remote repositories without full checkout.
    """
    pass


@tmp.command('clone')
@click.argument('repository')
@click.option('--owner', help='Override the GitHub owner for short names')
@click.option('--no-refresh', is_flag=True, help='Use cached clone without refreshing')
def tmp_clone(repository, owner, no_refresh):
    """Clone a repository into .dt/tmp/.
    
    Creates a sparse clone with only .dvc/ directory checked out.
    If the clone already exists, it is refreshed by default.
    
    \b
    Examples:
        dt tmp clone neochemo
        dt tmp clone git@github.com:swarbricklab/neochemo.git
        dt tmp clone neochemo --no-refresh
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
    
    Shows all repositories currently cached in .dt/tmp/.
    """
    repos = tmp_mod.list_repos()
    
    if not repos:
        click.echo("No cached repositories in .dt/tmp/")
        return
    
    click.echo(f"Cached repositories in .dt/tmp/:")
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


if __name__ == '__main__':
    cli()
