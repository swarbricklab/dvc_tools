"""Command-line interface for DVC Tools."""

import click

from . import config as cfg
from . import clone as clone_mod
from . import init as init_mod
from . import cache as cache_mod
from . import remote as remote_mod
from . import doctor as doctor_mod


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


if __name__ == '__main__':
    cli()
