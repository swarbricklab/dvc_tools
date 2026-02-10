"""Command-line interface for DVC Tools."""

import click

from . import config as cfg
from . import clone as clone_mod
from . import errors
from . import init as init_mod
from . import cache as cache_mod
from . import remote as remote_mod
from . import doctor as doctor_mod
from . import auth as auth_mod
from . import push as push_mod
from . import add as add_mod
from . import data_status as data_status_mod
from . import fetch as fetch_mod
from . import tmp as tmp_mod
from . import import_data as import_mod
from . import migrate as migrate_mod
from . import pull as pull_mod
from . import offline as offline_mod
from . import summary as summary_mod
from . import du as du_mod
from . import ls as ls_mod
from . import index as index_mod
from . import cache_index as cache_index_mod
from . import update as update_mod
from . import utils


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
@click.option('--team', help='GitHub team for access (only valid if owner is an org)')
@click.option('--cache-root', help='Override the cache root directory')
@click.option('--remote-root', help='Override the remote root directory')
@click.option('--no-git', is_flag=True, help='Skip git initialization')
@click.option('--no-dvc', is_flag=True, help='Skip DVC initialization')
@click.option('--no-cache', is_flag=True, help='Skip cache setup')
@click.option('--no-remote', is_flag=True, help='Skip remote setup')
def init(name, owner, team, cache_root, remote_root, no_git, no_dvc, no_cache, no_remote):
    """Initialize a new DVC project with proper cache and remote setup.
    
    This command creates a complete DVC project with git, DVC, external cache,
    and remote storage properly configured for HPC environments.
    """
    try:
        init_mod.init_project(
            name=name,
            owner=owner,
            team=team,
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
@click.option('--pull', 'do_pull', is_flag=True, help='Run dt pull after cloning to fetch data')
def clone(repository, path, owner, no_init, no_submodules, cache_name, remote_name, shallow, do_pull):
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
        target_dir = clone_mod.clone_repository(
            repository=repository,
            path=path,
            owner=owner,
            no_submodules=no_submodules,
            cache_name=cache_name,
            remote_name=remote_name,
            shallow=shallow,
            do_pull=do_pull,
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


@cache.command('rm')
@click.argument('targets', nargs=-1, required=True)
@click.option('--dry', is_flag=True, help='Show what would be deleted without deleting')
@click.option('--size', is_flag=True, help='Report file sizes')
@click.option('-v', '--verbose', is_flag=True, help='Print detailed progress')
@click.option('--force', '-f', is_flag=True, help='Delete even if files are not in remote')
def cache_rm(targets, dry, size, verbose, force):
    """Remove cached files for specified targets.
    
    Deletes cache files associated with the target(s) while leaving
    the workspace unchanged. Only affects the primary cache; alternate
    caches are never modified.
    
    By default, refuses to delete files that are not in the remote
    (to prevent data loss). Use --force to override.
    
    \b
    Examples:
        dt cache rm data/large_file.csv
        dt cache rm --dry data/
        dt cache rm --dry --size data/
        dt cache rm --force data/uncommitted.csv
    """
    try:
        result = cache_mod.remove_cache_files(
            targets=list(targets),
            dry_run=dry,
            show_size=size,
            verbose=verbose,
            force=force,
        )
    except cache_mod.CacheError as e:
        raise click.ClickException(str(e))
    
    deleted = result['deleted']
    missing = result['missing']
    failed = result['failed']
    not_in_remote = result.get('not_in_remote', [])
    total_size = result['total_size']
    blocked = result.get('blocked', False)
    
    # Handle blocked state (files not in remote)
    if blocked:
        click.echo("ERROR: Some files are not in the remote and would be permanently lost!", err=True)
        click.echo("\nFiles not in remote:", err=True)
        for workspace_path, file_hash in not_in_remote:
            click.echo(f"  {workspace_path}", err=True)
        click.echo(f"\nUse --force to delete anyway, or push these files first.", err=True)
        raise click.ClickException(f"Refusing to delete {len(not_in_remote)} file(s) not in remote")
    
    # Report results
    if dry:
        if deleted:
            click.echo("Would delete:")
            for workspace_path, file_hash, file_size in deleted:
                if size:
                    click.echo(f"  {workspace_path}  ({utils.format_size(file_size)})")
                else:
                    click.echo(f"  {workspace_path}")
            if size:
                click.echo(f"\nTotal: {utils.format_size(total_size)}")
            # Warn about files not in remote (only in verbose or if force was used)
            if not_in_remote and verbose:
                click.echo(f"\nNote: {len(not_in_remote)} file(s) not verified in remote (--force used)")
        else:
            click.echo("No cached files found for the specified targets.")
    else:
        if deleted:
            if not verbose:
                # Summary mode
                click.echo(f"Deleted {len(deleted)} file(s) from cache.")
            if size:
                click.echo(f"Freed: {utils.format_size(total_size)}")
            # Warn if force was used for files not in remote
            if not_in_remote:
                click.echo(f"\nWarning: {len(not_in_remote)} file(s) were not in remote", err=True)
        else:
            click.echo("No cached files found for the specified targets.")
    
    # Report missing files
    if missing and verbose:
        click.echo(f"\nNot in cache ({len(missing)}):")
        for workspace_path, file_hash in missing:
            click.echo(f"  {workspace_path}")
    
    # Report failures
    if failed:
        click.echo(f"\nFailed to delete ({len(failed)}):", err=True)
        for workspace_path, file_hash, error in failed:
            click.echo(f"  {workspace_path}: {error}", err=True)
        raise click.ClickException(f"Failed to delete {len(failed)} file(s)")


@cache.command('validate')
@click.argument('targets', nargs=-1)
@click.option('--fix', is_flag=True, help='Delete corrupted files')
@click.option('-v', '--verbose', is_flag=True, help='Show result for each file')
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON')
@click.option('--no-progress', is_flag=True, help='Disable progress counter')
def cache_validate(targets, fix, verbose, json_output, no_progress):
    """Validate cache files by checking MD5 checksums.
    
    Detects corrupted cache files (e.g., from interrupted transfers)
    by computing their actual MD5 hash and comparing to the expected
    hash from the filename.
    
    Without targets, validates the entire cache. With targets, validates
    only the files associated with those workspace paths.
    
    Use --fix to delete corrupted files. For files inside directories,
    use `dt pull --force` after fixing to re-fetch (it will delete the
    .dir manifest to trigger a fresh pull).
    
    \b
    Examples:
        dt cache validate                  # Validate entire cache
        dt cache validate data/            # Validate files for data/
        dt cache validate -v               # Verbose output
        dt cache validate --fix            # Delete corrupted files
        dt cache validate --fix data/      # Fix only specific targets
    """
    import json
    
    try:
        result = cache_mod.validate_cache(
            targets=list(targets) if targets else None,
            fix=fix,
            verbose=verbose,
            progress=not no_progress and not json_output,
        )
    except cache_mod.CacheError as e:
        raise click.ClickException(str(e))
    
    valid = result['valid']
    corrupted = result['corrupted']
    missing = result['missing']
    fixed = result['fixed']
    dir_fixed = result['dir_fixed']
    errors = result['errors']
    
    if json_output:
        output = {
            'valid_count': len(valid),
            'corrupted': [
                {'path': p, 'hash': h, 'expected': e, 'actual': a}
                for p, h, e, a in corrupted
            ],
            'missing': [{'path': p, 'hash': h} for p, h in missing],
            'fixed': [{'path': p, 'hash': h} for p, h in fixed],
            'dir_fixed': dir_fixed,
            'errors': [{'path': p, 'error': e} for p, e in errors],
        }
        click.echo(json.dumps(output, indent=2))
        return
    
    # Summary
    total = len(valid) + len(corrupted)
    
    if total == 0 and missing:
        # Only missing files (not in cache)
        click.echo(f"No cached files to validate ({len(missing)} file(s) not in cache)")
        if verbose:
            for workspace_path, file_hash in missing:
                click.echo(f"  {workspace_path}")
    elif not corrupted and not errors:
        click.echo(f"✓ All {total} files validated successfully")
        if missing:
            click.echo(f"  ({len(missing)} file(s) not in cache)")
    else:
        click.echo(f"Validated {total} files:")
        click.echo(f"  ✓ Valid: {len(valid)}")
        if corrupted:
            click.echo(f"  ✗ Corrupted: {len(corrupted)}")
        if missing:
            click.echo(f"  - Not in cache: {len(missing)}")
        if errors:
            click.echo(f"  ! Errors: {len(errors)}")
    
    # Details for corrupted files
    if corrupted and not verbose:
        click.echo("\nCorrupted files:")
        for workspace_path, file_hash, expected, actual in corrupted:
            click.echo(f"  {workspace_path}")
            if verbose:
                click.echo(f"    Expected: {expected}")
                click.echo(f"    Actual:   {actual}")
    
    # Report fixes
    if fix and fixed:
        click.echo(f"\nFixed: deleted {len(fixed)} corrupted file(s)")
        if dir_fixed:
            click.echo(f"\n{len(dir_fixed)} file(s) were inside directories.")
            click.echo("Run 'dt pull --force' to re-fetch those directories.")
        else:
            click.echo("\nRun 'dt pull' to re-fetch the deleted files.")
    elif corrupted and not fix:
        click.echo("\nRun with --fix to delete corrupted files, then 'dt pull' to re-fetch.")
    
    # Report errors
    if errors:
        click.echo(f"\nErrors ({len(errors)}):", err=True)
        for path, error in errors:
            click.echo(f"  {path}: {error}", err=True)
    
    # Exit with error if corruption found and not fixed
    if corrupted and not fix:
        raise SystemExit(1)


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
@click.option('--all', 'show_all', is_flag=True,
              help='Show remotes from all config scopes, including local overrides')
def remote_list(repository, owner, show_all):
    """List DVC remotes for a repository.
    
    Without arguments, lists remotes for the current repository.
    With a repository argument, lists remotes for that remote repository.
    
    By default shows only remotes defined in the shared project config
    (.dvc/config), excluding local overrides (.dvc/config.local).
    Use --all to include every config scope.
    
    \b
    Examples:
        dt remote list                    # Project-scope remotes
        dt remote list --all              # Include local overrides
        dt remote list otherproject       # List remotes for another repo
        dt remote list myorg/otherproject # List with explicit owner
    """
    project_only = not show_all
    if repository:
        # List remotes from a remote repository
        try:
            remotes = remote_mod.list_remotes_from_repo(
                repository, owner=owner, project_only=project_only)
        except Exception as e:
            raise click.ClickException(str(e))
    else:
        # List remotes for current repository
        remotes = remote_mod.list_remotes(project_only=project_only)
    
    if not remotes:
        click.echo("No remotes configured.")
        return
    
    for name, url, is_default in remotes:
        default_marker = " (default)" if is_default else ""
        click.echo(f"{name}{default_marker}: {url}")


@cli.command()
@click.argument('path', type=click.Path())
@click.option('--old', 'old_rev', default='HEAD',
              help='The older revision to compare (default: HEAD)')
@click.option('--new', 'new_rev', default=None,
              help='The newer revision to compare (default: workspace)')
@click.option('-o', '--output', 'output_format',
              type=click.Choice(['terminal', 'json', 'html', 'md']),
              default='terminal', help='Output format')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def diff(path, old_rev, new_rev, output_format, verbose):
    """Show content differences between versions of a DVC-tracked file.
    
    Compares the actual content of files (not just checksums) between
    git revisions. Uses format-specific handlers for smart diffing
    (e.g., daff for CSV files).
    
    \b
    Examples:
        dt diff data.csv                       # Compare HEAD → workspace
        dt diff data.csv --old HEAD~1          # Compare HEAD~1 → workspace
        dt diff data.csv --old v1.0 --new v2.0 # Compare two tags
        dt diff data.csv -o html > diff.html   # HTML output
    
    \b
    Supported formats:
        CSV/TSV: Uses daff for tabular diff (pip install daff)
        Other: Shows size/metadata comparison
    """
    from . import diff as diff_mod
    
    try:
        result = diff_mod.diff(
            path=path,
            old_rev=old_rev,
            new_rev=new_rev,
            output_format=output_format,
            verbose=verbose,
        )
        click.echo(result)
    except diff_mod.DiffError as e:
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


# =============================================================================
# dt auth
# =============================================================================

@cli.group()
def auth():
    """Verify and diagnose access to storage backends.
    
    Discovers every storage endpoint the current project relies on
    (filesystem, SSH, S3/R2, GCS, git) and can test access to each.
    """
    pass


@auth.command('list')
@click.option('--type', 'types', multiple=True,
              type=click.Choice(sorted(auth_mod.ENDPOINT_TYPES), case_sensitive=False),
              help='Filter to specific endpoint type(s). Repeat for multiple.')
@click.option('--repo', 'repo_url', default=None,
              help='Discover endpoints for a remote repo (URL or short name).')
@click.option('--json', 'as_json', is_flag=True, help='Output as JSON')
@click.option('-v', '--verbose', is_flag=True, help='Show discovery progress')
def auth_list(types, repo_url, as_json, verbose):
    """Discover every storage endpoint the project uses.
    
    Scans DVC remotes, dt config (cache.root, remote.root), git remotes,
    and import .dvc files to find all endpoints. For imports, also discovers
    DVC remotes of the source repository via tmp clones.

    Use --repo to discover endpoints for a repo you haven't cloned:

    \b
      dt auth list --repo git@github.com:org/data-repo.git
    """
    try:
        type_filter = set(types) if types else None
        if repo_url:
            endpoints = auth_mod.discover_endpoints_from_repo(
                repo_url, type_filter=type_filter, verbose=verbose,
            )
        else:
            endpoints = auth_mod.discover_endpoints(
                type_filter=type_filter,
                verbose=verbose,
            )
        if as_json:
            click.echo(auth_mod.format_endpoints_json(endpoints))
        else:
            click.echo(auth_mod.format_endpoints(endpoints))
    except auth_mod.AuthError as e:
        raise click.ClickException(str(e))


@auth.command('whoami')
@click.option('--detect', is_flag=True,
              help='Probe external tools (gh, gcloud, aws) to auto-detect identities.')
@click.option('--save', is_flag=True,
              help='Save detected identities to user config (implies --detect).')
@click.option('--json', 'as_json', is_flag=True, help='Output as JSON')
def auth_whoami(detect, save, as_json):
    """Show current user identities across systems.

    Without flags, displays the local username and any identities stored
    in dt config.

    \b
      dt auth whoami              # show stored identities
      dt auth whoami --detect     # probe gh, gcloud, aws for active accounts
      dt auth whoami --save       # detect + save to user config
    """
    if save:
        detect = True

    if detect:
        click.echo(click.style('Detecting identities...', dim=True))
        detected = auth_mod.detect_identities()
        stored = auth_mod.get_identities()

        if as_json:
            click.echo(auth_mod.format_identities_json(detected))
        else:
            comparisons = auth_mod.compare_identities(stored, detected)
            click.echo(auth_mod.format_whoami_comparison(comparisons))

            # If there are new or mismatched detections, suggest --save
            if not save:
                saveable = [
                    note for _, _, note in comparisons
                    if note in ('detected only', 'mismatch')
                ]
                if saveable:
                    click.echo(click.style(
                        'Run `dt auth whoami --save` to save detected '
                        'values to user config.',
                        dim=True,
                    ))

        if save:
            count = auth_mod.save_detected_identities(detected)
            if count:
                click.echo(click.style(
                    f'✓ Saved {count} identity value(s) to user config.',
                    fg='green',
                ))
            else:
                click.echo(click.style(
                    'Nothing new to save — config is up to date.',
                    dim=True,
                ))
    else:
        identities = auth_mod.get_identities()
        if as_json:
            click.echo(auth_mod.format_identities_json(identities))
        else:
            click.echo(auth_mod.format_identities(identities))


@auth.command('check')
@click.option('--type', 'types', multiple=True,
              type=click.Choice(sorted(auth_mod.ENDPOINT_TYPES), case_sensitive=False),
              help='Only check specific endpoint type(s). Repeat for multiple.')
@click.option('--repo', 'repo_url', default=None,
              help='Check endpoints for a remote repo (URL or short name).')
@click.option('--user', 'check_user', default=None,
              help="Check access from another user's perspective (filesystem + GitHub).")
@click.option('--json', 'as_json', is_flag=True, help='Output as JSON')
@click.option('-v', '--verbose', is_flag=True,
              help='Show per-subdirectory detail for filesystem checks')
def auth_check(types, repo_url, check_user, as_json, verbose):
    """Test access to each discovered endpoint.

    Discovers all endpoints (like ``dt auth list``) then runs a
    connectivity / permission check for each one.

    Use --user to check access from another user's perspective:

    \b
      dt auth check --user alice        # check all endpoints for alice
      dt auth check --user alice --type filesystem  # filesystem only

    Use --repo to check endpoints for a repo you haven't cloned:

    \b
      dt auth check --repo git@github.com:org/data-repo.git
    """
    try:
        type_filter = set(types) if types else None
        if check_user:
            click.echo(click.style(
                f'Checking access for user: {check_user}', dim=True,
            ))
        if repo_url:
            endpoints = auth_mod.discover_endpoints_from_repo(
                repo_url, type_filter=type_filter, verbose=verbose,
            )
        else:
            endpoints = None
        results = auth_mod.check_endpoints(
            endpoints=endpoints,
            type_filter=type_filter if not repo_url else None,
            verbose=verbose,
            user=check_user,
        )
        if as_json:
            click.echo(auth_mod.format_check_results_json(results))
        else:
            click.echo(auth_mod.format_check_results(results))

        # Exit non-zero if any checks failed
        if any(r.status == auth_mod.STATUS_FAIL for r in results):
            raise SystemExit(1)
    except auth_mod.AuthError as e:
        raise click.ClickException(str(e))


@auth.command('request')
@click.option('--type', 'types', multiple=True,
              type=click.Choice(sorted(auth_mod.ENDPOINT_TYPES), case_sensitive=False),
              help='Only include failures for specific endpoint type(s).')
@click.option('--repo', 'repo_url', default=None,
              help='Generate request for a remote repo (URL or short name).')
@click.option('--format', 'fmt', default='text',
              type=click.Choice(['text', 'markdown', 'json'], case_sensitive=False),
              help='Output format (default: text).')
@click.option('--send', 'send_via', default=None, is_flag=False,
              flag_value='auto',
              type=click.Choice(['slack', 'email'], case_sensitive=False),
              help='Send the request (slack, email, or omit value to auto-detect).')
@click.option('-v', '--verbose', is_flag=True,
              help='Show verbose check detail')
def auth_request(types, repo_url, fmt, verbose, send_via):
    """Generate an access-request template from check failures.

    Runs ``dt auth check`` internally, collects failures, and produces
    a template that can be sent to an administrator or pasted into a
    support ticket.

    Use --send to deliver the request directly:

    \b
      dt auth request --send          # auto-detect (Slack > email)
      dt auth request --send slack    # send to Slack webhook
      dt auth request --send email    # send via mail command

    Use --repo for a repo you haven't cloned:

    \b
      dt auth request --repo git@github.com:org/data-repo.git
    """
    try:
        type_filter = set(types) if types else None
        if repo_url:
            endpoints = auth_mod.discover_endpoints_from_repo(
                repo_url, type_filter=type_filter, verbose=verbose,
            )
            req = auth_mod.generate_request(
                type_filter=None, verbose=verbose,
                endpoints=endpoints,
            )
        else:
            req = auth_mod.generate_request(
                type_filter=type_filter,
                verbose=verbose,
            )

        if fmt == 'json':
            click.echo(auth_mod.format_request_json(req))
        elif fmt == 'markdown':
            click.echo(auth_mod.format_request_markdown(req))
        else:
            click.echo(auth_mod.format_request_text(req))

        # Send if requested
        if send_via is not None:
            if not req.items:
                click.echo('Nothing to send — all endpoints are accessible.')
            else:
                method = None if send_via == 'auto' else send_via
                msg = auth_mod.send_request(req, method=method)
                click.echo(click.style(f'\n✓ {msg}', fg='green'))

        # Exit non-zero if there are items to request
        if req.items:
            raise SystemExit(1)
    except auth_mod.AuthError as e:
        raise click.ClickException(str(e))


# -- dt auth teams ----------------------------------------------------------

@auth.group('teams')
def auth_teams():
    """Manage GitHub team access for repositories."""
    pass


@auth_teams.command('repo')
@click.argument('repo_url')
@click.option('--json', 'as_json', is_flag=True, help='Output as JSON')
def auth_teams_repo(repo_url, as_json):
    """List GitHub teams with access to a repository.

    REPO_URL is a GitHub repository URL (SSH or HTTPS) or a short name.

    \b
      dt auth teams repo git@github.com:org/data-repo.git
      dt auth teams repo neochemo
    """
    try:
        teams = auth_mod.list_repo_teams(repo_url)
        if as_json:
            click.echo(auth_mod.format_teams_json(teams))
        else:
            click.echo(auth_mod.format_teams(
                teams, header=f'Teams with access to {repo_url}',
            ))
    except auth_mod.AuthError as e:
        raise click.ClickException(str(e))


@auth_teams.command('user')
@click.argument('username')
@click.option('--org', required=True,
              help='GitHub organisation to search.')
@click.option('--json', 'as_json', is_flag=True, help='Output as JSON')
def auth_teams_user(username, org, as_json):
    """List GitHub teams that a user belongs to.

    \b
      dt auth teams user alice --org myorg
    """
    try:
        teams = auth_mod.list_user_teams(username, org=org)
        if as_json:
            click.echo(auth_mod.format_teams_json(teams))
        else:
            click.echo(auth_mod.format_teams(
                teams, header=f"Teams for '{username}' in {org}",
            ))
    except auth_mod.AuthError as e:
        raise click.ClickException(str(e))


@auth_teams.command('add-to-repo')
@click.argument('team_slug')
@click.argument('repo_url')
@click.option('--permission', default='push',
              type=click.Choice(['pull', 'push', 'admin', 'maintain', 'triage'],
                                case_sensitive=False),
              help='Permission level (default: push).')
@click.option('--dry', is_flag=True,
              help='Show what would be done without making changes.')
def auth_teams_add_to_repo(team_slug, repo_url, permission, dry):
    """Add a GitHub team to a repository.

    \b
      dt auth teams add-to-repo data-team git@github.com:org/repo.git
      dt auth teams add-to-repo data-team neochemo
      dt auth teams add-to-repo data-team neochemo --permission pull
      dt auth teams add-to-repo data-team neochemo --dry
    """
    try:
        repo_url = auth_mod.resolve_repo_url(repo_url)
        parsed = auth_mod._parse_github_owner_repo(repo_url)
        if parsed is None:
            raise click.ClickException(f'Not a GitHub URL: {repo_url}')
        owner, repo = parsed
        if dry:
            click.echo(
                f"Would grant '{permission}' access for team '{team_slug}' "
                f"to {owner}/{repo}"
            )
            click.echo(
                f"  gh api orgs/{owner}/teams/{team_slug}/repos/{owner}/{repo} "
                f"-X PUT -f permission={permission}"
            )
        else:
            msg = auth_mod.add_team_to_repo(repo_url, team_slug, permission)
            click.echo(click.style(f'✓ {msg}', fg='green'))
    except auth_mod.AuthError as e:
        raise click.ClickException(str(e))


@auth_teams.command('add-user')
@click.argument('username')
@click.argument('team_slug')
@click.option('--org', required=True,
              help='GitHub organisation.')
@click.option('--dry', is_flag=True,
              help='Show what would be done without making changes.')
def auth_teams_add_user(username, team_slug, org, dry):
    """Add a user to a GitHub team.

    \b
      dt auth teams add-user alice data-team --org myorg
      dt auth teams add-user alice data-team --org myorg --dry
    """
    try:
        if dry:
            click.echo(
                f"Would add '{username}' to team '{org}/{team_slug}'"
            )
            click.echo(
                f"  gh api orgs/{org}/teams/{team_slug}/memberships/{username} "
                f"-X PUT"
            )
        else:
            msg = auth_mod.add_user_to_team(org, team_slug, username)
            click.echo(click.style(f'✓ {msg}', fg='green'))
    except auth_mod.AuthError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument('targets', nargs=-1)
@click.option('-h', '--human-readable', 'human', is_flag=True,
              help='Print sizes in human-readable format (K, M, G)')
@click.option('-d', '--max-depth', type=int, default=None,
              help='Limit output to N levels of depth')
@click.option('-s', '--summarize', is_flag=True,
              help='Show only the total (equivalent to -d 0)')
@click.option('--inodes', is_flag=True,
              help='Count number of files instead of bytes')
@click.option('-c', '--total', 'show_total', is_flag=True,
              help='Show a grand total line at the end')
@click.option('--cached/--expected', default=True,
              help='Show cached sizes (default) or expected sizes from metadata')
def du(targets, human, max_depth, summarize, inodes, show_total, cached):
    """Report disk usage for DVC-tracked files.
    
    Similar to the standard `du` command, but for DVC-tracked data.
    Output is sorted by size (largest last).
    
    \b
    Examples:
        dt du                    # All tracked files
        dt du -h                 # Human-readable sizes
        dt du -s -h              # Summary total only
        dt du -d 1 -h            # One level deep
        dt du --inodes           # Count files
        dt du --expected -h      # Expected sizes (not just cached)
    """
    try:
        results = du_mod.calculate_du(
            targets=list(targets) if targets else None,
            cached=cached,
            max_depth=max_depth,
            count_inodes=inodes,
        )
    except du_mod.DuError as e:
        raise click.ClickException(str(e))
    
    if not results:
        click.echo("No tracked files found.")
        return
    
    # Calculate grand total first (needed for summarize mode and -c)
    grand_total = sum(value for value, _ in results)
    
    # Determine what to display
    if summarize:
        # -s shows only the grand total
        display_results = [(grand_total, '.')]
    else:
        display_results = results
    
    # Calculate column width for alignment
    if inodes:
        max_width = max(len(str(count)) for count, _ in display_results)
    else:
        if human:
            max_width = max(len(utils.format_size(size, True)) for size, _ in display_results)
        else:
            max_width = max(len(str(size)) for size, _ in display_results)
    
    # Also consider grand total width if showing total line
    if show_total and not summarize:
        if human and not inodes:
            total_width = len(utils.format_size(grand_total, True))
        else:
            total_width = len(str(grand_total))
        max_width = max(max_width, total_width)
    
    for value, path in display_results:
        if human and not inodes:
            size_str = utils.format_size(value, True)
        else:
            size_str = str(value)
        click.echo(f"{size_str:>{max_width}}   {path}")
    
    if show_total and not summarize:
        if human and not inodes:
            total_str = utils.format_size(grand_total, True)
        else:
            total_str = str(grand_total)
        click.echo(f"{total_str:>{max_width}}   total")


@cli.command()
@click.argument('hash')
@click.option('--dvc-file', is_flag=True, help='Show the .dvc file path')
@click.option('--dir-file', is_flag=True, help='Show the .dir hash if file is in a directory')
@click.option('--cache-path', is_flag=True, help='Show the full path in cache')
@click.option('--no-expand', is_flag=True, help='Do not search inside .dir manifests')
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON')
@click.option('-v', '--verbose', is_flag=True, help='Show all available details')
def find(hash, dvc_file, dir_file, cache_path, no_expand, json_output, verbose):
    """Find workspace path(s) for a given hash.
    
    Reverse lookup: given an MD5 hash, find which DVC-tracked file(s) it
    corresponds to in the workspace.
    
    Searches both top-level tracked files and files within tracked directories.
    Partial hashes are supported (minimum 4 characters).
    
    \b
    Examples:
        dt find cf7bfcb23f8c              # Find by partial hash
        dt find cf7bfcb23f8c0b12... -v    # Verbose with all details
        dt find abc123 --dvc-file         # Show which .dvc file tracks it
        dt find abc123 --cache-path       # Show path in cache
        dt find abc123 --json             # JSON output
    """
    from . import find as find_mod
    
    try:
        results = find_mod.find_by_hash(
            file_hash=hash,
            expand_dirs=not no_expand,
            show_dvc_file=dvc_file or verbose,
            show_dir_file=dir_file or verbose,
            show_cache_path=cache_path or verbose,
        )
        
        if not results and not json_output:
            click.echo(f"No matches found for hash: {hash}")
            raise SystemExit(1)
        
        output = find_mod.format_results(
            results,
            verbose=verbose,
            json_output=json_output,
        )
        click.echo(output)
        
    except find_mod.FindError as e:
        raise click.ClickException(str(e))


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
                click.echo(f"Files to push ({len(files)} files, {utils.format_size(total_size)}):")
                for file_hash in sorted(files):
                    # Show path if available, with full hash in parentheses
                    path = paths.get(file_hash)
                    if path:
                        click.echo(f"  {path}  ({file_hash})")
                    else:
                        click.echo(f"  {file_hash}")
            else:
                click.echo(f"Would push {len(files)} file(s), {utils.format_size(total_size)}")
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
@click.option('--no-index-sync', is_flag=True, help='Skip automatic index mirror sync')
@click.option('--worker', is_flag=True, hidden=True,
              help='Internal: run dvc add directly (used by compute node).')
@click.pass_context
def add(ctx, targets, threads, no_wait, verbose, no_index_sync, worker):
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
            elif not no_wait:
                # Job completed, sync index to mirror
                if not no_index_sync and index_mod.is_auto_sync_enabled():
                    try:
                        index_mod.push(quiet=not verbose, verbose=verbose)
                    except Exception as e:
                        if verbose:
                            click.echo(f"Warning: index sync failed: {e}")
                
    except add_mod.AddError as e:
        raise click.ClickException(str(e))


# =============================================================================
# dt data
# =============================================================================

@cli.group()
def data():
    """DVC data operations."""
    pass


@data.command(
    'status',
    context_settings=dict(
        ignore_unknown_options=True,
        allow_extra_args=True,
    ),
)
@click.option('-t', '--threads', type=int, default=None,
              help='Number of threads for checksum computation.')
@click.option('--no-wait', is_flag=True,
              help='Submit job and exit without waiting for completion.')
@click.option('-v', '--verbose', is_flag=True,
              help='Show detailed progress.')
@click.option('--no-index-sync', is_flag=True, help='Skip automatic index mirror sync')
@click.option('--worker', is_flag=True, hidden=True,
              help='Internal: run dvc data status directly (used by compute node).')
@click.pass_context
def data_status(ctx, threads, no_wait, verbose, no_index_sync, worker):
    """Show changes between the last git commit, DVC files and the workspace.

    Wraps ``dvc data status`` with parallel checksum computation and
    optional delegation to a compute node via qxub.

    \b
    Examples:
        dt data status                        # Run via compute node
        dt data status --granular             # File-level detail
        dt data status -t 48                  # Use 48 threads
        dt data status --no-wait              # Submit and exit

    All other options are passed through to ``dvc data status``.
    Run ``dvc data status --help`` for additional options.
    """
    try:
        # Collect extra args (pass-through to dvc data status)
        dvc_args = list(ctx.args) if ctx.args else None

        if worker:
            # Running on compute node — pull index, run status, push index
            if not no_index_sync and index_mod.is_auto_sync_enabled():
                try:
                    index_mod.pull(quiet=not verbose, verbose=verbose)
                except Exception as e:
                    if verbose:
                        click.echo(f"Warning: index pull failed: {e}")

            rc = data_status_mod.data_status(
                threads=threads,
                dvc_args=dvc_args,
                verbose=verbose,
            )

            if not no_index_sync and index_mod.is_auto_sync_enabled():
                try:
                    index_mod.push(quiet=not verbose, verbose=verbose)
                except Exception as e:
                    if verbose:
                        click.echo(f"Warning: index push failed: {e}")

            if rc != 0:
                raise SystemExit(rc)
        else:
            job_id = data_status_mod.data_status_via_qxub(
                threads=threads,
                dvc_args=dvc_args,
                verbose=verbose,
                wait=not no_wait,
                no_index_sync=no_index_sync,
            )

            if no_wait and job_id:
                click.echo(f"Submitted job: {job_id}")
                click.echo(f"Monitor with: qxub monitor {job_id}")
            elif not no_wait:
                # Job completed on compute node — push index from submitter too
                if not no_index_sync and index_mod.is_auto_sync_enabled():
                    try:
                        index_mod.push(quiet=not verbose, verbose=verbose)
                    except Exception as e:
                        if verbose:
                            click.echo(f"Warning: index sync failed: {e}")

    except data_status_mod.DataStatusError as e:
        raise click.ClickException(str(e))


@cli.command(context_settings=dict(
    ignore_unknown_options=True,
    allow_extra_args=True,
))
@click.argument('targets', nargs=-1, type=click.Path())
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
@click.option('--no-index-sync', is_flag=True, help='Skip automatic index mirror sync')
@click.option('--update', is_flag=True, help='Recover from .dir failures by rebuilding manifests with dt update')
@click.option('--network', is_flag=True, help='Fall back to dvc fetch (network) if local remote not available')
@click.option('--dry', is_flag=True, help='Show stage categorization without fetching (for troubleshooting)')
@click.option('--force', is_flag=True, help='Force re-fetch even if .dir exists in cache (ensures all child files are fetched)')
@click.option('--imports', is_flag=True, help='Only fetch repo imports (from dvc import)')
@click.option('--urls', is_flag=True, help='Only fetch URL imports (from dvc import-url)')
@click.option('--regular', is_flag=True, help='Only fetch regular stages (non-imports)')
@click.option('--source', type=click.Path(exists=True), help='Explicit source cache path (overrides auto-discovery)')
@click.option('--destination', type=click.Path(), help='Explicit destination cache path (overrides primary cache)')
@click.option('--cache-type', type=click.Choice(['reflink', 'hardlink', 'symlink', 'copy']),
              help='Link type for cache population. If not specified, tries reflink → hardlink → symlink → copy.')
@click.pass_context
def fetch(ctx, targets, verbose, no_index_sync, update, network, dry, force, imports, urls, regular, source, destination, cache_type):
    """Fetch DVC-tracked files into the primary cache.
    
    Populates the primary cache with symlinks to files from source caches.
    This is the dt equivalent of `dvc fetch` but for local caches.
    
    For repo import .dvc files (created by `dvc import`), automatically clones
    the source repository to find a locally-accessible cache and creates
    symlinks in the primary cache.
    
    For URL import .dvc files (created by `dvc import-url`), re-downloads data
    from the source URL using `dvc update`. If the source has changed, the .dvc
    file will be updated with the new hash.
    
    For regular .dvc files, checks if there's a locally-accessible remote
    and creates symlinks from it. If no local remote is available and --network
    is specified, falls back to `dvc fetch`.
    
    After fetch, run `dvc checkout` to link files to the workspace.
    
    \b
    Stage type filters (combinable, default is all types):
        --imports    Only repo imports (dvc import)
        --urls       Only URL imports (dvc import-url)  
        --regular    Only regular stages (non-imports)
    
    \b
    Explicit cache paths:
        --source       Use this cache as the source (overrides auto-discovery)
        --destination  Write to this cache instead of the primary cache
    
    \b
    Cache link type:
        --cache-type   Only use this link method (reflink, hardlink, symlink, copy).
                       If not specified, tries all in order until one succeeds.
    
    \b
    Examples:
        dt fetch                           # Fetch all .dvc files from local sources
        dt fetch data/external.dvc         # Fetch specific targets
        dt fetch -v                        # Show detailed progress
        dt fetch --update                  # Rebuild .dir files, update .dvc if needed
        dt fetch --force                   # Force re-fetch even if .dir exists (ensures children are fetched)
        dt fetch --network                 # Fall back to dvc fetch if local remote unavailable
        dt fetch --dry                     # Show what would be fetched without actually fetching
        dt fetch --dry -v                  # Show detailed categorization
        dt fetch --imports                 # Only fetch repo imports
        dt fetch --urls --imports          # Only fetch imports (both types)
        dt fetch --regular                 # Only fetch regular stages
        dt fetch --source /path/to/source  # Fetch from explicit source cache
        dt fetch --destination /path/to/dest # Fetch into explicit destination cache
        dt fetch --cache-type symlink      # Only use symlinks for cache population
    """
    from . import fetch as fetch_mod
    
    try:
        # Sync index from mirror before fetch (if configured)
        if not dry and not no_index_sync and index_mod.is_auto_sync_enabled():
            try:
                index_mod.pull(quiet=not verbose, verbose=verbose)
            except Exception as e:
                if verbose:
                    click.echo(f"Warning: index sync failed: {e}")
        
        results = fetch_mod.fetch(
            targets=list(targets) if targets else None,
            verbose=verbose,
            update=update,
            network=network,
            dry=dry,
            force=force,
            imports=imports,
            urls=urls,
            regular=regular,
            source=source,
            destination=destination,
            cache_type=cache_type,
        )
        
        # In dry mode, just exit (summary already printed)
        if dry:
            return
        
        # Count successes and failures
        successes = sum(1 for _, success, _ in results if success)
        failures = [(target, msg) for target, success, msg in results if not success]
        
        # In verbose mode, results were already printed during fetch
        # In non-verbose mode, progress was shown inline
        # Just show failures and final summary here
        if failures:
            click.echo()
            for target, msg in failures:
                click.echo(f"✗ {target}: {msg}")
        
        # Report summary
        if failures:
            click.echo(f"\nFetch complete with {len(failures)} error(s).")
        else:
            click.echo(f"\n✓ {successes} stages processed")
            # Sync index to mirror after fetch (if configured)
            if not no_index_sync and index_mod.is_auto_sync_enabled():
                try:
                    index_mod.push(quiet=not verbose, verbose=verbose)
                except Exception as e:
                    if verbose:
                        click.echo(f"Warning: index sync failed: {e}")
        
        if failures and successes == 0:
            raise SystemExit(1)
    
    except errors.HashMismatchError as e:
        # Clean error message with suggestion
        raise click.ClickException(str(e))
    except fetch_mod.FetchError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument('path', type=click.Path())
@click.option('-n', '--limit', type=int, default=None,
              help='Maximum number of versions to show')
@click.option('--since', default=None,
              help='Only show versions since date (e.g., "2025-01-01", "1 month ago")')
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON')
@click.option('-v', '--verbose', is_flag=True, help='Show full hashes and author')
def history(path, limit, since, json_output, verbose):
    """Show version history of a DVC-tracked file.
    
    Lists the different versions (checksums) of a file across git history,
    showing when each version was introduced.
    
    Use with `dt diff` to examine the actual content changes between versions.
    
    \b
    Examples:
        dt history data.csv                    # Show all versions
        dt history data.csv -n 5               # Show last 5 versions
        dt history data.csv --since "1 month ago"
        dt history data.csv --json             # Output as JSON
        dt history data.csv -v                 # Verbose with full hashes
    """
    from . import history as history_mod
    
    try:
        entries = history_mod.history(
            path=path,
            limit=limit,
            since=since,
            verbose=verbose,
        )
        output = history_mod.format_history(
            entries,
            json_output=json_output,
            verbose=verbose,
        )
        click.echo(output)
    except history_mod.HistoryError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument('url', default='.')
@click.argument('path', required=False)
@click.option('--rev', default=None, help='Git revision (e.g., SHA, branch, tag)')
@click.option('-R', '--recursive', is_flag=True, help='List recursively')
@click.option('--all', 'include_all', is_flag=True, help='Include non-DVC files (git files too)')
@click.option('--pattern', '-p', default=None, help='Glob pattern for path filtering (e.g., "*.csv", "data/**")')
@click.option('--regex', '-e', default=None, help='Regex pattern for path filtering')
@click.option('--min-size', default=None, help='Minimum size (e.g., 100K, 1M, 1G)')
@click.option('--max-size', default=None, help='Maximum size (e.g., 100M, 1G)')
@click.option('--files', 'files_only', is_flag=True, help='Show only files')
@click.option('--dirs', 'dirs_only', is_flag=True, help='Show only directories')
@click.option('--exec', 'exec_only', is_flag=True, help='Show only executable files')
@click.option('--hash', 'hash_prefix', default=None, help='Filter by hash prefix')
@click.option('-l', '--long', 'long_format', is_flag=True, help='Long format (show type and size)')
@click.option('--show-hash', is_flag=True, help='Show MD5 hash')
@click.option('--json', 'json_output', is_flag=True, help='Output as JSON')
def ls(url, path, rev, recursive, include_all, pattern, regex, min_size, max_size,
       files_only, dirs_only, exec_only, hash_prefix, long_format, show_hash, json_output):
    """List and filter DVC-tracked files.
    
    Wraps `dvc list` with filtering capabilities. By default lists only DVC
    outputs (tracked data). Use --all to include git-tracked files too.
    
    Output is pipe-friendly: one path per line by default.
    
    \b
    Examples:
        dt ls                              # List DVC outputs only
        dt ls --all                        # Include git files too
        dt ls -R                           # List recursively
        dt ls -l                           # Long format with size
        dt ls --pattern "*.csv"            # Filter by glob pattern
        dt ls --min-size 1M                # Files >= 1MB
        dt ls --files --min-size 100K      # Files only, >= 100KB
        dt ls --hash abc123                # Filter by hash prefix
        dt ls . data/                      # List specific directory
        dt ls --rev HEAD~5                 # List at specific revision
        dt ls . --json | jq '.[] | .path'  # Pipe JSON to jq
    """
    try:
        _, output = ls_mod.list_files(
            url=url,
            path=path,
            rev=rev,
            recursive=recursive,
            dvc_only=not include_all,
            pattern=pattern,
            regex=regex,
            min_size=min_size,
            max_size=max_size,
            files_only=files_only,
            dirs_only=dirs_only,
            exec_only=exec_only,
            hash_prefix=hash_prefix,
            long_format=long_format,
            show_hash=show_hash,
            json_output=json_output,
        )
        if output:
            click.echo(output)
    except ls_mod.LsError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument('src', type=click.Path(exists=True))
@click.argument('dst', type=click.Path())
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def mv(src, dst, verbose):
    """Move or rename DVC-tracked files, preserving import metadata.
    
    Works like `dvc mv`, but correctly preserves the `deps` section
    for import .dvc files (which `dvc mv` incorrectly drops).
    
    For non-import files, this is equivalent to `dvc mv`.
    
    \b
    Examples:
        dt mv data/old_name.dvc data/new_name.dvc
        dt mv data/file.csv new_location/
        dt mv -v imported_data.dvc renamed_data.dvc
    """
    from . import mv as mv_mod
    
    try:
        old_dvc, new_dvc = mv_mod.mv(src, dst, verbose=verbose)
        click.echo(f"Moved {old_dvc} -> {new_dvc}")
    except mv_mod.MvError as e:
        raise click.ClickException(str(e))


@cli.command()
@click.argument('targets', nargs=-1)
@click.option('--force', '-f', is_flag=True,
              help='Delete .dir manifests before pulling to force re-fetch. Useful after dt cache validate --fix.')
@click.option('--dry', '--dry-run', is_flag=True,
              help='Show what would be pulled without actually pulling.')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
@click.option('--update', is_flag=True, help='Rebuild .dir files and update .dvc hashes if mismatched')
@click.option('--network/--no-network', default=True,
              help='Enable/disable network access for fetching. Default: enabled.')
@click.option('--no-index-sync', is_flag=True, help='Skip automatic index mirror sync')
def pull(targets, force, dry, verbose, update, network, no_index_sync):
    """Pull DVC-tracked files (fetch + checkout).
    
    This is the dt equivalent of `dvc pull`. It fetches data to the cache
    and checks out to the workspace in one step.
    
    For imports and local-remote scenarios, uses dt fetch for efficient
    local cache symlinks. For data requiring network access, uses dvc fetch.
    
    By default, network access is enabled. Use --no-network to only fetch
    data available locally (from local remotes or import sources).
    
    \b
    Examples:
        dt pull                    # Pull all tracked files
        dt pull data/              # Pull specific target
        dt pull --dry              # Show what would be pulled
        dt pull -v                 # Show detailed progress
        dt pull --force data/      # Force re-fetch (after cache validate --fix)
        dt pull --update           # Rebuild .dir files, update .dvc if needed
        dt pull --no-network       # Only pull data available locally
    """
    try:
        # Sync index from mirror before pull (if configured)
        if not no_index_sync and not dry and index_mod.is_auto_sync_enabled():
            try:
                index_mod.pull(quiet=not verbose, verbose=verbose)
            except Exception as e:
                if verbose:
                    click.echo(f"Warning: index sync failed: {e}")
        
        # Convert tuple to list or None
        target_list = list(targets) if targets else None
        
        # Call the simplified pull function
        success, fetched, failed = pull_mod.pull(
            targets=target_list,
            verbose=verbose,
            force=force,
            update=update,
            network=network,
            dry=dry,
        )
        
        if not success:
            raise SystemExit(1)
        
        # Sync index to mirror after pull (if configured)
        if not no_index_sync and not dry and index_mod.is_auto_sync_enabled():
            try:
                index_mod.push(quiet=not verbose, verbose=verbose)
            except Exception as e:
                if verbose:
                    click.echo(f"Warning: index sync failed: {e}")
            
    except fetch_mod.FetchError as e:
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
@click.argument('path', required=False)
@click.option('-o', '--out', help='Destination path to download files to')
@click.option('--owner', help='Override the GitHub owner for short names')
@click.option('--no-checkout', is_flag=True, help='Skip checkout after import')
@click.option('--no-refresh', is_flag=True, help='Skip refreshing temp clone (for offline use)')
@click.option('--no-download', is_flag=True, help='Create .dvc file without downloading data (like dvc import --no-download)')
@click.option('--rev', default=None, help='Git revision to lock to (used with --no-download; defaults to tmp clone HEAD)')
@click.option('--csv', 'csv_path', default=None, type=click.Path(exists=True), help='CSV file with paths to import (requires "path" column, optional "output" column)')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def import_cmd(repository, path, out, owner, no_checkout, no_refresh, no_download, rev, csv_path, verbose):
    """Import DVC-tracked data from another repository.
    
    Creates a .dvc file pointing to data in REPOSITORY at PATH,
    then checks out the data from locally-accessible caches.
    
    Unlike `dvc import`, this does not require network access to
    the remote storage. Instead, it uses cache paths discovered
    via `dt cache add-from`.
    
    \b
    Options:
      --no-download   Create .dvc file without downloading data.
                      Uses the tmp clone HEAD (or --rev) for rev_lock.
      --rev REV       Lock to a specific git revision (with --no-download).
      --csv FILE      Import every row from a CSV file. The CSV must have
                      a "path" column; an optional "output" column maps
                      to -o/--out.
    
    \b
    Examples:
        dt import neochemo data/processed
        dt import neochemo data/samples.h5ad -o my_samples.h5ad
        dt import git@github.com:lab/project.git results/model
        dt import neochemo data/large --no-checkout
        dt import neochemo data/file --no-refresh
        dt import neochemo data/file --no-download
        dt import neochemo data/file --no-download --rev abc123
        dt import neochemo --csv paths.csv
        dt import neochemo --csv paths.csv --no-download
    """
    # --csv mode: path argument is not required
    if csv_path:
        if path:
            raise click.UsageError("Do not provide PATH when using --csv.")
        try:
            results = import_mod.import_from_csv(
                csv_path=csv_path,
                repository=repository,
                owner=owner,
                out=out,
                no_download=no_download,
                no_checkout=no_checkout,
                no_refresh=no_refresh,
                rev=rev,
                verbose=verbose,
            )
            
            successes = sum(1 for _, ok, _ in results if ok)
            failures = sum(1 for _, ok, _ in results if not ok)
            
            for row_path, ok, msg in results:
                status = "✓" if ok else "✗"
                click.echo(f"{status} {row_path}: {msg}")
            
            click.echo(f"\n{successes} imported, {failures} failed")
            
            if failures:
                raise SystemExit(1)
                
        except import_mod.ImportError as e:
            raise click.ClickException(str(e))
        return
    
    # Normal (non-CSV) mode: path is required
    if not path:
        raise click.UsageError("PATH is required (unless using --csv).")
    
    if no_download:
        try:
            dvc_file = import_mod.import_no_download(
                repository=repository,
                path=path,
                out=out,
                owner=owner,
                rev=rev,
                no_refresh=no_refresh,
                verbose=verbose,
            )
            click.echo(f"Created {dvc_file}")
        except import_mod.ImportError as e:
            raise click.ClickException(str(e))
        return
    
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


@cli.command()
@click.argument('targets', nargs=-1, type=click.Path())
@click.option('--rev', default=None, help='Git revision to update to. If not specified, checks for changes and auto-upgrades to HEAD if safe.')
@click.option('--no-download', is_flag=True, help='Rebuild .dir only, do not run dt fetch')
@click.option('--dry-run', is_flag=True, help='Show what would be done without making changes')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
@click.option('--no-index-sync', is_flag=True, help='Skip automatic index mirror sync')
def update(targets, rev, no_download, dry_run, verbose, no_index_sync):
    """Update imported data by rebuilding .dir manifests.
    
    Rebuilds .dir files for repo imports where the directory manifest
    doesn't exist or is stale. This fixes the metadata so dt fetch can
    populate the cache correctly.
    
    \b
    Smart Revision Detection:
    When --rev is not specified, dt update checks if the source data
    has changed between the locked revision and HEAD:
    
    - If no changes: safely upgrades to HEAD
    - If data changed: stops and shows options (--rev locked or --rev HEAD)
    
    Use --rev to explicitly specify a version:
    - --rev HEAD: update to latest
    - --rev v1.2.0: update to a tag
    - --rev abc1234: update to a specific commit
    - --rev <current>: refresh .dir without changing version
    
    \b
    Examples:
        dt update                    # Smart update all imports
        dt update data/external.dvc  # Update specific file
        dt update --rev HEAD         # Force update to latest
        dt update --no-download      # Rebuild .dir only
        dt update --dry-run          # Show what would be done
    
    Note: Rebuilt .dir files are always pushed to the source remote.
    """
    try:
        # Sync index from mirror before update (if configured)
        if not no_index_sync and index_mod.is_auto_sync_enabled():
            try:
                index_mod.pull(quiet=not verbose, verbose=verbose)
            except Exception as e:
                if verbose:
                    click.echo(f"Warning: index sync failed: {e}")
        
        results = update_mod.update(
            targets=list(targets) if targets else None,
            rev=rev,
            verbose=verbose,
            no_download=no_download,
            dry_run=dry_run,
        )
        
        any_success = False
        any_failure = False
        
        for target, success, message in results:
            status = "✓" if success else "✗"
            click.echo(f"{status} {target}: {message}")
            
            if success:
                any_success = True
            else:
                any_failure = True
        
        # Sync index to mirror after update (if configured)
        if any_success and not no_index_sync and index_mod.is_auto_sync_enabled():
            try:
                index_mod.push(quiet=not verbose, verbose=verbose)
            except Exception as e:
                if verbose:
                    click.echo(f"Warning: index sync failed: {e}")
        
        if any_failure and not any_success:
            raise SystemExit(1)
    
    except update_mod.UpdateError as e:
        raise click.ClickException(str(e))


# =============================================================================
# dt worktree - Git worktree management with DVC cache configuration
# =============================================================================

@cli.group()
def worktree():
    """Manage git worktrees with DVC cache configured.
    
    Git worktrees allow working on multiple branches simultaneously.
    These commands ensure DVC cache is properly shared between worktrees.
    """
    pass


@worktree.command('add')
@click.argument('path')
@click.option('-b', '--new-branch', help='Create a new branch with this name')
@click.option('--branch', help='Checkout this existing branch')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def worktree_add(path, new_branch, branch, verbose):
    """Create a git worktree with DVC cache configured.
    
    Creates a new worktree and configures it to use the same DVC cache
    as the current repository. Also initializes submodules.
    
    \b
    Examples:
        dt worktree add ../feature-branch --branch feature/new
        dt worktree add ../experiment -b experiment/test
    """
    from . import worktree as worktree_mod
    
    try:
        worktree_path = worktree_mod.add(
            path=path,
            branch=branch,
            new_branch=new_branch,
            verbose=verbose,
        )
        click.echo(f"Created worktree at: {worktree_path}")
    except worktree_mod.WorktreeError as e:
        raise click.ClickException(str(e))


@worktree.command('list')
def worktree_list():
    """List all git worktrees.
    
    Shows path, branch, and commit for each worktree.
    """
    from . import worktree as worktree_mod
    
    try:
        worktrees = worktree_mod.list_worktrees()
        
        if not worktrees:
            click.echo("No worktrees found")
            return
        
        for wt in worktrees:
            path = wt.get('path', 'unknown')
            branch = wt.get('branch', '').replace('refs/heads/', '')
            head = wt.get('head', '')[:8] if wt.get('head') else ''
            
            if wt.get('detached'):
                branch = f"(detached at {head})"
            elif wt.get('bare'):
                branch = "(bare)"
            
            click.echo(f"{path}")
            if branch:
                click.echo(f"  branch: {branch}")
            if head and not wt.get('detached'):
                click.echo(f"  commit: {head}")
    except worktree_mod.WorktreeError as e:
        raise click.ClickException(str(e))


@worktree.command('remove')
@click.argument('path')
@click.option('-f', '--force', is_flag=True, help='Force removal even if dirty')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
def worktree_remove(path, force, verbose):
    """Remove a git worktree.
    
    Removes the worktree at the specified path.
    
    \b
    Examples:
        dt worktree remove ../feature-branch
        dt worktree remove ../dirty-branch --force
    """
    from . import worktree as worktree_mod
    
    try:
        worktree_mod.remove(path=path, force=force, verbose=verbose)
        click.echo(f"Removed worktree: {path}")
    except worktree_mod.WorktreeError as e:
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


# =============================================================================
# Index commands
# =============================================================================

@cli.group()
def index():
    """Manage DVC site cache index mirror.
    
    The site cache index allows DVC to quickly look up files across caches.
    This command syncs the local index with a shared mirror so all users
    benefit from the same index without rebuilding it.
    
    \b
    Configure with:
        dt config set index.mirror_root /g/data/a56/dvc/mirror
    """
    pass


@index.command('pull')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
@click.option('-q', '--quiet', is_flag=True, help='Suppress all output')
@click.option('--dry', '--dry-run', is_flag=True, help='Show what would be synced')
def index_pull(verbose, quiet, dry):
    """Pull index from mirror to local.
    
    Syncs the shared index mirror to your local site cache index.
    This brings in index entries created by other users.
    
    \b
    Examples:
        dt index pull           # Pull latest index
        dt index pull -v        # Verbose output
        dt index pull -q        # Quiet (no output)
        dt index pull --dry     # Preview what would sync
    """
    try:
        success = index_mod.pull(verbose=verbose, quiet=quiet, dry=dry)
        if not success:
            raise SystemExit(1)
    except index_mod.IndexError as e:
        raise click.ClickException(str(e))


@index.command('push')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
@click.option('-q', '--quiet', is_flag=True, help='Suppress all output')
@click.option('--dry', '--dry-run', is_flag=True, help='Show what would be synced')
def index_push(verbose, quiet, dry):
    """Push index from local to mirror.
    
    Syncs your local site cache index to the shared mirror.
    This shares your index entries with other users.
    
    \b
    Examples:
        dt index push           # Push index to mirror
        dt index push -v        # Verbose output
        dt index push -q        # Quiet (no output)
        dt index push --dry     # Preview what would sync
    """
    try:
        success = index_mod.push(verbose=verbose, quiet=quiet, dry=dry)
        if not success:
            raise SystemExit(1)
    except index_mod.IndexError as e:
        raise click.ClickException(str(e))


@index.command('build')
@click.option('-v', '--verbose', is_flag=True, help='Show each file being indexed')
@click.option('--dry', '--dry-run', is_flag=True, help='Show what would be indexed')
@click.option('--cache', 'cache_path', type=click.Path(exists=True),
              help='Path to cache directory (default: current repo cache)')
def index_build(verbose, dry, cache_path):
    """Build index by trusting cache filenames.
    
    Walks the cache directory and builds the ODB index from filenames,
    avoiding the expensive hash computation. Cache files are named
    {hash[0:2]}/{hash[2:]}, so we can extract hashes directly.
    
    This is much faster than letting DVC build the index by hashing files,
    especially for large caches. Use `dt cache validate` separately if
    you need to verify checksum integrity.
    
    \b
    Examples:
        dt index build              # Build from current repo's cache
        dt index build -v           # Show each file being indexed
        dt index build --dry        # Preview what would be indexed
        dt index build --cache /path/to/cache  # Use specific cache
    """
    try:
        result = index_mod.build(
            cache_path=cache_path,
            verbose=verbose,
            dry=dry,
        )
        click.echo(f"Indexed: {result['dir_count']} dirs, {result['file_count']} files")
    except index_mod.IndexError as e:
        raise click.ClickException(str(e))


@index.command('status')
@click.option('-v', '--verbose', is_flag=True, help='Show additional details')
def index_status(verbose):
    """Show index mirror status.
    
    Displays information about the local index and mirror,
    including paths, existence, and lock status.
    """
    info = index_mod.status(verbose=verbose)
    
    if not info['configured']:
        click.echo("Index mirror not configured")
        if 'error' in info:
            click.echo(f"  {info['error']}")
        click.echo()
        click.echo("Configure with:")
        click.echo("  dt config set index.mirror_root /g/data/a56/dvc/mirror")
        return
    
    click.echo("Index configuration:")
    click.echo(f"  Local:  {info['local_index']}")
    click.echo(f"  Mirror: {info['mirror_path']}")
    click.echo()
    
    click.echo("Status:")
    click.echo(f"  Local exists:  {'yes' if info['local_exists'] else 'no'}")
    click.echo(f"  Mirror exists: {'yes' if info['mirror_exists'] else 'no'}")
    
    if info.get('local_locked'):
        owner = info.get('local_lock_owner', 'unknown')
        age = info.get('local_lock_age', 0)
        click.echo(f"  Local locked:  yes (by {owner}, {age:.0f}s ago)")
    
    if info.get('mirror_locked'):
        owner = info.get('mirror_lock_owner', 'unknown')
        age = info.get('mirror_lock_age', 0)
        click.echo(f"  Mirror locked: yes (by {owner}, {age:.0f}s ago)")


@index.group('cache')
def index_cache():
    """Manage the local cache index.

    The cache index is a lightweight SQLite database that tracks which OIDs
    exist in the DVC cache.  This allows `dt fetch` to skip files that are
    already cached without expensive per-file stat() calls on network
    filesystems.

    The index lives at <cache_root>/.dt/cache.db/ and is shared by all
    repos that use the same cache.

    \b
    Commands:
        dt index cache status   # Show index info
        dt index cache rebuild  # Rebuild from filesystem scan
    """
    pass


@index_cache.command('status')
@click.option('-v', '--verbose', is_flag=True, help='Show additional details')
def index_cache_status(verbose):
    """Show cache index status.

    Displays the index location, whether it exists, and how many OIDs
    it contains.
    """
    idx = cache_index_mod.open_index(read_only=True)
    if idx is None:
        click.echo("Cache not configured or not in a DVC repo.")
        raise SystemExit(1)

    info = idx.info()
    click.echo("Cache index:")
    click.echo(f"  Path:        {info['path']}")
    click.echo(f"  Cache root:  {info['cache_root']}")
    click.echo(f"  Exists:      {'yes' if info['exists'] else 'no'}")
    if info.get('entries') is not None:
        click.echo(f"  Entries:     {info['entries']:,}")
    if info.get('error'):
        click.echo(f"  Error:       {info['error']}")
    idx.close()


@index_cache.command('rebuild')
@click.option('-v', '--verbose', is_flag=True, help='Show detailed progress')
@click.option('-q', '--quiet', is_flag=True, help='Suppress all output')
@click.confirmation_option(
    prompt='This will clear and rebuild the cache index from the filesystem. Continue?',
)
def index_cache_rebuild(verbose, quiet):
    """Rebuild the cache index by scanning the filesystem.

    Clears the existing index and walks the cache directory tree to
    discover all OIDs.  Use this after manual cache modifications or
    after running ``dvc gc``.

    \b
    Examples:
        dt index cache rebuild         # Rebuild with confirmation
        dt index cache rebuild -v      # Verbose output
        dt index cache rebuild --yes   # Skip confirmation prompt
    """
    idx = cache_index_mod.open_index()
    if idx is None:
        click.echo("Cache not configured or not in a DVC repo.")
        raise SystemExit(1)

    if not quiet:
        click.echo(f"Scanning cache: {idx._cache_root}")

    n = idx.rebuild(verbose=verbose, show_progress=not quiet)

    if not quiet:
        click.echo(f"Index rebuilt: {n:,} entries")
    idx.close()


# =============================================================================
# Summary commands
# =============================================================================

@cli.command()
@click.option('--out', '-o', 'output_dir', help='Output directory (default: docs/)')
@click.option('--tree-only', is_flag=True, help='Generate only tree.txt')
@click.option('--dag-only', is_flag=True, help='Generate only dag.md')
def summary(output_dir, tree_only, dag_only):
    """Generate project summary files.
    
    Creates tree.txt (file listing via dvc list --tree) and dag.md
    (pipeline DAG via dvc dag --md) in the output directory.
    
    By default, both files are generated in docs/.
    
    \b
    Examples:
        dt summary              # Generate both to docs/
        dt summary --tree-only  # Generate only tree.txt
        dt summary --dag-only   # Generate only dag.md
        dt summary -o .         # Generate both to current directory
    """
    try:
        if tree_only and dag_only:
            raise click.ClickException("Cannot use both --tree-only and --dag-only")
        
        if tree_only:
            summary_mod.generate_tree(output_dir=output_dir, verbose=True)
        elif dag_only:
            summary_mod.generate_dag(output_dir=output_dir, verbose=True)
        else:
            summary_mod.generate_all(output_dir=output_dir, verbose=True)
            
    except summary_mod.SummaryError as e:
        raise click.ClickException(str(e))


# =============================================================================
# Migrate commands
# =============================================================================

@cli.command()
@click.argument('targets', nargs=-1)
@click.option('--dry', is_flag=True, help='Show what would change without modifying files')
@click.option('-v', '--verbose', is_flag=True, help='Print detailed progress')
@click.option('--cache-root', type=click.Path(exists=True), help='Override cache root directory')
@click.option('--find-v2', is_flag=True, help='List v2 .dvc files without migrating')
def migrate(targets, dry, verbose, cache_root, find_v2):
    """Migrate .dvc files from v2 to v3 format.

    Updates .dvc files in place to use v3 format (explicit hash field and
    plain md5 hashing instead of md5-dos2unix). This handles imports that
    'dvc cache migrate --dvc-files' may trip over.

    Without arguments, migrates all .dvc files in the project.
    With arguments, migrates only the specified files or directories.

    Run 'dvc cache migrate' first to relocate cache data to v3 layout.

    \b
    Examples:
        dt migrate                    # Migrate all .dvc files
        dt migrate --dry              # Preview changes
        dt migrate --find-v2          # List v2 files only
        dt migrate data.csv.dvc       # Migrate a single file
        dt migrate data/              # Migrate all .dvc files in data/
        dt migrate imported.dvc -v    # Migrate an import with verbose output
    """
    from pathlib import Path

    if find_v2:
        try:
            v2_files = migrate_mod.find_v2_files(
                targets=list(targets) if targets else None,
            )
        except migrate_mod.MigrateError as e:
            raise click.ClickException(str(e))

        if not v2_files:
            click.echo('No v2 .dvc files found.')
            return

        for f in v2_files:
            suffix = ' (import)' if f['is_import'] else ''
            click.echo(f"{f['path']}{suffix}")

        click.echo(f'\n{len(v2_files)} v2 .dvc file(s) found.')
        return

    try:
        result = migrate_mod.migrate_project(
            targets=list(targets) if targets else None,
            dry_run=dry,
            verbose=verbose,
            cache_root=Path(cache_root) if cache_root else None,
        )
    except migrate_mod.MigrateError as e:
        raise click.ClickException(str(e))

    total = result['total']
    migrated = result['migrated']
    skipped = result['skipped']
    error_count = result['errors']

    if total == 0:
        click.echo('No .dvc files found.')
        return

    # Summary
    action = 'Would migrate' if dry else 'Migrated'
    parts = []
    if migrated:
        parts.append(f'{action} {migrated}')
    if skipped:
        parts.append(f'skipped {skipped} (already v3)')
    if error_count:
        parts.append(f'{error_count} error(s)')

    click.echo(', '.join(parts) + f' of {total} .dvc file(s).')

    # Report errors
    if error_count:
        for f in result['files']:
            if f['status'] == 'error':
                click.echo(f"  ERROR: {f['path']}: {f['error']}", err=True)
        raise click.ClickException(
            f'{error_count} file(s) could not be migrated'
        )


if __name__ == '__main__':
    cli()
