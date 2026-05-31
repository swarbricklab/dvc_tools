# dt remote

Remote storage management commands for configuring and working with DVC remotes in HPC environments.

## dt remote init

Sets up remote storage for the project with both SSH and local access methods.

### Usage

```bash
dt remote init [options] [project_name]
```

### Options

- `--name <project_name>`: Override project name (defaults to current directory name)
- `--remote-root <path>`: Override remote root directory (defaults to `remote.root` config value)
- `--remote-path <path>`: Override complete remote path (absolute or relative to current directory)

### What it does

- Creates the remote directory structure with proper group permissions
- Sets up SSH remote accessible from external platforms via `dvc remote add -d`
- Creates a local remote override for efficient transfers within the same system
- Maintains portability by keeping local remote configuration workspace-specific

### Examples

```bash
# Set up remote with default settings
# Uses: ${remote.root config}/${current directory name}
dt remote init

# Set up remote with custom root
# Uses: /g/data/a56/my-dvc-storage/my-project  
dt remote init --name my-project --remote-root /g/data/a56/my-dvc-storage

# Set up remote with complete custom path (absolute)
dt remote init --remote-path /g/data/a56/special-project/custom-remote

# Set up remote with relative path
dt remote init --remote-path ../shared-remote
```

### Path Resolution

The remote location is determined by (in order of precedence):

1. **`--remote-path`** - Complete path override (absolute or relative to current directory)
2. **Constructed path** - `${remote_root}/${project_name}` where:
   - **remote_root**: `--remote-root` argument OR `remote.root` config value
   - **project_name**: `--name` argument OR current directory name

**Default behavior** (no options): Uses `${remote.root config}/${current directory name}`

### Remote Configuration

Two remotes are configured:

- **Official remote**: Named after the platform (e.g., "nci"), accessible via SSH from anywhere
- **Local remote**: Named "local", provides direct filesystem access within the same platform
- SSH host determined by `ssh.host` config item (typically `gadi-dm.nci.org.au` on NCI)

## dt remote list

List DVC remotes from a repository (local or remote).

### Usage

```bash
dt remote list [repository] [--owner <owner>]
```

### Examples

```bash
# List remotes from current repository
dt remote list

# List remotes from a remote repository
dt remote list git@github.com:myorg/otherproject.git

# Using short name
dt remote list otherproject --owner myorg
```

### Output

```
storage    ssh://gadi-dm.nci.org.au/g/data/a56/dvc/neochemo (default)
local      /g/data/a56/dvc/neochemo [local]
```

The `[local]` marker indicates paths accessible on the local filesystem.

## dt remote archive

Archive a DVC remote to cold storage (e.g. NCI MDSS), verify it,
restore from it, and prune the on-disk copy once verified. See
[archive.md](archive.md) for the full reference.

```bash
dt remote archive create  <name>   # stage + deposit in one go
dt remote archive stage   <name>   # parallel inner tars on compute node
dt remote archive deposit <name>   # parallel uploads on data mover
dt remote archive list             # list archives recorded in .dt/archives/
dt remote archive verify  <name>   # sidecar + per-file existence/size
dt remote archive restore <name>   # full / per-prefix / single-object restore
dt remote archive prune   <name>   # delete on-disk remote after verify
dt remote archive destroy <name>   # delete an archive copy from the backend (does NOT touch source)
```

## Related Commands

- [`dt init`](init.md) - Initialize projects with remote setup
- [`dt cache init`](cache.md#init) - Set up local cache
- [`dt fetch`](fetch.md) - Fetch imports from local caches
- [`dt config`](config.md) - Configure remote settings
- [`dt remote archive`](archive.md) - Archive a remote to cold storage
- [`dt tmp`](tmp.md) - Manage temporary repository clones