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
dt remote list git@github.com:swarbricklab/neochemo.git

# Using short name
dt remote list neochemo --owner swarbricklab
```

### Output

```
storage    ssh://gadi-dm.nci.org.au/g/data/a56/dvc/neochemo (default)
local      /g/data/a56/dvc/neochemo [local]
```

The `[local]` marker indicates paths accessible on the local filesystem.

## Related Commands

- [`dt init`](init.md) - Initialize projects with remote setup
- [`dt cache init`](cache.md#init) - Set up local cache
- [`dt cache add-from`](cache.md#dt-cache-add-from) - Add cache from remote repo
- [`dt config`](config.md) - Configure remote settings
- [`dt tmp`](tmp.md) - Manage temporary repository clones