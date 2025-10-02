# DVC Basics

DVC (=Data Version Control) is to data as git is to code. As well as tracking different versions of data, DVC allows us to track which _particular_ version of the code used which _particular_ inputs to generate which _particular_ outputs. This is very good for sanity, as well as computational reproducibility.

### Architecture

With DVC, data can live in up to three places:
1. the workspace
2. local cache ("the cache")
3. remote cache ("the remote")

#### Workspace

The workspace is easy to undertand. This consists of the files and folders in the project directory. These files can be
1. tracked by git (code, small text files, etc)
2. tracked by DVC (larger data files) -- such files are ignored by git
3. ignored by both git and DVC

Once a file has been tracked by DVC, it will be "linked" to a file in the cache (see below).

#### Cache

The local cache contains one or more versions of the files in the project, stored based on checksum hashes rather than file names. 
When a file is "added" to DVC tracking, the following happens:
1. the checksum of the file is calculated
2. the file is copied to the cache under a new file name based on the checksum
3. a link (*) to the cache is created in the workspace with the original file name
4. a `.dvc` file is created containing metadata about the file -- this file will be tracked by git
5. a `.gitignore` file is created or updated to exclude the data file that was just added

(*) Links can be hardlinks, symlinks, reflinks or plain copies. On NCI we use hardlinks when possible to reduce the inode count, and fall back to symlinks when hardlinks are not available. (Hardlinks cannot be created across different volumns, or to files owned by somebody else.) The link type is determined by the `cache.type` configuration item.

Notes:
- by default, the cache location is `.dvc/cache` within the project directory
- the cache location can be set to an external directory via `dvc cache dir path/to/cache`
- multiple clones can share an external cache if the `cache.shared` configuration item is set to `group`