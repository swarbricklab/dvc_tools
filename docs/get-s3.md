# Plan: `dt get` to an S3 destination

**Status:** proposal, not implemented. Written for review before any code.

## Goal

Let a collaborator pull DVC-tracked data **directly from our remote (R2 or SSH) into their own
AWS S3 bucket**, without the bytes ever landing on a local disk.

```bash
dt get my-registry data/fq/AF013-A -o s3://their-bucket/fastqs/ --dest-profile their-aws
dt get my-registry --csv samples.csv -o s3://their-bucket/fastqs/ --dest-profile their-aws
```

This is the hand-off case `dt get` already exists for, with the destination moved off the local
filesystem. Everything about *selecting* data — subpaths inside a tracked directory, `--csv`,
`--rev`, the filters — is unchanged.

## Scope

In scope:

- Source: any DVC remote the source repo configures, i.e. R2 (`s3://` + `endpointurl`) or SSH.
- Destination: AWS S3, or any S3-compatible endpoint, with the collaborator's own credentials.
- `--resume` and `--check` against an S3 destination.

Out of scope for v1 (see [Deferred: `ssh://` destinations](#deferred-ssh-destinations)):

- `ssh://` destinations.
- Local cache → S3. If a local cache is reachable, `dt get` to a local path and upload separately;
  there is no reason to stream through this code path on NCI.
- Making `s3://` a *source* argument. The source is still a repository.

## Why this is easier than it looks

The awkward parts of a general "S3 destination" feature all belong to the **local-cache** path.
Restricting to remote → S3 removes them:

| Concern | Local-cache path | Remote → S3 |
|---|---|---|
| Staging disk for a 358 GiB transfer | needed | none — streamed |
| `--check` verification | ETag ≠ md5 for multipart | md5 computed **in flight**, free |
| Link types (reflink/hardlink/symlink) | meaningful | not applicable |
| `dvc get` subprocess per file | forces rows to run serially | gone — direct fsspec |

The middle row is the important one. Because the bytes transit our process, we hash them as they
pass and compare against the md5 DVC recorded. That is a stronger guarantee than the local path's
post-hoc re-read, not a weaker one.

## Verified foundations

Everything below was checked in the `dt3` environment
(`/g/data/a56/conda/envs/dt3`, Python 3.14.4). **No new dependencies are required.**

| package | version | role |
|---|---|---|
| `fsspec` | 2026.4.0 | copy substrate |
| `s3fs` | 2026.4.0 | R2 source *and* AWS destination |
| `sshfs` | 2025.11.0 | SSH source |
| `dvc-s3` / `dvc-ssh` | 3.3.0 / 4.3.0 | thin wrappers over exactly those |
| `boto3` / `botocore` | 1.42.91 | credential resolution, preflight |

`dvc-s3` and `dvc-ssh` are `s3fs.S3FileSystem` and `sshfs.SSHFileSystem` underneath, so both source
transports are already plain fsspec.

### Source resolution

DVC hands us the source filesystem directly, with credentials and endpoint already resolved from
the clone's `.dvc/config`:

```python
remote = Repo(clone).cloud.get_remote()      # R2 endpoint or ssh, from .dvc/config
src    = remote.odb.oid_to_path(md5)         # -> <root>/files/md5/xx/yyyy...
remote.fs.open(src, 'rb')                    # dvc_objects FileSystem; .fs is raw fsspec
```

This is not a new pattern for this repo — `_check_dvc_remote_impl` (`dt/auth/checks.py:849-924`)
already uses `Repo().cloud.get_remote_odb(name)` and `odb.fs.exists/ls`, and is the one existing
check that correctly honours `.dvc/config`'s `profile` + `endpointurl`.

The relpath → md5 mapping still comes from `dvc list --json --show-hash --size -R` against the
clone (`dt/get.py:169`), unchanged. No local cache is consulted.

### The copy

```python
def stream_copy(src_fs, src, dst_fs, dst, chunk):
    h = hashlib.md5()
    with src_fs.open(src, 'rb') as r, dst_fs.open(dst, 'wb') as w:
        while (buf := r.read(chunk)):
            h.update(buf)
            w.write(buf)
    return h.hexdigest()
```

Prototyped end to end (`file://` → `memory://`, 5 MB): correct byte count, md5 match, correct size
at the destination.

### Atomicity comes free on S3

`s3fs` writes via multipart upload with an explicit `commit()` (`CompleteMultipartUpload`) and
`discard()` → `_abort_mpu()` (`AbortMultipartUpload`). **The object does not exist until commit**,
so an interrupted transfer leaves no partial object — the failure mode `decide()` was written to
defend against simply cannot occur.

Operational consequence: aborted multipart uploads leave orphaned parts that continue to accrue
storage charges. The collaborator's bucket wants a lifecycle rule expiring incomplete multipart
uploads. This belongs in the instructions we hand over, not in the code.

## Destination credentials

### The design constraint

Our convention, which this mirrors: **the endpoint URL is not a secret.** R2 `endpointurl` values
are committed publicly in `.dvc/config` remote definitions, which is exactly why they are absent
from the AWS profile — the profile carries key material only
(`dt/auth/credentials.py:169-202` writes `aws_access_key_id`/`aws_secret_access_key` and nothing
else; `endpointurl` is written to `.dvc/config` by `configure-remotes`,
`dt/auth/credentials.py:876-955`).

The destination splits the same way:

| | secret | how it is supplied |
|---|---|---|
| access key / secret key | yes | named AWS profile, or instance role |
| endpoint URL | no | `--dest-endpoint-url`, or dt config |
| region | no | `--dest-region`, or the profile |
| bucket + prefix | no | the `-o` argument |

### Why explicit passing is safe

Verified in `dt3`, with `AWS_ACCESS_KEY_ID` exported in the environment:

| resolution | credentials used |
|---|---|
| explicit `profile=` | shared-credentials-file — **ambient env ignored** |
| no profile | env |

An explicit `profile=` beats ambient environment variables. Since DVC passes the source remote's
profile explicitly from `.dvc/config`, **the source connection is immune to whatever the
collaborator exports for their destination.** That asymmetry is what makes two S3 endpoints in one
process safe.

fsspec instance caching also keys on `storage_options`, so the two never share a client:

```
S3FileSystem(profile='repo-r2', endpoint_url=...) is S3FileSystem(profile='collab-aws')  -> False
```

**Therefore: never set `AWS_*` in `os.environ`.** Nothing in `dt/` does today (the only `os.environ`
mutations are `COLUMNS` and `SSH_AUTH_SOCK` for subprocesses), and this feature must not be the
first. There is a second reason beyond correctness — see [Related fix](#related-fix) below.

### Two hazards this must design around

**1. Profile namespace collision.** `dt auth setup` writes profiles named after the repo — the cwd
directory name (`dt/auth/credentials.py:512` → `utils.get_project_name()`) — into the *same*
`~/.aws/credentials` as the user's own AWS profiles. On a real machine today:

```
default, ummcr, bcarc_wts, bcarc_wgs, bcarc_visium,
bcarc_xenium, bcarc_chromium, 533267394226_Garvan.Data.Upload, umccr
```

The `bcarc_*` entries are dt-managed **R2** profiles. So `--dest-profile bcarc_wts` would aim the
destination at R2. Note also `ummcr` alongside `umccr` — a mistyped profile name that happens to be
another valid profile never raises `ProfileNotFound`. **`--dest-profile` alone is not sufficient
protection.**

**2. `region = auto`.** `dt auth setup` hardcodes `region = 'auto'` — the R2 convention — into
`~/.aws/config` for every profile it creates (`dt/auth/credentials.py:173`). A dt-managed profile
used against real AWS therefore carries a region that is not a region.

### Resolution rules

```
-o s3://bucket/prefix/
   --dest-profile NAME          # BYO: named profile from ~/.aws/credentials
 [ --dest-endpoint-url URL ]    # non-AWS S3-compatible destination
 [ --dest-region REGION ]       # required if the profile's region is 'auto'
 [ --dest-account-id ID ]       # assertion for unattended runs
```

`--dest-*` rather than `--profile`, because the command already has a source credential context and
the two must stay visibly distinct at the call site.

1. `--dest-profile` given → `S3FileSystem(profile=..., endpoint_url=..., ...)`.
2. `--dest-profile` omitted → boto3 default chain (env → shared files → instance role). This is
   deliberate: it is what makes the **EC2 instance-role case zero-config**, which is the expected
   topology. It is never silent — see preflight.
3. Resolved `region == 'auto'` with no `--dest-endpoint-url` → **hard error**. This catches hazard 2
   by construction rather than by hoping the user reads the output.

### Preflight

Before a single byte moves, roughly three API calls and about a second:

1. `sts:GetCallerIdentity` → **print the account ID and ARN we are about to write as.**
2. `head_bucket` → the bucket exists and is reachable.
3. Zero-byte probe object: put, then delete → catches a missing `s3:PutObject` grant *before* a
   six-hour transfer rather than after it.

`--dest-account-id` makes this enforceable for unattended runs: resolved account ≠ asserted →
abort. There is precedent for preflight checks of this shape in `dt/auth/checks.py:353-416`.

Preflight is what converts both hazards above from silent misroutes into a visible line of output.

## `--resume` and `--check` on S3

`decide()` (`dt/get.py:216-252`) is unchanged in structure; only its two primitives are re-bound:

| primitive | local | S3 |
|---|---|---|
| `dest.exists()` | `stat` | `head_object` |
| `verify_file(dest, md5)` | re-read and hash | compare stored md5 metadata |

On upload we write the DVC md5 as object metadata (`x-amz-meta-dvc-md5`). Verified at source level
that this survives multipart: `S3File._call_s3` passes `self.s3_additional_kwargs` through to
`fs.call_s3`, which merges it into every call including `create_multipart_upload`. **This relies on
s3fs internals and must be confirmed by a live smoke test against a real bucket** before we depend
on it.

The honest limitation, which belongs in the user-facing docs: `--check` against S3 verifies the md5
*we recorded at upload time*, not a fresh hash of the stored bytes. It will not detect an object
replaced out of band, and it sees nothing for objects uploaded by other means. This is weaker than
the local path's guarantee. It is also the best available without re-downloading the object, and
the atomicity property above means the specific failure `--check` exists for — a truncated file that
looks complete — cannot arise on S3 in the first place.

Fresh uploads are verified in flight and so are strictly better off than the local path.

## Code changes

| file | change | size |
|---|---|---|
| `dt/get_dest.py` (new) | `Dest` protocol: `exists` / `verify` / `remove` / `put` / `mkdir`. `LocalDest` wraps today's behaviour verbatim; `S3Dest` streams via s3fs. | ~180 |
| `dt/get.py` | `_dest_for` returns a `Dest`; `_place_one` and `fetch_via_remote` call the protocol; relax `_reject_url_destination` for `s3://` only | ~40 |
| `dt/cli.py` | `--dest-profile`, `--dest-endpoint-url`, `--dest-region`, `--dest-account-id` | ~20 |
| `dt/secrets/base.py` | revive the dead `S3Credentials` dataclass (`base.py:14-42`, already has `endpoint_url`/`region`) rather than adding a type | ~0 |
| `tests/unit/` | preflight rules, credential resolution, `decide()` against a fake S3 | ~250 |
| `docs/get.md` | destination section, lifecycle-rule note, `--check` limitation | — |

**Do not unify `LocalDest` with the existing local path beyond the protocol boundary.** The local
path's reflink/hardlink chain has no fsspec expression, and `resolve_link_types`
(`dt/get.py:43-102`) encodes a hard-won lesson about EXDEV falling through to symlink
(commit `15dc42f`). That logic stays exactly where it is.

Estimated: about a day, weighted toward credentials and tests rather than transfer.

### Parallelism and memory

The existing `ThreadPoolExecutor` (`dt/get.py:326`) carries over, and the remote path can finally
use it — the per-file `dvc get` subprocess that forced rows to run serially
(`dt/get.py:664-670`) is gone, along with its SQLite lock contention.

New constraint: each worker holds a read chunk plus s3fs's multipart write buffer. Peak memory is
roughly `jobs × (chunk + s3fs blocksize)`. With an 8 MiB chunk and the default `-j 8` that is about
**104 MiB** — modest, but worth stating rather than discovering on a small EC2 instance.
`get_dest.memory_estimate()` computes it, and `--verbose` prints it before the transfer starts.
`--chunk-size` (MiB) tunes it.

## Deferred: `ssh://` destinations

Deferred for v1, but the `Dest` protocol should be shaped so this is a small increment — roughly
30 lines, since `sshfs` is already installed and already fsspec.

**Credentials are genuinely free.** `~/.ssh/config` plus the agent is already the BYO mechanism, and
`dt auth setup` already manages exactly that surface (`dt/auth/ssh.py:200-236` writes host stanzas
with `IdentityFile` and `AddKeysToAgent`). No new flags in the common case.

**Reliability is not free, and that is the reason to defer.** SSH has no multipart equivalent, so
the atomicity property that makes S3 easy is absent:

| | S3 | SSH |
|---|---|---|
| Interrupted write leaves | nothing (MPU never committed) | a truncated file that looks complete |
| `--check` costs | one `head_object` | re-reading the whole file across the WAN |
| `--resume` guarantee | strong | weak — the exact failure `decide()` was written for |

So `--resume` would mean something materially weaker over `ssh://` than over `s3://`, under the same
flag name. That is a correctness and documentation burden, not a coding one, and it deserves its own
decision rather than riding along.

There is also a good chance the honest answer for an HPC-to-HPC hand-off remains `dt get` to a local
path followed by `rsync`, which handles partial transfers better than anything we would write.
Revisit when a concrete user asks.

## Related fix

`_check_s3` (`dt/auth/checks.py:374-399`) shells out to `aws sts get-caller-identity` and
`aws s3 ls` **without `--profile`**, so it validates ambient credentials rather than the repo
profile from `.dvc/config`. Today that is a latent weakness. If a destination ever introduced
`AWS_PROFILE` or `AWS_ACCESS_KEY_ID` into the environment, `dt auth check` would report the
*destination's* identity while claiming to verify the *source* remote.

Worth fixing independently of this feature, and it reinforces the no-`os.environ` rule above.

## Decisions

Settled before implementation:

1. **`--dest-profile` is optional.** Omitting it uses the boto3 default chain, so an EC2 instance
   role needs no configuration — the expected topology. The safety comes from preflight printing the
   resolved account and ARN **unconditionally**, not only under `--verbose`, plus `--dest-account-id`
   for unattended runs.
2. **The resume md5 lives in object metadata** (`x-amz-meta-dvc-md5`), not a sidecar manifest. No
   second thing to keep in sync. The limitation — an object replaced out of band still looks
   verified, and objects uploaded by other means carry no metadata — is documented in
   `S3DestFile.verify` and accepted.
3. **The collaborator runs `dt` themselves** on their own EC2 instance, with `dvc-tools` installed
   and read access to the repository. This keeps R2 egress free and the transfer close to the
   destination bucket.
4. **8 MiB chunks, `-j 8` default**, tunable with `--chunk-size`.

## Still open

- **Live smoke test against a real bucket.** The metadata-through-multipart behaviour is verified at
  the s3fs source level and covered by a fake in the unit tests, but has not been exercised against
  real S3. This is the one assumption that could still be wrong.
- **`ssh://` destinations**, deferred as below.

## Non-goals

- Server-side copy. R2 → AWS is cross-provider; bytes must transit compute. On EC2 this is close to
  free (R2 charges no egress, AWS ingress is free), which is why the topology assumption matters.
- Replacing `aws s3 sync` for cases where a local copy already exists.
- Any change to how source credentials are provisioned. That stays `dt auth setup`.
