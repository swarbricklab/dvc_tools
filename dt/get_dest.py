"""Destinations for ``dt get``.

A local destination is a path, and stays one -- the reflink/hardlink/copy chain
in :mod:`dt.cache_ops` has no expression outside a POSIX filesystem, and the
lesson encoded in ``resolve_link_types`` about EXDEV falling through to symlink
is worth keeping exactly where it is.

An S3 destination is different in kind. There is no cache to link from and no
directory to create; there is a stream of bytes from the source repository's
remote straight into object storage, never touching local disk. That is the
point -- it is what lets a collaborator on EC2 pull 358 GiB out of R2 without
provisioning 358 GiB of scratch.

Two properties fall out of streaming that the local path cannot have:

*Verification is free.* The bytes pass through this process, so we hash them on
the way past and compare against the md5 DVC recorded. Nothing is trusted after
the fact.

*Writes are atomic.* ``s3fs`` uploads via multipart and the object does not
exist until the upload is committed, so an interrupted transfer leaves nothing
behind rather than a truncated file that looks complete.
"""

import hashlib
import posixpath
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Tuple

from .errors import GetError

# 8 MiB balances two things: S3 wants parts of at least 5 MiB, and every worker
# holds one of these plus s3fs's own write buffer. See ``memory_estimate``.
DEFAULT_CHUNK_SIZE = 8 * 1024 * 1024

# Written as user metadata on every object we upload, and read back by --check.
# s3fs turns underscores into hyphens on the way out of head_object, so use a
# hyphen here and the key survives the round trip unchanged.
MD5_METADATA_KEY = 'dvc-md5'

PROBE_NAME = '.dt-write-probe'

_S3_URL_RE = re.compile(r'^s3://([^/]+)(?:/(.*))?$')


def is_s3_url(out: str) -> bool:
    """Is *out* an S3 destination?"""
    return out.startswith('s3://')


def parse_s3_url(url: str) -> Tuple[str, str]:
    """Split ``s3://bucket/prefix`` into ``(bucket, prefix)``.

    The prefix is returned without leading or trailing slashes, so callers can
    join onto it without worrying about doubling them.

    Raises:
        GetError: If *url* names no bucket.
    """
    match = _S3_URL_RE.match(url)
    if not match or not match.group(1):
        raise GetError(f"Malformed S3 URL: {url}")
    return match.group(1), (match.group(2) or '').strip('/')


@dataclass
class S3DestConfig:
    """Credentials and addressing for the *destination* bucket.

    Deliberately separate from anything the source uses. The source remote's
    profile is passed explicitly by DVC from ``.dvc/config``, and an explicit
    profile beats ambient ``AWS_*`` environment variables in botocore's
    resolution order -- so the two cannot contaminate each other as long as we
    never resolve destination credentials by mutating the environment.
    """

    profile: Optional[str] = None
    endpoint_url: Optional[str] = None
    region: Optional[str] = None
    account_id: Optional[str] = None


def _require(module: str, extra: str):
    """Import an optional dependency, or explain how to install it."""
    try:
        return __import__(module)
    except ImportError:
        raise GetError(
            f"S3 destinations need {module}. Install it with:\n"
            f"    pip install 'dvc-tools[{extra}]'"
        )


class LocalDestFile:
    """A destination file on the local filesystem.

    Exists so :func:`dt.get.decide` can be written once against a uniform
    interface rather than branching on destination type.
    """

    def __init__(self, path: Path):
        self.path = path

    def exists(self) -> bool:
        return self.path.exists()

    def verify(self, md5: str) -> bool:
        from . import utils
        try:
            return utils.md5_file(self.path) == md5
        except OSError:
            return False

    def __str__(self) -> str:
        return str(self.path)


class S3DestFile:
    """One object at the destination."""

    def __init__(self, dest: 'S3Dest', relpath: str):
        self._dest = dest
        self.relpath = relpath
        self.key = posixpath.join(dest.prefix, relpath) if dest.prefix else relpath
        self.url = f's3://{dest.bucket}/{self.key}'

    def exists(self) -> bool:
        return bool(self._dest.fs.exists(self.url))

    def verify(self, md5: str) -> bool:
        """Does the stored object carry the md5 we recorded when we wrote it?

        This checks metadata, not the bytes. Re-hashing an object in S3 means
        downloading it, and the ETag is not an md5 once an upload is multipart
        -- which is every file large enough to care about. The limitation is
        real and documented: an object replaced out of band still looks
        verified, and an object we did not upload has no metadata at all.

        What makes that acceptable is the atomicity above. The failure --check
        exists for on a local disk -- a half-written file indistinguishable
        from a whole one -- cannot occur here.
        """
        try:
            meta = self._dest.fs.metadata(self.url)
        except Exception:
            return False
        recorded = meta.get(MD5_METADATA_KEY)
        return bool(recorded) and recorded == md5

    def remove(self) -> None:
        try:
            self._dest.fs.rm_file(self.url)
        except Exception as e:
            raise GetError(f'could not remove {self.url}: {e}')

    def write_from(self, src_fs, src_path: str,
                   md5: str, chunk_size: int = DEFAULT_CHUNK_SIZE) -> str:
        """Stream *src_path* into this object, hashing on the way past.

        Returns the md5 of the bytes actually transferred, for the caller to
        compare against what DVC recorded. The comparison is the caller's job
        because only it knows whether to delete the object or report it.
        """
        h = hashlib.md5()
        extra = {'Metadata': {MD5_METADATA_KEY: md5}}
        with src_fs.open(src_path, 'rb') as reader, \
                self._dest.fs.open(self.url, 'wb',
                                   s3_additional_kwargs=extra) as writer:
            while True:
                buf = reader.read(chunk_size)
                if not buf:
                    break
                h.update(buf)
                writer.write(buf)
        return h.hexdigest()

    def __str__(self) -> str:
        return self.url


class S3Dest:
    """A destination prefix in an S3 bucket."""

    def __init__(self, url: str, config: Optional[S3DestConfig] = None):
        self.bucket, self.prefix = parse_s3_url(url)
        self.config = config or S3DestConfig()
        self._fs = None

    # -- addressing ------------------------------------------------------

    @property
    def url(self) -> str:
        return f's3://{self.bucket}/{self.prefix}' if self.prefix \
            else f's3://{self.bucket}'

    def under(self, name: str) -> 'S3Dest':
        """A destination one level down, e.g. one sample directory of a CSV."""
        child = S3Dest.__new__(S3Dest)
        child.bucket = self.bucket
        child.prefix = posixpath.join(self.prefix, name) if self.prefix else name
        child.config = self.config
        # Share the filesystem: building a second one would re-resolve
        # credentials for no reason, and fsspec would hand back the same
        # instance anyway.
        child._fs = self._fs
        return child

    def child(self, relpath: str) -> S3DestFile:
        return S3DestFile(self, relpath)

    def prepare(self) -> None:
        """No-op. Object storage has no directories to create."""

    # -- credentials -----------------------------------------------------

    def resolved_region(self) -> Optional[str]:
        """The region this destination will actually use.

        Resolved through botocore rather than read off the flag, because the
        interesting case is the one the user did not type: a profile written by
        ``dt auth setup`` carries ``region = auto``, the Cloudflare R2
        convention, which is not a region any AWS endpoint will accept.
        """
        if self.config.region:
            return self.config.region
        botocore = _require('botocore', 's3')
        import botocore.session
        try:
            session = botocore.session.Session(profile=self.config.profile)
            return session.get_config_variable('region')
        except Exception:
            return None

    def _check_region(self) -> None:
        region = self.resolved_region()
        if region == 'auto' and not self.config.endpoint_url:
            raise GetError(
                f"Destination region resolves to 'auto', which is the "
                f"Cloudflare R2 convention, but no --dest-endpoint-url was "
                f"given.\n"
                f"This usually means --dest-profile names a profile written by "
                f"'dt auth setup' (those are R2 profiles, named after the "
                f"repo) rather than one of your own AWS profiles.\n"
                f"Pass --dest-region for a real AWS bucket, or "
                f"--dest-endpoint-url for an S3-compatible one."
            )

    @property
    def fs(self):
        """The destination filesystem, built once, credentials passed explicitly.

        Never via ``os.environ``: an explicit ``profile=`` outranks ambient
        ``AWS_ACCESS_KEY_ID`` in botocore, which is exactly the property that
        keeps the source remote's credentials and these from colliding. Setting
        environment variables would also silently redirect ``dt auth check``,
        which shells out to ``aws`` without ``--profile``.
        """
        if self._fs is None:
            s3fs = _require('s3fs', 's3')
            self._check_region()
            kwargs: Dict[str, object] = {}
            if self.config.profile:
                kwargs['profile'] = self.config.profile
            if self.config.endpoint_url:
                kwargs['endpoint_url'] = self.config.endpoint_url
            client_kwargs: Dict[str, object] = {}
            region = self.config.region
            if region:
                client_kwargs['region_name'] = region
            if client_kwargs:
                kwargs['client_kwargs'] = client_kwargs
            self._fs = s3fs.S3FileSystem(**kwargs)
        return self._fs

    def _session(self):
        """A boto3 session for the destination, or a readable error.

        A mistyped profile is the common case and botocore's ``ProfileNotFound``
        traceback is not a useful thing to show for it. Worth noting the trap
        this cannot catch: a typo that lands on a *different real profile* --
        ``~/.aws/credentials`` here contains both ``umccr`` and ``ummcr`` --
        raises nothing at all. That is what the printed identity is for.
        """
        boto3 = _require('boto3', 's3')
        import boto3.session
        try:
            return boto3.session.Session(
                profile_name=self.config.profile,
                region_name=self.config.region,
            )
        except Exception as e:
            known = ''
            try:
                import botocore.session
                profiles = botocore.session.Session().available_profiles
                if profiles:
                    known = '\nProfiles found: ' + ', '.join(sorted(profiles))
            except Exception:
                pass
            raise GetError(f"Destination credentials unavailable: {e}{known}")

    def _client(self):
        """A boto3 S3 client matching :attr:`fs`, for the preflight calls."""
        try:
            return self._session().client(
                's3', endpoint_url=self.config.endpoint_url
            )
        except GetError:
            raise
        except Exception as e:
            raise GetError(f"Could not build an S3 client for the destination: {e}")

    # -- preflight -------------------------------------------------------

    def preflight(self) -> str:
        """Fail before the transfer rather than during it.

        Three cheap calls that between them catch every way this goes wrong
        quietly: credentials that resolve to an account nobody meant, a bucket
        that is not there, and a key that can read but not write. A six-hour
        transfer that dies on the last one has wasted six hours.

        Returns a one-line description of the identity being used, which the
        caller prints. Printing it is the point: omitting --dest-profile is
        legal, so that an EC2 instance role needs no configuration at all, and
        the only thing standing between that convenience and writing to the
        wrong account is saying out loud which account it is.
        """
        self._check_region()
        client = self._client()
        identity = self._describe_identity()

        try:
            client.head_bucket(Bucket=self.bucket)
        except Exception as e:
            raise GetError(
                f"Cannot reach destination bucket {self.bucket!r}: {e}\n"
                f"Writing as: {identity}"
            )

        probe_key = posixpath.join(self.prefix, PROBE_NAME) if self.prefix \
            else PROBE_NAME
        try:
            client.put_object(Bucket=self.bucket, Key=probe_key, Body=b'')
        except Exception as e:
            raise GetError(
                f"No write permission at s3://{self.bucket}/{probe_key}: {e}\n"
                f"Writing as: {identity}"
            )
        try:
            client.delete_object(Bucket=self.bucket, Key=probe_key)
        except Exception:
            # Writing worked, which is what we were testing. A stray zero-byte
            # probe object is not worth failing a transfer over.
            pass

        return identity

    def _describe_identity(self) -> str:
        """Who are we about to write as?

        STS is an AWS service, so this is skipped for S3-compatible endpoints
        where it does not exist. There the profile name is all we can honestly
        report.
        """
        if self.config.endpoint_url:
            return (f"profile {self.config.profile}"
                    if self.config.profile else "default credentials")

        session = self._session()
        try:
            ident = session.client('sts').get_caller_identity()
        except Exception as e:
            raise GetError(
                f"Could not resolve destination credentials: {e}\n"
                f"Pass --dest-profile, or configure the default AWS "
                f"credential chain."
            )

        account = ident.get('Account', '?')
        arn = ident.get('Arn', '?')
        if self.config.account_id and account != self.config.account_id:
            raise GetError(
                f"Destination account mismatch: resolved {account}, "
                f"but --dest-account-id said {self.config.account_id}.\n"
                f"Writing as: {arn}"
            )
        return f"account {account} as {arn}"

    def __str__(self) -> str:
        return self.url


def memory_estimate(jobs: int, chunk_size: int = DEFAULT_CHUNK_SIZE) -> int:
    """Rough peak bytes held in memory by a streaming transfer.

    Each worker holds one read chunk plus one s3fs multipart write buffer.
    Worth surfacing: the default ``-j 8`` is around 200 MiB, which is fine on a
    login node and less fine on a small EC2 instance.
    """
    s3fs_write_buffer = 5 * 1024 * 1024
    return max(1, jobs) * (chunk_size + s3fs_write_buffer)
