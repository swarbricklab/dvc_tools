"""Unit tests for S3 destinations in ``dt get``.

No network and no moto. The pieces worth testing here are addressing, the
credential guards, and the streaming/verification contract -- all of which are
exercised through a fake filesystem that records what it was asked to do. What
this cannot cover is whether s3fs really forwards our metadata through a
multipart upload; that needs a live bucket and is called out in docs/get-s3.md.
"""

import hashlib

import pytest

from dt import get as get_mod
from dt import get_dest
from dt.errors import GetError


# =============================================================================
# Fakes
# =============================================================================

class FakeS3FS:
    """Enough of s3fs to exercise the destination logic."""

    def __init__(self):
        self.objects = {}        # url -> bytes
        self.meta = {}           # url -> {key: value}
        self.removed = []
        self.opened_with = []    # (url, s3_additional_kwargs)

    def exists(self, url):
        return url in self.objects

    def metadata(self, url):
        if url not in self.objects:
            raise FileNotFoundError(url)
        return self.meta.get(url, {})

    def rm_file(self, url):
        self.removed.append(url)
        self.objects.pop(url, None)
        self.meta.pop(url, None)

    def open(self, url, mode='rb', s3_additional_kwargs=None, **kw):
        if 'w' in mode:
            self.opened_with.append((url, s3_additional_kwargs))
            return _FakeWriter(self, url, s3_additional_kwargs or {})
        return _FakeReader(self.objects[url])


class _FakeWriter:
    def __init__(self, fs, url, extra):
        self.fs, self.url, self.extra = fs, url, extra
        self.buf = bytearray()

    def write(self, data):
        self.buf.extend(data)

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        # Mirrors S3 multipart: the object appears only on clean close.
        if exc[0] is None:
            self.fs.objects[self.url] = bytes(self.buf)
            self.fs.meta[self.url] = dict(self.extra.get('Metadata', {}))
        return False


class _FakeReader:
    def __init__(self, data):
        self.data, self.pos = data, 0

    def read(self, n=-1):
        if n < 0:
            n = len(self.data) - self.pos
        chunk = self.data[self.pos:self.pos + n]
        self.pos += len(chunk)
        return chunk

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeODB:
    """Stands in for a DVC remote's object database."""

    def __init__(self, fs, objects):
        self.fs = fs
        # md5 -> content
        for md5, content in objects.items():
            fs.objects[self._path(md5)] = content

    @staticmethod
    def _path(md5):
        return f'remote://files/md5/{md5[:2]}/{md5[2:]}'

    def oid_to_path(self, md5):
        return self._path(md5)


def md5_of(data):
    return hashlib.md5(data).hexdigest()


@pytest.fixture
def fs():
    return FakeS3FS()


def dest_with(fs, url='s3://bucket/fastqs', config=None):
    d = get_dest.S3Dest(url, config or get_dest.S3DestConfig())
    d._fs = fs
    return d


# =============================================================================
# URL parsing and addressing
# =============================================================================

class TestUrls:

    @pytest.mark.parametrize('url,expected', [
        ('s3://bucket/prefix', ('bucket', 'prefix')),
        ('s3://bucket/prefix/', ('bucket', 'prefix')),
        ('s3://bucket/a/b/c', ('bucket', 'a/b/c')),
        ('s3://bucket', ('bucket', '')),
        ('s3://bucket/', ('bucket', '')),
    ])
    def test_parse(self, url, expected):
        assert get_dest.parse_s3_url(url) == expected

    @pytest.mark.parametrize('bad', ['s3://', 's3:///prefix'])
    def test_malformed_rejected(self, bad):
        with pytest.raises(GetError, match='Malformed S3 URL'):
            get_dest.parse_s3_url(bad)

    def test_is_s3_url(self):
        assert get_dest.is_s3_url('s3://bucket/p')
        assert not get_dest.is_s3_url('fastqs/')
        assert not get_dest.is_s3_url('gs://bucket/p')

    def test_child_key_joins_under_prefix(self, fs):
        d = dest_with(fs, 's3://bucket/fastqs')
        assert d.child('r1.fq.gz').url == 's3://bucket/fastqs/r1.fq.gz'

    def test_child_of_bare_bucket_has_no_leading_slash(self, fs):
        d = dest_with(fs, 's3://bucket')
        assert d.child('r1.fq.gz').url == 's3://bucket/r1.fq.gz'

    def test_under_descends(self, fs):
        d = dest_with(fs, 's3://bucket/fastqs')
        assert d.under('AF013-A').child('r1.fq.gz').url == \
            's3://bucket/fastqs/AF013-A/r1.fq.gz'

    def test_under_shares_the_filesystem(self, fs):
        """Rebuilding it per row would re-resolve credentials for nothing."""
        d = dest_with(fs, 's3://bucket/fastqs')
        assert d.under('AF013-A').fs is fs

    def test_nested_relpath_is_preserved(self, fs):
        d = dest_with(fs, 's3://bucket/out')
        assert d.child('sub/dir/r1.fq.gz').url == \
            's3://bucket/out/sub/dir/r1.fq.gz'


# =============================================================================
# Streaming and in-flight verification
# =============================================================================

class TestWriteFrom:

    def test_streams_bytes_and_returns_their_md5(self, fs):
        payload = b'ACGT' * 5000
        odb = FakeODB(fs, {md5_of(payload): payload})
        md5 = md5_of(payload)
        target = dest_with(fs).child('r1.fq.gz')

        got = target.write_from(odb.fs, odb.oid_to_path(md5), md5, chunk_size=1024)

        assert got == md5
        assert fs.objects['s3://bucket/fastqs/r1.fq.gz'] == payload

    def test_records_md5_as_object_metadata(self, fs):
        payload = b'ACGT' * 10
        md5 = md5_of(payload)
        odb = FakeODB(fs, {md5: payload})
        target = dest_with(fs).child('r1.fq.gz')

        target.write_from(odb.fs, odb.oid_to_path(md5), md5)

        _, extra = fs.opened_with[-1]
        assert extra['Metadata'] == {get_dest.MD5_METADATA_KEY: md5}
        assert fs.metadata('s3://bucket/fastqs/r1.fq.gz') == \
            {get_dest.MD5_METADATA_KEY: md5}

    def test_chunking_does_not_change_the_result(self, fs):
        payload = b'x' * 9999
        md5 = md5_of(payload)
        odb = FakeODB(fs, {md5: payload})
        for chunk in (1, 7, 1024, 1 << 20):
            fs.objects.pop('s3://bucket/fastqs/r1.fq.gz', None)
            target = dest_with(fs).child('r1.fq.gz')
            assert target.write_from(
                odb.fs, odb.oid_to_path(md5), md5, chunk_size=chunk
            ) == md5

    def test_empty_object_round_trips(self, fs):
        md5 = md5_of(b'')
        odb = FakeODB(fs, {md5: b''})
        target = dest_with(fs).child('empty')
        assert target.write_from(odb.fs, odb.oid_to_path(md5), md5) == md5


class TestVerify:

    def test_matching_metadata_verifies(self, fs):
        target = dest_with(fs).child('r1.fq.gz')
        fs.objects[target.url] = b'data'
        fs.meta[target.url] = {get_dest.MD5_METADATA_KEY: 'a' * 32}
        assert target.verify('a' * 32)

    def test_mismatched_metadata_fails(self, fs):
        target = dest_with(fs).child('r1.fq.gz')
        fs.objects[target.url] = b'data'
        fs.meta[target.url] = {get_dest.MD5_METADATA_KEY: 'a' * 32}
        assert not target.verify('b' * 32)

    def test_object_without_our_metadata_fails(self, fs):
        """Uploaded by some other tool: we have no basis to call it verified."""
        target = dest_with(fs).child('r1.fq.gz')
        fs.objects[target.url] = b'data'
        assert not target.verify('a' * 32)

    def test_missing_object_fails_rather_than_raising(self, fs):
        assert not dest_with(fs).child('nope').verify('a' * 32)


# =============================================================================
# decide() against an S3 destination
# =============================================================================

class TestDecideOnS3:
    """The same matrix as the local path, over object metadata."""

    def _present(self, fs, md5):
        target = dest_with(fs).child('r1.fq.gz')
        fs.objects[target.url] = b'data'
        fs.meta[target.url] = {get_dest.MD5_METADATA_KEY: md5}
        return target

    def test_missing_is_fetched(self, fs):
        target = dest_with(fs).child('r1.fq.gz')
        assert get_mod.decide(target, 'a' * 32, False, False, False) == \
            ('fetch', 'missing')

    def test_force_refetches(self, fs):
        target = self._present(fs, 'a' * 32)
        assert get_mod.decide(target, 'a' * 32, True, False, False)[0] == 'fetch'

    def test_resume_skips_present(self, fs):
        target = self._present(fs, 'a' * 32)
        assert get_mod.decide(target, 'a' * 32, False, True, False) == \
            ('skip', 'already present')

    def test_check_verifies(self, fs):
        target = self._present(fs, 'a' * 32)
        assert get_mod.decide(target, 'a' * 32, False, False, True) == \
            ('skip', 'verified')

    def test_check_alone_reports_mismatch(self, fs):
        target = self._present(fs, 'a' * 32)
        action, note = get_mod.decide(target, 'b' * 32, False, False, True)
        assert action == 'bad' and 'MISMATCH' in note

    def test_check_with_resume_refetches_mismatch(self, fs):
        target = self._present(fs, 'a' * 32)
        action, _ = get_mod.decide(target, 'b' * 32, False, True, True)
        assert action == 'fetch'

    def test_exists_without_flags_is_left_alone(self, fs):
        target = self._present(fs, 'a' * 32)
        action, note = get_mod.decide(target, 'a' * 32, False, False, False)
        assert action == 'skip' and '-f' in note


# =============================================================================
# _upload_one
# =============================================================================

class TestUploadOne:

    def test_happy_path(self, fs):
        payload = b'ACGT' * 100
        md5 = md5_of(payload)
        odb = FakeODB(fs, {md5: payload})
        entry = {'relpath': 'r1.fq.gz', 'md5': md5, 'size': len(payload)}

        relpath, ok, note = get_mod._upload_one(
            entry, odb, dest_with(fs), False, False, False,
            get_dest.DEFAULT_CHUNK_SIZE,
        )
        assert (relpath, ok, note) == ('r1.fq.gz', True, 'uploaded')
        assert fs.objects['s3://bucket/fastqs/r1.fq.gz'] == payload

    def test_object_absent_from_source_remote(self, fs):
        odb = FakeODB(fs, {})
        entry = {'relpath': 'r1.fq.gz', 'md5': 'a' * 32, 'size': 1}
        _, ok, note = get_mod._upload_one(
            entry, odb, dest_with(fs), False, False, False,
            get_dest.DEFAULT_CHUNK_SIZE,
        )
        assert not ok and 'not on the source remote' in note

    def test_corrupt_source_is_detected_and_the_object_deleted(self, fs):
        """The source bytes do not hash to what DVC recorded.

        The upload has already committed by the time we know, so the object has
        to be removed -- it carries our md5 metadata and would pass --check for
        ever after.
        """
        payload = b'corrupted'
        wrong_md5 = 'a' * 32
        odb = FakeODB(fs, {wrong_md5: payload})
        entry = {'relpath': 'r1.fq.gz', 'md5': wrong_md5, 'size': len(payload)}

        _, ok, note = get_mod._upload_one(
            entry, odb, dest_with(fs), False, False, False,
            get_dest.DEFAULT_CHUNK_SIZE,
        )
        assert not ok
        assert 'CHECKSUM MISMATCH in flight' in note
        assert 's3://bucket/fastqs/r1.fq.gz' in fs.removed
        assert 's3://bucket/fastqs/r1.fq.gz' not in fs.objects

    def test_resume_skips_an_already_uploaded_object(self, fs):
        payload = b'ACGT'
        md5 = md5_of(payload)
        odb = FakeODB(fs, {md5: payload})
        dest = dest_with(fs)
        target = dest.child('r1.fq.gz')
        fs.objects[target.url] = payload
        fs.meta[target.url] = {get_dest.MD5_METADATA_KEY: md5}
        entry = {'relpath': 'r1.fq.gz', 'md5': md5, 'size': 4}

        _, ok, note = get_mod._upload_one(
            entry, odb, dest, False, True, False, get_dest.DEFAULT_CHUNK_SIZE,
        )
        assert ok and note == 'already present'
        assert fs.opened_with == []   # nothing was uploaded


# =============================================================================
# Credential guards
# =============================================================================

class TestRegionGuard:
    """`dt auth setup` writes region = auto for R2. Against real AWS that is
    not a region, and the profile is named after the repo -- so it is easy to
    aim a destination at one by accident."""

    def test_auto_region_without_endpoint_is_rejected(self):
        d = get_dest.S3Dest(
            's3://bucket/p', get_dest.S3DestConfig(region='auto')
        )
        with pytest.raises(GetError, match="resolves to 'auto'"):
            d._check_region()

    def test_auto_region_with_endpoint_is_fine(self):
        d = get_dest.S3Dest('s3://bucket/p', get_dest.S3DestConfig(
            region='auto', endpoint_url='https://acct.r2.cloudflarestorage.com'
        ))
        d._check_region()   # does not raise

    def test_real_region_is_fine(self):
        d = get_dest.S3Dest(
            's3://bucket/p', get_dest.S3DestConfig(region='ap-southeast-2')
        )
        d._check_region()

    def test_error_names_the_likely_cause(self):
        d = get_dest.S3Dest(
            's3://bucket/p', get_dest.S3DestConfig(region='auto')
        )
        with pytest.raises(GetError, match='dt auth setup'):
            d._check_region()


class TestIdentity:

    def test_compatible_endpoint_reports_the_profile_without_sts(self):
        """STS is an AWS service; an S3-compatible endpoint has none."""
        d = get_dest.S3Dest('s3://bucket/p', get_dest.S3DestConfig(
            profile='minio', endpoint_url='https://minio.local'
        ))
        assert d._describe_identity() == 'profile minio'

    def test_compatible_endpoint_without_profile(self):
        d = get_dest.S3Dest('s3://bucket/p', get_dest.S3DestConfig(
            endpoint_url='https://minio.local'
        ))
        assert d._describe_identity() == 'default credentials'


class TestProfileErrors:

    def test_unknown_profile_is_reported_readably(self):
        """Not a botocore traceback."""
        d = get_dest.S3Dest('s3://bucket/p', get_dest.S3DestConfig(
            profile='definitely-not-a-real-profile-xyz'
        ))
        with pytest.raises(GetError, match='Destination credentials unavailable'):
            d._session()

    def test_unknown_profile_lists_the_real_ones(self):
        """A typo that lands on another *valid* profile raises nothing at all,
        so showing what exists is the next best help."""
        d = get_dest.S3Dest('s3://bucket/p', get_dest.S3DestConfig(
            profile='definitely-not-a-real-profile-xyz'
        ))
        with pytest.raises(GetError) as excinfo:
            d._session()
        assert 'Profiles found:' in str(excinfo.value)


class TestMemoryEstimate:

    def test_scales_with_workers(self):
        one = get_dest.memory_estimate(1)
        assert get_dest.memory_estimate(8) == 8 * one

    def test_default_is_modest(self):
        """A number worth knowing before running this on a small instance."""
        mib = get_dest.memory_estimate(8) // (1024 * 1024)
        assert 50 < mib < 250
