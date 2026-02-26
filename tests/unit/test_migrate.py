"""Tests for dt migrate module."""

import hashlib
import json
from pathlib import Path
from unittest.mock import patch

import pytest
import yaml

from dt import migrate, utils
from dt.errors import MigrateError


# =============================================================================
# Helpers
# =============================================================================

def _make_v2_dvc(tmp_path, name='data.txt', content=b'hello world', **extra_out):
    """Create a v2 .dvc file and its workspace data file.

    Returns (dvc_path, data_path, md5).
    """
    data_path = tmp_path / name
    data_path.write_bytes(content)
    md5 = hashlib.md5(content).hexdigest()

    out = {'md5': md5, 'size': len(content), 'path': name}
    out.update(extra_out)
    dvc_data = {'outs': [out]}

    dvc_path = tmp_path / f'{name}.dvc'
    dvc_path.write_text(yaml.dump(dvc_data, sort_keys=False))
    return dvc_path, data_path, md5


def _make_v3_dvc(tmp_path, name='data.txt', content=b'hello world'):
    """Create a v3 .dvc file and its workspace data file.

    Returns (dvc_path, data_path, md5).
    """
    data_path = tmp_path / name
    data_path.write_bytes(content)
    md5 = hashlib.md5(content).hexdigest()

    dvc_data = {
        'outs': [{'md5': md5, 'size': len(content), 'hash': 'md5', 'path': name}]
    }

    dvc_path = tmp_path / f'{name}.dvc'
    dvc_path.write_text(yaml.dump(dvc_data, sort_keys=False))
    return dvc_path, data_path, md5


def _make_cache_file(cache_root, md5, content, v3=True, is_dir=False):
    """Place content into the cache under the given hash."""
    suffix = '.dir' if is_dir else ''
    hash_clean = md5.replace('.dir', '')

    if v3:
        dest = cache_root / 'files' / 'md5' / hash_clean[:2] / (hash_clean[2:] + suffix)
    else:
        dest = cache_root / hash_clean[:2] / (hash_clean[2:] + suffix)

    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_bytes(content)
    return dest


# =============================================================================
# Hash computation
# =============================================================================

class TestMd5File:
    """Tests for utils.md5_file."""

    def test_hashes_file_correctly(self, tmp_path):
        content = b'test content'
        f = tmp_path / 'file.bin'
        f.write_bytes(content)

        assert utils.md5_file(f) == hashlib.md5(content).hexdigest()

    def test_binary_file(self, tmp_path):
        content = bytes(range(256))
        f = tmp_path / 'file.bin'
        f.write_bytes(content)

        assert utils.md5_file(f) == hashlib.md5(content).hexdigest()

    def test_crlf_not_normalised(self, tmp_path):
        """v3 hashing does NOT normalise line endings."""
        content = b'line1\r\nline2\r\n'
        f = tmp_path / 'file.txt'
        f.write_bytes(content)

        # Hash should match the raw content including \r\n
        assert utils.md5_file(f) == hashlib.md5(content).hexdigest()
        # And should differ from the dos2unix'd version
        normalised = content.replace(b'\r\n', b'\n')
        assert utils.md5_file(f) != hashlib.md5(normalised).hexdigest()


class TestMd5Bytes:
    def test_basic(self):
        data = b'hello'
        assert utils.md5_bytes(data) == hashlib.md5(data).hexdigest()


# =============================================================================
# .dvc file analysis
# =============================================================================

class TestIsV3:
    def test_v3_file(self):
        data = {'outs': [{'md5': 'abc', 'hash': 'md5', 'path': 'x'}]}
        assert migrate.is_v3(data) is True

    def test_v2_file(self):
        data = {'outs': [{'md5': 'abc', 'path': 'x'}]}
        assert migrate.is_v3(data) is False

    def test_empty_outs(self):
        assert migrate.is_v3({'outs': []}) is True

    def test_no_outs(self):
        assert migrate.is_v3({}) is True

    def test_mixed_outs(self):
        """If one output has hash and another doesn't, not v3."""
        data = {
            'outs': [
                {'md5': 'a', 'hash': 'md5', 'path': 'x'},
                {'md5': 'b', 'path': 'y'},
            ]
        }
        assert migrate.is_v3(data) is False


class TestIsImport:
    def test_regular_file(self):
        data = {'outs': [{'md5': 'abc', 'path': 'x'}]}
        assert migrate.is_import(data) is False

    def test_import_file(self):
        data = {
            'deps': [{'path': 'data/file.csv', 'repo': {'url': 'https://example.com'}}],
            'outs': [{'md5': 'abc', 'path': 'x'}],
        }
        assert migrate.is_import(data) is True

    def test_dep_without_repo(self):
        data = {
            'deps': [{'path': 'input.csv'}],
            'outs': [{'md5': 'abc', 'path': 'x'}],
        }
        assert migrate.is_import(data) is False


# =============================================================================
# Directory manifest helpers
# =============================================================================

class TestBuildDirManifest:
    def test_single_entry(self):
        entries = [{'md5': 'abc123', 'relpath': 'file.txt'}]
        result = utils.build_dir_manifest(entries)
        assert result == b'[{"md5": "abc123", "relpath": "file.txt"}]'

    def test_sorted_by_relpath(self):
        entries = [
            {'md5': 'bbb', 'relpath': 'z.txt'},
            {'md5': 'aaa', 'relpath': 'a.txt'},
        ]
        result = utils.build_dir_manifest(entries)
        parsed = json.loads(result)
        assert parsed[0]['relpath'] == 'a.txt'
        assert parsed[1]['relpath'] == 'z.txt'

    def test_no_trailing_newline(self):
        entries = [{'md5': 'abc', 'relpath': 'x'}]
        result = utils.build_dir_manifest(entries)
        assert not result.endswith(b'\n')



# =============================================================================
# Single file migration
# =============================================================================

class TestMigrateSingleOutput:
    def test_hash_unchanged_for_binary(self, tmp_path):
        """Binary files have same hash in v2 and v3."""
        content = bytes(range(256))
        md5 = hashlib.md5(content).hexdigest()
        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, md5, content, v3=True)

        out = {'md5': md5, 'size': len(content), 'path': 'bin.dat'}
        result = migrate.migrate_single_output(out, cache_root)

        assert result['md5'] == md5
        assert result['hash'] == 'md5'

    def test_hash_unchanged_for_unix_text(self, tmp_path):
        """Unix text files (LF only) have same hash in v2 and v3."""
        content = b'line1\nline2\n'
        md5 = hashlib.md5(content).hexdigest()
        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, md5, content, v3=True)

        out = {'md5': md5, 'size': len(content), 'path': 'unix.txt'}
        result = migrate.migrate_single_output(out, cache_root)

        assert result['md5'] == md5
        assert result['hash'] == 'md5'

    def test_hash_changes_for_crlf(self, tmp_path):
        """CRLF text files get a different hash in v3."""
        content = b'line1\r\nline2\r\n'
        normalised = content.replace(b'\r\n', b'\n')
        v2_hash = hashlib.md5(normalised).hexdigest()
        v3_hash = hashlib.md5(content).hexdigest()

        cache_root = tmp_path / 'cache'
        # v2 stored the file under the dos2unix'd hash, but the actual
        # content on disk is the original (with CRLF)
        _make_cache_file(cache_root, v2_hash, content, v3=True)

        out = {'md5': v2_hash, 'size': len(content), 'path': 'crlf.txt'}
        result = migrate.migrate_single_output(out, cache_root)

        assert result['md5'] == v3_hash
        assert result['md5'] != v2_hash
        assert result['hash'] == 'md5'

    def test_hashes_from_v2_cache_layout(self, tmp_path):
        """Finds files in v2 cache layout (XX/hash at root)."""
        content = b'v2 cached content'
        md5 = hashlib.md5(content).hexdigest()

        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, md5, content, v3=False)

        out = {'md5': md5, 'size': len(content), 'path': 'file.txt'}
        result = migrate.migrate_single_output(out, cache_root)

        assert result['md5'] == md5
        assert result['hash'] == 'md5'

    def test_keeps_hash_when_not_in_cache(self, tmp_path):
        """When file not in cache, keeps existing hash and adds hash field."""
        cache_root = tmp_path / 'empty_cache'
        cache_root.mkdir()

        old_md5 = 'deadbeef' * 4
        out = {'md5': old_md5, 'size': 100, 'path': 'gone.txt'}
        result = migrate.migrate_single_output(out, cache_root)

        assert result['md5'] == old_md5
        assert result['hash'] == 'md5'

    def test_ensures_v3_cache_on_hash_change(self, tmp_path):
        """When hash changes, new hash is written to v3 cache."""
        content = b'line\r\n'
        normalised = content.replace(b'\r\n', b'\n')
        v2_hash = hashlib.md5(normalised).hexdigest()
        v3_hash = hashlib.md5(content).hexdigest()

        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, v2_hash, content, v3=True)

        out = {'md5': v2_hash, 'size': len(content), 'path': 'crlf.txt'}
        migrate.migrate_single_output(out, cache_root)

        # New hash should exist in v3 cache
        from dt.cache_ops import get_cache_file_path
        new_cache_path = get_cache_file_path(v3_hash, cache_root, use_v3_layout=True)
        assert new_cache_path.exists()


# =============================================================================
# Directory migration
# =============================================================================

class TestMigrateDirectoryOutput:
    def test_migrates_directory(self, tmp_path):
        """Migrates a directory with child files in cache."""
        a_md5 = hashlib.md5(b'aaa').hexdigest()
        b_md5 = hashlib.md5(b'bbb').hexdigest()

        # Build old manifest
        old_entries = [
            {'md5': a_md5, 'relpath': 'a.txt'},
            {'md5': b_md5, 'relpath': 'b.txt'},
        ]
        old_manifest = utils.build_dir_manifest(old_entries)
        old_dir_md5 = hashlib.md5(old_manifest).hexdigest()
        old_dir_hash = old_dir_md5 + '.dir'

        # Put manifest and child files in cache
        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, old_dir_hash, old_manifest, v3=True, is_dir=True)
        _make_cache_file(cache_root, a_md5, b'aaa', v3=True)
        _make_cache_file(cache_root, b_md5, b'bbb', v3=True)

        out = {
            'md5': old_dir_hash,
            'size': 100,
            'nfiles': 2,
            'path': 'mydir',
        }

        result = migrate.migrate_directory_output(out, cache_root)

        assert result['hash'] == 'md5'
        assert result['md5'].endswith('.dir')

    def test_keeps_hash_when_manifest_not_found(self, tmp_path):
        """When .dir manifest not in cache, keeps existing hash and adds hash field."""
        cache_root = tmp_path / 'cache'
        cache_root.mkdir()

        old_md5 = 'deadbeef' * 4 + '.dir'
        out = {'md5': old_md5, 'path': 'mydir'}
        result = migrate.migrate_directory_output(out, cache_root)

        assert result['md5'] == old_md5
        assert result['hash'] == 'md5'

    def test_error_child_not_in_cache(self, tmp_path):
        """Raises MigrateError when a child file is missing from cache."""
        a_md5 = hashlib.md5(b'aaa').hexdigest()
        missing_md5 = 'deadbeef' * 4

        old_entries = [
            {'md5': a_md5, 'relpath': 'a.txt'},
            {'md5': missing_md5, 'relpath': 'missing.txt'},
        ]
        old_manifest = utils.build_dir_manifest(old_entries)
        old_dir_md5 = hashlib.md5(old_manifest).hexdigest()
        old_dir_hash = old_dir_md5 + '.dir'

        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, old_dir_hash, old_manifest, v3=True, is_dir=True)
        _make_cache_file(cache_root, a_md5, b'aaa', v3=True)
        # missing_md5 is NOT placed in cache

        out = {'md5': old_dir_hash, 'path': 'mydir'}
        with pytest.raises(MigrateError, match='Cannot find'):
            migrate.migrate_directory_output(out, cache_root)


# =============================================================================
# Top-level checksum
# =============================================================================

class TestRecomputeDvcMd5:
    def test_recomputes_for_import(self):
        """Recomputes the top-level md5 for import files."""
        data = {
            'md5': 'old_hash',
            'frozen': True,
            'deps': [{'path': 'data/file.csv', 'repo': {'url': 'https://example.com'}}],
            'outs': [{'md5': 'abc', 'hash': 'md5', 'path': 'file.csv'}],
        }
        result = utils.recompute_dvc_md5(data)
        assert result['md5'] != 'old_hash'
        assert len(result['md5']) == 32  # valid md5

    def test_noop_without_top_level_md5(self):
        """No-op for files without a top-level md5."""
        data = {'outs': [{'md5': 'abc', 'path': 'x'}]}
        result = utils.recompute_dvc_md5(data)
        assert 'md5' not in result

    def test_deterministic(self):
        """Same content produces same checksum."""
        data = {
            'md5': 'placeholder',
            'outs': [{'md5': 'abc', 'hash': 'md5', 'path': 'x'}],
        }
        r1 = utils.recompute_dvc_md5(dict(data))
        r2 = utils.recompute_dvc_md5(dict(data))
        assert r1['md5'] == r2['md5']

    def test_matches_dvc_dict_md5(self):
        """Must produce the same hash as DVC's own dict_md5."""
        from dvc.utils import dict_md5

        data = {
            'md5': 'old_hash',
            'frozen': True,
            'deps': [
                {
                    'path': 'data/file.csv',
                    'repo': {
                        'url': 'https://github.com/example/repo',
                        'rev_lock': 'abc123def456',
                    },
                },
            ],
            'outs': [
                {'md5': 'f4265d6b19fbb80c34d6f47a0048107f',
                 'size': 75, 'hash': 'md5', 'path': 'file.csv'},
            ],
        }

        result = utils.recompute_dvc_md5(data)

        # Manually compute what DVC's compute_md5 would produce
        d = {k: v for k, v in data.items() if k not in ('md5', 'meta', 'desc')}
        expected = dict_md5(d, exclude=utils._DVC_MD5_EXCLUDE_FIELDS)
        assert result['md5'] == expected


# =============================================================================
# Full file migration
# =============================================================================

class TestMigrateDvcFile:
    def test_skips_v3_file(self, tmp_path):
        dvc_path, _, _ = _make_v3_dvc(tmp_path)
        cache_root = tmp_path / 'cache'
        cache_root.mkdir()

        result = migrate.migrate_dvc_file(dvc_path, cache_root=cache_root)

        assert result['status'] == 'skipped'

    def test_migrates_v2_file(self, tmp_path):
        content = b'simple data'
        dvc_path, _, md5 = _make_v2_dvc(tmp_path, content=content)

        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, md5, content, v3=True)

        result = migrate.migrate_dvc_file(dvc_path, cache_root=cache_root)

        assert result['status'] == 'migrated'

        # Verify the file was updated
        updated = yaml.safe_load(dvc_path.read_text())
        assert updated['outs'][0]['hash'] == 'md5'
        assert updated['outs'][0]['md5'] == md5

    def test_dry_run_no_modification(self, tmp_path):
        content = b'dry run data'
        dvc_path, _, md5 = _make_v2_dvc(tmp_path, content=content)
        original_text = dvc_path.read_text()

        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, md5, content, v3=True)

        result = migrate.migrate_dvc_file(dvc_path, cache_root=cache_root, dry_run=True)

        assert result['status'] == 'would_migrate'
        assert dvc_path.read_text() == original_text

    def test_migrates_import(self, tmp_path):
        """Migrates an import .dvc file, updating the top-level md5."""
        content = b'imported data'
        md5 = hashlib.md5(content).hexdigest()

        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, md5, content, v3=True)

        dvc_data = {
            'md5': 'old_top_level_hash',
            'frozen': True,
            'deps': [
                {
                    'path': 'data/file.csv',
                    'repo': {
                        'url': 'https://github.com/example/repo',
                        'rev_lock': 'abc123',
                    },
                }
            ],
            'outs': [{'md5': md5, 'size': len(content), 'path': 'imported.csv'}],
        }
        dvc_path = tmp_path / 'imported.csv.dvc'
        dvc_path.write_text(yaml.dump(dvc_data, sort_keys=False))

        result = migrate.migrate_dvc_file(dvc_path, cache_root=cache_root)

        assert result['status'] == 'migrated'
        assert result['is_import'] is True

        updated = yaml.safe_load(dvc_path.read_text())
        assert updated['outs'][0]['hash'] == 'md5'
        assert updated['md5'] != 'old_top_level_hash'

    def test_error_without_cache(self, tmp_path):
        """Raises MigrateError when no cache is available."""
        dvc_path, _, _ = _make_v2_dvc(tmp_path, content=b'data')

        with patch.object(migrate, '_detect_cache_root', return_value=None):
            with pytest.raises(MigrateError, match='no cache found'):
                migrate.migrate_dvc_file(dvc_path)


# =============================================================================
# Project-level migration
# =============================================================================

class TestMigrateProject:
    def _setup_cache(self, cache_root, *contents):
        """Helper to populate cache with files for each content bytes."""
        for content in contents:
            md5 = hashlib.md5(content).hexdigest()
            _make_cache_file(cache_root, md5, content, v3=True)

    def test_finds_and_migrates_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cache_root = tmp_path / 'cache'

        _make_v2_dvc(tmp_path, 'a.txt', b'aaa')
        _make_v2_dvc(tmp_path, 'b.txt', b'bbb')
        _make_v3_dvc(tmp_path, 'c.txt', b'ccc')
        self._setup_cache(cache_root, b'aaa', b'bbb', b'ccc')

        result = migrate.migrate_project(cache_root=cache_root)

        assert result['total'] == 3
        assert result['migrated'] == 2
        assert result['skipped'] == 1
        assert result['errors'] == 0

    def test_with_targets(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        cache_root = tmp_path / 'cache'

        _make_v2_dvc(tmp_path, 'a.txt', b'aaa')
        _make_v2_dvc(tmp_path, 'b.txt', b'bbb')
        self._setup_cache(cache_root, b'aaa', b'bbb')

        result = migrate.migrate_project(targets=['a.txt.dvc'], cache_root=cache_root)

        assert result['total'] == 1
        assert result['migrated'] == 1

    def test_target_by_data_path(self, tmp_path, monkeypatch):
        """Can specify a data path instead of .dvc path."""
        monkeypatch.chdir(tmp_path)
        cache_root = tmp_path / 'cache'

        _make_v2_dvc(tmp_path, 'data.csv', b'data')
        self._setup_cache(cache_root, b'data')

        result = migrate.migrate_project(targets=['data.csv'], cache_root=cache_root)

        assert result['total'] == 1
        assert result['migrated'] == 1

    def test_target_directory(self, tmp_path, monkeypatch):
        """Can target a directory of .dvc files."""
        monkeypatch.chdir(tmp_path)
        cache_root = tmp_path / 'cache'
        subdir = tmp_path / 'data'
        subdir.mkdir()

        _make_v2_dvc(subdir, 'x.txt', b'xxx')
        _make_v2_dvc(subdir, 'y.txt', b'yyy')
        _make_v2_dvc(tmp_path, 'root.txt', b'rrr')  # not in subdir
        self._setup_cache(cache_root, b'xxx', b'yyy', b'rrr')

        result = migrate.migrate_project(targets=['data'], cache_root=cache_root)

        assert result['total'] == 2
        assert result['migrated'] == 2

    def test_migrates_without_cache(self, tmp_path, monkeypatch):
        """Files not in cache are migrated by keeping existing hash."""
        monkeypatch.chdir(tmp_path)
        cache_root = tmp_path / 'cache'

        _make_v2_dvc(tmp_path, 'good.txt', b'good')
        self._setup_cache(cache_root, b'good')

        # Create a .dvc file whose data is missing from cache
        missing_dvc = tmp_path / 'missing.txt.dvc'
        missing_dvc.write_text(yaml.dump({
            'outs': [{'md5': 'deadbeef' * 4, 'size': 100, 'path': 'missing.txt'}],
        }, sort_keys=False))

        result = migrate.migrate_project(cache_root=cache_root)

        assert result['total'] == 2
        assert result['migrated'] == 2
        assert result['errors'] == 0


# =============================================================================
# File collection
# =============================================================================

class TestCollectDvcFiles:
    def test_excludes_dvc_config_dir(self, tmp_path, monkeypatch):
        """Should not pick up files inside .dvc/ directory."""
        monkeypatch.chdir(tmp_path)

        (tmp_path / '.dvc').mkdir()
        (tmp_path / '.dvc' / 'something.dvc').write_text('test')
        _make_v2_dvc(tmp_path, 'real.txt', b'data')

        result = migrate._collect_dvc_files()
        assert len(result) == 1
        assert result[0].name == 'real.txt.dvc'

    def test_invalid_target(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        with pytest.raises(MigrateError, match='Target not found'):
            migrate._collect_dvc_files(['nonexistent.dvc'])


# =============================================================================
# Analyse
# =============================================================================

class TestAnalyseDvcFile:
    def test_v3_file(self, tmp_path):
        dvc_path, _, _ = _make_v3_dvc(tmp_path)
        result = migrate.analyse_dvc_file(dvc_path)

        assert result['is_v3'] is True
        assert result['can_migrate'] is True

    def test_v2_file_in_cache(self, tmp_path):
        content = b'hello world'
        dvc_path, _, md5 = _make_v2_dvc(tmp_path, content=content)

        cache_root = tmp_path / 'cache'
        _make_cache_file(cache_root, md5, content, v3=True)

        result = migrate.analyse_dvc_file(dvc_path, cache_root=cache_root)

        assert result['is_v3'] is False
        assert result['can_migrate'] is True
        assert result['outputs'][0]['in_cache'] is True

    def test_v2_file_missing_from_cache(self, tmp_path):
        """Cannot migrate when data is not in cache."""
        dvc_path = tmp_path / 'gone.txt.dvc'
        dvc_path.write_text(yaml.dump({
            'outs': [{'md5': 'deadbeef' * 4, 'size': 100, 'path': 'gone.txt'}],
        }, sort_keys=False))

        result = migrate.analyse_dvc_file(dvc_path)

        assert result['is_v3'] is False
        assert result['can_migrate'] is False
        assert 'not found in cache' in result['reason']


# =============================================================================
# Find v2 files
# =============================================================================

class TestFindV2Files:
    def test_finds_v2_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _make_v2_dvc(tmp_path, 'a.txt', b'aaa')
        _make_v2_dvc(tmp_path, 'b.txt', b'bbb')
        _make_v3_dvc(tmp_path, 'c.txt', b'ccc')

        result = migrate.find_v2_files()

        assert len(result) == 2
        paths = [f['path'] for f in result]
        assert any('a.txt.dvc' in p for p in paths)
        assert any('b.txt.dvc' in p for p in paths)

    def test_returns_empty_when_all_v3(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _make_v3_dvc(tmp_path, 'a.txt', b'aaa')
        _make_v3_dvc(tmp_path, 'b.txt', b'bbb')

        result = migrate.find_v2_files()
        assert result == []

    def test_marks_imports(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)

        dvc_data = {
            'deps': [{'path': 'data.csv', 'repo': {'url': 'https://example.com'}}],
            'outs': [{'md5': 'abc123', 'size': 10, 'path': 'data.csv'}],
        }
        (tmp_path / 'data.csv.dvc').write_text(yaml.dump(dvc_data, sort_keys=False))

        result = migrate.find_v2_files()

        assert len(result) == 1
        assert result[0]['is_import'] is True

    def test_with_targets(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _make_v2_dvc(tmp_path, 'a.txt', b'aaa')
        _make_v2_dvc(tmp_path, 'b.txt', b'bbb')

        result = migrate.find_v2_files(targets=['a.txt.dvc'])

        assert len(result) == 1
        assert 'a.txt.dvc' in result[0]['path']

    def test_skips_unparseable_files(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        _make_v2_dvc(tmp_path, 'good.txt', b'good')
        (tmp_path / 'bad.txt.dvc').write_text('{{invalid yaml')

        result = migrate.find_v2_files()

        assert len(result) == 1
        assert 'good.txt.dvc' in result[0]['path']
