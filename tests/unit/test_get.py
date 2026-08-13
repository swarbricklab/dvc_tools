"""Unit tests for dt/get.py.

Covers the pieces that carry real risk: which link type gets used (it decides
whether output is a copy or a hardlink into the shared cache), how a subpath
inside a tracked directory is resolved, and the CSV contract shared with
``dt import --csv``.
"""

import json
import time
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dt import get as get_mod
from dt import utils
from dt.errors import GetError


# =============================================================================
# Link type resolution
# =============================================================================

class TestResolveLinkTypes:
    """Which link types dt get will attempt, in order."""

    def test_default_excludes_symlink(self):
        """Without config, never symlink.

        A symlink points into a cache the recipient may not be able to read,
        and it would 'succeed' silently while producing an unusable file.
        """
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='')
            assert get_mod.resolve_link_types() == ['reflink', 'hardlink', 'copy']

    def test_honours_configured_cache_type(self):
        """A configured cache.type wins over the default ordering."""
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='reflink,hardlink\n')
            assert get_mod.resolve_link_types() == ['reflink', 'hardlink', 'copy']

    def test_drops_symlink_from_configured_cache_type(self):
        """The real bug: cache.type here is "hardlink,symlink".

        /scratch and /g/data are different filesystems, so hardlink fails EXDEV
        and the chain lands on symlink -- which "succeeds" having moved no
        bytes. dt get then reported "2 fetched, 0 failed" over four dangling
        pointers into a cache the recipient cannot reach.
        """
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='hardlink,symlink\n')
            assert get_mod.resolve_link_types() == ['hardlink', 'copy']

    def test_copy_always_terminates_a_configured_chain(self):
        """A bare "hardlink" config would otherwise fail outright cross-fs."""
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='hardlink\n')
            assert get_mod.resolve_link_types() == ['hardlink', 'copy']

    def test_symlink_only_config_falls_back_to_the_default(self):
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='symlink\n')
            assert get_mod.resolve_link_types() == list(get_mod.DEFAULT_LINK_TYPES)

    def test_explicit_link_symlink_is_still_allowed(self):
        """Config is a default; an explicit flag is a decision."""
        assert get_mod.resolve_link_types('symlink') == ['symlink']

    def test_ignores_unknown_entries_in_config(self):
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='hardlink,bogus\n')
            assert get_mod.resolve_link_types() == ['hardlink', 'copy']

    def test_falls_back_when_config_is_all_junk(self):
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='bogus\n')
            assert get_mod.resolve_link_types() == list(get_mod.DEFAULT_LINK_TYPES)

    def test_falls_back_when_not_in_a_repo(self):
        """dvc config exits non-zero outside a repo; that is the off-NCI case."""
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=251, stdout='', stderr='no repo')
            assert get_mod.resolve_link_types() == list(get_mod.DEFAULT_LINK_TYPES)

    def test_falls_back_when_dvc_missing(self):
        with patch.object(get_mod.subprocess, 'run', side_effect=OSError('no dvc')):
            assert get_mod.resolve_link_types() == list(get_mod.DEFAULT_LINK_TYPES)

    def test_explicit_override_wins(self):
        with patch.object(get_mod.subprocess, 'run') as mock_run:
            mock_run.return_value = MagicMock(returncode=0, stdout='hardlink\n')
            assert get_mod.resolve_link_types('copy') == ['copy']
            mock_run.assert_not_called()

    def test_explicit_override_accepts_a_list(self):
        assert get_mod.resolve_link_types('reflink,copy') == ['reflink', 'copy']

    def test_rejects_unknown_override(self):
        with pytest.raises(GetError, match='Unknown link type'):
            get_mod.resolve_link_types('teleport')


# =============================================================================
# Listing source files
# =============================================================================

def _dvc_list_json(entries):
    return MagicMock(returncode=0, stdout=json.dumps(entries), stderr='')


class TestListSourceFiles:
    """Resolving a path -- including a subpath of a tracked dir -- to hashes."""

    def test_returns_relpaths_and_hashes(self, tmp_path):
        listing = [
            {'path': 'R1.fq.gz', 'md5': 'a' * 32, 'size': 10, 'isout': False, 'isdir': False},
            {'path': 'R2.fq.gz', 'md5': 'b' * 32, 'size': 20, 'isout': False, 'isdir': False},
        ]
        with patch.object(get_mod.subprocess, 'run', return_value=_dvc_list_json(listing)):
            files = get_mod.list_source_files(tmp_path, 'data/fq/AF013-A')

        assert [f['relpath'] for f in files] == ['R1.fq.gz', 'R2.fq.gz']
        assert files[0]['md5'] == 'a' * 32
        assert files[1]['size'] == 20

    def test_passes_recursive_and_hash_flags(self, tmp_path):
        """These flags are what make subpath resolution work."""
        listing = [{'path': 'f', 'md5': 'c' * 32, 'size': 1, 'isout': False, 'isdir': False}]
        with patch.object(get_mod.subprocess, 'run', return_value=_dvc_list_json(listing)) as run:
            get_mod.list_source_files(tmp_path, 'data/fq/AF013-A', rev='abc123')

        cmd = run.call_args[0][0]
        assert '--show-hash' in cmd and '-R' in cmd and '--json' in cmd
        assert cmd[-2:] == ['--rev', 'abc123']
        assert 'data/fq/AF013-A' in cmd

    def test_skips_the_dir_entry_itself(self, tmp_path):
        """The .dir output is metadata, not a file to place."""
        listing = [
            {'path': 'AF013-A', 'md5': 'd' * 32 + '.dir', 'isout': True, 'isdir': True},
            {'path': 'R1.fq.gz', 'md5': 'e' * 32, 'size': 5, 'isout': False, 'isdir': False},
        ]
        with patch.object(get_mod.subprocess, 'run', return_value=_dvc_list_json(listing)):
            files = get_mod.list_source_files(tmp_path, 'data/fq/AF013-A')

        assert [f['relpath'] for f in files] == ['R1.fq.gz']

    def test_missing_path_gets_a_readable_error(self, tmp_path):
        """DVC reports the miss against our internal clone dir, which is noise.

        The reader asked for a path in a repository, not one in
        .dt/tmp/clones/<mangled-url>/.
        """
        failed = MagicMock(
            returncode=1, stdout='',
            stderr=f"ERROR: [Errno 2] No such file or directory: '{tmp_path}/data/nope'",
        )
        with patch.object(get_mod.subprocess, 'run', return_value=failed):
            with pytest.raises(GetError) as excinfo:
                get_mod.list_source_files(tmp_path, 'data/nope')

        assert str(excinfo.value) == 'Not found in the source repository: data/nope'
        assert str(tmp_path) not in str(excinfo.value)

    def test_support_footer_does_not_displace_the_real_error(self, tmp_path):
        """DVC signs off on its own line after the error.

        Taking the last line of stderr reported "Having any troubles? Hit us up
        at ..." and threw the actual message away -- seen for real on a
        database-is-locked failure.
        """
        failed = MagicMock(
            returncode=1, stdout='',
            stderr=(
                'ERROR: unexpected error - database is locked\n'
                '\n'
                'Having any troubles? Hit us up at https://dvc.org/support, '
                'we are always happy to help!\n'
            ),
        )
        with patch.object(get_mod.subprocess, 'run', return_value=failed):
            with pytest.raises(GetError) as excinfo:
                get_mod.list_source_files(tmp_path, 'data/x')

        assert 'database is locked' in str(excinfo.value)
        assert 'dvc.org/support' not in str(excinfo.value)

    def test_retries_while_the_state_db_is_locked(self, tmp_path):
        """A cold clone's SQLite db is created on first read; readers contend."""
        locked = MagicMock(
            returncode=1, stdout='',
            stderr='ERROR: unexpected error - database is locked\n',
        )
        ok = _dvc_list_json(
            [{'path': 'f', 'md5': 'a' * 32, 'size': 1, 'isout': True, 'isdir': False}]
        )
        with patch.object(get_mod.subprocess, 'run', side_effect=[locked, locked, ok]), \
             patch.object(get_mod.time, 'sleep'):
            files = get_mod.list_source_files(tmp_path, 'data/x')

        assert [f['relpath'] for f in files] == ['f']

    def test_gives_up_on_a_persistent_lock(self, tmp_path):
        locked = MagicMock(
            returncode=1, stdout='',
            stderr='ERROR: unexpected error - database is locked\n',
        )
        with patch.object(get_mod.subprocess, 'run', return_value=locked), \
             patch.object(get_mod.time, 'sleep'):
            with pytest.raises(GetError, match='database is locked'):
                get_mod.list_source_files(tmp_path, 'data/x')

    def test_does_not_retry_a_real_failure(self, tmp_path):
        """Only lock contention is transient; retrying a missing path is waste."""
        missing = MagicMock(
            returncode=1, stdout='', stderr="ERROR: 'nope' does not exist",
        )
        with patch.object(get_mod.subprocess, 'run', return_value=missing) as run:
            with pytest.raises(GetError):
                get_mod.list_source_files(tmp_path, 'nope')

        assert run.call_count == 1

    def test_other_errors_keep_their_detail_but_hide_the_clone(self, tmp_path):
        failed = MagicMock(
            returncode=1, stdout='',
            stderr=f"ERROR: unexpected trouble reading {tmp_path}/x",
        )
        with patch.object(get_mod.subprocess, 'run', return_value=failed):
            with pytest.raises(GetError) as excinfo:
                get_mod.list_source_files(tmp_path, 'data/x')

        assert 'unexpected trouble' in str(excinfo.value)
        assert str(tmp_path) not in str(excinfo.value)
        assert '<source>' in str(excinfo.value)

    def test_raises_when_listing_is_empty(self, tmp_path):
        with patch.object(get_mod.subprocess, 'run', return_value=_dvc_list_json([])):
            with pytest.raises(GetError, match='No tracked files found'):
                get_mod.list_source_files(tmp_path, 'data/empty')

    def test_raises_on_unparseable_output(self, tmp_path):
        bad = MagicMock(returncode=0, stdout='not json', stderr='')
        with patch.object(get_mod.subprocess, 'run', return_value=bad):
            with pytest.raises(GetError, match='Could not parse'):
                get_mod.list_source_files(tmp_path, 'data/x')


# =============================================================================
# Materialising files
# =============================================================================

def _seed_cache(cache_root: Path, md5: str, content: bytes) -> Path:
    """Write an object into a v3-layout cache and make it read-only like DVC."""
    blob = cache_root / 'files' / 'md5' / md5[:2] / md5[2:]
    blob.parent.mkdir(parents=True, exist_ok=True)
    blob.write_bytes(content)
    blob.chmod(0o444)
    return blob


class TestMaterialise:
    """Placing cache objects at real destination paths."""

    def test_writes_files_under_dest(self, tmp_path):
        cache = tmp_path / 'cache'
        md5 = 'a' * 32
        _seed_cache(cache, md5, b'hello')
        dest = tmp_path / 'out'

        results = get_mod.materialise(
            [{'relpath': 'sub/R1.fq.gz', 'md5': md5, 'size': 5}],
            cache, dest, ['copy'], jobs=1,
        )

        assert results == [('sub/R1.fq.gz', True, 'copy')]
        assert (dest / 'sub' / 'R1.fq.gz').read_bytes() == b'hello'

    def test_copied_output_is_writable(self, tmp_path):
        """The recipient owns this data; 0444 would be actively unhelpful.

        link_file protects cache entries with 0o444, which is right for a cache
        and wrong for a hand-off.
        """
        cache = tmp_path / 'cache'
        md5 = 'b' * 32
        _seed_cache(cache, md5, b'data')
        dest = tmp_path / 'out'

        get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': 4}], cache, dest, ['copy'], jobs=1,
        )

        assert os.access(dest / 'f.txt', os.W_OK)

    def test_hardlink_leaves_cache_mode_alone(self, tmp_path):
        """A hardlink shares the cache inode -- widening it would expose the cache."""
        cache = tmp_path / 'cache'
        md5 = 'c' * 32
        blob = _seed_cache(cache, md5, b'shared')
        dest = tmp_path / 'out'

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': 6}], cache, dest, ['hardlink'], jobs=1,
        )

        assert results[0][2] == 'hardlink'
        assert (blob.stat().st_mode & 0o777) == 0o444

    def test_falls_through_to_next_link_type(self, tmp_path):
        """Hardlink fails across filesystems; copy must still happen."""
        cache = tmp_path / 'cache'
        md5 = 'd' * 32
        _seed_cache(cache, md5, b'xyz')
        dest = tmp_path / 'out'

        real = get_mod.cache_ops.link_file

        def fail_hardlink(source, target, **kwargs):
            if kwargs.get('cache_type') == 'hardlink':
                return False, 'failed'
            return real(source, target, **kwargs)

        with patch.object(get_mod.cache_ops, 'link_file', side_effect=fail_hardlink):
            results = get_mod.materialise(
                [{'relpath': 'f.txt', 'md5': md5, 'size': 3}],
                cache, dest, ['hardlink', 'copy'], jobs=1,
            )

        assert results[0][1] is True
        assert results[0][2] == 'copy'
        assert (dest / 'f.txt').read_bytes() == b'xyz'

    def test_reports_object_missing_from_cache(self, tmp_path):
        cache = tmp_path / 'cache'
        cache.mkdir()
        dest = tmp_path / 'out'

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': 'e' * 32, 'size': 1}], cache, dest, ['copy'], jobs=1,
        )

        assert results[0][1] is False
        assert 'not in source cache' in results[0][2]

    def test_existing_file_is_kept_without_force(self, tmp_path):
        cache = tmp_path / 'cache'
        md5 = 'f' * 32
        _seed_cache(cache, md5, b'new')
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'f.txt').write_bytes(b'old')

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': 3}], cache, dest, ['copy'], jobs=1,
        )

        assert results[0][1] is True
        assert 'exists' in results[0][2]
        assert (dest / 'f.txt').read_bytes() == b'old'

    def test_force_overwrites(self, tmp_path):
        cache = tmp_path / 'cache'
        md5 = '0' * 32
        _seed_cache(cache, md5, b'new')
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'f.txt').write_bytes(b'old')

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': 3}],
            cache, dest, ['copy'], jobs=1, force=True,
        )

        assert results[0][1] is True
        assert (dest / 'f.txt').read_bytes() == b'new'

    def test_parallel_places_every_file(self, tmp_path):
        cache = tmp_path / 'cache'
        entries = []
        for i in range(12):
            md5 = f'{i:032x}'
            _seed_cache(cache, md5, f'content-{i}'.encode())
            entries.append({'relpath': f'f{i}.txt', 'md5': md5, 'size': 9})
        dest = tmp_path / 'out'

        results = get_mod.materialise(entries, cache, dest, ['copy'], jobs=4)

        assert all(ok for _, ok, _ in results)
        assert len(list(dest.iterdir())) == 12


# =============================================================================
# Destination layout
# =============================================================================

class TestDestFor:
    """Where a source path lands on disk."""

    def test_defaults_to_basename(self):
        assert get_mod._dest_for(None, 'data/fq/AF013-A') == Path('AF013-A')

    def test_trailing_slash_collects_under_directory(self):
        """This is what makes a CSV of 82 samples land as 82 siblings."""
        assert get_mod._dest_for('fastqs/', 'data/fq/AF013-A') == Path('fastqs/AF013-A')

    def test_existing_directory_collects_under_it(self, tmp_path):
        target = tmp_path / 'fastqs'
        target.mkdir()
        assert get_mod._dest_for(str(target), 'data/fq/AF013-A') == target / 'AF013-A'

    def test_plain_name_is_used_verbatim(self):
        assert get_mod._dest_for('renamed', 'data/fq/AF013-A') == Path('renamed')

    @pytest.mark.parametrize('url', [
        's3://bucket/prefix/',
        's3://bucket/prefix',
        'gs://bucket/prefix/',
        'ssh://host/path',
        'https://example.com/data',
    ])
    def test_url_destination_is_rejected(self, url):
        """``Path('s3://b/p')`` is a valid relative path, so without this guard
        the fetch silently created a local directory named ``s3:``."""
        with pytest.raises(GetError, match='must be a local path'):
            get_mod._dest_for(url, 'data/fq/AF013-A')

    def test_url_error_names_the_scheme(self):
        with pytest.raises(GetError, match=r's3:// URL'):
            get_mod._dest_for('s3://bucket/prefix/', 'data/fq/AF013-A')

    @pytest.mark.parametrize('out', [
        'fastqs:backup/',      # colon, but no scheme separator
        './s3:/not-a-url',
        'C:/looks-like-a-drive',
        'run:1/sample',
    ])
    def test_colon_in_a_relative_path_is_not_a_url(self, out):
        """The guard is anchored on ``://`` so ordinary paths still work."""
        assert isinstance(get_mod._dest_for(out, 'data/fq/AF013-A'), Path)


# =============================================================================
# CSV contract (shared with dt import --csv)
# =============================================================================

def _write_csv(path, text):
    path.write_text(text)
    return str(path)


class TestReadCsvTargets:
    """utils.read_csv_targets -- the contract dt get and dt import share."""

    def test_reads_paths(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\ndata/one\ndata/two\n')
        assert utils.read_csv_targets(csv_file) == [('data/one', None), ('data/two', None)]

    def test_output_column_overrides_fallback(self, tmp_path):
        csv_file = _write_csv(
            tmp_path / 'a.csv', 'path,output\ndata/one,here/\ndata/two,\n'
        )
        assert utils.read_csv_targets(csv_file, out='fallback/') == [
            ('data/one', 'here/'), ('data/two', 'fallback/')
        ]

    def test_custom_path_column(self, tmp_path):
        """The motivating CSV keys fastqs as fq_dir, not path."""
        csv_file = _write_csv(tmp_path / 'a.csv', 'sample,fq_dir\nAF013,data/fq/AF013\n')
        assert utils.read_csv_targets(csv_file, path_col='fq_dir') == [
            ('data/fq/AF013', None)
        ]

    def test_filter_on_empty_cell_selects_setup_rows(self, tmp_path):
        """SETUP = rows with an empty wts_lib."""
        csv_file = _write_csv(
            tmp_path / 'a.csv',
            'path,wts_lib\ndata/a,\ndata/b,LIB1\ndata/c,\n',
        )
        assert utils.read_csv_targets(csv_file, filters=['wts_lib=']) == [
            ('data/a', None), ('data/c', None)
        ]

    def test_filter_negation(self, tmp_path):
        csv_file = _write_csv(
            tmp_path / 'a.csv', 'path,kind\ndata/a,wgs\ndata/b,wts\n'
        )
        assert utils.read_csv_targets(csv_file, filters=['kind!=wgs']) == [
            ('data/b', None)
        ]

    def test_filters_are_anded(self, tmp_path):
        csv_file = _write_csv(
            tmp_path / 'a.csv',
            'path,kind,wts_lib\ndata/a,wts,\ndata/b,wts,L1\ndata/c,wgs,\n',
        )
        assert utils.read_csv_targets(
            csv_file, filters=['kind=wts', 'wts_lib=']
        ) == [('data/a', None)]

    def test_unknown_filter_column_is_an_error(self, tmp_path):
        """Otherwise every row silently fails to match and it reads as bad data."""
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\ndata/a\n')
        with pytest.raises(ValueError, match='Filter column'):
            utils.read_csv_targets(csv_file, filters=['nope=1'])

    def test_malformed_filter_is_an_error(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\ndata/a\n')
        with pytest.raises(ValueError, match='expected COL=VALUE'):
            utils.read_csv_targets(csv_file, filters=['garbage'])

    def test_missing_path_column(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'name\nfoo\n')
        with pytest.raises(ValueError, match="must have a 'path' column"):
            utils.read_csv_targets(csv_file)

    def test_empty_csv(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\n')
        with pytest.raises(ValueError, match='CSV file is empty'):
            utils.read_csv_targets(csv_file)

    def test_missing_file(self, tmp_path):
        with pytest.raises(ValueError, match='CSV file not found'):
            utils.read_csv_targets(str(tmp_path / 'nope.csv'))

    def test_empty_path_cell_is_kept_for_reporting(self, tmp_path):
        """A silently dropped row in a 400-row manifest is very hard to notice.

        Note this is an empty *cell* in a populated row -- a wholly blank line
        is skipped by csv.DictReader before we ever see it.
        """
        csv_file = _write_csv(tmp_path / 'a.csv', 'path,name\n,foo\ndata/b,bar\n')
        assert utils.read_csv_targets(csv_file) == [('', None), ('data/b', None)]


# =============================================================================
# Batch orchestration
# =============================================================================

class TestGetFromCsv:
    """The batch path -- above all, that it resolves the source once."""

    @pytest.fixture
    def stub_source(self, tmp_path):
        clone = tmp_path / 'clone'
        clone.mkdir()
        cache = tmp_path / 'cache'
        cache.mkdir()
        with patch.object(get_mod, '_resolve_source', return_value=(clone, cache)) as m:
            yield m

    def test_clones_once_for_many_rows(self, tmp_path, stub_source, monkeypatch):
        """The whole point: 82 samples must not cost 82 clones."""
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\nd/three\n')

        with patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files',
                          return_value=[{'relpath': 'f', 'md5': 'a' * 32, 'size': 1}]), \
             patch.object(get_mod, '_place_all',
                          side_effect=lambda tasks, *a, **k: [('f', True, 'copy')] * len(tasks)):
            results = get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        assert stub_source.call_count == 1
        assert len(results) == 3
        assert all(ok for _, ok, _ in results)

    def test_places_every_row_through_one_pool(self, tmp_path, stub_source, monkeypatch):
        """Fan out across rows, not within them.

        Batching per row would idle most workers on rows with few files, and a
        row is often a sample directory holding two fastqs.
        """
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\nd/three\n')

        with patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files',
                          return_value=[
                              {'relpath': 'R1', 'md5': 'a' * 32, 'size': 1},
                              {'relpath': 'R2', 'md5': 'b' * 32, 'size': 1},
                          ]), \
             patch.object(get_mod, '_place_all',
                          side_effect=lambda tasks, *a, **k:
                              [(str(i), True, 'copy') for i in range(len(tasks))]) as place:
            get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        # One call carrying all 3 rows x 2 files, not three calls of two.
        assert place.call_count == 1
        assert len(place.call_args[0][0]) == 6

    def test_first_row_resolves_alone_to_warm_the_state_db(
        self, tmp_path, stub_source, monkeypatch
    ):
        """Fanning out on a cold clone races to create DVC's SQLite state db.

        Seen for real: two concurrent `dvc list` against a fresh clone, one
        fails with "database is locked" every time.
        """
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\nd/three\n')

        in_flight = []
        peak = []

        def listing(source, path, rev=None):
            in_flight.append(path)
            peak.append(len(in_flight))
            time.sleep(0.05)
            in_flight.remove(path)
            return [{'relpath': 'f', 'md5': 'a' * 32, 'size': 1}]

        with patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files', side_effect=listing), \
             patch.object(get_mod, '_place_all',
                          side_effect=lambda tasks, *a, **k: [('f', True, 'copy')] * len(tasks)):
            get_mod.get_from_csv(csv_file, 'myrepo', out='out/', jobs=8)

        # The very first resolve must have had the clone to itself.
        assert peak[0] == 1

    def test_row_results_stay_in_csv_order(self, tmp_path, stub_source, monkeypatch):
        """Parallel placement must not scramble which row got which outcome."""
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\nd/three\n')

        counts = {'d/one': 1, 'd/two': 3, 'd/three': 2}

        def listing(source, path, rev=None):
            return [
                {'relpath': f'f{i}', 'md5': f'{i:032x}', 'size': 1}
                for i in range(counts[path])
            ]

        with patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files', side_effect=listing), \
             patch.object(get_mod, '_place_all',
                          side_effect=lambda tasks, *a, **k:
                              [('f', True, 'copy')] * len(tasks)):
            results = get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        assert [r[0] for r in results] == ['d/one', 'd/two', 'd/three']
        assert '1 file ->' in results[0][2]
        assert '3 files ->' in results[1][2]
        assert '2 files ->' in results[2][2]

    def test_row_failure_does_not_stop_the_batch(self, tmp_path, stub_source, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/bad\nd/good\n')

        def listing(source, path, rev=None):
            if path == 'd/bad':
                raise GetError('not tracked')
            return [{'relpath': 'f', 'md5': 'a' * 32, 'size': 1}]

        with patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files', side_effect=listing), \
             patch.object(get_mod, '_place_all',
                          side_effect=lambda tasks, *a, **k: [('f', True, 'copy')] * len(tasks)):
            results = get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        assert results[0] == ('d/bad', False, 'not tracked')
        assert results[1][1] is True

    def test_partial_row_is_reported_as_failure(self, tmp_path, stub_source, monkeypatch):
        """Some files placed and some missing must not read as success."""
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\n')

        with patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files',
                          return_value=[
                              {'relpath': 'a', 'md5': 'a' * 32, 'size': 1},
                              {'relpath': 'b', 'md5': 'b' * 32, 'size': 1},
                          ]), \
             patch.object(get_mod, '_place_all',
                          return_value=[('a', True, 'copy'), ('b', False, 'gone')]):
            results = get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        assert results[0][1] is False
        assert '1 written, 1 failed' in results[0][2]

    def test_empty_path_row_reported_not_dropped(self, tmp_path, stub_source, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path,name\n,foo\nd/good,bar\n')

        with patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files',
                          return_value=[{'relpath': 'f', 'md5': 'a' * 32, 'size': 1}]), \
             patch.object(get_mod, 'materialise', return_value=[('f', True, 'copy')]):
            results = get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        assert results[0] == ('(empty)', False, 'Missing path')
        assert len(results) == 2

    def test_filter_selecting_nothing_is_an_error(self, tmp_path, monkeypatch):
        """Better than silently reporting success over zero rows."""
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path,kind\nd/one,wgs\n')

        with pytest.raises(GetError, match='No rows selected'):
            get_mod.get_from_csv(csv_file, 'myrepo', filters=['kind=wts'])

    def test_csv_problems_surface_as_get_error(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'name\nfoo\n')

        with pytest.raises(GetError, match="must have a 'path' column"):
            get_mod.get_from_csv(csv_file, 'myrepo')


class TestGetData:
    """The single-path entry point."""

    def test_returns_written_and_failed_counts(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        clone, cache = tmp_path / 'clone', tmp_path / 'cache'

        with patch.object(get_mod, '_resolve_source', return_value=(clone, cache)), \
             patch.object(get_mod, 'resolve_link_types', return_value=['copy']), \
             patch.object(get_mod, 'list_source_files',
                          return_value=[{'relpath': 'f', 'md5': 'a' * 32, 'size': 1}]), \
             patch.object(get_mod, 'materialise',
                          return_value=[('a', True, 'copy'), ('b', False, 'gone')]):
            written, failed = get_mod.get_data('myrepo', 'data/x', out='out/')

        assert (written, failed) == (1, 1)

    def test_no_local_cache_downloads_instead_of_failing(self, tmp_path, monkeypatch):
        """The off-system case: no mount, so fetch over the network.

        This used to be a hard error, which meant dt get could not be run by
        the very people it was built for.
        """
        monkeypatch.chdir(tmp_path)
        with patch.object(get_mod.tmp_mod, 'clone_repo', return_value=tmp_path), \
             patch.object(get_mod.remote_mod, 'find_local_remote_from_repo',
                          return_value=None), \
             patch.object(get_mod, 'fetch_via_remote',
                          return_value=(3, 'downloaded')) as fetch:
            written, failed = get_mod.get_data('myrepo', 'data/x', out='out/')

        assert (written, failed) == (3, 0)
        assert fetch.call_count == 1


# =============================================================================
# No tracking files left behind
# =============================================================================

class TestNoTrackingFiles:
    """The defining difference from dt import."""

    def test_materialise_writes_no_dvc_file(self, tmp_path):
        cache = tmp_path / 'cache'
        md5 = '1' * 32
        _seed_cache(cache, md5, b'plain')
        dest = tmp_path / 'out'

        get_mod.materialise(
            [{'relpath': 'sub/f.txt', 'md5': md5, 'size': 5}],
            cache, dest, ['copy'], jobs=1,
        )

        assert list(dest.rglob('*.dvc')) == []
        assert not (dest / '.gitignore').exists()


# =============================================================================
# Remote fallback (the off-NCI collaborator path)
# =============================================================================

class TestRemoteFallback:
    """dt get on a machine with no filesystem access to any cache.

    This is the case the command exists for -- someone on a different system
    entirely -- and the one it used to refuse outright.
    """

    def test_no_local_cache_falls_back_instead_of_failing(self, tmp_path):
        with patch.object(get_mod.tmp_mod, 'clone_repo', return_value=tmp_path), \
             patch.object(get_mod.remote_mod, 'find_local_remote_from_repo',
                          return_value=None):
            clone, cache_root = get_mod._resolve_source(
                'myrepo', None, None, False, False
            )

        assert clone == tmp_path
        assert cache_root is None

    def test_local_cache_still_preferred(self, tmp_path):
        with patch.object(get_mod.tmp_mod, 'clone_repo', return_value=tmp_path), \
             patch.object(get_mod.remote_mod, 'find_local_remote_from_repo',
                          return_value=('nci', '/g/data/cache')):
            _, cache_root = get_mod._resolve_source(
                'myrepo', None, None, False, False
            )

        assert cache_root == Path('/g/data/cache')

    def test_opting_out_restores_the_hard_error(self, tmp_path):
        with patch.object(get_mod.tmp_mod, 'clone_repo', return_value=tmp_path), \
             patch.object(get_mod.remote_mod, 'find_local_remote_from_repo',
                          return_value=None):
            with pytest.raises(GetError, match='no-remote-fallback'):
                get_mod._resolve_source(
                    'myrepo', None, None, False, False, allow_remote=False
                )

    def test_hands_the_local_clone_to_dvc_get(self, tmp_path):
        """Passing the clone keeps resolve-once: DVC won't re-clone a local path."""
        clone = tmp_path / 'clone'
        dest = tmp_path / 'out'
        entries = [{'relpath': 'R1.fq', 'md5': 'a' * 32, 'size': 1}]

        def fake_run(cmd, **kwargs):
            out = Path(cmd[cmd.index('--out') + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text('x')
            return MagicMock(returncode=0, stdout='', stderr='')

        with patch.object(get_mod, 'list_source_files', return_value=entries), \
             patch.object(get_mod.subprocess, 'run', side_effect=fake_run) as run:
            written, note = get_mod.fetch_via_remote(clone, 'data/fq/S1', dest, jobs=4)

        cmd = run.call_args[0][0]
        assert cmd[:2] == ['dvc', 'get']
        assert cmd[2] == str(clone)
        assert 'data/fq/S1/R1.fq' in cmd
        assert '--jobs' in cmd and '4' in cmd
        assert written == 1 and 'downloaded' in note

    def test_passes_rev_through(self, tmp_path):
        dest = tmp_path / 'out'
        entries = [{'relpath': 'f', 'md5': 'a' * 32, 'size': 1}]

        def fake_run(cmd, **kwargs):
            out = Path(cmd[cmd.index('--out') + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_text('x')
            return MagicMock(returncode=0, stdout='', stderr='')

        with patch.object(get_mod, 'list_source_files', return_value=entries), \
             patch.object(get_mod.subprocess, 'run', side_effect=fake_run) as run:
            get_mod.fetch_via_remote(tmp_path / 'c', 'p', dest, rev='v1.0')

        assert ['--rev', 'v1.0'] == run.call_args[0][0][-2:]

    def test_download_failure_is_reported(self, tmp_path):
        failed = MagicMock(
            returncode=1, stdout='',
            stderr='ERROR: unable to find remote\n\nHaving any troubles? '
                   'Hit us up at https://dvc.org/support\n',
        )
        with patch.object(get_mod.subprocess, 'run', return_value=failed):
            with pytest.raises(GetError) as excinfo:
                get_mod.fetch_via_remote(tmp_path / 'c', 'p', tmp_path / 'o')

        assert 'unable to find remote' in str(excinfo.value)
        assert 'dvc.org/support' not in str(excinfo.value)

    def test_silent_no_op_download_is_a_failure(self, tmp_path):
        """dvc get exiting 0 having written nothing must not read as success."""
        entries = [{'relpath': 'f', 'md5': 'a' * 32, 'size': 1}]
        with patch.object(get_mod, 'list_source_files', return_value=entries), \
             patch.object(get_mod.subprocess, 'run',
                          return_value=MagicMock(returncode=0, stdout='', stderr='')):
            with pytest.raises(GetError, match='produced no file'):
                get_mod.fetch_via_remote(tmp_path / 'c', 'p', tmp_path / 'o')

    def test_csv_batch_downloads_every_row(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\n')

        with patch.object(get_mod, '_resolve_source',
                          return_value=(tmp_path / 'clone', None)), \
             patch.object(get_mod, 'fetch_via_remote',
                          return_value=(2, 'downloaded')) as fetch:
            results = get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        assert fetch.call_count == 2
        assert all(ok for _, ok, _ in results)
        assert '2 files' in results[0][2]

    def test_csv_batch_reports_a_failed_row(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/bad\nd/good\n')

        def fetch(clone, path, dest, *args, **kwargs):
            if path == 'd/bad':
                raise GetError('not in remote')
            return 1, 'downloaded'

        with patch.object(get_mod, '_resolve_source',
                          return_value=(tmp_path / 'clone', None)), \
             patch.object(get_mod, 'fetch_via_remote', side_effect=fetch):
            results = get_mod.get_from_csv(csv_file, 'myrepo', out='out/')

        assert results[0] == ('d/bad', False, 'not in remote')
        assert results[1][1] is True


# =============================================================================
# --resume and --check
# =============================================================================

class TestDecide:
    """The skip/fetch/bad decision, shared by the local and remote paths."""

    def _f(self, tmp_path, content=b'data'):
        p = tmp_path / 'f.bin'
        p.write_bytes(content)
        return p, utils.md5_file(p)

    def test_missing_file_is_fetched(self, tmp_path):
        assert get_mod.decide(tmp_path / 'nope', 'a' * 32, False, False, False) \
            == ('fetch', 'missing')

    def test_existing_is_skipped_by_default(self, tmp_path):
        f, md5 = self._f(tmp_path)
        action, note = get_mod.decide(f, md5, False, False, False)
        assert action == 'skip'
        assert 'use -f' in note

    def test_force_refetches(self, tmp_path):
        f, md5 = self._f(tmp_path)
        assert get_mod.decide(f, md5, True, False, False)[0] == 'fetch'

    def test_resume_skips_present(self, tmp_path):
        f, md5 = self._f(tmp_path)
        assert get_mod.decide(f, md5, False, True, False) == ('skip', 'already present')

    def test_check_passes_a_good_file(self, tmp_path):
        f, md5 = self._f(tmp_path)
        assert get_mod.decide(f, md5, False, False, True) == ('skip', 'verified')

    def test_check_alone_reports_a_bad_file(self, tmp_path):
        f, _ = self._f(tmp_path)
        action, note = get_mod.decide(f, 'b' * 32, False, False, True)
        assert action == 'bad'
        assert 'MISMATCH' in note

    def test_resume_plus_check_refetches_a_bad_file(self, tmp_path):
        """The pairing that matters.

        An interrupted transfer leaves a truncated file. By existence alone it
        is indistinguishable from a complete one, so --resume would keep the
        corruption forever. --check is what catches it.
        """
        f, _ = self._f(tmp_path)
        action, note = get_mod.decide(f, 'b' * 32, False, True, True)
        assert action == 'fetch'
        assert 'mismatch' in note.lower()

    def test_truncated_file_is_caught(self, tmp_path):
        """Concretely: the real failure mode of an interrupted download."""
        full = tmp_path / 'full.bin'
        full.write_bytes(b'x' * 10000)
        good_md5 = utils.md5_file(full)

        truncated = tmp_path / 'truncated.bin'
        truncated.write_bytes(b'x' * 4000)

        # Resume alone keeps it; resume+check replaces it.
        assert get_mod.decide(truncated, good_md5, False, True, False)[0] == 'skip'
        assert get_mod.decide(truncated, good_md5, False, True, True)[0] == 'fetch'


class TestVerifyFile:
    def test_matches(self, tmp_path):
        f = tmp_path / 'f'
        f.write_bytes(b'hello')
        assert get_mod.verify_file(f, utils.md5_file(f)) is True

    def test_mismatch(self, tmp_path):
        f = tmp_path / 'f'
        f.write_bytes(b'hello')
        assert get_mod.verify_file(f, 'a' * 32) is False

    def test_unreadable_is_not_a_crash(self, tmp_path):
        assert get_mod.verify_file(tmp_path / 'nope', 'a' * 32) is False


class TestResumeOnLocalPath:
    """--resume / --check through materialise()."""

    def test_resume_skips_and_still_reports_success(self, tmp_path):
        cache = tmp_path / 'cache'
        md5 = '2' * 32
        _seed_cache(cache, md5, b'payload')
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'f.txt').write_bytes(b'payload')

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': 7}],
            cache, dest, ['copy'], jobs=1, resume=True,
        )
        assert results == [('f.txt', True, 'already present')]

    def test_check_flags_a_corrupt_existing_file(self, tmp_path):
        cache = tmp_path / 'cache'
        md5 = '3' * 32
        _seed_cache(cache, md5, b'payload')
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'f.txt').write_bytes(b'WRONG')

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': 7}],
            cache, dest, ['copy'], jobs=1, check=True,
        )
        assert results[0][1] is False
        assert 'MISMATCH' in results[0][2]

    def test_resume_and_check_repairs_a_corrupt_file(self, tmp_path):
        cache = tmp_path / 'cache'
        payload = b'the real bytes'
        md5 = utils.md5_bytes(payload)
        _seed_cache(cache, md5, payload)
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'f.txt').write_bytes(b'truncated')

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': len(payload)}],
            cache, dest, ['copy'], jobs=1, resume=True, check=True,
        )
        assert results[0][1] is True
        assert (dest / 'f.txt').read_bytes() == payload

    def test_check_verifies_a_correct_existing_file(self, tmp_path):
        """--check standalone validates a finished download."""
        cache = tmp_path / 'cache'
        payload = b'good bytes'
        md5 = utils.md5_bytes(payload)
        _seed_cache(cache, md5, payload)
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'f.txt').write_bytes(payload)

        results = get_mod.materialise(
            [{'relpath': 'f.txt', 'md5': md5, 'size': len(payload)}],
            cache, dest, ['copy'], jobs=1, check=True,
        )
        assert results == [('f.txt', True, 'verified')]


class TestResumeOnRemotePath:
    """The path that matters most: 358 GiB over the network."""

    def test_only_missing_files_are_downloaded(self, tmp_path):
        clone = tmp_path / 'clone'
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'R1.fq').write_bytes(b'already here')

        entries = [
            {'relpath': 'R1.fq', 'md5': utils.md5_bytes(b'already here'), 'size': 12},
            {'relpath': 'R2.fq', 'md5': 'c' * 32, 'size': 5},
        ]

        def fake_run(cmd, **kwargs):
            out = Path(cmd[cmd.index('--out') + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b'fetched')
            return MagicMock(returncode=0, stdout='', stderr='')

        with patch.object(get_mod, 'list_source_files', return_value=entries), \
             patch.object(get_mod.subprocess, 'run', side_effect=fake_run) as run:
            total, note = get_mod.fetch_via_remote(
                clone, 'data/fq/S1', dest, resume=True
            )

        assert run.call_count == 1
        assert 'data/fq/S1/R2.fq' in run.call_args[0][0]
        assert total == 2
        assert 'already present' in note

    def test_partial_directory_is_no_longer_skipped_wholesale(self, tmp_path):
        """The bug --resume exists to fix.

        The remote path used to skip on `dest_root.exists()`, so a row
        interrupted midway was reported as complete on the next run.
        """
        clone = tmp_path / 'clone'
        dest = tmp_path / 'out'
        dest.mkdir()
        (dest / 'R1.fq').write_bytes(b'a')

        entries = [
            {'relpath': 'R1.fq', 'md5': utils.md5_bytes(b'a'), 'size': 1},
            {'relpath': 'R2.fq', 'md5': 'd' * 32, 'size': 1},
            {'relpath': 'R3.fq', 'md5': 'e' * 32, 'size': 1},
        ]

        def fake_run(cmd, **kwargs):
            out = Path(cmd[cmd.index('--out') + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b'z')
            return MagicMock(returncode=0, stdout='', stderr='')

        with patch.object(get_mod, 'list_source_files', return_value=entries), \
             patch.object(get_mod.subprocess, 'run', side_effect=fake_run) as run:
            total, _ = get_mod.fetch_via_remote(clone, 'p', dest, resume=True)

        assert run.call_count == 2
        assert total == 3

    def test_checksum_failure_after_download_is_reported(self, tmp_path):
        clone = tmp_path / 'clone'
        dest = tmp_path / 'out'
        entries = [{'relpath': 'R1.fq', 'md5': 'f' * 32, 'size': 1}]

        def fake_run(cmd, **kwargs):
            out = Path(cmd[cmd.index('--out') + 1])
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(b'wrong bytes')
            return MagicMock(returncode=0, stdout='', stderr='')

        with patch.object(get_mod, 'list_source_files', return_value=entries), \
             patch.object(get_mod.subprocess, 'run', side_effect=fake_run):
            with pytest.raises(GetError, match='CHECKSUM MISMATCH after download'):
                get_mod.fetch_via_remote(clone, 'p', dest, check=True)
