"""Unit tests for dt update module."""

import os
import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch, mock_open

import pytest
import yaml

from dt import update, utils


# =============================================================================
# Test _find_import_files
# =============================================================================

class TestFindImportFiles:
    """Tests for _find_import_files function."""
    
    @patch('dt.update.subprocess.run')
    def test_finds_import_files(self, mock_run, tmp_path, monkeypatch):
        """Finds import files using git ls-files."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        # Create regular file
        regular_file = tmp_path / "regular.dvc"
        regular_file.write_text(yaml.dump({
            'outs': [{'path': 'regular.csv'}]
        }))
        
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="imported.dvc\nregular.dvc\n"
        )
        
        result = update._find_import_files()
        
        assert len(result) == 1
        assert 'imported.dvc' in result[0]
    
    @patch('dt.update.subprocess.run')
    def test_returns_empty_when_no_imports(self, mock_run, tmp_path, monkeypatch):
        """Returns empty list when no import files found."""
        monkeypatch.chdir(tmp_path)
        
        # Create only regular file
        regular_file = tmp_path / "regular.dvc"
        regular_file.write_text(yaml.dump({
            'outs': [{'path': 'regular.csv'}]
        }))
        
        mock_run.return_value = MagicMock(
            returncode=0,
            stdout="regular.dvc\n"
        )
        
        result = update._find_import_files()
        
        assert result == []


# =============================================================================
# Test update function
# =============================================================================

class TestUpdate:
    """Tests for update function."""
    
    @patch('dt.update.subprocess.run')
    def test_update_nonexistent_target(self, mock_run, tmp_path, monkeypatch):
        """Returns failure for non-existent target."""
        monkeypatch.chdir(tmp_path)
        
        results = update.update(targets=['nonexistent.dvc'])
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is False
        assert 'not found' in message.lower()
    
    @patch('dt.update.subprocess.run')
    def test_update_non_import_file(self, mock_run, tmp_path, monkeypatch):
        """Returns failure for non-import .dvc file."""
        monkeypatch.chdir(tmp_path)
        
        # Create regular file (not an import)
        regular_file = tmp_path / "regular.dvc"
        regular_file.write_text(yaml.dump({
            'outs': [{'path': 'regular.csv'}]
        }))
        
        results = update.update(targets=['regular.dvc'])
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is False
        assert 'not an import' in message.lower()
    
    @patch('dt.update.subprocess.run')
    @patch('dt.update._find_import_files')
    def test_update_no_targets_finds_imports(self, mock_find, mock_run, tmp_path, monkeypatch):
        """When no targets specified, finds and updates all imports."""
        monkeypatch.chdir(tmp_path)
        
        # Create import file
        import_file = tmp_path / "imported.dvc"
        import_file.write_text(yaml.dump({
            'deps': [{'path': 'data.csv', 'repo': {'url': 'http://example.com'}}],
            'outs': [{'path': 'data.csv'}]
        }))
        
        mock_find.return_value = ['imported.dvc']
        mock_run.return_value = MagicMock(returncode=0, stdout='', stderr='')
        
        results = update.update()
        
        assert len(results) == 1
        mock_find.assert_called_once()
    
    @patch('dt.update.subprocess.run')
    @patch('dt.update._find_import_files')
    def test_update_no_imports_found(self, mock_find, mock_run, tmp_path, monkeypatch):
        """Returns success message when no imports found."""
        monkeypatch.chdir(tmp_path)
        
        mock_find.return_value = []
        
        results = update.update()
        
        assert len(results) == 1
        target, success, message = results[0]
        assert success is True
        assert 'no import' in message.lower()


# =============================================================================
# Test _push_dir_to_remote (regression: must never write a symlink into a remote)
# =============================================================================

class TestPushDirToRemote:
    """A .dir seeded into a shared remote must be a real file, never a symlink.

    Regression guard for the bug where the cache link-ladder's cross-filesystem
    fallback planted a symlinked <hash>.dir in the remote pointing at a
    per-machine cache path.
    """

    def test_writes_real_file_not_symlink(self, tmp_path):
        cache = tmp_path / "cache"
        remote = tmp_path / "remote"
        cache.mkdir()
        remote.mkdir()

        content = b'[{"md5":"deadbeef","relpath":"x.txt"}]'
        md5 = utils.md5_bytes(content)
        dir_file = cache / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        dir_file.parent.mkdir(parents=True)
        dir_file.write_bytes(content)

        pushed = update._push_dir_to_remote(dir_file, remote, md5 + ".dir")

        dest = remote / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        assert pushed is True
        assert dest.exists()
        assert not os.path.islink(dest), "must not plant a symlink in the remote"
        assert dest.read_bytes() == content

    def test_idempotent_when_already_present(self, tmp_path):
        cache = tmp_path / "cache"
        remote = tmp_path / "remote"
        cache.mkdir()
        remote.mkdir()

        content = b'{"a":1}'
        md5 = utils.md5_bytes(content)
        dir_file = cache / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        dir_file.parent.mkdir(parents=True)
        dir_file.write_bytes(content)

        assert update._push_dir_to_remote(dir_file, remote, md5 + ".dir") is True
        # Second push sees it already there and is a no-op.
        assert update._push_dir_to_remote(dir_file, remote, md5 + ".dir") is False
        dest = remote / "files" / "md5" / md5[:2] / f"{md5[2:]}.dir"
        assert not os.path.islink(dest)


# =============================================================================
# outs.size must never be left describing the previous hash (#182)
# =============================================================================

def _write_import_dvc(path, outs, deps_repo):
    """Write a minimal import .dvc file."""
    path.write_text(yaml.dump({
        'frozen': True,
        'deps': [{'path': 'data/thing', 'repo': deps_repo}],
        'outs': [outs],
    }))
    return path


class TestSizeIsNeverStale:
    """A size that belongs to the old hash is a false statement, not a default.

    Regression guard for issue #182: `dvc list --size` against a repo URL
    returns no sizes, so the size argument was None, and the old code read that
    as "leave it alone" -- retaining a figure 3x the real directory.
    """

    def test_clear_size_removes_stale_value(self, tmp_path):
        dvc = _write_import_dvc(
            tmp_path / "thing.dvc",
            {'md5': 'old.dir', 'size': 534483329917, 'nfiles': 169, 'path': 'thing'},
            {'url': 'git@example.com:o/r.git', 'rev_lock': 'a' * 40},
        )

        modified = update._update_dvc_file(
            dvc, 'new.dir', size=None, clear_size=True, nfiles=1092
        )

        out = yaml.safe_load(dvc.read_text())['outs'][0]
        assert modified is True
        assert 'size' not in out, "stale size must be removed, not retained"
        assert out['md5'] == 'new.dir'
        assert out['nfiles'] == 1092

    def test_size_left_alone_without_clear_flag(self, tmp_path):
        """The rev_lock-only fast path must not disturb a valid size."""
        dvc = _write_import_dvc(
            tmp_path / "thing.dvc",
            {'md5': 'same.dir', 'size': 4096, 'nfiles': 2, 'path': 'thing'},
            {'url': 'git@example.com:o/r.git', 'rev_lock': 'a' * 40},
        )

        update._update_dvc_file(dvc, 'same.dir', new_rev='b' * 40)

        out = yaml.safe_load(dvc.read_text())['outs'][0]
        assert out['size'] == 4096

    def test_backfills_size_by_stat_when_listing_has_none(self, tmp_path):
        """No sizes from the source listing: stat the objects instead."""
        remote = tmp_path / "remote"
        blob = b'x' * 1234
        md5 = utils.md5_bytes(blob)
        # v2 layout, to cover the half-migrated remotes seen in the field.
        obj = remote / md5[:2] / md5[2:]
        obj.parent.mkdir(parents=True)
        obj.write_bytes(blob)

        entries = [{'md5': md5, 'relpath': 'a.fq'}]
        assert update._total_size_from_entries(entries, str(remote)) == 1234

    def test_returns_none_when_any_object_is_unsized(self, tmp_path):
        """A partial total would read as fact downstream, so refuse to give one."""
        remote = tmp_path / "remote"
        blob = b'y' * 10
        md5 = utils.md5_bytes(blob)
        obj = remote / 'files' / 'md5' / md5[:2] / md5[2:]
        obj.parent.mkdir(parents=True)
        obj.write_bytes(blob)

        entries = [{'md5': md5, 'relpath': 'a'}, {'md5': 'f' * 32, 'relpath': 'b'}]
        assert update._total_size_from_entries(entries, str(remote)) is None

    def test_listing_sizes_win_when_present(self, tmp_path):
        entries = [{'md5': 'a' * 32, 'relpath': 'a', 'size': 7},
                   {'md5': 'b' * 32, 'relpath': 'b', 'size': 8}]
        assert update._total_size_from_entries(entries, str(tmp_path)) == 15


# =============================================================================
# deps.repo.rev must not contradict rev_lock (#182)
# =============================================================================

class TestRevSpecConsistency:
    """`rev` is the spec, `rev_lock` its resolution -- they must agree.

    Regression guard for issue #182: advancing rev_lock while leaving a pinned
    `rev` behind made the next plain `dvc update` resolve `rev` and roll the
    import back to the commit just updated away from.
    """

    def test_stale_pinned_sha_is_dropped(self, tmp_path):
        dvc = _write_import_dvc(
            tmp_path / "thing.dvc",
            {'md5': 'h.dir', 'size': 1, 'path': 'thing'},
            {'url': 'git@example.com:o/r.git',
             'rev': 'ec71bce',
             'rev_lock': 'ec71bcee429cee141a6893a593beddca10f7774e'},
        )

        update._update_dvc_file(dvc, 'h.dir', new_rev='8024f5cd5fef7a123765ae289fa6471c7d6398a4')

        repo = yaml.safe_load(dvc.read_text())['deps'][0]['repo']
        assert repo['rev_lock'] == '8024f5cd5fef7a123765ae289fa6471c7d6398a4'
        assert 'rev' not in repo, "a pin that contradicts rev_lock must go"

    def test_pinned_sha_kept_when_lock_still_matches_it(self, tmp_path):
        """The short sha is a prefix of the new lock: no contradiction."""
        full = 'ec71bcee429cee141a6893a593beddca10f7774e'
        dvc = _write_import_dvc(
            tmp_path / "thing.dvc",
            {'md5': 'h.dir', 'size': 1, 'path': 'thing'},
            {'url': 'git@example.com:o/r.git', 'rev': 'ec71bce', 'rev_lock': 'old'},
        )

        update._update_dvc_file(dvc, 'h.dir', new_rev=full)

        repo = yaml.safe_load(dvc.read_text())['deps'][0]['repo']
        assert repo['rev'] == 'ec71bce'
        assert repo['rev_lock'] == full

    def test_branch_spec_survives_a_lock_advance(self, tmp_path):
        """rev_lock moving under a tracked branch is normal, not a conflict."""
        dvc = _write_import_dvc(
            tmp_path / "thing.dvc",
            {'md5': 'h.dir', 'size': 1, 'path': 'thing'},
            {'url': 'git@example.com:o/r.git', 'rev': 'seed', 'rev_lock': 'a' * 40},
        )

        update._update_dvc_file(dvc, 'h.dir', new_rev='b' * 40)

        repo = yaml.safe_load(dvc.read_text())['deps'][0]['repo']
        assert repo['rev'] == 'seed'
        assert repo['rev_lock'] == 'b' * 40

    def test_explicit_spec_is_recorded(self, tmp_path):
        dvc = _write_import_dvc(
            tmp_path / "thing.dvc",
            {'md5': 'h.dir', 'size': 1, 'path': 'thing'},
            {'url': 'git@example.com:o/r.git', 'rev_lock': 'a' * 40},
        )

        update._update_dvc_file(
            dvc, 'h.dir', new_rev='b' * 40, new_rev_spec='seed'
        )

        repo = yaml.safe_load(dvc.read_text())['deps'][0]['repo']
        assert repo['rev'] == 'seed'
        assert repo['rev_lock'] == 'b' * 40


class TestIsShaLike:
    """Distinguishing an immutable pin from a moving ref."""

    @pytest.mark.parametrize('rev', ['ec71bce', 'a' * 40, 'ABCDEF1'])
    def test_sha_like(self, rev):
        assert update._is_sha_like(rev) is True

    @pytest.mark.parametrize('rev', ['main', 'seed', 'v1.2.3', 'feat/abc', '', 'abc', 'a' * 41])
    def test_not_sha_like(self, rev):
        assert update._is_sha_like(rev) is False



# =============================================================================
# Refusing to rebuild a path that is not a single out (#182)
# =============================================================================

class TestMixedTreeRefusal:
    """A rebuilt .dir that omits git-tracked files disagrees with dvc's.

    Worse, `dt update` pushes it to the *source* remote -- so the divergence
    lands in a shared registry as an object no dvc operation would produce.
    """

    LISTING = [
        {'path': '.gitignore', 'isout': False, 'md5': None, 'size': 18},
        {'path': 'in_house.dvc', 'isout': False, 'md5': None, 'size': 100},
        {'path': 'in_house/a.txt', 'isout': True, 'md5': 'a' * 32, 'size': 13},
    ]

    def test_listing_reports_unhashed_paths(self):
        with patch('dt.update.subprocess.run') as mock_run:
            mock_run.return_value = MagicMock(
                returncode=0, stdout=__import__('json').dumps(self.LISTING)
            )
            entries, unhashed = update._get_file_listing('repo', 'p', 'rev')

        assert [e['relpath'] for e in entries] == ['in_house/a.txt']
        assert unhashed == ['.gitignore'], "the .dvc file is not a divergence"

    def test_change_detection_reports_the_mixed_tree(self):
        with patch.object(update, '_compute_source_hash',
                          return_value=(None, 0, ['.gitignore'])):
            changes = update._check_source_changes(
                Path('.'), 'data/annotations', 'a' * 40, 'b' * 40,
                repo_url='repo', current_hash='old.dir',
            )

        assert changes.mixed_tree == ['.gitignore']
        assert 'git-tracked' in changes.diff_summary

    def test_update_refuses_and_does_not_touch_the_dvc_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        dvc = _write_import_dvc(
            tmp_path / "thing.dvc",
            {'md5': 'old.dir', 'size': 31534770, 'nfiles': 169, 'path': 'thing'},
            {'url': 'git@example.com:o/r.git', 'rev_lock': 'a' * 40},
        )
        before = dvc.read_text()

        with patch.object(update.tmp_mod, 'clone_repo', return_value=tmp_path), \
             patch.object(update, '_tracked_tip', return_value='b' * 40), \
             patch.object(update, '_compute_source_hash',
                          return_value=(None, 0, ['.gitignore'])), \
             patch.object(update, '_push_dir_to_remote') as push:
            results = update.update(targets=[str(dvc)], verbose=False)

        assert results[0][1] is False
        assert 'git-tracked' in results[0][2]
        assert dvc.read_text() == before, "a refusal must not rewrite metadata"
        push.assert_not_called()
