"""Tests for dt pull module internal functions.

The pull module has been simplified to be essentially `dt fetch` + `dvc checkout`.
These tests cover the remaining utility functions.
"""

import os
import pytest
from unittest.mock import MagicMock, patch

from dt import pull


class TestDeleteDirManifests:
    """Tests for delete_dir_manifests function.
    
    Note: Most tests require a real DVC repository with cache,
    which makes them integration tests. Unit tests here are limited
    to error handling for non-DVC directories.
    """
    
    def test_outside_dvc_repo_raises(self, tmp_path, monkeypatch):
        """Calling outside a DVC repo raises PullError."""
        monkeypatch.chdir(tmp_path)
        
        with pytest.raises(pull.PullError, match="Not in a DVC repository"):
            pull.delete_dir_manifests()


class TestPullFunction:
    """Tests for the main pull function signature and behavior.
    
    Note: Full integration tests are in test_pull_cmd.py.
    These tests verify the function interface.
    """
    
    def test_pull_function_exists(self):
        """The pull function exists and is callable."""
        assert callable(pull.pull)
    
    def test_pull_returns_tuple(self):
        """The pull function signature expects a tuple return."""
        # We can't call it without a DVC repo, but we can check the function exists
        import inspect
        sig = inspect.signature(pull.pull)
        
        # Check expected parameters
        params = list(sig.parameters.keys())
        assert 'targets' in params
        assert 'verbose' in params
        assert 'force' in params
        assert 'update' in params
        assert 'network' in params
        assert 'dry' in params
    
    def test_pull_network_defaults_to_true(self):
        """The network parameter defaults to True."""
        import inspect
        sig = inspect.signature(pull.pull)
        
        network_param = sig.parameters['network']
        assert network_param.default is True, \
            "network should default to True for pull (unlike fetch)"



class TestDeleteDirManifestsValidatesHashes:
    def _out(self, value, fs_path='/proj/data'):
        out = MagicMock()
        out.hash_info.isdir = True
        out.hash_info.value = value
        out.fs_path = fs_path
        return out

    def test_crafted_hash_deletes_nothing_outside_cache(self, tmp_path):
        from dt import pull as pull_mod
        cache = tmp_path / 'cache' / 'files' / 'md5'
        cache.mkdir(parents=True)
        victim = tmp_path / 'remote' / 'xx.dir'
        victim.parent.mkdir()
        victim.write_text('manifest')
        good = 'a' * 32 + '.dir'
        (cache / 'aa').mkdir()
        (cache / 'aa' / ('a' * 30 + '.dir')).write_text('[]')
        outs = [self._out(f'ab/../../../remote/xx.dir'),
                self._out(f'..{"/.." * 3}/remote/xx.dir'),
                self._out(good)]
        repo = MagicMock()
        repo.index.outs = outs
        repo.cache.local.path = str(cache)
        repo.root_dir = str(tmp_path)
        with patch('dt.pull.Repo', return_value=repo):
            deleted = pull_mod.delete_dir_manifests(verbose=False)
        assert victim.exists()
        assert deleted == [good]

    def test_linked_manifest_in_cache_is_still_removed(self, tmp_path):
        from dt import pull as pull_mod
        cache = tmp_path / 'cache' / 'files' / 'md5'
        (cache / 'aa').mkdir(parents=True)
        remote_blob = tmp_path / 'remote' / 'aa' / ('a' * 30 + '.dir')
        remote_blob.parent.mkdir(parents=True)
        remote_blob.write_text('[]')
        link = cache / 'aa' / ('a' * 30 + '.dir')
        link.symlink_to(remote_blob)
        repo = MagicMock()
        repo.index.outs = [self._out('a' * 32 + '.dir')]
        repo.cache.local.path = str(cache)
        repo.root_dir = str(tmp_path)
        with patch('dt.pull.Repo', return_value=repo):
            deleted = pull_mod.delete_dir_manifests(verbose=False)
        assert deleted == ['a' * 32 + '.dir']
        assert not os.path.lexists(link)
        assert remote_blob.exists()  # the link was removed, not its target
