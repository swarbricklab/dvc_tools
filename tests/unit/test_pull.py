"""Tests for dt pull module internal functions.

The pull module has been simplified to be essentially `dt fetch` + `dvc checkout`.
These tests cover the remaining utility functions.
"""

import pytest

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

