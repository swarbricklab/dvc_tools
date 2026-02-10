"""Tests for dt.errors module.

Tests the exception hierarchy for DVC Tools.
"""

import pytest

from dt import errors


class TestDTError:
    """Tests for the base DTError exception."""
    
    def test_dterror_is_exception(self):
        """DTError is an Exception subclass."""
        assert issubclass(errors.DTError, Exception)
    
    def test_dterror_can_be_raised(self):
        """DTError can be raised and caught."""
        with pytest.raises(errors.DTError):
            raise errors.DTError("test error")
    
    def test_dterror_message(self):
        """DTError stores message correctly."""
        err = errors.DTError("test message")
        assert str(err) == "test message"


class TestExceptionHierarchy:
    """Tests that all exceptions inherit from DTError."""
    
    @pytest.mark.parametrize("exception_class", [
        errors.CacheError,
        errors.CheckoutError,
        errors.CloneError,
        errors.DependencyError,
        errors.DiffError,
        errors.DuError,
        errors.DVCFileError,
        errors.FetchError,
        errors.FindError,
        errors.HistoryError,
        errors.LsError,
        errors.HPCError,
        errors.ImportError_,
        errors.InitError,
        errors.MvError,
        errors.OfflineError,
        errors.PullError,
        errors.PushError,
        errors.RemoteError,
        errors.SummaryError,
        errors.TmpError,
        errors.WorktreeError,
        errors.AddError,
    ])
    def test_inherits_from_dterror(self, exception_class):
        """Exception class inherits from DTError."""
        assert issubclass(exception_class, errors.DTError)
    
    @pytest.mark.parametrize("exception_class", [
        errors.CacheError,
        errors.CheckoutError,
        errors.CloneError,
        errors.DependencyError,
        errors.DiffError,
        errors.DuError,
        errors.DVCFileError,
        errors.FetchError,
        errors.FindError,
        errors.HistoryError,
        errors.LsError,
        errors.HPCError,
        errors.ImportError_,
        errors.InitError,
        errors.MigrateError,
        errors.MvError,
        errors.OfflineError,
        errors.PullError,
        errors.PushError,
        errors.RemoteError,
        errors.SummaryError,
        errors.TmpError,
        errors.WorktreeError,
        errors.AddError,
    ])
    def test_can_be_raised_and_caught(self, exception_class):
        """Each exception can be raised and caught."""
        with pytest.raises(exception_class):
            raise exception_class(f"test {exception_class.__name__}")
    
    @pytest.mark.parametrize("exception_class", [
        errors.CacheError,
        errors.CheckoutError,
        errors.CloneError,
        errors.DependencyError,
        errors.DiffError,
        errors.DuError,
        errors.DVCFileError,
        errors.FetchError,
        errors.FindError,
        errors.HistoryError,
        errors.LsError,
        errors.HPCError,
        errors.ImportError_,
        errors.InitError,
        errors.MigrateError,
        errors.MvError,
        errors.OfflineError,
        errors.PullError,
        errors.PushError,
        errors.RemoteError,
        errors.SummaryError,
        errors.TmpError,
        errors.WorktreeError,
        errors.AddError,
    ])
    def test_caught_by_dterror(self, exception_class):
        """Each exception can be caught by catching DTError."""
        with pytest.raises(errors.DTError):
            raise exception_class("test error")


class TestSpecificExceptions:
    """Tests for specific exception semantics."""
    
    def test_cache_error(self):
        """CacheError for cache operations."""
        err = errors.CacheError("Cache not found")
        assert "Cache" in str(err)
    
    def test_dependency_error(self):
        """DependencyError for missing dependencies."""
        err = errors.DependencyError("dvc command not found")
        assert "dvc" in str(err)
    
    def test_dvc_file_error(self):
        """DVCFileError for .dvc file parsing issues."""
        err = errors.DVCFileError("Invalid YAML in data.csv.dvc")
        assert "dvc" in str(err).lower()
    
    def test_fetch_error(self):
        """FetchError for fetch operations."""
        err = errors.FetchError("Failed to fetch from remote")
        assert "fetch" in str(err).lower()
    
    def test_import_error_underscore(self):
        """ImportError_ named to avoid builtin collision."""
        # Verify it's different from builtin ImportError
        assert errors.ImportError_ is not ImportError
        assert not issubclass(errors.ImportError_, ImportError)
        
        err = errors.ImportError_("Failed to import data")
        assert "import" in str(err).lower()
    
    def test_hpc_error(self):
        """HPCError for HPC/qxub operations."""
        err = errors.HPCError("qxub not available")
        assert isinstance(err, errors.DTError)
    
    def test_pull_error(self):
        """PullError for pull operations."""
        err = errors.PullError("Failed to pull files")
        assert "pull" in str(err).lower()
    
    def test_push_error(self):
        """PushError for push operations."""
        err = errors.PushError("Failed to push files")
        assert "push" in str(err).lower()
    
    def test_tmp_error(self):
        """TmpError for tmp/clone operations."""
        err = errors.TmpError("Failed to clone repository")
        assert isinstance(err, errors.DTError)
    
    def test_worktree_error(self):
        """WorktreeError for git worktree operations."""
        err = errors.WorktreeError("Failed to create worktree")
        assert isinstance(err, errors.DTError)


class TestExceptionUsage:
    """Tests for typical exception usage patterns."""
    
    def test_catch_all_dt_errors(self):
        """Can catch all DT errors with single except clause."""
        exceptions_to_test = [
            errors.CacheError("cache"),
            errors.FetchError("fetch"),
            errors.PullError("pull"),
            errors.PushError("push"),
        ]
        
        for exc in exceptions_to_test:
            try:
                raise exc
            except errors.DTError as e:
                assert isinstance(e, errors.DTError)
    
    def test_exception_with_context(self):
        """Exceptions can be raised with context."""
        original = ValueError("original error")
        try:
            try:
                raise original
            except ValueError as e:
                raise errors.CacheError("Wrapper error") from e
        except errors.CacheError as e:
            assert e.__cause__ is original
    
    def test_exception_chain(self):
        """Exceptions can be chained."""
        try:
            try:
                raise errors.DVCFileError("File error")
            except errors.DVCFileError:
                raise errors.FetchError("Fetch failed")
        except errors.FetchError as e:
            assert isinstance(e.__context__, errors.DVCFileError)


# Run with: pytest tests/test_errors.py -v
