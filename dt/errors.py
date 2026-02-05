"""Exception hierarchy for DVC Tools.

All dt-specific exceptions inherit from DTError, enabling:
- Catch-all error handling: except DTError
- Consistent error messaging
- Future enhancements (logging, error codes)
"""


class DTError(Exception):
    """Base exception for all DVC Tools errors."""
    pass


class CacheError(DTError):
    """Error during cache operations."""
    pass


class CheckoutError(DTError):
    """Error during checkout operations."""
    pass


class CloneError(DTError):
    """Error during clone operations."""
    pass


class DependencyError(DTError):
    """Missing or incompatible dependency."""
    pass


class DiffError(DTError):
    """Error during diff operations."""
    pass


class DuError(DTError):
    """Error during disk usage operations."""
    pass


class DVCFileError(DTError):
    """Error parsing or loading .dvc files."""
    pass


class HashMismatchError(DTError):
    """Hash mismatch during fetch/import operations.
    
    Raised when a computed .dir hash doesn't match expected.
    Suggests using --update flag.
    """
    
    def __init__(self, expected_hash: str, actual_hash: str, message: str = None):
        self.expected_hash = expected_hash
        self.actual_hash = actual_hash
        if message is None:
            message = (
                f"Hash mismatch: expected {expected_hash[:12]}..., got {actual_hash[:12]}...\n"
                f"Try: dt fetch --update <file> to rebuild and update the .dvc file"
            )
        super().__init__(message)


class FetchError(DTError):
    """Error during fetch operations."""
    pass


class FindError(DTError):
    """Error during find operations."""
    pass


class HistoryError(DTError):
    """Error during history operations."""
    pass


class LsError(DTError):
    """Error during ls operations."""
    pass


class HPCError(DTError):
    """Error during HPC/qxub operations."""
    pass


class ImportError_(DTError):
    """Error during import operations.
    
    Named ImportError_ to avoid shadowing builtin ImportError.
    """
    pass


class InitError(DTError):
    """Error during initialization."""
    pass


class MvError(DTError):
    """Error during move/rename operations."""
    pass


class OfflineError(DTError):
    """Error during offline mode operations."""
    pass


class PullError(DTError):
    """Error during pull operations."""
    pass


class PushError(DTError):
    """Error during push operations."""
    pass


class RemoteError(DTError):
    """Error during remote operations."""
    pass


class SummaryError(DTError):
    """Error during summary operations."""
    pass


class TmpError(DTError):
    """Error during tmp operations."""
    pass


class WorktreeError(DTError):
    """Error during worktree operations."""
    pass


class AddError(DTError):
    """Error during add operations."""
    pass


class UpdateError(DTError):
    """Error during update operations."""
    pass
