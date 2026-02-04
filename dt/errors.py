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


class DuError(DTError):
    """Error during disk usage operations."""
    pass


class DVCFileError(DTError):
    """Error parsing or loading .dvc files."""
    pass


class FetchError(DTError):
    """Error during fetch operations."""
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


class AddError(DTError):
    """Error during add operations."""
    pass
