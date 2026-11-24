"""Unit tests for dt.index module."""

from click.testing import CliRunner

from dt import index as index_mod
from dt.index import IndexError


class TestIndexErrors:
    def test_index_error_inherits_from_exception(self):
        error = IndexError("test error")
        assert isinstance(error, Exception)
        assert str(error) == "test error"


class TestMirrorRemoved:
    """The index archive mirror (dt index push|pull) no longer exists."""

    def test_module_has_no_mirror_api(self):
        for name in ('pull', 'push', 'get_index_paths', 'is_auto_sync_enabled',
                     '_merge_db', '_backup_db', 'acquire_lock'):
            assert not hasattr(index_mod, name), name

    def test_cli_has_no_push_or_pull(self):
        from dt.cli import cli
        runner = CliRunner()
        for sub in ('push', 'pull'):
            result = runner.invoke(cli, ['index', sub])
            assert result.exit_code != 0
            assert 'No such command' in result.output

    def test_status_never_syncs(self):
        import inspect
        from dt import status as status_mod
        assert 'index_mod' not in inspect.getsource(status_mod)

    def test_index_sync_hook_check_is_a_noop(self):
        from dt import install
        assert install._run_index_sync(verbose=False) is True
