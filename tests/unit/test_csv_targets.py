"""Tests for utils.read_csv_target_list -- the CSV reader behind
``dt fetch --csv`` and ``dt pull --csv``.

These are pure: no repo, no DVC, no network. The function carries the safety
properties the commands rely on, above all that it can never hand back an empty
list -- both commands read an empty target list as "operate on the whole repo".
"""

from unittest.mock import patch

import pytest
from click.testing import CliRunner

from dt import utils
from dt.cli import cli


def _write_csv(path, text):
    path.write_text(text)
    return str(path)


def _invoke(args):
    return CliRunner().invoke(cli, args)


class TestReadCsvTargetList:
    """The contract dt fetch --csv and dt pull --csv share."""

    def test_reads_paths(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\ndata/one\ndata/two\n')
        assert utils.read_csv_target_list(csv_file) == ['data/one', 'data/two']

    def test_custom_path_column(self, tmp_path):
        csv_file = _write_csv(
            tmp_path / 'a.csv', 'sample,fq_dir\nAF013,data/fq/AF013\n'
        )
        assert utils.read_csv_target_list(csv_file, path_col='fq_dir') == [
            'data/fq/AF013'
        ]

    def test_output_column_gets_no_special_treatment(self, tmp_path):
        """Unlike read_csv_targets, there is no destination to choose here.

        A sheet written for `dt get` stays usable -- its `output` column is
        ignored exactly like any other column, and --path-col reaches it if
        that is the column you actually meant.
        """
        csv_file = _write_csv(
            tmp_path / 'a.csv',
            'path,output,kind\nfq/AF013,data/fq/AF013,wts\n',
        )
        assert utils.read_csv_target_list(csv_file) == ['fq/AF013']
        assert utils.read_csv_target_list(csv_file, path_col='output') == [
            'data/fq/AF013'
        ]

    def test_whitespace_is_stripped(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\n  data/one  \n')
        assert utils.read_csv_target_list(csv_file) == ['data/one']

    def test_duplicates_collapse_preserving_order(self, tmp_path):
        """Sheets are often one row per file with a shared directory."""
        csv_file = _write_csv(
            tmp_path / 'a.csv',
            'path,file\nd/b,r1.fq\nd/a,r1.fq\nd/b,r2.fq\n',
        )
        assert utils.read_csv_target_list(csv_file) == ['d/b', 'd/a']


class TestNeverEmpty:
    """The whole point: no input may produce an empty list.

    Both callers turn an empty target list into "everything", so an empty
    return here is a full-repo fetch or pull rather than a no-op.
    """

    def test_header_only_csv_raises(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\n')
        with pytest.raises(ValueError, match='CSV file is empty'):
            utils.read_csv_target_list(csv_file)

    def test_blank_path_cell_raises(self, tmp_path):
        """read_csv_targets keeps these for per-row reporting; we cannot.

        An empty string target resolves to the working directory, so a stray
        blank row would quietly widen the operation to the whole repo.
        """
        csv_file = _write_csv(tmp_path / 'a.csv', 'path,name\n,foo\nd/b,bar\n')
        with pytest.raises(ValueError, match="Blank 'path' cell"):
            utils.read_csv_target_list(csv_file)

    def test_blank_cell_error_names_the_line(self, tmp_path):
        """Line numbers count the header, so they match a text editor."""
        csv_file = _write_csv(
            tmp_path / 'a.csv', 'path,name\nd/a,x\n,y\nd/c,z\n'
        )
        with pytest.raises(ValueError, match='line 3'):
            utils.read_csv_target_list(csv_file)

    def test_many_blank_cells_are_truncated(self, tmp_path):
        rows = ''.join(',x\n' for _ in range(15))
        csv_file = _write_csv(tmp_path / 'a.csv', 'path,name\n' + rows)
        with pytest.raises(ValueError, match='and 5 more'):
            utils.read_csv_target_list(csv_file)

    def test_whitespace_only_cell_counts_as_blank(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path,name\n   ,foo\n')
        with pytest.raises(ValueError, match="Blank 'path' cell"):
            utils.read_csv_target_list(csv_file)

    def test_wholly_blank_lines_are_skipped_by_the_reader(self, tmp_path):
        """csv.DictReader drops empty lines before we see them.

        So a trailing newline is harmless and needs no special handling -- only
        a blank *cell* in a populated row can reach the check above.
        """
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/a\n\nd/b\n\n')
        assert utils.read_csv_target_list(csv_file) == ['d/a', 'd/b']


class TestBadInput:
    """Inherited from _read_csv_rows, shared with read_csv_targets."""

    def test_missing_path_column(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'name\nfoo\n')
        with pytest.raises(ValueError, match="must have a 'path' column"):
            utils.read_csv_target_list(csv_file)

    def test_missing_path_column_lists_what_is_there(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'sample,fq_dir\nAF013,d/a\n')
        with pytest.raises(ValueError, match='fq_dir'):
            utils.read_csv_target_list(csv_file)

    def test_unknown_custom_path_column(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/a\n')
        with pytest.raises(ValueError, match="must have a 'nope' column"):
            utils.read_csv_target_list(csv_file, path_col='nope')

    def test_missing_file(self, tmp_path):
        with pytest.raises(ValueError, match='CSV file not found'):
            utils.read_csv_target_list(str(tmp_path / 'nope.csv'))


class TestFiltersAreGone:
    """--filter was removed from every CSV command; the helpers went with it."""

    def test_parse_row_filters_removed(self):
        assert not hasattr(utils, 'parse_row_filters')

    def test_row_matches_removed(self):
        assert not hasattr(utils, 'row_matches')

    def test_read_csv_targets_takes_no_filters(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/a\n')
        with pytest.raises(TypeError):
            utils.read_csv_targets(csv_file, filters=['kind=wts'])


# =============================================================================
# CLI routing -- dt fetch --csv and dt pull --csv
# =============================================================================

class TestFetchCsvRouting:
    """--csv builds the target list dt fetch would otherwise take from argv."""

    def test_csv_targets_reach_fetch(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\n')
        with patch('dt.fetch.fetch', return_value=[]) as m:
            result = _invoke(['fetch', '--csv', csv_file])
        assert result.exit_code == 0, result.output
        assert m.call_args.kwargs['targets'] == ['d/one', 'd/two']

    def test_path_col_reaches_the_other_column(self, tmp_path):
        csv_file = _write_csv(
            tmp_path / 'a.csv', 'sample,fq_dir\nAF013,d/fq/AF013\n'
        )
        with patch('dt.fetch.fetch', return_value=[]) as m:
            _invoke(['fetch', '--csv', csv_file, '--path-col', 'fq_dir'])
        assert m.call_args.kwargs['targets'] == ['d/fq/AF013']

    def test_bare_invocation_still_means_everything(self):
        """The one case where targets=None is correct."""
        with patch('dt.fetch.fetch', return_value=[]) as m:
            _invoke(['fetch'])
        assert m.call_args.kwargs['targets'] is None

    def test_positional_targets_unaffected(self):
        with patch('dt.fetch.fetch', return_value=[]) as m:
            _invoke(['fetch', 'data/a.dvc', 'data/b.dvc'])
        assert m.call_args.kwargs['targets'] == ['data/a.dvc', 'data/b.dvc']

    def test_csv_plus_positional_is_refused(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\n')
        with patch('dt.fetch.fetch', return_value=[]) as m:
            result = _invoke(['fetch', '--csv', csv_file, 'data/b.dvc'])
        assert result.exit_code == 2
        assert 'Do not provide TARGETS' in result.output
        m.assert_not_called()

    def test_missing_csv_file_is_refused(self, tmp_path):
        with patch('dt.fetch.fetch', return_value=[]) as m:
            result = _invoke(['fetch', '--csv', str(tmp_path / 'nope.csv')])
        assert result.exit_code == 2
        m.assert_not_called()


class TestPullCsvRouting:
    """Same contract for dt pull."""

    def test_csv_targets_reach_pull(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\n')
        with patch('dt.pull.pull', return_value=(True, 2, 0)) as m:
            result = _invoke(['pull', '--csv', csv_file])
        assert result.exit_code == 0, result.output
        assert m.call_args.kwargs['targets'] == ['d/one', 'd/two']

    def test_bare_invocation_still_means_everything(self):
        with patch('dt.pull.pull', return_value=(True, 0, 0)) as m:
            _invoke(['pull'])
        assert m.call_args.kwargs['targets'] is None

    def test_csv_plus_positional_is_refused(self, tmp_path):
        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\n')
        with patch('dt.pull.pull', return_value=(True, 0, 0)) as m:
            result = _invoke(['pull', '--csv', csv_file, 'data/b'])
        assert result.exit_code == 2
        m.assert_not_called()


class TestBadCsvNeverWidensTheOperation:
    """The failure that matters: a bad sheet must not become "everything".

    Both commands read targets=None as the whole repo, so for each way a CSV
    can be wrong, the command must refuse *without ever calling through*.
    """

    @pytest.mark.parametrize('command,target', [
        ('fetch', 'dt.fetch.fetch'),
        ('pull', 'dt.pull.pull'),
    ])
    @pytest.mark.parametrize('body,expected', [
        ('path\n', 'CSV file is empty'),
        ('path,name\n,foo\n', "Blank 'path' cell"),
        ('path,name\n   ,foo\n', "Blank 'path' cell"),
        ('name\nfoo\n', "must have a 'path' column"),
    ])
    def test_refused_without_calling_through(
        self, tmp_path, command, target, body, expected
    ):
        csv_file = _write_csv(tmp_path / 'a.csv', body)
        with patch(target, return_value=[]) as m:
            result = _invoke([command, '--csv', csv_file])
        assert result.exit_code == 2, result.output
        assert expected in result.output
        m.assert_not_called()


class TestCsvProvenanceOnErrors:
    """DVC names the first unresolvable target and stops.

    Over a 400-row sheet that is one run per typo, and nothing in the message
    says the path came from a file -- let alone which file or which column.
    """

    def test_fetch_error_names_the_csv(self, tmp_path):
        from dt import fetch as fetch_mod

        csv_file = _write_csv(tmp_path / 'a.csv', 'path\nd/one\nd/two\n')
        with patch('dt.fetch.fetch',
                   side_effect=fetch_mod.FetchError("'d/one' does not exist")):
            result = _invoke(['fetch', '--csv', csv_file])

        assert result.exit_code != 0
        assert 'does not exist' in result.output
        assert str(csv_file) in result.output
        assert "column 'path'" in result.output
        assert '2 target(s)' in result.output

    def test_pull_error_names_the_csv(self, tmp_path):
        from dt import pull as pull_mod

        csv_file = _write_csv(tmp_path / 'a.csv', 'fq\nd/one\n')
        with patch('dt.pull.pull',
                   side_effect=pull_mod.PullError("'d/one' does not exist")):
            result = _invoke(['pull', '--csv', csv_file, '--path-col', 'fq'])

        assert result.exit_code != 0
        assert str(csv_file) in result.output
        assert "column 'fq'" in result.output

    def test_no_csv_context_without_a_csv(self):
        """A plain `dt fetch` failure must not grow a CSV footnote."""
        from dt import fetch as fetch_mod

        with patch('dt.fetch.fetch',
                   side_effect=fetch_mod.FetchError('boom')):
            result = _invoke(['fetch', 'data/a.dvc'])

        assert result.exit_code != 0
        assert 'boom' in result.output
        assert 'Targets came from' not in result.output
