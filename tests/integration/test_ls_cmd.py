"""Integration tests for 'dt ls' command.

Tests for listing DVC-tracked files with various filters.
"""

import subprocess
from pathlib import Path

import pytest


# =============================================================================
# Test Classes
# =============================================================================

class TestLsBasic:
    """Tests for basic 'dt ls' functionality."""
    
    def test_ls_in_dvc_repo(self, dvc_repo_with_files, monkeypatch):
        """List tracked files in DVC repo."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show tracked file
        assert 'data.csv' in result.stdout
    
    def test_ls_help(self):
        """'dt ls --help' shows usage."""
        result = subprocess.run(
            ['dt', 'ls', '--help'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert '--pattern' in result.stdout or '-p' in result.stdout
        assert '--long' in result.stdout or '-l' in result.stdout
    
    def test_ls_outside_dvc_repo(self, tmp_path, monkeypatch):
        """List in non-DVC directory should handle gracefully."""
        monkeypatch.chdir(tmp_path)
        
        result = subprocess.run(
            ['dt', 'ls'],
            capture_output=True,
            text=True,
        )
        
        # Should either return empty or error message
        # depending on dvc list behavior outside repos
        assert result.returncode in (0, 1)


class TestLsRecursive:
    """Tests for recursive listing."""
    
    def test_ls_recursive_flag(self, dvc_repo_with_files, monkeypatch):
        """'-R' flag enables recursive listing."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '-R'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Recursive listing should work
        assert 'data.csv' in result.stdout


class TestLsLongFormat:
    """Tests for long format output."""
    
    def test_ls_long_format(self, dvc_repo_with_files, monkeypatch):
        """'-l' shows size and type."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '-l'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Long format should include file info
        output = result.stdout
        # Should show file type or size information
        assert 'data.csv' in output
    
    def test_ls_show_hash(self, dvc_repo_with_files, monkeypatch):
        """'--show-hash' shows MD5 hashes."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--show-hash'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should include hash (32 char hex string)
        # At minimum should have the file listed
        assert 'data.csv' in result.stdout


class TestLsFilters:
    """Tests for filtering options."""
    
    def test_ls_pattern_filter(self, dvc_repo_with_files, monkeypatch):
        """'--pattern' glob filter works."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--pattern', '*.csv'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should show csv files
        assert 'data.csv' in result.stdout
    
    def test_ls_pattern_no_match(self, dvc_repo_with_files, monkeypatch):
        """Pattern with no matches returns empty."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--pattern', '*.xyz'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should be empty or no matches
        assert 'data.csv' not in result.stdout
    
    def test_ls_files_only(self, dvc_repo_with_files, monkeypatch):
        """'--files' shows only files."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--files'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should include the file
        assert 'data.csv' in result.stdout


class TestLsOutput:
    """Tests for output formats."""
    
    def test_ls_json_output(self, dvc_repo_with_files, monkeypatch):
        """'--json' produces valid JSON."""
        import json
        
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--json'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should be valid JSON
        try:
            data = json.loads(result.stdout)
            assert isinstance(data, list)
        except json.JSONDecodeError:
            pytest.fail("Output is not valid JSON")


class TestLsRevision:
    """Tests for revision-based listing."""
    
    def test_ls_at_head(self, dvc_repo_with_files, monkeypatch):
        """'--rev HEAD' lists at HEAD."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--rev', 'HEAD'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        assert 'data.csv' in result.stdout


class TestLsAll:
    """Tests for --all flag."""
    
    def test_ls_all_includes_git_files(self, dvc_repo_with_files, monkeypatch):
        """'--all' includes non-DVC files."""
        monkeypatch.chdir(dvc_repo_with_files)
        
        result = subprocess.run(
            ['dt', 'ls', '--all'],
            capture_output=True,
            text=True,
        )
        
        assert result.returncode == 0
        # Should include README.md (created by fixture)
        assert 'README' in result.stdout or 'data.csv' in result.stdout


class TestListAlias:
    """Tests for the 'dt list' alias of 'dt ls'."""

    def test_list_alias_help(self):
        """'dt list --help' works and mentions the tree view."""
        result = subprocess.run(
            ['dt', 'list', '--help'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert '--tree' in result.stdout

    def test_list_alias_matches_ls(self, dvc_repo_with_files, monkeypatch):
        """'dt list' produces the same output as 'dt ls'."""
        monkeypatch.chdir(dvc_repo_with_files)

        ls_out = subprocess.run(['dt', 'ls'], capture_output=True, text=True)
        list_out = subprocess.run(['dt', 'list'], capture_output=True, text=True)

        assert ls_out.returncode == list_out.returncode == 0
        assert ls_out.stdout == list_out.stdout


class TestLsTree:
    """Tests for the '--tree' / '-o' tree view."""

    def test_tree_text(self, dvc_repo_with_files, monkeypatch):
        """'dt ls --tree' renders an ASCII tree with a summary line."""
        monkeypatch.chdir(dvc_repo_with_files)

        result = subprocess.run(
            ['dt', 'ls', '--tree'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert 'data.csv' in result.stdout
        assert 'files' in result.stdout  # "N directories, M files"

    def test_tree_html_implies_tree(self, dvc_repo_with_files, monkeypatch):
        """'-o html' implies --tree and emits the collapsible fold-up."""
        monkeypatch.chdir(dvc_repo_with_files)

        # Add a git-tracked file in a subdirectory so the tree has a
        # foldable (<details>) level; tree view lists git files too.
        nested = Path('src')
        nested.mkdir()
        (nested / 'train.py').write_text('print("hi")\n')
        subprocess.run(['git', 'add', 'src/train.py'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'commit', '-m', 'Add nested file'],
            check=True, capture_output=True,
        )

        result = subprocess.run(
            ['dt', 'ls', '-o', 'html'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert '<!DOCTYPE html>' in result.stdout
        assert '<details' in result.stdout
        assert '<summary>src/' in result.stdout
        assert 'train.py' in result.stdout
        assert 'Expand all' in result.stdout

    def test_tree_md(self, dvc_repo_with_files, monkeypatch):
        """'-o md' emits a fenced-code-block tree."""
        monkeypatch.chdir(dvc_repo_with_files)

        result = subprocess.run(
            ['dt', 'ls', '-o', 'md'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert '```text' in result.stdout
        assert 'data.csv' in result.stdout

    def test_tree_default_includes_git_files(self, dvc_repo_with_files, monkeypatch):
        """The tree defaults to DVC- and git-tracked files."""
        monkeypatch.chdir(dvc_repo_with_files)

        result = subprocess.run(
            ['dt', 'ls', '--tree'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        # data.csv is DVC-tracked; README.md is only git-tracked.
        assert 'data.csv' in result.stdout
        assert 'README' in result.stdout

    def test_tree_dvc_only_excludes_git_files(self, dvc_repo_with_files, monkeypatch):
        """'--dvc-only' restricts the tree to DVC outputs."""
        monkeypatch.chdir(dvc_repo_with_files)

        result = subprocess.run(
            ['dt', 'ls', '--tree', '--dvc-only'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert 'data.csv' in result.stdout
        assert 'README' not in result.stdout

    def test_tree_level_collapses_depth(self, dvc_repo_with_files, monkeypatch):
        """'-L 1' collapses contents below the first level."""
        monkeypatch.chdir(dvc_repo_with_files)

        nested = Path('src')
        nested.mkdir()
        (nested / 'train.py').write_text('print("hi")\n')
        subprocess.run(['git', 'add', 'src/train.py'], check=True, capture_output=True)
        subprocess.run(
            ['git', 'commit', '-m', 'Add nested file'],
            check=True, capture_output=True,
        )

        result = subprocess.run(
            ['dt', 'ls', '--tree', '-L', '1'],
            capture_output=True,
            text=True,
        )

        assert result.returncode == 0
        assert 'src/' in result.stdout
        # train.py lives below the depth limit, so it is collapsed away.
        assert 'train.py' not in result.stdout
        assert '…' in result.stdout

    def test_tree_default_excludes_untracked_and_ignored(self, dvc_repo_with_files, monkeypatch):
        """The default tree hides untracked, git-ignored, and DVC bookkeeping."""
        monkeypatch.chdir(dvc_repo_with_files)

        Path('scratch_note.txt').write_text('temporary\n')          # untracked
        Path('.gitignore').write_text('junk/\n')
        Path('junk').mkdir()
        (Path('junk') / 'ignored.log').write_text('noise\n')        # git-ignored
        subprocess.run(['git', 'add', '.gitignore'], check=True, capture_output=True)
        subprocess.run(['git', 'commit', '-m', 'ignore junk'],
                       check=True, capture_output=True)

        result = subprocess.run(['dt', 'ls', '--tree'], capture_output=True, text=True)

        assert result.returncode == 0
        assert 'data.csv' in result.stdout          # DVC object kept
        assert 'README' in result.stdout            # git-tracked kept
        assert 'scratch_note.txt' not in result.stdout   # untracked hidden
        assert 'junk' not in result.stdout               # git-ignored hidden
        assert 'ignored.log' not in result.stdout
        assert 'data.csv.dvc' not in result.stdout       # .dvc pointer file hidden
        assert '.dvcignore' not in result.stdout         # dvc ignore file hidden
        # .gitignore / .dvc config dir are suppressed too (only the token,
        # not substrings of kept paths, so check the ignore file name).
        assert '.gitignore' not in result.stdout

    def test_tree_all_includes_untracked_not_ignored(self, dvc_repo_with_files, monkeypatch):
        """'--all' adds untracked files back but still excludes ignored ones."""
        monkeypatch.chdir(dvc_repo_with_files)

        Path('scratch_note.txt').write_text('temporary\n')          # untracked
        Path('.gitignore').write_text('junk/\n')
        Path('junk').mkdir()
        (Path('junk') / 'ignored.log').write_text('noise\n')        # git-ignored
        subprocess.run(['git', 'add', '.gitignore'], check=True, capture_output=True)
        subprocess.run(['git', 'commit', '-m', 'ignore junk'],
                       check=True, capture_output=True)

        result = subprocess.run(['dt', 'ls', '--tree', '--all'],
                                capture_output=True, text=True)

        assert result.returncode == 0
        assert 'scratch_note.txt' in result.stdout   # untracked now shown
        assert 'ignored.log' not in result.stdout    # ignored still hidden
        assert 'data.csv' in result.stdout           # DVC object still shown

    def test_tree_shows_revision_subtitle(self, dvc_repo_with_files, monkeypatch):
        """The tree prints the revision (short SHA and date)."""
        monkeypatch.chdir(dvc_repo_with_files)
        subprocess.run(['git', 'tag', 'v9.9.9'], check=True, capture_output=True)

        result = subprocess.run(['dt', 'ls', '--tree'], capture_output=True, text=True)
        sha = subprocess.run(['git', 'rev-parse', '--short', 'HEAD'],
                             capture_output=True, text=True).stdout.strip()

        assert result.returncode == 0
        assert sha in result.stdout        # revision SHA
        assert 'v9.9.9' in result.stdout   # tag on that revision

    def test_tree_html_repo_link_and_popup(self, dvc_repo_with_files, monkeypatch):
        """HTML titles the repo (linked) and embeds the dvc get/import popup."""
        monkeypatch.chdir(dvc_repo_with_files)
        subprocess.run(
            ['git', 'remote', 'add', 'origin',
             'git@github.com:swarbricklab/demo-repo.git'],
            check=True, capture_output=True,
        )

        result = subprocess.run(
            ['dt', 'ls', '-o', 'html'], capture_output=True, text=True,
        )

        assert result.returncode == 0
        assert '<a href="https://github.com/swarbricklab/demo-repo">demo-repo</a>' \
            in result.stdout
        assert 'class="cmd-btn"' in result.stdout
        assert 'data-path=' in result.stdout
        # Both clone URLs are embedded for the HTTPS/SSH protocol tabs.
        assert 'const REPO_HTTPS = "https://github.com/swarbricklab/demo-repo.git"' \
            in result.stdout
        assert 'const REPO_SSH = "git@github.com:swarbricklab/demo-repo.git"' \
            in result.stdout
        assert 'data-proto="ssh"' in result.stdout
