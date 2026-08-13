"""Tests for ``dt auth setup``, in particular its ``--repo`` mode."""

from pathlib import Path
from unittest.mock import patch

import pytest

from dt.auth.endpoints import Endpoint
from dt.auth.setup import (
    SetupReport,
    _install_credentials_for_repos,
    _prompt_username,
    _s3_secret_names,
    auth_setup,
)
from dt.errors import AuthError


def _s3(url='s3://bucket', source="DVC remote 'storage'"):
    return Endpoint(type='s3', url=url, source=source)


def _import_source(url, children):
    return Endpoint(
        type='git', url=url, source='import source (data.dvc)',
        children=children,
    )


# =============================================================================
# _s3_secret_names
# =============================================================================

class TestS3SecretNames:
    """Mapping S3 endpoints back to the repo whose secret unlocks them."""

    def test_top_level_s3_belongs_to_the_named_repo(self):
        names = _s3_secret_names('bcarc_wts', [_s3()])
        assert names == ['bcarc_wts']

    def test_repo_url_is_reduced_to_a_short_name(self):
        """The secret is named after the repo, not its URL."""
        names = _s3_secret_names(
            'git@github.com:swarbricklab/bcarc_wts.git', [_s3()],
        )
        assert names == ['bcarc_wts']

    def test_no_s3_endpoints_needs_no_secrets(self):
        endpoints = [
            Endpoint(type='ssh', url='ssh://host/data', source='remote'),
            Endpoint(type='git', url='git@github.com:org/repo.git', source='git'),
        ]
        assert _s3_secret_names('repo', endpoints) == []

    def test_import_source_with_s3_remote_contributes_its_own_name(self):
        """Data imported from elsewhere needs the source repo's credentials."""
        endpoints = [
            _import_source(
                'git@github.com:swarbricklab/other_repo.git',
                [_s3(url='s3://other-bucket')],
            ),
        ]
        assert _s3_secret_names('bcarc_wts', endpoints) == ['other_repo']

    def test_import_source_without_s3_children_is_ignored(self):
        endpoints = [
            _import_source(
                'git@github.com:swarbricklab/projects.git',
                [Endpoint(type='ssh', url='ssh://host/p', source='remote')],
            ),
        ]
        assert _s3_secret_names('bcarc_wts', endpoints) == []

    def test_own_remote_and_import_source_both_collected(self):
        endpoints = [
            _s3(),
            _import_source(
                'git@github.com:swarbricklab/other_repo.git',
                [_s3(url='s3://other-bucket')],
            ),
        ]
        assert _s3_secret_names('bcarc_wts', endpoints) == [
            'bcarc_wts', 'other_repo',
        ]

    def test_names_are_deduplicated(self):
        """Two remotes of one repo, or two spellings of one URL, name it once."""
        endpoints = [
            _s3(url='s3://bucket-a'),
            _s3(url='s3://bucket-b'),
            _import_source(
                'git@github.com:swarbricklab/projects.git', [_s3()],
            ),
            _import_source(
                'git@github.com:Swarbricklab/projects.git', [_s3()],
            ),
        ]
        assert _s3_secret_names('bcarc_wts', endpoints) == [
            'bcarc_wts', 'projects',
        ]


# =============================================================================
# _install_credentials_for_repos
# =============================================================================

class TestInstallCredentialsForRepos:

    def test_installs_each_named_repo(self):
        report = SetupReport()
        with patch('dt.auth.setup.install_credentials',
                   side_effect=lambda verbose, repo_name: {repo_name: True}) as m:
            result = _install_credentials_for_repos(
                ['repo_a', 'repo_b'], report=report, verbose=False,
            )

        assert result == {'repo_a': True, 'repo_b': True}
        assert [c.kwargs['repo_name'] for c in m.call_args_list] == [
            'repo_a', 'repo_b',
        ]
        assert report.errors == []

    def test_one_failure_does_not_abort_the_others(self):
        """install_credentials raises when its one repo yields nothing usable."""
        def fake(verbose, repo_name):
            if repo_name == 'broken':
                raise AuthError('no usable secrets found')
            return {repo_name: True}

        report = SetupReport()
        with patch('dt.auth.setup.install_credentials', side_effect=fake):
            result = _install_credentials_for_repos(
                ['broken', 'fine'], report=report, verbose=False,
            )

        assert result == {'broken': False, 'fine': True}
        assert len(report.errors) == 1
        assert 'broken' in report.errors[0]

    def test_no_names_is_a_no_op(self):
        report = SetupReport()
        with patch('dt.auth.setup.install_credentials') as m:
            assert _install_credentials_for_repos(
                [], report=report, verbose=False,
            ) == {}
        m.assert_not_called()


# =============================================================================
# auth_setup(--repo)
# =============================================================================

@pytest.fixture
def no_ssh_or_gcp():
    """Neutralise the SSH and GCP-precheck arms so credentials are isolated."""
    with patch('dt.auth.setup._do_ssh_setup', return_value=[]), \
         patch('dt.secrets.gcp.GCPSecretBackend._has_adc_credentials',
               return_value=True):
        yield


class TestAuthSetupRepo:

    def test_repo_discovers_from_the_clone_not_the_cwd(self, no_ssh_or_gcp):
        with patch('dt.auth.setup.discover_endpoints_from_repo',
                   return_value=[]) as from_repo, \
             patch('dt.auth.setup.discover_endpoints') as local:
            auth_setup(repo_url='bcarc_wts', ssh_config_file=Path('/tmp/nope'))

        from_repo.assert_called_once()
        assert from_repo.call_args.args[0] == 'bcarc_wts'
        local.assert_not_called()

    def test_without_repo_discovers_locally(self, no_ssh_or_gcp):
        with patch('dt.auth.setup.discover_endpoints',
                   return_value=[]) as local, \
             patch('dt.auth.setup.discover_endpoints_from_repo') as from_repo:
            auth_setup(ssh_config_file=Path('/tmp/nope'))

        local.assert_called_once()
        from_repo.assert_not_called()

    def test_credentials_scoped_to_the_repo_not_the_cwd(self, no_ssh_or_gcp):
        """The regression this guards: install_credentials() with no repo_name
        discovers repos from the current directory, which under --repo is a
        different project entirely."""
        # Tolerate the unscoped call signature so that dropping the scoping
        # fails on the assertion below, not on a TypeError from this stub.
        def fake(verbose, repo_name=None):
            return {repo_name or 'whatever-the-cwd-is': True}

        with patch('dt.auth.setup.discover_endpoints_from_repo',
                   return_value=[_s3()]), \
             patch('dt.auth.setup.install_credentials',
                   side_effect=fake) as m:
            report = auth_setup(
                repo_url='bcarc_wts', ssh_config_file=Path('/tmp/nope'),
            )

        m.assert_called_once()
        assert m.call_args.kwargs.get('repo_name') == 'bcarc_wts'
        assert report.credentials_installed == {'bcarc_wts': True}
        assert not report.skipped_credentials

    def test_without_repo_credentials_are_discovered(self, no_ssh_or_gcp):
        """Unscoped install is still the behaviour when --repo is absent."""
        with patch('dt.auth.setup.discover_endpoints', return_value=[_s3()]), \
             patch('dt.auth.setup.install_credentials',
                   return_value={'proj': True}) as m:
            auth_setup(ssh_config_file=Path('/tmp/nope'))

        m.assert_called_once_with(verbose=False)

    def test_repo_without_s3_skips_credentials(self, no_ssh_or_gcp):
        with patch('dt.auth.setup.discover_endpoints_from_repo',
                   return_value=[Endpoint(type='ssh', url='ssh://h/d',
                                          source='remote')]), \
             patch('dt.auth.setup.install_credentials') as m:
            report = auth_setup(
                repo_url='bcarc_wts', ssh_config_file=Path('/tmp/nope'),
            )

        m.assert_not_called()
        assert report.skipped_credentials

    def test_repo_endpoints_drive_ssh_setup(self, no_ssh_or_gcp):
        """SSH setup acts on the target repo's hosts, not the cwd project's."""
        endpoints = [Endpoint(type='git', url='git@github.com:org/r.git',
                              source='git remote')]
        with patch('dt.auth.setup.discover_endpoints_from_repo',
                   return_value=endpoints), \
             patch('dt.auth.setup._do_ssh_setup', return_value=[]) as ssh:
            auth_setup(repo_url='r', ssh_config_file=Path('/tmp/nope'))

        assert ssh.call_args.kwargs['endpoints'] == endpoints


# =============================================================================
# _prompt_username
# =============================================================================

class TestPromptUsername:

    def test_non_interactive_stdin_explains_itself(self):
        """A closed stdin used to surface as a bare 'SSH setup error:'."""
        with patch('dt.auth.setup.sys.stdin.isatty', return_value=False):
            with pytest.raises(AuthError) as exc:
                _prompt_username('gadi-dm.nci.org.au')

        message = str(exc.value)
        assert 'gadi-dm.nci.org.au' in message
        assert '--username' in message

    def test_interactive_stdin_prompts(self):
        with patch('dt.auth.setup.sys.stdin.isatty', return_value=True), \
             patch('dt.auth.setup.click.prompt', return_value='alice') as p:
            assert _prompt_username('host') == 'alice'
        p.assert_called_once()
