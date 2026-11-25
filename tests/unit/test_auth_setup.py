"""Tests for ``dt auth setup``, in particular its ``--repo`` mode."""

from pathlib import Path
from unittest.mock import MagicMock, patch

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
        names = _s3_secret_names('example_wts', [_s3()])
        assert names == ['example_wts']

    def test_repo_url_is_reduced_to_a_short_name(self):
        """The secret is named after the repo, not its URL."""
        names = _s3_secret_names(
            'git@github.com:swarbricklab/example_wts.git', [_s3()],
        )
        assert names == ['example_wts']

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
        assert _s3_secret_names('example_wts', endpoints) == ['other_repo']

    def test_import_source_without_s3_children_is_ignored(self):
        endpoints = [
            _import_source(
                'git@github.com:swarbricklab/project-index.git',
                [Endpoint(type='ssh', url='ssh://host/p', source='remote')],
            ),
        ]
        assert _s3_secret_names('example_wts', endpoints) == []

    def test_own_remote_and_import_source_both_collected(self):
        endpoints = [
            _s3(),
            _import_source(
                'git@github.com:swarbricklab/other_repo.git',
                [_s3(url='s3://other-bucket')],
            ),
        ]
        assert _s3_secret_names('example_wts', endpoints) == [
            'example_wts', 'other_repo',
        ]

    def test_names_are_deduplicated(self):
        """Two remotes of one repo, or two spellings of one URL, name it once."""
        endpoints = [
            _s3(url='s3://bucket-a'),
            _s3(url='s3://bucket-b'),
            _import_source(
                'git@github.com:swarbricklab/project-index.git', [_s3()],
            ),
            _import_source(
                'git@github.com:Swarbricklab/project-index.git', [_s3()],
            ),
        ]
        assert _s3_secret_names('example_wts', endpoints) == [
            'example_wts', 'project-index',
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
            auth_setup(repo_url='example_wts', ssh_config_file=Path('/tmp/nope'))

        from_repo.assert_called_once()
        assert from_repo.call_args.args[0] == 'example_wts'
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
                repo_url='example_wts', ssh_config_file=Path('/tmp/nope'),
            )

        m.assert_called_once()
        assert m.call_args.kwargs.get('repo_name') == 'example_wts'
        assert report.credentials_installed == {'example_wts': True}
        assert not report.skipped_credentials

    def test_without_repo_credentials_are_discovered(self, no_ssh_or_gcp):
        """Unscoped install is still the behaviour when --repo is absent."""
        with patch('dt.auth.setup.discover_endpoints', return_value=[_s3()]), \
             patch('dt.auth.setup.install_credentials',
                   return_value={'proj': True}) as m:
            auth_setup(ssh_config_file=Path('/tmp/nope'))

        m.assert_called_once()
        assert m.call_args.kwargs['verbose'] is False
        assert callable(m.call_args.kwargs['may_contact'])

    def test_repo_chosen_secret_store_is_not_used_automatically(self, no_ssh_or_gcp):
        with patch('dt.auth.setup.discover_endpoints', return_value=[_s3()]), \
             patch('dt.auth.setup.secret_store_set_by_repo', return_value=True), \
             patch('dt.auth.setup.install_credentials') as m:
            report = auth_setup(ssh_config_file=Path('/tmp/nope'))

        m.assert_not_called()
        assert report.skipped_credentials
        assert 'repository' in report.credentials_skip_reason

    def test_repo_without_s3_skips_credentials(self, no_ssh_or_gcp):
        with patch('dt.auth.setup.discover_endpoints_from_repo',
                   return_value=[Endpoint(type='ssh', url='ssh://h/d',
                                          source='remote')]), \
             patch('dt.auth.setup.install_credentials') as m:
            report = auth_setup(
                repo_url='example_wts', ssh_config_file=Path('/tmp/nope'),
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


class TestNewSshHostsNeedApproval:
    """Hosts from the project's remotes reach ~/.ssh/config only when agreed."""

    @pytest.fixture
    def run_setup(self, tmp_path):
        from dt.auth import setup as setup_mod

        def run(add_new_hosts=None, tty=False, answer=False, host_configs=None,
                existing=''):
            config = tmp_path / 'config'
            if existing:
                config.write_text(existing)
            ep = Endpoint(type='ssh', url='ssh://xy1234@hpc.example.org/data',
                          source="DVC remote 'storage'")
            skipped = []
            with patch.object(setup_mod, '_ensure_ssh_dir'), \
                 patch.object(setup_mod, '_find_existing_key',
                              return_value=tmp_path / 'id'), \
                 patch.object(setup_mod, '_key_has_passphrase', return_value=False), \
                 patch.object(setup_mod, 'check_endpoints',
                              return_value=[]) as checked, \
                 patch.object(setup_mod, '_key_accepted_by_host', return_value=True), \
                 patch.object(setup_mod, '_deploy_keys_and_report',
                              side_effect=lambda hosts, **kw: list(hosts)), \
                 patch.object(setup_mod.sys.stdin, 'isatty', return_value=tty), \
                 patch.object(setup_mod.click, 'confirm',
                              return_value=answer) as confirm:
                out = setup_mod._do_ssh_setup(
                    endpoints=[ep], host_configs=host_configs or {},
                    default_username=None, ssh_config_file=config,
                    verbose=False, add_new_hosts=add_new_hosts,
                    skipped_hosts=skipped)
            text = config.read_text() if config.exists() else ''
            self.checked = checked
            return out, skipped, text, confirm
        return run

    def test_not_interactive_leaves_new_host_out(self, run_setup, capsys):
        out, skipped, text, confirm = run_setup()
        assert out == [] and skipped == ['hpc.example.org']
        assert 'hpc.example.org' not in text
        confirm.assert_not_called()
        assert '--add-ssh-hosts' in capsys.readouterr().out

    def test_interactive_asks_and_respects_no(self, run_setup, capsys):
        out, skipped, text, confirm = run_setup(tty=True, answer=False)
        confirm.assert_called_once()
        assert not self.checked.called or \
            self.checked.call_args.kwargs['endpoints'] == []
        assert 'hpc.example.org' in capsys.readouterr().out
        assert out == [] and 'hpc.example.org' not in text

    def test_interactive_yes_adds_host(self, run_setup):
        out, skipped, text, _ = run_setup(tty=True, answer=True)
        assert out == ['hpc.example.org'] and skipped == []
        assert 'Host hpc.example.org' in text

    def test_flag_adds_without_asking(self, run_setup):
        out, skipped, text, confirm = run_setup(add_new_hosts=True)
        confirm.assert_not_called()
        assert 'Host hpc.example.org' in text

    def test_host_in_user_config_file_counts_as_agreed(self, run_setup):
        from dt.auth.setup import HostConfig
        out, skipped, text, confirm = run_setup(
            host_configs={'hpc.example.org': HostConfig(username='xy1234')})
        confirm.assert_not_called()
        assert out == ['hpc.example.org']

    def test_host_already_in_ssh_config_needs_no_answer(self, run_setup):
        out, skipped, text, confirm = run_setup(
            existing='Host hpc.example.org\n    User xy1234\n')
        confirm.assert_not_called()
        assert out == ['hpc.example.org'] and skipped == []

    def test_declined_host_is_not_contacted(self, tmp_path):
        from dt.auth import setup as setup_mod
        config = tmp_path / 'config'
        config.write_text('Host known.example.org\n    User xy1234\n')
        known = Endpoint(type='ssh', url='ssh://xy1234@known.example.org/d',
                         source='r1')
        known.children = [Endpoint(type='ssh', url='ssh://xy1234@new.example.org/e',
                                   source='child'),
                          Endpoint(type='http', url='https://other.example.org/x',
                                   source='child')]
        new = Endpoint(type='ssh', url='ssh://xy1234@new.example.org/d',
                       source='r2')
        with patch.object(setup_mod, '_ensure_ssh_dir'), \
             patch.object(setup_mod, '_find_existing_key',
                          return_value=tmp_path / 'id'), \
             patch.object(setup_mod, '_key_has_passphrase', return_value=False), \
             patch.object(setup_mod, 'check_endpoints', return_value=[]) as checked, \
             patch.object(setup_mod, '_key_accepted_by_host', return_value=True), \
             patch.object(setup_mod, '_deploy_keys_and_report',
                          side_effect=lambda hosts, **kw: list(hosts)), \
             patch.object(setup_mod.sys.stdin, 'isatty', return_value=False):
            out = setup_mod._do_ssh_setup(
                endpoints=[known, new], host_configs={}, default_username=None,
                ssh_config_file=config, verbose=False)
        assert out == ['known.example.org']
        eps = checked.call_args.kwargs['endpoints']
        urls = [e.url for e in eps] + [c.url for e in eps for c in e.children]
        assert urls == ['ssh://xy1234@known.example.org/d']


class TestImportSourcesAskedFirst:
    def test_new_import_host_not_cloned_without_agreement(self, tmp_path, monkeypatch):
        from dt.auth import endpoints as ep_mod, setup as setup_mod
        from dt import remote as remote_mod
        monkeypatch.setattr('dt.auth._helpers._get_import_urls',
                            lambda repo_path=None: {'git@new.example.org:o/r.git': ['a.dvc']})
        for name in ('_discover_dt_config',):
            monkeypatch.setattr(ep_mod, name, lambda: [])
        monkeypatch.setattr(ep_mod, '_discover_dvc_remotes', lambda p=None: [])
        monkeypatch.setattr(ep_mod, '_discover_git_remotes', lambda p=None: [])
        monkeypatch.setattr(ep_mod.utils, 'get_project_name', lambda: 'p')
        cloned = []
        monkeypatch.setattr(remote_mod, 'list_remotes_from_repo',
                            lambda url, **k: cloned.append(url) or [])
        monkeypatch.setattr(setup_mod.sys.stdin, 'isatty', lambda: False)
        ssh_calls = []
        monkeypatch.setattr(setup_mod, '_do_ssh_setup',
                            lambda **kw: ssh_calls.append(kw) or [])
        report = setup_mod.auth_setup(ssh_config_file=tmp_path / 'cfg')
        assert cloned == []
        assert ssh_calls and ssh_calls[0]['decisions'] == {'new.example.org': False}

    def test_agreed_import_host_is_cloned(self, tmp_path, monkeypatch):
        from dt.auth import endpoints as ep_mod, setup as setup_mod
        from dt import remote as remote_mod
        monkeypatch.setattr('dt.auth._helpers._get_import_urls',
                            lambda repo_path=None: {'git@new.example.org:o/r.git': ['a.dvc']})
        monkeypatch.setattr(ep_mod, '_discover_dt_config', lambda: [])
        monkeypatch.setattr(ep_mod, '_discover_dvc_remotes', lambda p=None: [])
        monkeypatch.setattr(ep_mod, '_discover_git_remotes', lambda p=None: [])
        monkeypatch.setattr(ep_mod.utils, 'get_project_name', lambda: 'p')
        cloned = []
        monkeypatch.setattr(remote_mod, 'list_remotes_from_repo',
                            lambda url, **k: cloned.append(url) or [])
        monkeypatch.setattr(setup_mod, '_do_ssh_setup', lambda **kw: [])
        setup_mod.auth_setup(ssh_config_file=tmp_path / 'cfg', add_ssh_hosts=True)
        assert cloned == ['git@new.example.org:o/r.git']


class TestContactGate:
    @pytest.mark.parametrize('url', [
        'git+ssh://evil.example.org/o/r.git',
        'ssh+git://evil.example.org/o/r.git',
        'ssh://[evil.example.org]/o/r.git',
        '[evil.example.org]:o/r.git',
        'ssh://a@b@evil.example.org/o/r',
        'git@new.example.org:o/r.git',
        'ssh://new.example.org/o/r.git',
        'foo::bar',
    ])
    def test_unknown_or_new_hosts_need_a_yes(self, tmp_path, url, monkeypatch):
        from dt.auth import setup as setup_mod
        monkeypatch.setattr(setup_mod.sys.stdin, 'isatty', lambda: False)
        gate = setup_mod.contact_gate({}, tmp_path / 'cfg', None, {})
        assert gate(url) is False

    @pytest.mark.parametrize('url', [
        'https://github.com/o/r.git', '/g/data/x/repo', 'file:///x/repo', '../repo',
    ])
    def test_local_and_https_are_fine(self, tmp_path, url):
        from dt.auth import setup as setup_mod
        gate = setup_mod.contact_gate({}, tmp_path / 'cfg', None, {})
        assert gate(url) is True

    def test_known_host_is_fine(self, tmp_path):
        from dt.auth import setup as setup_mod
        cfg = tmp_path / 'cfg'
        cfg.write_text('Host known.example.org\n    User u\n')
        gate = setup_mod.contact_gate({}, cfg, None, {})
        assert gate('git@known.example.org:o/r.git') is True

    def test_credential_step_does_not_clone_refused_sources(self, tmp_path, monkeypatch):
        from dt.auth import credentials, setup as setup_mod
        from dt import remote as remote_mod
        monkeypatch.setattr('dt.auth._helpers._get_import_urls',
                            lambda repo_path=None: {'git@new.example.org:o/r.git': ['a.dvc']})
        cloned = []
        monkeypatch.setattr(remote_mod, 'list_remotes_from_repo',
                            lambda url, **k: cloned.append(url) or [])
        monkeypatch.setattr(credentials.utils, 'get_project_name', lambda: 'p')
        monkeypatch.setattr(credentials.subprocess, 'run',
                            lambda *a, **k: MagicMock(returncode=1, stdout=''))
        monkeypatch.setattr(setup_mod.sys.stdin, 'isatty', lambda: False)
        gate = setup_mod.contact_gate({}, tmp_path / 'cfg', None, {})
        credentials._get_repos_needing_credentials(may_contact=gate)
        assert cloned == []
