"""Tests for the dt auth setup failure modes, and the loop they shared.

All three made `dt auth setup` misreport a working environment as a broken
one, and all three were found together on a single run in a containerised
`module load dt` environment:

1. `gcloud auth list` timed out at 10s on a cold /g/data start, and the
   timeout was reported as "not authenticated" -- sending the user off to
   re-run `gcloud auth login`, which cannot fix a slow filesystem.
2. `ssh-copy-id` is absent from the containerised `module load dt` image, and
   the call was unguarded, so FileNotFoundError escaped the whole SSH phase
   and discarded results for hosts that had already succeeded.
3. The GCP quota project was never attached to the credentials, so every
   Secret Manager call emitted a UserWarning.

Bug 2 was only half-fixable in place: the key-deployment loop existed twice,
in ssh.ssh_setup and setup._do_ssh_setup, so the last section here covers
the single extracted copy both now call.
"""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dt.auth import setup as setup_mod
from dt.auth.ssh import (
    _AUTHORIZED_KEYS_SH,
    _deploy_key_ssh_copy_id,
    _deploy_keys_and_report,
)
from dt.secrets.gcp import GCLOUD_AUTH_TIMEOUT, GCPSecretBackend


# =============================================================================
# 1. gcloud auth status
# =============================================================================

class TestGcloudAuthStatus:
    """A slow gcloud must be distinguishable from an unauthenticated one."""

    @patch('dt.secrets.gcp.shutil.which', return_value=None)
    def test_missing_binary(self, _which):
        assert GCPSecretBackend.gcloud_auth_status() == ('missing', None)

    @patch('dt.secrets.gcp.subprocess.run')
    @patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud')
    def test_active_account(self, _which, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout='u@e.com\n')
        assert GCPSecretBackend.gcloud_auth_status() == ('ok', 'u@e.com')

    @patch('dt.secrets.gcp.subprocess.run')
    @patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud')
    def test_no_active_account(self, _which, mock_run):
        mock_run.return_value = MagicMock(returncode=0, stdout='\n')
        assert GCPSecretBackend.gcloud_auth_status() == ('unauthenticated', None)

    @patch('dt.secrets.gcp.subprocess.run',
           side_effect=subprocess.TimeoutExpired('gcloud', 30))
    @patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud')
    def test_timeout_is_its_own_status(self, _which, _run):
        """The bug: this used to be indistinguishable from 'not logged in'."""
        assert GCPSecretBackend.gcloud_auth_status() == ('timeout', None)

    @patch('dt.secrets.gcp.subprocess.run', side_effect=OSError('boom'))
    @patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud')
    def test_os_error(self, _which, _run):
        assert GCPSecretBackend.gcloud_auth_status() == ('error', None)

    @patch('dt.secrets.gcp.subprocess.run')
    @patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud')
    def test_timeout_budget_is_generous(self, _which, mock_run):
        """Measured cold start on a Gadi ps node was 20.4s; 10s was too tight."""
        mock_run.return_value = MagicMock(returncode=0, stdout='u@e.com\n')
        GCPSecretBackend.gcloud_auth_status()
        assert mock_run.call_args.kwargs['timeout'] == GCLOUD_AUTH_TIMEOUT
        assert GCLOUD_AUTH_TIMEOUT >= 25


class TestGcloudAuthHint:
    """Each failure gets advice that can actually fix it."""

    def test_only_unauthenticated_suggests_login(self):
        for status in ('missing', 'timeout', 'error'):
            assert 'gcloud auth login' not in \
                GCPSecretBackend.gcloud_auth_hint(status)
        assert 'gcloud auth login' in \
            GCPSecretBackend.gcloud_auth_hint('unauthenticated')

    def test_timeout_says_it_is_probably_not_auth(self):
        hint = GCPSecretBackend.gcloud_auth_hint('timeout')
        assert 'did not respond' in hint

    def test_missing_says_install(self):
        assert 'PATH' in GCPSecretBackend.gcloud_auth_hint('missing')


class TestCheckGcloudAuthenticatedStillWorks:
    """The yes/no wrapper keeps its old contract for existing callers."""

    @patch('dt.secrets.gcp.GCPSecretBackend.gcloud_auth_status',
           return_value=('ok', 'u@e.com'))
    def test_ok_returns_account(self, _s):
        assert GCPSecretBackend.check_gcloud_authenticated() == 'u@e.com'

    @pytest.mark.parametrize('status',
                             ['missing', 'timeout', 'error', 'unauthenticated'])
    def test_every_failure_returns_none(self, status):
        with patch('dt.secrets.gcp.GCPSecretBackend.gcloud_auth_status',
                   return_value=(status, None)):
            assert GCPSecretBackend.check_gcloud_authenticated() is None


# =============================================================================
# 2. GCP quota project
# =============================================================================

class TestQuotaProject:
    """The configured project must reach the credentials, not just the path."""

    def test_quota_project_is_attached(self):
        backend = GCPSecretBackend(project='bcarc-489101')
        creds = MagicMock()
        scoped = MagicMock()
        creds.with_quota_project.return_value = scoped

        with patch('google.auth.default', return_value=(creds, 'p')):
            assert backend._credentials() is scoped
        creds.with_quota_project.assert_called_once_with('bcarc-489101')

    def test_service_account_credentials_are_left_alone(self):
        """Only user credentials need a quota project; SA creds bill their own."""
        backend = GCPSecretBackend(project='bcarc-489101')
        creds = MagicMock(spec=[])  # no with_quota_project attribute

        with patch('google.auth.default', return_value=(creds, 'p')):
            assert backend._credentials() is creds

    def test_unresolvable_credentials_fall_back_to_default_lookup(self):
        backend = GCPSecretBackend(project='bcarc-489101')
        with patch('google.auth.default', side_effect=Exception('no ADC')):
            assert backend._credentials() is None

    def test_client_is_built_with_those_credentials(self):
        """The property must actually pass them; building a real client would
        need live credentials, so stand in for the secretmanager module."""
        import google.cloud

        backend = GCPSecretBackend(project='bcarc-489101')
        creds = MagicMock()
        fake_client = MagicMock()
        fake_sm = MagicMock()
        fake_sm.SecretManagerServiceClient.return_value = fake_client

        with patch.object(backend, '_has_adc_credentials', return_value=True), \
             patch.object(backend, '_credentials', return_value=creds), \
             patch.object(google.cloud, 'secretmanager', fake_sm, create=True):
            result = backend.client

        assert result is fake_client
        fake_sm.SecretManagerServiceClient.assert_called_once_with(
            credentials=creds,
        )


# =============================================================================
# 3. ssh-copy-id fallback
# =============================================================================

@pytest.fixture
def pub_key(tmp_path):
    key = tmp_path / 'id_ed25519'
    (tmp_path / 'id_ed25519.pub').write_text('ssh-ed25519 AAAAC3Nz dt@node\n')
    return key


class TestSshCopyIdFallback:
    """The containerised dt image ships ssh but not ssh-copy-id."""

    def test_uses_ssh_copy_id_when_present(self, pub_key):
        with patch('dt.auth.ssh.shutil.which', return_value='/usr/bin/ssh-copy-id'), \
             patch('dt.auth.ssh.subprocess.run',
                   return_value=MagicMock(returncode=0)) as run:
            assert _deploy_key_ssh_copy_id('h', 'u', pub_key) is True
        assert run.call_args.args[0][0] == 'ssh-copy-id'

    def test_falls_back_to_ssh_when_absent(self, pub_key):
        """The actual bug: this used to raise FileNotFoundError."""
        def which(name):
            return None if name == 'ssh-copy-id' else '/usr/bin/ssh'

        with patch('dt.auth.ssh.shutil.which', side_effect=which), \
             patch('dt.auth.ssh.subprocess.run',
                   return_value=MagicMock(returncode=0)) as run:
            assert _deploy_key_ssh_copy_id('h', 'u', pub_key) is True

        argv = run.call_args.args[0]
        assert argv[0] == 'ssh'
        assert argv[1] == 'u@h'
        assert 'authorized_keys' in argv[2]

    def test_key_is_passed_as_an_argument_not_interpolated(self, pub_key):
        """Key content must never be parsed as shell."""
        (pub_key.parent / 'id_ed25519.pub').write_text(
            'ssh-ed25519 AAAA$(rm -rf /) dt@node\n'
        )

        def which(name):
            return None if name == 'ssh-copy-id' else '/usr/bin/ssh'

        with patch('dt.auth.ssh.shutil.which', side_effect=which), \
             patch('dt.auth.ssh.subprocess.run',
                   return_value=MagicMock(returncode=0)) as run:
            _deploy_key_ssh_copy_id('h', 'u', pub_key)

        remote = run.call_args.args[0][2]
        # The script body is quoted as one unit and the key as another, so
        # the substitution can never reach a shell unquoted.
        assert "'ssh-ed25519 AAAA$(rm -rf /) dt@node'" in remote

    def test_returns_false_when_neither_tool_exists(self, pub_key):
        with patch('dt.auth.ssh.shutil.which', return_value=None), \
             patch('dt.auth.ssh.subprocess.run') as run:
            assert _deploy_key_ssh_copy_id('h', 'u', pub_key) is False
        run.assert_not_called()

    def test_returns_false_when_pubkey_unreadable(self, tmp_path):
        with patch('dt.auth.ssh.shutil.which',
                   side_effect=lambda n: None if n == 'ssh-copy-id' else '/usr/bin/ssh'):
            assert _deploy_key_ssh_copy_id(
                'h', 'u', tmp_path / 'missing') is False

    def test_stdin_is_left_free_for_a_password_prompt(self, pub_key):
        """The key isn't deployed yet, so ssh may need to prompt."""
        def which(name):
            return None if name == 'ssh-copy-id' else '/usr/bin/ssh'

        with patch('dt.auth.ssh.shutil.which', side_effect=which), \
             patch('dt.auth.ssh.subprocess.run',
                   return_value=MagicMock(returncode=0)) as run:
            _deploy_key_ssh_copy_id('h', 'u', pub_key)
        assert run.call_args.kwargs['stdin'] is None


class TestAuthorizedKeysScript:
    """The remote script, executed for real against a throwaway HOME."""

    def _run(self, home, key):
        import shlex
        import os
        remote = (f"sh -c {shlex.quote(_AUTHORIZED_KEYS_SH)} "
                  f"{shlex.quote(key)}")
        # Two layers of shell, exactly as ssh does it.
        return subprocess.run(['sh', '-c', remote],
                              env={**os.environ, 'HOME': str(home)})

    def test_appends_key_and_sets_permissions(self, tmp_path):
        key = 'ssh-ed25519 AAAAC3Nz dt@node'
        assert self._run(tmp_path, key).returncode == 0

        keys = tmp_path / '.ssh' / 'authorized_keys'
        assert keys.read_text().splitlines() == [key]
        assert oct(keys.stat().st_mode)[-3:] == '600'
        assert oct(keys.parent.stat().st_mode)[-3:] == '700'

    def test_is_idempotent(self, tmp_path):
        key = 'ssh-ed25519 AAAAC3Nz dt@node'
        self._run(tmp_path, key)
        self._run(tmp_path, key)
        keys = tmp_path / '.ssh' / 'authorized_keys'
        assert len(keys.read_text().splitlines()) == 1

    def test_survives_shell_metacharacters_in_the_key(self, tmp_path):
        key = 'ssh-ed25519 AAAA+slash/and"quote$dollar`tick` dt@node'
        assert self._run(tmp_path, key).returncode == 0
        keys = tmp_path / '.ssh' / 'authorized_keys'
        assert keys.read_text().splitlines() == [key]

    def test_preserves_an_existing_authorized_keys(self, tmp_path):
        ssh_dir = tmp_path / '.ssh'
        ssh_dir.mkdir()
        existing = 'ssh-rsa PREEXISTING other@host'
        (ssh_dir / 'authorized_keys').write_text(existing + '\n')

        key = 'ssh-ed25519 AAAAC3Nz dt@node'
        assert self._run(tmp_path, key).returncode == 0
        assert (ssh_dir / 'authorized_keys').read_text().splitlines() == [
            existing, key,
        ]


# =============================================================================
# 4. The shared deployment loop
# =============================================================================

def _report(hosts, failing, **kw):
    return _deploy_keys_and_report(
        hosts=hosts,
        host_users={h: 'u' for h in hosts},
        failing_hosts=set(failing),
        stanzas_written=kw.pop('stanzas', {h: False for h in hosts}),
        key_path=Path('/tmp/id_ed25519'),
        key_generated=False,
        has_passphrase=kw.pop('passphrase', False),
        verbose=False,
    )


class TestDeployKeysAndReport:
    """One loop, shared by ssh_setup and setup._do_ssh_setup.

    They previously held byte-identical copies, which drifted: a fix applied
    to one left the other broken.
    """

    def test_one_hosts_failure_does_not_lose_the_others(self):
        """The bug that hid a successful GitHub registration."""
        def deploy(host, user, key_path, verbose=False):
            if host == 'bad.example':
                raise FileNotFoundError('ssh-copy-id')
            return True

        with patch('dt.auth.ssh._deploy_key_ssh_copy_id', side_effect=deploy):
            results = _report(
                ['good.example', 'bad.example', 'other.example'],
                failing=['good.example', 'bad.example', 'other.example'],
            )

        by_host = {r.host: r for r in results}
        assert by_host['good.example'].key_deployed is True
        assert by_host['other.example'].key_deployed is True
        assert by_host['bad.example'].key_deployed is False
        assert by_host['bad.example'].manual_action_needed is True

    def test_forge_hosts_use_the_forge_path(self):
        with patch('dt.auth.ssh._deploy_key_forge', return_value=True) as forge, \
             patch('dt.auth.ssh._deploy_key_ssh_copy_id') as copy_id:
            _report(['github.com'], failing=['github.com'])
        forge.assert_called_once()
        copy_id.assert_not_called()

    def test_hosts_needing_nothing_are_omitted(self):
        results = _report(['a.example'], failing=[])
        assert len(results) == 1
        assert results[0].host == '(all)'
        assert results[0].already_ok is True

    def test_config_only_change_is_reported(self):
        results = _report(['a.example'], failing=[],
                          stanzas={'a.example': True})
        assert results[0].host == 'a.example'
        assert results[0].message == 'config stanza added'

    def test_passphrase_warning_lands_on_the_first_result(self):
        with patch('dt.auth.ssh._deploy_key_ssh_copy_id', return_value=True):
            results = _report(['a.example'], failing=['a.example'],
                              passphrase=True)
        assert 'passphrase-protected' in results[0].message


class TestBothEntryPointsShareTheLoop:
    """Guards against the two copies reappearing."""

    def test_setup_imports_the_shared_helper(self):
        assert setup_mod._deploy_keys_and_report is _deploy_keys_and_report

    def test_setup_no_longer_calls_the_deploy_helpers_directly(self):
        """If these come back, the duplication has come back with them."""
        assert not hasattr(setup_mod, '_deploy_key_ssh_copy_id')
        assert not hasattr(setup_mod, '_deploy_key_forge')


class TestQuotaProjectDegradesSafely:
    """Setting a quota project asks GCP to bill it, which needs
    serviceusage.services.use on that project.

    A user who can read the secret but lacks that permission would get
    USER_PROJECT_DENIED -- a 403, i.e. PermissionDenied -- on a call that
    would have worked without the header. That must not be fatal: the
    existing gcloud CLI fallback sends no quota header, so it still works.
    """

    def test_permission_denied_falls_back_to_the_cli(self):
        from google.api_core import exceptions as gcp_exceptions

        backend = GCPSecretBackend(project='bcarc-489101')
        client = MagicMock()
        client.access_secret_version.side_effect = \
            gcp_exceptions.PermissionDenied('USER_PROJECT_DENIED')

        with patch.object(type(backend), 'client', property(lambda s: client)), \
             patch.object(backend, '_cli_access_secret',
                          return_value='[core]\n') as cli:
            assert backend.get_raw_config('visium') == '[core]\n'

        # The cause travels with it, so it survives if the fallback is
        # itself unavailable.
        assert cli.call_args.args[0] == 'visium'
        assert isinstance(cli.call_args.kwargs['cause'],
                          gcp_exceptions.PermissionDenied)
        assert backend._use_cli is True, "should stay on the CLI for the session"


# =============================================================================
# 5. Secret errors must name the project and preserve the real cause
# =============================================================================

class TestSecretErrorsNameTheProject:
    """PERMISSION_DENIED is also what GCP returns for a secret that is simply
    not in the project you asked.

    Secret Manager will not distinguish the two -- doing so would let a denial
    be used to probe for which secrets exist. So "you lack permission" is at
    best half the story, and when secrets.gcp.project points somewhere
    unexpected it is the wrong half: no amount of granting access fixes a
    secret that lives in another project.
    """

    def _denied(self, project='ctp-archive'):
        backend = GCPSecretBackend(project=project)
        result = MagicMock(returncode=1, stdout='',
                           stderr='ERROR: PERMISSION_DENIED: denied on resource')
        with patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud'), \
             patch('dt.secrets.gcp.subprocess.run', return_value=result):
            with pytest.raises(Exception) as exc:
                backend._cli_access_secret('bcarc_visium')
        return str(exc.value)

    def test_names_the_project(self):
        assert "project 'ctp-archive'" in self._denied()

    def test_says_the_secret_may_not_exist_there(self):
        assert 'does not exist there' in self._denied()

    def test_shows_how_to_check_which_project_is_configured(self):
        message = self._denied()
        assert 'dt config get secrets.gcp.project' in message
        assert 'gcloud secrets list --project=ctp-archive' in message

    def test_still_names_the_secret(self):
        assert 'dvc-remote-bcarc_visium' in self._denied()


class TestFallbackPreservesTheRealCause:
    """When the CLI fallback is itself unavailable, report why we needed it."""

    def test_permission_denied_survives_a_missing_gcloud(self):
        """The bug: this reported "package not installed" for a 403.

        The package was installed. The Python client raised PermissionDenied,
        the CLI fallback found no gcloud on the container PATH, and only the
        second failure was reported -- sending the user off to install
        software that was already there.
        """
        from google.api_core import exceptions as gcp_exceptions

        backend = GCPSecretBackend(project='ctp-archive')
        client = MagicMock()
        client.access_secret_version.side_effect = \
            gcp_exceptions.PermissionDenied('403 denied on resource')

        with patch.object(type(backend), 'client', property(lambda s: client)), \
             patch('dt.secrets.gcp.shutil.which', return_value=None):
            with pytest.raises(Exception) as exc:
                backend.get_raw_config('bcarc_visium')

        message = str(exc.value)
        assert '403 denied on resource' in message, "real cause was discarded"
        assert 'gcloud is not on PATH' in message
        assert 'Original error' in message

    def test_plain_missing_gcloud_still_reads_simply(self):
        """With no prior failure, the old install advice is still right."""
        backend = GCPSecretBackend(project='ctp-archive')
        with patch('dt.secrets.gcp.shutil.which', return_value=None):
            with pytest.raises(Exception) as exc:
                backend._require_gcloud()
        message = str(exc.value)
        assert 'Neither the google-cloud-secret-manager' in message
        assert 'Original error' not in message
