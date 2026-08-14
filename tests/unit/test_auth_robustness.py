"""Tests for three dt auth setup failure modes.

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
"""

import subprocess
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

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

from dt.auth.ssh import _AUTHORIZED_KEYS_SH, _deploy_key_ssh_copy_id


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
