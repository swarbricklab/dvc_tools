"""Failure reasons and the GCP identity must survive without ``-v``.

A collaborator hit `dt auth credentials install bcarc_wts` and got exactly
one line back:

    Error: No credentials were installed (no usable secrets found).

Five distinct causes collapse into that sentence -- denied, absent, empty,
legacy-format, unrecognised-format -- and every one of them was printed only
under ``--verbose``. The reason was known and thrown away.

Worse, a denial is uninterpretable on its own. GCP returns PERMISSION_DENIED
for secrets that do not exist, and dt may be authenticating as a service
account picked up from GOOGLE_APPLICATION_CREDENTIALS rather than as the
human, in which case "I already granted her access" and "permission denied"
are simultaneously true. So the identity has to be reported too, every time,
not just when someone remembers to ask for it.
"""

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dt.auth import credentials as creds_mod
from dt.auth.credentials import AuthError, install_credentials
from dt.auth.setup import SetupReport, format_setup_report
from dt.secrets import GCPIdentity, GCPSecretBackend, SecretError


SA_JSON = {
    'type': 'service_account',
    'client_email': 'ctp-archive@ctp-archive.iam.gserviceaccount.com',
    'project_id': 'ctp-archive',
}
USER_JSON = {
    'type': 'authorized_user',
    'client_id': '764086051850-abc.apps.googleusercontent.com',
    'quota_project_id': 'bcarc-489101',
}


def _write(path: Path, payload) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload))
    return path


def _backend(project='bcarc-489101'):
    """A real backend whose identity is pre-resolved, so nothing shells out."""
    b = GCPSecretBackend(project=project)
    b._identity = GCPIdentity(kind='cli', source='gcloud CLI', account='her@example.org')
    return b


# =============================================================================
# Identity description
# =============================================================================

class TestGCPIdentityDescribe:

    def test_service_account_is_named(self):
        i = GCPIdentity(kind='service-account', source='$GOOGLE_APPLICATION_CREDENTIALS=/k.json',
                        account='sa@p.iam.gserviceaccount.com')
        assert 'sa@p.iam.gserviceaccount.com' in i.describe()

    def test_service_account_says_where_it_came_from(self):
        i = GCPIdentity(kind='service-account', source='$GOOGLE_APPLICATION_CREDENTIALS=/k.json',
                        account='sa@p.iam.gserviceaccount.com')
        assert '/k.json' in i.describe()

    def test_user_adc_admits_the_email_is_unknown(self):
        """The ADC file genuinely does not record it; do not invent one."""
        i = GCPIdentity(kind='user', source='ADC file /h/adc.json')
        described = i.describe()
        assert 'does not record' in described
        assert 'ADC file /h/adc.json' in described

    def test_user_with_known_account_is_named(self):
        i = GCPIdentity(kind='user', source='ADC file /h/adc.json', account='her@example.org')
        assert 'her@example.org' in i.describe()

    def test_cli_identity_names_the_mechanism(self):
        i = GCPIdentity(kind='cli', source='gcloud CLI', account='her@example.org')
        assert 'her@example.org' in i.describe()
        assert 'gcloud CLI' in i.describe()

    def test_absent_credentials_carry_the_hint(self):
        i = GCPIdentity(kind='none', source='No active GCP account. Run ...')
        assert 'No active GCP account' in i.describe()


class TestGCPIdentityCaveat:
    """Only the service-account case is a trap worth warning about."""

    def test_service_account_warns_grants_do_not_apply(self):
        i = GCPIdentity(kind='service-account', source='ADC file /h/adc.json', account='sa@x')
        assert 'do not apply' in i.caveat

    def test_env_var_source_suggests_unsetting_it(self):
        i = GCPIdentity(kind='service-account',
                        source='$GOOGLE_APPLICATION_CREDENTIALS=/k.json', account='sa@x')
        assert 'Unset GOOGLE_APPLICATION_CREDENTIALS' in i.caveat

    def test_non_env_source_does_not_suggest_unsetting(self):
        i = GCPIdentity(kind='service-account', source='ADC file /h/adc.json', account='sa@x')
        assert 'Unset GOOGLE_APPLICATION_CREDENTIALS' not in i.caveat

    @pytest.mark.parametrize('kind', ['user', 'cli', 'none'])
    def test_other_kinds_have_no_caveat(self, kind):
        assert GCPIdentity(kind=kind, source='x').caveat is None


# =============================================================================
# Identity resolution -- must mirror real ADC precedence
# =============================================================================

class TestActiveIdentityResolutionOrder:

    def test_env_var_wins_over_adc_file(self, tmp_path, monkeypatch):
        gac = _write(tmp_path / 'sa.json', SA_JSON)
        adc = _write(tmp_path / 'home' / '.config' / 'gcloud'
                     / 'application_default_credentials.json', USER_JSON)
        assert adc.exists()
        monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', str(gac))
        monkeypatch.setattr(Path, 'home', staticmethod(lambda: tmp_path / 'home'))

        i = GCPSecretBackend.active_identity()
        assert i.kind == 'service-account'
        assert i.account == SA_JSON['client_email']

    def test_adc_file_used_when_env_var_absent(self, tmp_path, monkeypatch):
        _write(tmp_path / 'home' / '.config' / 'gcloud'
               / 'application_default_credentials.json', USER_JSON)
        monkeypatch.delenv('GOOGLE_APPLICATION_CREDENTIALS', raising=False)
        monkeypatch.setattr(Path, 'home', staticmethod(lambda: tmp_path / 'home'))

        i = GCPSecretBackend.active_identity()
        assert i.kind == 'user'
        assert i.quota_project == 'bcarc-489101'

    def test_env_var_pointing_at_nothing_is_ignored(self, tmp_path, monkeypatch):
        _write(tmp_path / 'home' / '.config' / 'gcloud'
               / 'application_default_credentials.json', USER_JSON)
        monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', str(tmp_path / 'gone.json'))
        monkeypatch.setattr(Path, 'home', staticmethod(lambda: tmp_path / 'home'))

        assert GCPSecretBackend.active_identity().kind == 'user'

    def test_falls_through_to_cli_account(self, tmp_path, monkeypatch):
        monkeypatch.delenv('GOOGLE_APPLICATION_CREDENTIALS', raising=False)
        monkeypatch.setattr(Path, 'home', staticmethod(lambda: tmp_path / 'empty'))

        i = GCPSecretBackend.active_identity(cli_account='her@example.org')
        assert i.kind == 'cli'
        assert i.account == 'her@example.org'

    def test_supplied_cli_account_avoids_shelling_out(self, tmp_path, monkeypatch):
        """gcloud auth list costs ~20s cold on NCI; never pay it twice."""
        monkeypatch.delenv('GOOGLE_APPLICATION_CREDENTIALS', raising=False)
        monkeypatch.setattr(Path, 'home', staticmethod(lambda: tmp_path / 'empty'))

        with patch.object(GCPSecretBackend, 'gcloud_auth_status') as status:
            GCPSecretBackend.active_identity(cli_account='her@example.org')
        status.assert_not_called()

    def test_no_credentials_reports_the_gcloud_hint(self, tmp_path, monkeypatch):
        monkeypatch.delenv('GOOGLE_APPLICATION_CREDENTIALS', raising=False)
        monkeypatch.setattr(Path, 'home', staticmethod(lambda: tmp_path / 'empty'))

        with patch.object(GCPSecretBackend, 'gcloud_auth_status',
                          return_value=('unauthenticated', None)):
            i = GCPSecretBackend.active_identity()
        assert i.kind == 'none'
        assert 'gcloud auth login' in i.describe()


class TestIdentityFromFile:

    def test_malformed_json_still_reports_the_source(self, tmp_path):
        bad = tmp_path / 'bad.json'
        bad.write_text('{not json')
        i = GCPSecretBackend._identity_from_file(bad, 'ADC file bad.json')
        assert 'bad.json' in i.source
        assert 'unreadable' in i.source

    def test_memoised_per_instance(self, tmp_path, monkeypatch):
        monkeypatch.delenv('GOOGLE_APPLICATION_CREDENTIALS', raising=False)
        monkeypatch.setattr(Path, 'home', staticmethod(lambda: tmp_path / 'empty'))
        b = GCPSecretBackend(project='p')
        b._cli_account = 'her@example.org'

        with patch.object(GCPSecretBackend, 'active_identity',
                          wraps=GCPSecretBackend.active_identity) as spy:
            b.identity()
            b.identity()
        assert spy.call_count == 1


class TestBackendCarriesTheAuthCheckResult:
    """_get_secret_backend already paid for gcloud; hand the answer over."""

    def test_cli_account_is_passed_to_the_backend(self):
        with patch('dt.config.get_value', side_effect=lambda k, *a: {
                    'secrets.backend': 'gcp',
                    'secrets.gcp.project': 'bcarc-489101',
                   }.get(k)), \
             patch.object(GCPSecretBackend, '_has_adc_credentials', return_value=False), \
             patch.object(GCPSecretBackend, 'gcloud_auth_status',
                          return_value=('ok', 'her@example.org')):
            backend = creds_mod._get_secret_backend()
        assert backend._cli_account == 'her@example.org'

    def test_adc_path_leaves_cli_account_unset(self):
        with patch('dt.config.get_value', side_effect=lambda k, *a: {
                    'secrets.backend': 'gcp',
                    'secrets.gcp.project': 'bcarc-489101',
                   }.get(k)), \
             patch.object(GCPSecretBackend, '_has_adc_credentials', return_value=True):
            backend = creds_mod._get_secret_backend()
        assert backend._cli_account is None


# =============================================================================
# describe_secret_identity
# =============================================================================

class TestDescribeSecretIdentity:

    def test_names_identity_and_project(self):
        text = creds_mod.describe_secret_identity(_backend())
        assert 'her@example.org' in text
        assert 'bcarc-489101' in text

    def test_includes_the_service_account_caveat(self):
        b = GCPSecretBackend(project='bcarc-489101')
        b._identity = GCPIdentity(
            kind='service-account', account='sa@x.iam.gserviceaccount.com',
            source='$GOOGLE_APPLICATION_CREDENTIALS=/k.json')
        assert 'do not apply' in creds_mod.describe_secret_identity(b)

    def test_non_gcp_backend_yields_nothing(self):
        assert creds_mod.describe_secret_identity(object()) is None


# =============================================================================
# install_credentials -- the reason must reach a user who did not pass -v
# =============================================================================

def _patched_backend(**kwargs):
    """Patch _get_secret_backend to return a mock with the given behaviour."""
    backend = MagicMock()
    for key, value in kwargs.items():
        setattr(backend.get_raw_config, key, value)
    return patch.object(creds_mod, '_get_secret_backend', return_value=backend)


class TestFailureReasonsAreNotVerboseOnly:

    DENIED = SecretError("Cannot read secret 'dvc-remote-bcarc_wts' in project 'x'.")

    def test_denial_is_reported_without_verbose(self, capsys):
        """On stderr, so it survives the output being piped elsewhere."""
        with _patched_backend(side_effect=self.DENIED):
            with pytest.raises(AuthError):
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'Cannot read secret' in capsys.readouterr().err

    def test_denial_is_in_the_exception_too(self):
        """stdout gets piped away and pasted selectively; the error must stand alone."""
        with _patched_backend(side_effect=self.DENIED):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'Cannot read secret' in str(exc.value)

    def test_exception_names_the_repo(self):
        with _patched_backend(side_effect=self.DENIED):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'bcarc_wts' in str(exc.value)

    def test_old_uninformative_wording_is_gone(self):
        with _patched_backend(side_effect=self.DENIED):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'no usable secrets found' not in str(exc.value)

    def test_empty_secret_says_empty(self):
        with _patched_backend(return_value='   '):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'empty' in str(exc.value)

    def test_unrecognised_format_says_so(self):
        with _patched_backend(return_value='just some prose, not an ini file'):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'recognised' in str(exc.value)

    def test_legacy_format_keeps_its_own_migrate_hint(self):
        legacy = "['remote \"storage\"']\n    access_key_id = AK\n    secret_access_key = SK\n"
        with _patched_backend(return_value=legacy):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'migrate' in str(exc.value)

    def test_multiline_reasons_are_indented(self):
        multi = SecretError("Cannot read secret 'x'.\nEither you lack access, or it is absent.")
        with _patched_backend(side_effect=multi):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert '    Either you lack access' in str(exc.value)


class TestIdentityIsReportedOnEveryFetch:

    def test_identity_printed_before_any_failure(self, capsys):
        with _patched_backend(side_effect=SecretError('denied')), \
             patch.object(creds_mod, 'describe_secret_identity',
                          return_value='Authenticating to GCP as: sa@x'):
            with pytest.raises(AuthError):
                install_credentials(verbose=False, repo_name='bcarc_wts')
        captured = capsys.readouterr()
        assert 'Authenticating to GCP as: sa@x' in captured.out
        assert 'denied' in captured.err

    def test_identity_printed_on_success_too(self, capsys):
        valid = '[bcarc_wts]\naws_access_key_id = AK\naws_secret_access_key = SK\n'
        with _patched_backend(return_value=valid), \
             patch.object(creds_mod, 'describe_secret_identity',
                          return_value='Authenticating to GCP as: her@example.org'), \
             patch.object(creds_mod, '_install_aws_profile',
                          return_value=(Path('/c'), Path('/g'))), \
             patch.object(creds_mod, '_get_project_s3_remotes', return_value={}):
            install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'her@example.org' in capsys.readouterr().out

    def test_identity_is_repeated_in_the_exception(self):
        with _patched_backend(side_effect=SecretError('denied')), \
             patch.object(creds_mod, 'describe_secret_identity',
                          return_value='Authenticating to GCP as: sa@x'):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'sa@x' in str(exc.value)


# =============================================================================
# Permission denials must be echoed, even when the fallback rescues them
# =============================================================================

class TestDenialsAreEchoed:
    """A silently-retried denial is a denial nobody learns from.

    The gcloud CLI fallback frequently succeeds, so the Python client can be
    refused on every call while everything appears to work -- right up until
    the same credentials are used where no fallback exists (a container with
    no gcloud), where it surfaces as a mystery.
    """

    def _denied_backend(self, tmp_path, monkeypatch, sa=True):
        gac = _write(tmp_path / 'key.json', SA_JSON if sa else USER_JSON)
        monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', str(gac))
        b = GCPSecretBackend(project='bcarc-489101')
        b._cli_account = 'her@example.org'
        return b

    def _permission_denied(self):
        from google.api_core import exceptions as gcp_exceptions
        return gcp_exceptions.PermissionDenied('denied by test')

    def test_library_denial_is_echoed_even_when_fallback_succeeds(
            self, tmp_path, monkeypatch, capsys):
        b = self._denied_backend(tmp_path, monkeypatch)
        client = MagicMock()
        client.access_secret_version.side_effect = self._permission_denied()
        with patch.object(type(b), 'client', property(lambda _self: client)), \
             patch.object(b, '_cli_access_secret', return_value='[r]\nk = v\n'):
            b.get_raw_config('bcarc_wts')

        err = capsys.readouterr().err
        assert 'Permission denied' in err
        assert 'dvc-remote-bcarc_wts' in err
        assert 'bcarc-489101' in err

    def test_echo_names_the_refused_identity(self, tmp_path, monkeypatch, capsys):
        b = self._denied_backend(tmp_path, monkeypatch)
        client = MagicMock()
        client.access_secret_version.side_effect = self._permission_denied()
        with patch.object(type(b), 'client', property(lambda _self: client)), \
             patch.object(b, '_cli_access_secret', return_value='x'):
            b.get_raw_config('bcarc_wts')
        assert SA_JSON['client_email'] in capsys.readouterr().err

    def test_echo_carries_the_service_account_caveat(self, tmp_path, monkeypatch, capsys):
        b = self._denied_backend(tmp_path, monkeypatch)
        client = MagicMock()
        client.access_secret_version.side_effect = self._permission_denied()
        with patch.object(type(b), 'client', property(lambda _self: client)), \
             patch.object(b, '_cli_access_secret', return_value='x'):
            b.get_raw_config('bcarc_wts')
        assert 'do not apply' in capsys.readouterr().err

    def test_echo_says_it_is_retrying_elsewhere(self, tmp_path, monkeypatch, capsys):
        b = self._denied_backend(tmp_path, monkeypatch)
        client = MagicMock()
        client.access_secret_version.side_effect = self._permission_denied()
        with patch.object(type(b), 'client', property(lambda _self: client)), \
             patch.object(b, '_cli_access_secret', return_value='x'):
            b.get_raw_config('bcarc_wts')
        assert 'retrying via the gcloud CLI' in capsys.readouterr().err

    def test_prior_identity_reaches_the_cli_call(self, tmp_path, monkeypatch):
        """So a second denial can name both accounts."""
        b = self._denied_backend(tmp_path, monkeypatch)
        client = MagicMock()
        client.access_secret_version.side_effect = self._permission_denied()
        with patch.object(type(b), 'client', property(lambda _self: client)), \
             patch.object(b, '_cli_access_secret', return_value='x') as cli:
            b.get_raw_config('bcarc_wts')
        assert SA_JSON['client_email'] in cli.call_args.kwargs['prior_identity']


class TestBothIdentitiesRefused:
    """When the fallback is refused too, name both -- not just the last."""

    def _run(self, tmp_path, monkeypatch):
        _write(tmp_path / 'key.json', SA_JSON)
        monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', str(tmp_path / 'key.json'))
        b = GCPSecretBackend(project='bcarc-489101')
        b._cli_account = 'her@example.org'
        denied = MagicMock(returncode=1, stderr='PERMISSION_DENIED: nope', stdout='')
        with patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud'), \
             patch('dt.secrets.gcp.subprocess.run', return_value=denied):
            with pytest.raises(SecretError) as exc:
                b._cli_access_secret('bcarc_wts',
                                     prior_identity='service account sa@x (from env)')
        return str(exc.value)

    def test_names_the_cli_identity(self, tmp_path, monkeypatch):
        assert 'her@example.org' in self._run(tmp_path, monkeypatch)

    def test_also_names_the_library_identity(self, tmp_path, monkeypatch):
        msg = self._run(tmp_path, monkeypatch)
        assert 'Also refused for' in msg
        assert 'sa@x' in msg

    def test_keeps_the_ambiguity_warning(self, tmp_path, monkeypatch):
        """PERMISSION_DENIED also means 'absent'; do not assert it is permissions."""
        assert 'does not exist there' in self._run(tmp_path, monkeypatch)

    def test_keeps_the_raw_gcloud_error(self, tmp_path, monkeypatch):
        assert 'PERMISSION_DENIED: nope' in self._run(tmp_path, monkeypatch)

    def test_suggests_checking_the_project(self, tmp_path, monkeypatch):
        assert 'dt config get secrets.gcp.project' in self._run(tmp_path, monkeypatch)


class TestCliIdentityDescription:

    def test_cached_account_avoids_shelling_out(self):
        b = GCPSecretBackend(project='p')
        b._cli_account = 'her@example.org'
        with patch.object(GCPSecretBackend, 'gcloud_auth_status') as status:
            desc = b._cli_identity_desc()
        status.assert_not_called()
        assert 'her@example.org' in desc

    def test_resolves_and_caches_when_unknown(self):
        b = GCPSecretBackend(project='p')
        with patch.object(GCPSecretBackend, 'gcloud_auth_status',
                          return_value=('ok', 'him@example.org')) as status:
            b._cli_identity_desc()
            b._cli_identity_desc()
        assert status.call_count == 1
        assert b._cli_account == 'him@example.org'

    def test_degrades_without_claiming_an_account(self):
        b = GCPSecretBackend(project='p')
        with patch.object(GCPSecretBackend, 'gcloud_auth_status',
                          return_value=('unauthenticated', None)):
            assert b._cli_identity_desc() == 'the gcloud CLI account'


class TestNotFoundIsNotADenial:

    def test_missing_secret_does_not_echo_a_denial(self, capsys):
        b = GCPSecretBackend(project='bcarc-489101')
        missing = MagicMock(returncode=1, stderr='NOT_FOUND: no such secret', stdout='')
        with patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud'), \
             patch('dt.secrets.gcp.subprocess.run', return_value=missing):
            with pytest.raises(SecretError, match='not found'):
                b._cli_access_secret('bcarc_wts')
        assert 'Permission denied' not in capsys.readouterr().err


class TestDiagnosticsGoToStderr:
    """Diagnostics must survive `dt ... > file` and `| head`."""

    def test_denial_echo_is_not_on_stdout(self, tmp_path, monkeypatch, capsys):
        _write(tmp_path / 'key.json', SA_JSON)
        monkeypatch.setenv('GOOGLE_APPLICATION_CREDENTIALS', str(tmp_path / 'key.json'))
        b = GCPSecretBackend(project='bcarc-489101')
        b._cli_account = 'her@example.org'
        from google.api_core import exceptions as gcp_exceptions
        client = MagicMock()
        client.access_secret_version.side_effect = gcp_exceptions.PermissionDenied('no')
        with patch.object(type(b), 'client', property(lambda _s: client)), \
             patch.object(b, '_cli_access_secret', return_value='x'):
            b.get_raw_config('bcarc_wts')
        assert 'Permission denied' not in capsys.readouterr().out

    def test_failure_reason_is_not_on_stdout(self, capsys):
        with _patched_backend(side_effect=SecretError('denied for reasons')):
            with pytest.raises(AuthError):
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'denied for reasons' not in capsys.readouterr().out


# =============================================================================
# `dt auth credentials list` -- the "what CAN I read?" escape hatch
# =============================================================================

class TestListIsSuggestedOnFailure:
    """Listing catches what a denial message structurally cannot.

    PERMISSION_DENIED cannot distinguish "you lack access" from "it is not
    here". A secret named differently from the repo, or a typo, looks
    identical to a missing grant -- unless you list what is actually visible.
    """

    def test_summary_error_suggests_listing(self):
        with _patched_backend(side_effect=SecretError('denied')):
            with pytest.raises(AuthError) as exc:
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'dt auth credentials list' in str(exc.value)

    def test_denied_message_suggests_listing(self):
        b = GCPSecretBackend(project='bcarc-489101')
        assert 'dt auth credentials list' in b._denied_message('dvc-remote-x', 'detail')

    def test_not_found_suggests_listing(self):
        b = GCPSecretBackend(project='bcarc-489101')
        missing = MagicMock(returncode=1, stderr='NOT_FOUND: nope', stdout='')
        with patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud'), \
             patch('dt.secrets.gcp.subprocess.run', return_value=missing):
            with pytest.raises(SecretError) as exc:
                b._cli_access_secret('bcarc_wts')
        assert 'dt auth credentials list' in str(exc.value)


class TestListSecretsErrorsAreReported:
    """`list` had a bare `gcloud error listing secrets: <stderr>`."""

    def _list_failing(self, stderr):
        b = GCPSecretBackend(project='bcarc-489101')
        b._cli_account = 'her@example.org'
        failed = MagicMock(returncode=1, stderr=stderr, stdout='')
        with patch('dt.secrets.gcp.shutil.which', return_value='/usr/bin/gcloud'), \
             patch('dt.secrets.gcp.subprocess.run', return_value=failed):
            with pytest.raises(SecretError) as exc:
                b.list_secrets()
        return str(exc.value)

    def test_denial_names_the_project(self):
        assert "bcarc-489101" in self._list_failing('PERMISSION_DENIED: nope')

    def test_denial_names_the_identity(self):
        assert 'her@example.org' in self._list_failing('PERMISSION_DENIED: nope')

    def test_denial_explains_list_is_a_separate_grant(self):
        """Being able to read one secret does not imply being able to list."""
        assert 'secretmanager.secrets.list' in self._list_failing('PERMISSION_DENIED: x')

    def test_denial_is_echoed(self, capsys):
        self._list_failing('PERMISSION_DENIED: nope')
        assert 'Permission denied' in capsys.readouterr().err

    def test_other_errors_still_name_the_project(self):
        msg = self._list_failing('some other gcloud failure')
        assert 'bcarc-489101' in msg
        assert 'some other gcloud failure' in msg


class TestCredentialsCliCommands:

    def _invoke(self, args):
        from click.testing import CliRunner
        from dt.cli import cli
        return CliRunner().invoke(cli, args)

    def test_list_names_the_project(self):
        with patch('dt.auth.list_repo_secrets', return_value=['bcarc_wts']), \
             patch('dt.config.get_value', return_value='bcarc-489101'):
            result = self._invoke(['auth', 'credentials', 'list'])
        assert 'bcarc-489101' in result.output

    def test_empty_list_names_the_project_too(self):
        with patch('dt.auth.list_repo_secrets', return_value=[]), \
             patch('dt.config.get_value', return_value='bcarc-489101'):
            result = self._invoke(['auth', 'credentials', 'list'])
        assert 'bcarc-489101' in result.output

    def test_empty_list_admits_it_may_be_visibility(self):
        with patch('dt.auth.list_repo_secrets', return_value=[]), \
             patch('dt.config.get_value', return_value='bcarc-489101'):
            result = self._invoke(['auth', 'credentials', 'list'])
        assert 'cannot see them' in result.output

    def test_list_error_reaches_the_user(self):
        with patch('dt.auth.list_repo_secrets',
                   side_effect=AuthError('Cannot list secrets in project X')):
            result = self._invoke(['auth', 'credentials', 'list'])
        assert result.exit_code != 0
        assert 'Cannot list secrets' in result.output

    def test_check_does_not_call_a_denial_not_found(self):
        from dt.auth.credentials import SecretInfo
        info = SecretInfo(repo_name='bcarc_wts', exists=False, accessible=False,
                          error='Cannot read secret ... PERMISSION_DENIED')
        with patch('dt.auth.check_secret', return_value=info):
            result = self._invoke(['auth', 'credentials', 'check', 'bcarc_wts'])
        assert 'could not confirm' in result.output
        assert 'secret not found' not in result.output

    def test_check_genuine_absence_still_says_not_found(self):
        from dt.auth.credentials import SecretInfo
        info = SecretInfo(repo_name='bcarc_wts', exists=False, accessible=False, error=None)
        with patch('dt.auth.check_secret', return_value=info):
            result = self._invoke(['auth', 'credentials', 'check', 'bcarc_wts'])
        assert 'secret not found' in result.output
        assert 'dt auth credentials list' in result.output

    def test_install_partial_failure_is_not_called_not_found(self):
        with patch('dt.auth.install_credentials',
                   return_value={'a': True, 'b': False}):
            result = self._invoke(['auth', 'credentials', 'install'])
        assert 'Not found' not in result.output
        assert 'Failed: b' in result.output


# =============================================================================
# setup report -- stop claiming there are no S3 endpoints when there are
# =============================================================================

class TestSetupReportSkipReason:

    def test_auth_failure_does_not_claim_there_are_no_endpoints(self):
        report = SetupReport(
            skipped_credentials=True,
            credentials_skip_reason='GCP authentication unavailable — see the error below',
            errors=['GCP authentication unavailable. No active GCP account.'],
        )
        out = format_setup_report(report)
        assert 'No S3 endpoints' not in out
        assert 'GCP authentication unavailable' in out

    def test_genuinely_absent_endpoints_still_say_so(self):
        out = format_setup_report(SetupReport(skipped_credentials=True))
        assert 'No S3 endpoints' in out

    def test_errors_are_still_rendered(self):
        out = format_setup_report(SetupReport(errors=['boom']))
        assert 'boom' in out
