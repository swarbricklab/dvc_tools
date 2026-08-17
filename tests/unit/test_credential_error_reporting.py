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

    def test_denial_reaches_stdout_without_verbose(self, capsys):
        with _patched_backend(side_effect=self.DENIED):
            with pytest.raises(AuthError):
                install_credentials(verbose=False, repo_name='bcarc_wts')
        assert 'Cannot read secret' in capsys.readouterr().out

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
        out = capsys.readouterr().out
        assert 'Authenticating to GCP as: sa@x' in out
        assert out.index('Authenticating') < out.index('denied')

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
