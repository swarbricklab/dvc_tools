"""Unit tests for the AWS-shared-files credential layout.

Covers the refactor of ``dt.auth.credentials`` that moved credentials out
of ``~/.config/dvc/config`` and into ``~/.aws/credentials`` /
``~/.aws/config``, plus the new ``configure_remotes`` and
``migrate_credentials`` commands.
"""

import configparser
import io
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from dt.auth import AuthError
from dt.auth.credentials import (
    ConfigureRemotesResult,
    MigrateResult,
    RepoCredentialInfo,
    _build_aws_secret,
    _detect_secret_format,
    _extract_creds_from_dvc_secret,
    _format_dvc_ini,
    _install_aws_profile,
    _list_aws_profiles,
    _make_aws_parser,
    _parse_aws_secret,
    _remove_aws_profile,
    _strip_dvc_global_creds,
    configure_remotes,
    format_credentials_status,
    get_credentials_status,
    install_credentials,
    migrate_credentials,
    uninstall_credentials,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def aws_dir(tmp_path, monkeypatch):
    """Redirect ~/.aws/* to a temp dir via env vars."""
    creds = tmp_path / 'credentials'
    cfg = tmp_path / 'config'
    monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', str(creds))
    monkeypatch.setenv('AWS_CONFIG_FILE', str(cfg))
    return tmp_path


@pytest.fixture
def fake_dvc_global(tmp_path, monkeypatch):
    """Point ``_get_dvc_global_config_path`` at a temp file."""
    path = tmp_path / 'dvc_global_config'
    monkeypatch.setattr(
        'dt.auth.credentials._get_dvc_global_config_path',
        lambda: path,
    )
    return path


def _write_global_dvc(path: Path, remotes: dict) -> None:
    """``remotes = {name: {key: value, ...}}`` -> DVC INI on disk."""
    sections = {f"'remote \"{name}\"'": dict(values)
                for name, values in remotes.items()}
    path.write_text(_format_dvc_ini(sections))


# ---------------------------------------------------------------------------
# Format detection
# ---------------------------------------------------------------------------

class TestDetectSecretFormat:

    def test_aws_format(self):
        content = (
            "[example_wts]\n"
            "aws_access_key_id = AKIA\n"
            "aws_secret_access_key = SECRET\n"
        )
        assert _detect_secret_format(content) == 'aws'

    def test_legacy_dvc_format(self):
        content = (
            "['remote \"example-wts\"']\n"
            "    url = s3://demo-wts\n"
            "    access_key_id = AKIA\n"
            "    secret_access_key = SECRET\n"
        )
        assert _detect_secret_format(content) == 'dvc'

    def test_unknown(self):
        assert _detect_secret_format("# nothing here\n") == 'unknown'
        assert _detect_secret_format("") == 'unknown'


class TestParseAwsSecret:

    def test_single_section(self):
        content = "[example_wts]\naws_access_key_id = A\naws_secret_access_key = B\n"
        assert _parse_aws_secret(content, 'example_wts') == ('A', 'B')

    def test_multiple_sections_same_keys_ok(self):
        content = (
            "[a]\naws_access_key_id = A\naws_secret_access_key = B\n"
            "[b]\naws_access_key_id = A\naws_secret_access_key = B\n"
        )
        assert _parse_aws_secret(content, 'x') == ('A', 'B')

    def test_multiple_sections_different_keys_fails(self):
        content = (
            "[a]\naws_access_key_id = A1\naws_secret_access_key = B\n"
            "[b]\naws_access_key_id = A2\naws_secret_access_key = B\n"
        )
        with pytest.raises(AuthError, match="distinct"):
            _parse_aws_secret(content, 'x')

    def test_no_keys_fails(self):
        with pytest.raises(AuthError, match="no aws_access_key_id"):
            _parse_aws_secret("[only_section]\nfoo = bar\n", 'x')


class TestExtractCredsFromDvcSecret:

    def test_single_remote(self):
        content = (
            "['remote \"example-wts\"']\n"
            "    url = s3://demo-wts\n"
            "    access_key_id = A\n"
            "    secret_access_key = B\n"
        )
        assert _extract_creds_from_dvc_secret(content, 'example_wts') == ('A', 'B')

    def test_multiple_remotes_same_keys_ok(self):
        content = (
            "['remote \"r1\"']\n    access_key_id = A\n    secret_access_key = B\n"
            "['remote \"r2\"']\n    access_key_id = A\n    secret_access_key = B\n"
        )
        assert _extract_creds_from_dvc_secret(content, 'x') == ('A', 'B')

    def test_multiple_remotes_diff_keys_fails(self):
        content = (
            "['remote \"r1\"']\n    access_key_id = A1\n    secret_access_key = B\n"
            "['remote \"r2\"']\n    access_key_id = A2\n    secret_access_key = B\n"
        )
        with pytest.raises(AuthError, match="distinct"):
            _extract_creds_from_dvc_secret(content, 'x')


class TestBuildAwsSecret:

    def test_roundtrip(self):
        out = _build_aws_secret('myrepo', 'AKIA123', 'shh')
        assert _detect_secret_format(out) == 'aws'
        assert _parse_aws_secret(out, 'myrepo') == ('AKIA123', 'shh')


# ---------------------------------------------------------------------------
# AWS file helpers
# ---------------------------------------------------------------------------

class TestInstallAwsProfile:

    def test_writes_credentials_and_config(self, aws_dir):
        creds_p, cfg_p = _install_aws_profile('myrepo', 'AKIA', 'SECRET')
        assert creds_p.exists()
        assert cfg_p.exists()
        # 600 perms
        assert (creds_p.stat().st_mode & 0o777) == 0o600

        creds = _make_aws_parser()
        creds.read(creds_p)
        assert creds.get('myrepo', 'aws_access_key_id') == 'AKIA'
        assert creds.get('myrepo', 'aws_secret_access_key') == 'SECRET'

        cfg = _make_aws_parser()
        cfg.read(cfg_p)
        assert cfg.has_section('profile myrepo')
        assert cfg.get('profile myrepo', 'region') == 'auto'

    def test_default_profile_is_never_written(self, aws_dir):
        with pytest.raises(AuthError, match='cannot be used'):
            _install_aws_profile('default', 'AKIA', 'SECRET')
        assert not (aws_dir / 'credentials').exists()

    @pytest.mark.parametrize('name', ['', '-x', 'a b', 'a]\n[b', 'x/y'])
    def test_unusable_profile_names_refused(self, aws_dir, name):
        with pytest.raises(AuthError):
            _install_aws_profile(name, 'AKIA', 'SECRET')

    def test_existing_profile_from_elsewhere_is_left_alone(self, aws_dir):
        (aws_dir / 'credentials').write_text(
            '[myrepo]\naws_access_key_id = MINE\naws_secret_access_key = S\n')
        with pytest.raises(AuthError, match='left unchanged'):
            _install_aws_profile('myrepo', 'AKIA', 'SECRET', source='gcp:p/dvc-remote-myrepo')
        assert 'MINE' in (aws_dir / 'credentials').read_text()

    def test_profile_from_same_source_is_updated(self, aws_dir):
        src = 'gcp:p/dvc-remote-myrepo'
        _install_aws_profile('myrepo', 'OLD', 'S', source=src)
        _install_aws_profile('myrepo', 'NEW', 'S', source=src)
        text = (aws_dir / 'credentials').read_text()
        assert 'NEW' in text and 'OLD' not in text
        with pytest.raises(AuthError):
            _install_aws_profile('myrepo', 'X', 'S', source='gcp:other/dvc-remote-myrepo')

    def test_replace_overwrites(self, aws_dir):
        (aws_dir / 'credentials').write_text(
            '[myrepo]\naws_access_key_id = MINE\naws_secret_access_key = S\n')
        _install_aws_profile('myrepo', 'AKIA', 'SECRET', source='gcp:p/x', replace=True)
        assert 'AKIA' in (aws_dir / 'credentials').read_text()

    def test_discovered_removal_needs_matching_source(self, aws_dir):
        _install_aws_profile('myrepo', 'A', 'B', source='gcp:p/x')
        assert _remove_aws_profile('myrepo', source='gcp:q/x') == []
        assert _remove_aws_profile('myrepo', source='gcp:p/x')

    def test_existing_profile_preserves_other_keys(self, aws_dir):
        # Pre-seed config with extra keys
        cfg_p = aws_dir / 'config'
        cfg_p.write_text(
            "[profile myrepo]\n"
            "region = ap-southeast-2\n"
            "output = json\n"
        )
        _install_aws_profile('myrepo', 'AKIA', 'SECRET')
        cfg = _make_aws_parser()
        cfg.read(cfg_p)
        # region preserved, not overwritten with 'auto'
        assert cfg.get('profile myrepo', 'region') == 'ap-southeast-2'
        assert cfg.get('profile myrepo', 'output') == 'json'


class TestRemoveAwsProfile:

    def test_removes_from_both_files(self, aws_dir):
        _install_aws_profile('myrepo', 'AKIA', 'SECRET')
        modified = _remove_aws_profile('myrepo', force=True)
        assert len(modified) == 2

        creds = _make_aws_parser()
        creds.read(aws_dir / 'credentials')
        assert not creds.has_section('myrepo')

        cfg = _make_aws_parser()
        cfg.read(aws_dir / 'config')
        assert not cfg.has_section('profile myrepo')

    def test_missing_profile_is_noop(self, aws_dir):
        assert _remove_aws_profile('absent') == []


class TestListAwsProfiles:

    def test_lists_credentials_sections(self, aws_dir):
        _install_aws_profile('a', 'A', 'B')
        _install_aws_profile('b', 'A', 'B')
        assert _list_aws_profiles() == {'a', 'b'}

    def test_empty_when_no_file(self, aws_dir):
        assert _list_aws_profiles() == set()


# ---------------------------------------------------------------------------
# DVC global cleanup
# ---------------------------------------------------------------------------

class TestStripDvcGlobalCreds:

    def test_strips_only_named_remotes(self, fake_dvc_global):
        _write_global_dvc(fake_dvc_global, {
            'r1': {
                'url': 's3://r1',
                'access_key_id': 'A',
                'secret_access_key': 'B',
                'endpointurl': 'https://e',
            },
            'r2': {
                'url': 's3://r2',
                'access_key_id': 'A2',
                'secret_access_key': 'B2',
            },
        })
        touched = _strip_dvc_global_creds({'r1'})
        assert touched == ['r1']
        text = fake_dvc_global.read_text()
        # r1 cred keys gone, endpointurl preserved
        assert 'access_key_id' in text  # r2 still has it
        # r1 no longer has access_key_id
        from dt.auth.credentials import _parse_dvc_ini
        sections = _parse_dvc_ini(text)
        r1 = sections["'remote \"r1\"'"]
        assert 'access_key_id' not in r1
        assert 'secret_access_key' not in r1
        assert r1.get('endpointurl') == 'https://e'  # preserved
        r2 = sections["'remote \"r2\"'"]
        assert r2.get('access_key_id') == 'A2'

    def test_missing_global_is_noop(self, fake_dvc_global):
        assert _strip_dvc_global_creds({'r1'}) == []


# ---------------------------------------------------------------------------
# install_credentials end-to-end (with mocked backend + dvc remote list)
# ---------------------------------------------------------------------------

def _patch_project_remotes(monkeypatch, remotes: dict) -> None:
    monkeypatch.setattr(
        'dt.auth.credentials._get_project_s3_remotes',
        lambda: remotes,
    )


class TestInstallCredentialsAws:

    def test_installs_aws_secret(self, aws_dir, fake_dvc_global, monkeypatch):
        secret = "[example_wts]\naws_access_key_id = AKIA\naws_secret_access_key = SS\n"
        backend = MagicMock()
        backend.get_raw_config.return_value = secret
        monkeypatch.setattr(
            'dt.auth.credentials._get_secret_backend', lambda: backend,
        )
        _patch_project_remotes(monkeypatch, {})

        results = install_credentials(repo_name='example_wts')

        assert results == {'example_wts': True}
        creds = _make_aws_parser()
        creds.read(aws_dir / 'credentials')
        assert creds.get('example_wts', 'aws_access_key_id') == 'AKIA'

    def test_legacy_secret_raises_with_helpful_message(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        secret = (
            "['remote \"example-wts\"']\n"
            "    access_key_id = A\n"
            "    secret_access_key = B\n"
        )
        backend = MagicMock()
        backend.get_raw_config.return_value = secret
        monkeypatch.setattr(
            'dt.auth.credentials._get_secret_backend', lambda: backend,
        )
        _patch_project_remotes(monkeypatch, {})

        with pytest.raises(AuthError, match="legacy DVC-INI"):
            install_credentials(repo_name='example_wts')

    def test_strips_legacy_global_for_project_remotes(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        # Pre-existing legacy creds in DVC global
        _write_global_dvc(fake_dvc_global, {
            'example-wts': {
                'url': 's3://demo-wts',
                'access_key_id': 'OLD',
                'secret_access_key': 'OLD2',
            },
        })
        # Project has the matching remote
        _patch_project_remotes(monkeypatch, {
            'example-wts': {
                'url': 's3://demo-wts',
                'profile': 'example_wts',
            },
        })

        secret = "[example_wts]\naws_access_key_id = NEW\naws_secret_access_key = NEW2\n"
        backend = MagicMock()
        backend.get_raw_config.return_value = secret
        monkeypatch.setattr(
            'dt.auth.credentials._get_secret_backend', lambda: backend,
        )

        install_credentials(repo_name='example_wts', verbose=False)

        from dt.auth.credentials import _parse_dvc_ini
        sections = _parse_dvc_ini(fake_dvc_global.read_text())
        r = sections["'remote \"example-wts\"'"]
        assert 'access_key_id' not in r
        assert 'secret_access_key' not in r


# ---------------------------------------------------------------------------
# uninstall_credentials
# ---------------------------------------------------------------------------

class TestUninstallCredentials:

    def test_removes_profile_and_legacy_global(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        _install_aws_profile('example_wts', 'A', 'B')
        _write_global_dvc(fake_dvc_global, {
            'example-wts': {
                'url': 's3://demo-wts',
                'access_key_id': 'OLD',
                'secret_access_key': 'OLD2',
            },
        })
        _patch_project_remotes(monkeypatch, {
            'example-wts': {'url': 's3://demo-wts', 'profile': 'example_wts'},
        })

        touched = uninstall_credentials(repo_name='example_wts')

        assert 'example_wts' in touched  # AWS profile
        assert 'example-wts' in touched  # legacy global remote
        assert not (aws_dir / 'credentials').read_text().strip() or \
            'example_wts' not in (aws_dir / 'credentials').read_text()

    def test_remote_only_targets_global(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        _write_global_dvc(fake_dvc_global, {
            'r1': {
                'url': 's3://r1',
                'access_key_id': 'A',
                'secret_access_key': 'B',
            },
        })
        _patch_project_remotes(monkeypatch, {})

        touched = uninstall_credentials(remote='r1')
        assert touched == ['r1']


# ---------------------------------------------------------------------------
# get_credentials_status / format
# ---------------------------------------------------------------------------

class TestGetCredentialsStatus:

    def test_status_ready(self, aws_dir, fake_dvc_global, monkeypatch):
        _install_aws_profile('example_wts', 'A', 'B')
        _patch_project_remotes(monkeypatch, {
            'example-wts': {
                'url': 's3://demo-wts',
                'profile': 'example_wts',
                'endpointurl': 'https://e',
            },
        })
        statuses = get_credentials_status()
        assert len(statuses) == 1
        st = statuses[0]
        assert st.remote_name == 'example-wts'
        assert st.profile == 'example_wts'
        assert st.profile_installed is True
        assert st.endpointurl == 'https://e'
        assert st.legacy_creds_in_global is False

    def test_status_missing_profile(self, aws_dir, fake_dvc_global, monkeypatch):
        # Profile referenced in .dvc/config but not in ~/.aws/credentials
        _patch_project_remotes(monkeypatch, {
            'example-wts': {'url': 's3://demo-wts', 'profile': 'example_wts'},
        })
        statuses = get_credentials_status()
        assert statuses[0].profile_installed is False

    def test_status_no_profile_configured(self, aws_dir, fake_dvc_global, monkeypatch):
        _patch_project_remotes(monkeypatch, {
            'example-wts': {'url': 's3://demo-wts'},
        })
        statuses = get_credentials_status()
        assert statuses[0].profile is None
        assert statuses[0].profile_installed is False

    def test_status_legacy_creds_flag(self, aws_dir, fake_dvc_global, monkeypatch):
        _write_global_dvc(fake_dvc_global, {
            'example-wts': {
                'url': 's3://demo-wts',
                'access_key_id': 'A',
                'secret_access_key': 'B',
            },
        })
        _patch_project_remotes(monkeypatch, {
            'example-wts': {'url': 's3://demo-wts', 'profile': 'example_wts'},
        })
        statuses = get_credentials_status()
        assert statuses[0].legacy_creds_in_global is True


class TestFormatCredentialsStatus:

    def test_empty(self):
        assert "No S3 remotes" in format_credentials_status([])

    def test_ready_line(self, aws_dir, fake_dvc_global, monkeypatch):
        _install_aws_profile('myrepo', 'A', 'B')
        _patch_project_remotes(monkeypatch, {
            'cloud': {'url': 's3://cloud', 'profile': 'myrepo',
                      'endpointurl': 'https://e'},
        })
        out = format_credentials_status(get_credentials_status())
        assert '✓' in out
        assert 'cloud' in out
        assert 'myrepo' in out


# ---------------------------------------------------------------------------
# configure_remotes
# ---------------------------------------------------------------------------

class TestConfigureRemotes:

    def _make_repo(self, tmp_path, dvc_config_text='[core]\n'):
        repo = tmp_path / 'fakerepo'
        (repo / '.dvc').mkdir(parents=True)
        (repo / '.dvc' / 'config').write_text(dvc_config_text)
        # Initialise as a git repo so `git add` does not error noisily
        import subprocess
        subprocess.run(['git', 'init', '-q'], cwd=str(repo), check=True)
        return repo

    def test_adds_endpoint_and_profile(self, tmp_path, monkeypatch):
        cfg = (
            "[core]\n"
            "    remote = example-wts\n"
            "['remote \"example-wts\"']\n"
            "    url = s3://demo-wts\n"
        )
        repo = self._make_repo(tmp_path, cfg)
        monkeypatch.chdir(repo)
        monkeypatch.setattr('dt.auth.credentials.utils.get_project_name',
                            lambda: 'example_wts')

        result = configure_remotes(endpoint='https://r2.example.com')

        assert result.updated_remotes == ['example-wts']
        text = (repo / '.dvc' / 'config').read_text()
        assert 'profile = example_wts' in text
        assert 'endpointurl = https://r2.example.com' in text

    def test_skips_already_configured(self, tmp_path, monkeypatch):
        cfg = (
            "[core]\n"
            "    remote = example-wts\n"
            "['remote \"example-wts\"']\n"
            "    url = s3://demo-wts\n"
            "    endpointurl = https://e\n"
            "    profile = example_wts\n"
        )
        repo = self._make_repo(tmp_path, cfg)
        monkeypatch.chdir(repo)
        monkeypatch.setattr('dt.auth.credentials.utils.get_project_name',
                            lambda: 'example_wts')

        result = configure_remotes(endpoint='https://e')
        assert result.updated_remotes == []
        assert result.skipped_remotes == ['example-wts']

    def test_no_endpoint_no_default_fails(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path)
        monkeypatch.chdir(repo)
        monkeypatch.setattr('dt.config.get_trusted_value', lambda *a, **kw: None)
        with pytest.raises(AuthError, match='No endpoint'):
            configure_remotes(endpoint=None)

    def test_uses_config_default_when_no_flag(self, tmp_path, monkeypatch):
        cfg = (
            "[core]\n"
            "['remote \"x\"']\n"
            "    url = s3://x\n"
        )
        repo = self._make_repo(tmp_path, cfg)
        monkeypatch.chdir(repo)
        monkeypatch.setattr('dt.auth.credentials.utils.get_project_name',
                            lambda: 'x')
        monkeypatch.setattr('dt.config.get_trusted_value',
                            lambda key, default=None: 'https://default'
                            if key == 'secrets.default_endpointurl' else default)
        result = configure_remotes()
        assert 'https://default' in (repo / '.dvc' / 'config').read_text()
        assert result.updated_remotes == ['x']

    def test_no_s3_remotes_fails(self, tmp_path, monkeypatch):
        repo = self._make_repo(tmp_path, '[core]\n')
        monkeypatch.chdir(repo)
        monkeypatch.setattr('dt.auth.credentials.utils.get_project_name',
                            lambda: 'x')
        with pytest.raises(AuthError, match='No S3 remotes'):
            configure_remotes(endpoint='https://e')


# ---------------------------------------------------------------------------
# migrate_credentials
# ---------------------------------------------------------------------------

class TestMigrateCredentials:

    def test_legacy_secret_is_reuploaded_and_installed(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        legacy = (
            "['remote \"example-wts\"']\n"
            "    url = s3://demo-wts\n"
            "    access_key_id = A\n"
            "    secret_access_key = B\n"
        )
        backend = MagicMock()
        backend.get_raw_config.return_value = legacy
        monkeypatch.setattr(
            'dt.auth.credentials._get_secret_backend', lambda: backend,
        )
        _patch_project_remotes(monkeypatch, {})

        result = migrate_credentials(repo_name='example_wts')

        assert result.reuploaded == ['example_wts']
        assert result.installed == ['example_wts']
        # Backend was asked to write new AWS-INI content
        backend.set_secret.assert_called_once()
        new_content = backend.set_secret.call_args.args[1]
        assert _detect_secret_format(new_content) == 'aws'
        # Credentials installed
        creds = _make_aws_parser()
        creds.read(aws_dir / 'credentials')
        assert creds.get('example_wts', 'aws_access_key_id') == 'A'

    def test_already_aws_secret_just_installs(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        secret = "[x]\naws_access_key_id = A\naws_secret_access_key = B\n"
        backend = MagicMock()
        backend.get_raw_config.return_value = secret
        monkeypatch.setattr(
            'dt.auth.credentials._get_secret_backend', lambda: backend,
        )
        _patch_project_remotes(monkeypatch, {})

        result = migrate_credentials(repo_name='x')
        assert result.reuploaded == []
        assert result.installed == ['x']
        backend.set_secret.assert_not_called()

    def test_dry_run_writes_nothing(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        legacy = (
            "['remote \"r\"']\n"
            "    access_key_id = A\n"
            "    secret_access_key = B\n"
        )
        backend = MagicMock()
        backend.get_raw_config.return_value = legacy
        monkeypatch.setattr(
            'dt.auth.credentials._get_secret_backend', lambda: backend,
        )
        _patch_project_remotes(monkeypatch, {})

        result = migrate_credentials(repo_name='r', dry_run=True)
        assert result.reuploaded == ['r']
        assert result.installed == ['r']
        backend.set_secret.assert_not_called()
        assert not (aws_dir / 'credentials').exists()

    def test_strips_legacy_global_for_project_remotes(
        self, aws_dir, fake_dvc_global, monkeypatch,
    ):
        _write_global_dvc(fake_dvc_global, {
            'example-wts': {
                'url': 's3://demo-wts',
                'access_key_id': 'A', 'secret_access_key': 'B',
            },
        })
        _patch_project_remotes(monkeypatch, {
            'example-wts': {'url': 's3://demo-wts', 'profile': 'example_wts'},
        })
        secret = "[example_wts]\naws_access_key_id = A\naws_secret_access_key = B\n"
        backend = MagicMock()
        backend.get_raw_config.return_value = secret
        monkeypatch.setattr(
            'dt.auth.credentials._get_secret_backend', lambda: backend,
        )

        result = migrate_credentials(repo_name='example_wts')
        assert result.stripped_remotes == ['example-wts']


class TestProfilesThatAreNotDts:
    """Profiles dt did not install are neither replaced nor removed."""

    def test_config_only_profile_is_not_taken_over(self, aws_dir):
        (aws_dir / 'config').write_text('[profile myrepo]\nsso_session = s\nregion = x\n')
        with pytest.raises(AuthError, match='already defined'):
            _install_aws_profile('myrepo', 'AKIA', 'S', source='gcp:p/x')
        assert not (aws_dir / 'credentials').exists()

    def test_config_only_profile_survives_discovered_uninstall(self, aws_dir):
        (aws_dir / 'config').write_text('[profile myrepo]\nrole_arn = r\n')
        assert _remove_aws_profile('myrepo', source='gcp:p/x') == []
        assert 'role_arn' in (aws_dir / 'config').read_text()

    def test_older_dt_profile_with_same_key_is_adopted(self, aws_dir):
        (aws_dir / 'credentials').write_text(
            '[myrepo]\naws_access_key_id = AKIA\naws_secret_access_key = OLD\n')
        _install_aws_profile('myrepo', 'AKIA', 'NEW', source='gcp:p/x')
        text = (aws_dir / 'credentials').read_text()
        assert 'NEW' in text and 'dt_source = gcp:p/x' in text

    def test_source_none_never_matches(self, aws_dir):
        (aws_dir / 'credentials').write_text(
            '[myrepo]\naws_access_key_id = MINE\naws_secret_access_key = S\n')
        with pytest.raises(AuthError):
            _install_aws_profile('myrepo', 'AKIA', 'S', source=None)

    @pytest.mark.parametrize('name', ['Default', 'DEFAULT'])
    def test_default_in_any_case_refused(self, aws_dir, name):
        with pytest.raises(AuthError):
            _install_aws_profile(name, 'AKIA', 'S', source='gcp:p/x')


class TestRepoChosenStore:
    def _backend(self):
        b = MagicMock()
        b._repo_scoped = True
        b.project, b.prefix = 'theirs', 'dvc-remote-'
        return b

    def test_set_refuses_repo_only_store(self):
        from dt.auth import credentials as c
        b = self._backend()
        with patch.object(c, '_get_secret_backend', return_value=b):
            with pytest.raises(AuthError, match='allow-repo-store'):
                c.set_secret('x', '[x]\naws_access_key_id = A\naws_secret_access_key = B\n')
            b.set_secret.assert_not_called()
            c.set_secret('x', '[x]\naws_access_key_id = A\naws_secret_access_key = B\n',
                         allow_repo_store=True)
            b.set_secret.assert_called_once()

    def test_install_refuses_repo_only_store(self, aws_dir):
        from dt.auth import credentials as c
        b = self._backend()
        with patch.object(c, '_get_secret_backend', return_value=b):
            with pytest.raises(AuthError, match='allow-repo-store'):
                c.install_credentials(repo_name='x')
        b.get_raw_config.assert_not_called()


class TestStripScope:
    def test_other_projects_global_creds_are_kept(self, tmp_path, monkeypatch, fake_dvc_global):
        from dt.auth import credentials as c
        monkeypatch.setattr(c, '_get_project_s3_remotes', lambda *a: {
            'storage': {'url': 's3://mine', 'profile': 'proj'},
            'other': {'url': 's3://o', 'profile': 'unrelated'},
        })
        fake_dvc_global.write_text(
            "['remote \"storage\"']\n    url = s3://elsewhere\n    access_key_id = K\n")
        assert c._strippable_remotes({'proj'}) == set()
        fake_dvc_global.write_text("['remote \"storage\"']\n    access_key_id = K\n")
        assert c._strippable_remotes({'proj'}) == {'storage'}
        assert c._strippable_remotes(set()) == set()


class TestAdoptionAndPlainProfiles:
    def test_older_profile_with_other_key_is_refused(self, aws_dir):
        (aws_dir / 'credentials').write_text(
            '[myrepo]\naws_access_key_id = OTHER\naws_secret_access_key = S\n')
        with pytest.raises(AuthError, match='no record of its source'):
            _install_aws_profile('myrepo', 'AKIA', 'S', source='gcp:p/x')
        assert 'OTHER' in (aws_dir / 'credentials').read_text()

    def test_config_profile_with_own_settings_is_left_alone(self, aws_dir):
        (aws_dir / 'config').write_text('[profile myrepo]\nendpoint_url = https://e\n')
        with pytest.raises(AuthError, match='already defined'):
            _install_aws_profile('myrepo', 'AKIA', 'S', source='gcp:p/x')

    def test_config_profile_with_region_only_is_used(self, aws_dir):
        (aws_dir / 'config').write_text('[profile myrepo]\nregion = auto\n')
        _install_aws_profile('myrepo', 'AKIA', 'S', source='gcp:p/x')
        assert 'AKIA' in (aws_dir / 'credentials').read_text()


class TestCredentialsFileIsPrivateThroughout:
    def test_new_file_created_owner_only_under_lax_umask(self, tmp_path, monkeypatch):
        import stat as stat_mod
        target = tmp_path / 'newdir' / 'credentials'
        monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', str(target))
        monkeypatch.setenv('AWS_CONFIG_FILE', str(tmp_path / 'newdir' / 'config'))
        seen = []
        real_open = os.open

        def spy(path, flags, mode=0o777, *a, **kw):
            if flags & os.O_CREAT:
                seen.append(mode)
            return real_open(path, flags, mode, *a, **kw)

        old = os.umask(0o002)
        try:
            monkeypatch.setattr(os, 'open', spy)
            _install_aws_profile('myrepo', 'AKIA', 'SECRET', source='gcp:p/x')
        finally:
            os.umask(old)
        assert seen and all(m == 0o600 for m in seen)
        assert stat_mod.S_IMODE(target.stat().st_mode) == 0o600
        assert stat_mod.S_IMODE(target.parent.stat().st_mode) & 0o077 == 0
        assert [p.name for p in target.parent.iterdir() if p.name.endswith('.tmp')] == []

    def test_stricter_existing_mode_is_kept(self, aws_dir):
        import stat as stat_mod
        creds = aws_dir / 'credentials'
        creds.write_text('')
        creds.chmod(0o400)
        _install_aws_profile('myrepo', 'AKIA', 'SECRET', source='gcp:p/x')
        assert stat_mod.S_IMODE(creds.stat().st_mode) == 0o400

    def test_group_readable_file_ends_owner_only(self, aws_dir):
        import stat as stat_mod
        creds = aws_dir / 'credentials'
        creds.write_text('')
        creds.chmod(0o644)
        _install_aws_profile('myrepo', 'AKIA', 'SECRET', source='gcp:p/x')
        assert stat_mod.S_IMODE(creds.stat().st_mode) == 0o600

    @pytest.mark.skipif(os.geteuid() == 0, reason='root reads anything')
    def test_unreadable_file_is_not_replaced(self, aws_dir):
        creds = aws_dir / 'credentials'
        creds.write_text('[other]\naws_access_key_id = KEEP\naws_secret_access_key = S\n')
        creds.chmod(0o000)
        try:
            with pytest.raises(OSError):
                _install_aws_profile('myrepo', 'AKIA', 'SECRET', source='gcp:p/x')
        finally:
            creds.chmod(0o600)
        assert 'KEEP' in creds.read_text()

    def test_symlinked_credentials_stay_a_link(self, aws_dir, tmp_path, monkeypatch):
        real = tmp_path / 'real_credentials'
        link = aws_dir / 'linked'
        link.symlink_to(real)
        monkeypatch.setenv('AWS_SHARED_CREDENTIALS_FILE', str(link))
        _install_aws_profile('myrepo', 'AKIA', 'SECRET', source='gcp:p/x')
        assert link.is_symlink() and 'AKIA' in real.read_text()

    def test_dvc_global_config_rewrite_is_owner_only(self, fake_dvc_global):
        import stat as stat_mod
        from dt.auth import credentials as c
        fake_dvc_global.write_text("['remote \"r\"']\n    access_key_id = K\n")
        fake_dvc_global.chmod(0o644)
        assert c._strip_dvc_global_creds({'r'}) == ['r']
        assert stat_mod.S_IMODE(fake_dvc_global.stat().st_mode) == 0o600
