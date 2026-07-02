"""Tests for the GCP Secret Manager backend, focused on the replication
policy applied when creating secrets (issue #149)."""

from unittest.mock import patch, MagicMock

from dt.secrets import gcp


class TestReplicationFlags:
    """secrets.gcp.locations config -> gcloud replication flags."""

    def test_no_flags_when_unset(self):
        """Unset config preserves the default (automatic/global) replication."""
        with patch('dt.secrets.gcp.cfg.get_value', return_value=None):
            assert gcp._replication_create_flags() == []

    def test_user_managed_flags_for_single_region(self):
        with patch('dt.secrets.gcp.cfg.get_value', return_value='australia-southeast1'):
            flags = gcp._replication_create_flags()
        assert flags == [
            '--replication-policy=user-managed',
            '--locations=australia-southeast1',
        ]

    def test_list_of_regions_joined(self):
        with patch('dt.secrets.gcp.cfg.get_value',
                   return_value=['australia-southeast1', 'australia-southeast2']):
            flags = gcp._replication_create_flags()
        assert flags == [
            '--replication-policy=user-managed',
            '--locations=australia-southeast1,australia-southeast2',
        ]

    def test_blank_value_treated_as_unset(self):
        with patch('dt.secrets.gcp.cfg.get_value', return_value='   '):
            assert gcp._replication_create_flags() == []


class TestCreateSecretHint:
    """The 'Create it with: ...' hint mirrors the create-command flags."""

    def test_hint_includes_replication_flags_when_configured(self):
        backend = gcp.GCPSecretBackend(project='proj')
        with patch('dt.secrets.gcp.cfg.get_value', return_value='australia-southeast1'):
            hint = backend._create_secret_hint('dvc-remote-foo')
        assert 'gcloud secrets create dvc-remote-foo' in hint
        assert '--project=proj' in hint
        assert '--replication-policy=user-managed' in hint
        assert '--locations=australia-southeast1' in hint

    def test_hint_bare_when_unset(self):
        backend = gcp.GCPSecretBackend(project='proj')
        with patch('dt.secrets.gcp.cfg.get_value', return_value=None):
            hint = backend._create_secret_hint('dvc-remote-foo')
        assert hint == 'gcloud secrets create dvc-remote-foo --project=proj'


class TestCliSetSecretPassesReplication:
    """The actual `gcloud secrets create` call carries the configured flags."""

    def test_create_command_includes_flags(self, tmp_path):
        backend = gcp.GCPSecretBackend(project='proj')

        completed = MagicMock()
        completed.returncode = 0

        with patch('dt.secrets.gcp.cfg.get_value', return_value='australia-southeast1'), \
             patch.object(backend, '_require_gcloud', return_value='gcloud'), \
             patch.object(backend, '_cli_secret_exists', return_value=False), \
             patch('dt.secrets.gcp.subprocess.run', return_value=completed) as mock_run:
            backend._cli_set_secret('foo', 'creds')

        args = mock_run.call_args[0][0]
        assert 'create' in args
        assert '--replication-policy=user-managed' in args
        assert '--locations=australia-southeast1' in args

    def test_create_command_bare_when_unset(self, tmp_path):
        backend = gcp.GCPSecretBackend(project='proj')

        completed = MagicMock()
        completed.returncode = 0

        with patch('dt.secrets.gcp.cfg.get_value', return_value=None), \
             patch.object(backend, '_require_gcloud', return_value='gcloud'), \
             patch.object(backend, '_cli_secret_exists', return_value=False), \
             patch('dt.secrets.gcp.subprocess.run', return_value=completed) as mock_run:
            backend._cli_set_secret('foo', 'creds')

        args = mock_run.call_args[0][0]
        assert 'create' in args
        assert not any(a.startswith('--replication-policy') for a in args)
        assert not any(a.startswith('--locations') for a in args)
