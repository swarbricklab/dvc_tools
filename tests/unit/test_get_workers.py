"""Unit tests for ``dt get --workers``: distributed streaming to S3 via qxub.

No qxub and no network. The interesting behaviour is the *round trip* -- the
submitter resolves and partitions, each worker reads its partition back off
disk and reports, and the submitter reassembles per-row outcomes from those
reports. So the central test here replaces ``hpc.submit_workers`` with a stub
that runs ``worker_upload_to_s3`` inline, which exercises the manifest as a
real interface rather than asserting on how it happens to be spelled.

What this cannot cover is anything about PBS: whether the queue has outbound
network access, whether the manifest directory is visible from a compute node,
or whether more nodes actually move more bytes. Those need a real submission.
"""

import json
from pathlib import Path
from unittest.mock import patch

import pytest

from dt import get as get_mod
from dt import get_dest
from dt import hpc
from dt.errors import GetError

# The S3 fakes are the ones the single-node tests already run against, so a
# behaviour change there fails here too rather than passing against a second,
# drifted copy.
from tests.unit.test_get_s3 import FakeODB, FakeS3FS, md5_of


# =============================================================================
# Fakes
# =============================================================================

class FakeRepo:
    """Stands in for the DVC Repo opened against the source clone."""

    def __init__(self):
        self.closed = False

    def close(self):
        self.closed = True


@pytest.fixture
def fs():
    return FakeS3FS()


@pytest.fixture
def source(fs):
    """A source remote holding two files per sample, of differing sizes."""
    blobs = {
        'AF013-A/R1.fq': b'a' * 400,
        'AF013-A/R2.fq': b'b' * 100,
        'AF013-B/R1.fq': b'c' * 50,
    }
    objects = {md5_of(data): data for data in blobs.values()}
    odb = FakeODB(fs, objects)
    entries = {
        'data/fq/AF013-A': [
            {'relpath': 'R1.fq', 'md5': md5_of(blobs['AF013-A/R1.fq']), 'size': 400},
            {'relpath': 'R2.fq', 'md5': md5_of(blobs['AF013-A/R2.fq']), 'size': 100},
        ],
        'data/fq/AF013-B': [
            {'relpath': 'R1.fq', 'md5': md5_of(blobs['AF013-B/R1.fq']), 'size': 50},
        ],
    }
    return odb, entries, blobs


def s3_patches(fs):
    """Patch the destination's filesystem and preflight, leaving addressing real."""
    return (
        patch.object(get_dest.S3Dest, 'fs', property(lambda self: fs)),
        patch.object(get_dest.S3Dest, 'preflight',
                     lambda self: 'account 000000000000 as arn:aws:iam::test'),
    )


def run_workers_inline(manifest_dir, num_workers, operation, **kwargs):
    """Stand-in for hpc.submit_workers that runs each worker in-process."""
    job_ids = []
    for worker_id in range(num_workers):
        if not (manifest_dir / f'worker_{worker_id}.json').exists():
            continue
        _, tasks = hpc.load_worker_partition(manifest_dir, worker_id)
        if not tasks:
            continue
        get_mod.worker_upload_to_s3(manifest_dir, worker_id)
        job_ids.append(f'{worker_id}.fake-pbs')
    return job_ids


# =============================================================================
# hpc.partition_by_size -- the bin-packing itself
# =============================================================================

class TestPartitionBySize:

    def test_balances_bytes_rather_than_counts(self):
        """One big item plus many small ones must not all land together."""
        items = [{'n': 'big', 'size': 1000}] + [
            {'n': str(i), 'size': 100} for i in range(10)
        ]
        parts = hpc.partition_by_size(
            items, 2, weight=lambda i: i['size'], tiebreak=lambda i: i['n'])

        loads = [sum(i['size'] for i in v) for v in parts.values()]
        # 2000 bytes over 2 workers, and the 1000-byte item is indivisible.
        assert sorted(loads) == [1000, 1000]

    def test_partitions_are_disjoint_and_complete(self):
        items = [{'n': str(i), 'size': i} for i in range(20)]
        parts = hpc.partition_by_size(
            items, 4, weight=lambda i: i['size'], tiebreak=lambda i: i['n'])

        names = [i['n'] for v in parts.values() for i in v]
        assert sorted(names) == sorted(i['n'] for i in items)
        assert len(names) == len(set(names))

    def test_assignment_is_independent_of_input_order(self):
        items = [{'n': str(i), 'size': (i % 3) * 10} for i in range(12)]
        args = dict(weight=lambda i: i['size'], tiebreak=lambda i: i['n'])

        forward = hpc.partition_by_size(items, 3, **args)
        backward = hpc.partition_by_size(list(reversed(items)), 3, **args)

        assert forward == backward

    def test_every_worker_gets_a_key_even_with_nothing_to_do(self):
        parts = hpc.partition_by_size(
            [{'n': 'only', 'size': 1}], 4,
            weight=lambda i: i['size'], tiebreak=lambda i: i['n'])

        assert set(parts) == {0, 1, 2, 3}
        assert sum(len(v) for v in parts.values()) == 1

    def test_rejects_zero_workers(self):
        with pytest.raises(ValueError, match='at least 1'):
            hpc.partition_by_size(
                [], 0, weight=lambda i: 0, tiebreak=lambda i: '')


class TestSaveManifestMetadata:

    def test_metadata_reaches_the_worker(self, tmp_path, monkeypatch):
        """Whatever the submitter records must come back out the other side."""
        monkeypatch.chdir(tmp_path)
        manifest_dir = hpc.save_manifest(
            {'files': ['x']}, {0: ['x']}, 'job1', 'get',
            metadata={'root_url': 's3://b/p', 'jobs': 4},
        )

        metadata, files = hpc.load_worker_partition(manifest_dir, 0)

        assert metadata['root_url'] == 's3://b/p'
        assert metadata['jobs'] == 4
        assert files == ['x']

    def test_standard_fields_survive_without_metadata(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        manifest_dir = hpc.save_manifest(
            {'files': ['a', 'b'], 'remote': 'r2', 'repo_root': '/repo'},
            {0: ['a'], 1: ['b']}, 'job2', 'push',
        )

        metadata, _ = hpc.load_worker_partition(manifest_dir, 1)

        assert metadata['remote'] == 'r2'
        assert metadata['repo_root'] == '/repo'
        assert metadata['total_files'] == 2
        assert metadata['num_workers'] == 2


# =============================================================================
# Task construction and collation
# =============================================================================

class TestWorkerTasks:

    def test_tags_every_file_with_its_row(self):
        targets = [('data/a', 'a'), ('data/b', 'b')]
        resolved = [
            ([{'relpath': 'f1', 'md5': 'x' * 32, 'size': 10},
              {'relpath': 'f2', 'md5': 'y' * 32, 'size': 20}], None),
            ([{'relpath': 'f3', 'md5': 'z' * 32, 'size': 30}], None),
        ]

        tasks, counts = get_mod._worker_tasks(targets, resolved)

        assert [t['row'] for t in tasks] == [0, 0, 1]
        assert [t['name'] for t in tasks] == ['a', 'a', 'b']
        assert counts == {0: 2, 1: 1}

    def test_unresolved_rows_contribute_no_tasks_and_no_count(self):
        targets = [('data/a', 'a'), ('', None), ('data/c', 'c')]
        resolved = [
            ([{'relpath': 'f1', 'md5': 'x' * 32, 'size': 1}], None),
            (None, 'Missing path'),
            (None, 'Not found in the source repository: data/c'),
        ]

        tasks, counts = get_mod._worker_tasks(targets, resolved)

        assert [t['row'] for t in tasks] == [0]
        # No count for a row that never resolved -- collation must not read
        # that as "0 of 0 files arrived, so this row succeeded".
        assert counts == {0: 1}


class TestCollateWorkerResults:

    root = None

    def _root(self, fs):
        dest = get_dest.S3Dest('s3://bucket/fastqs')
        dest._fs = fs
        return dest

    def _write(self, manifest_dir, worker_id, results):
        (manifest_dir / f'result_{worker_id}.json').write_text(
            json.dumps({'results': results}))

    def test_rows_reported_in_order_though_files_span_workers(self, tmp_path, fs):
        targets = [('data/a', 'a'), ('data/b', 'b')]
        resolved = [([{}, {}], None), ([{}], None)]
        counts = {0: 2, 1: 1}
        # Row 0's two files were split across two workers.
        self._write(tmp_path, 0, [
            {'row': 0, 'relpath': 'f1', 'ok': True, 'message': 'uploaded'},
            {'row': 1, 'relpath': 'f3', 'ok': True, 'message': 'uploaded'},
        ])
        self._write(tmp_path, 1, [
            {'row': 0, 'relpath': 'f2', 'ok': True, 'message': 'uploaded'},
        ])

        results = get_mod._collate_worker_results(
            tmp_path, targets, resolved, counts, self._root(fs))

        assert [r[0] for r in results] == ['data/a', 'data/b']
        assert all(ok for _, ok, _ in results)
        assert results[0][2] == '2 files -> s3://bucket/fastqs/a'
        assert results[1][2] == '1 file -> s3://bucket/fastqs/b'

    def test_unresolved_row_keeps_its_own_error(self, tmp_path, fs):
        targets = [('data/a', 'a'), ('data/gone', 'gone')]
        resolved = [([{}], None), (None, 'Not found in the source repository')]
        self._write(tmp_path, 0, [
            {'row': 0, 'relpath': 'f1', 'ok': True, 'message': 'uploaded'},
        ])

        results = get_mod._collate_worker_results(
            tmp_path, targets, resolved, {0: 1}, self._root(fs))

        assert results[1] == ('data/gone', False,
                              'Not found in the source repository')

    def test_a_worker_that_never_reported_fails_its_rows(self, tmp_path, fs):
        """The failure mode that matters: a job killed on walltime.

        Every result that arrived succeeded, so a naive tally would call this a
        clean run of a one-file row.
        """
        targets = [('data/a', 'a')]
        resolved = [([{}, {}, {}], None)]
        self._write(tmp_path, 0, [
            {'row': 0, 'relpath': 'f1', 'ok': True, 'message': 'uploaded'},
        ])

        results = get_mod._collate_worker_results(
            tmp_path, targets, resolved, {0: 3}, self._root(fs))

        assert results[0][1] is False
        assert '2 unreported' in results[0][2]

    def test_no_result_files_at_all_is_a_failure(self, tmp_path, fs):
        results = get_mod._collate_worker_results(
            tmp_path, [('data/a', 'a')], [([{}], None)], {0: 1},
            self._root(fs))

        assert results[0][1] is False
        assert '1 unreported' in results[0][2]

    def test_reported_failures_are_counted(self, tmp_path, fs):
        self._write(tmp_path, 0, [
            {'row': 0, 'relpath': 'f1', 'ok': True, 'message': 'uploaded'},
            {'row': 0, 'relpath': 'f2', 'ok': False,
             'message': 'object abc not on the source remote'},
        ])

        results = get_mod._collate_worker_results(
            tmp_path, [('data/a', 'a')], [([{}, {}], None)], {0: 2},
            self._root(fs))

        assert results[0] == ('data/a', False, '1 uploaded, 1 failed')

    def test_a_truncated_result_file_reads_as_unfinished(self, tmp_path, fs):
        (tmp_path / 'result_0.json').write_text('{"results": [{"row": 0,')

        results = get_mod._collate_worker_results(
            tmp_path, [('data/a', 'a')], [([{}], None)], {0: 1},
            self._root(fs))

        assert results[0][1] is False
        assert 'unreported' in results[0][2]


# =============================================================================
# The worker
# =============================================================================

class TestWorkerUploadToS3:

    def _manifest(self, tmp_path, source, tasks, **metadata):
        odb, entries, _ = source
        base = {
            'clone': str(tmp_path / 'clone'),
            'rev': None,
            'root_url': 's3://bucket/fastqs',
            'dest_config': {'profile': None, 'endpoint_url': None,
                            'region': None, 'account_id': None},
            'chunk_size': 64,
            'jobs': 2,
            'force': False, 'resume': False, 'check': False,
        }
        base.update(metadata)
        return hpc.save_manifest(
            {'files': tasks}, {0: tasks}, 'jobw', 'get', metadata=base)

    def test_uploads_its_partition_and_records_every_file(
        self, tmp_path, monkeypatch, fs, source
    ):
        monkeypatch.chdir(tmp_path)
        odb, entries, blobs = source
        tasks = [
            {'row': 0, 'name': 'AF013-A', 'relpath': 'R1.fq',
             'md5': md5_of(blobs['AF013-A/R1.fq']), 'size': 400},
            {'row': 1, 'name': 'AF013-B', 'relpath': 'R1.fq',
             'md5': md5_of(blobs['AF013-B/R1.fq']), 'size': 50},
        ]
        manifest_dir = self._manifest(tmp_path, source, tasks)
        repo = FakeRepo()

        p1, p2 = s3_patches(fs)
        with p1, p2, patch.object(get_mod, '_open_source_odb',
                                  return_value=(repo, odb)):
            written, failed = get_mod.worker_upload_to_s3(manifest_dir, 0)

        assert (written, failed) == (2, 0)
        # The objects landed under the right per-row prefix...
        assert fs.objects['s3://bucket/fastqs/AF013-A/R1.fq'] == \
            blobs['AF013-A/R1.fq']
        assert fs.objects['s3://bucket/fastqs/AF013-B/R1.fq'] == \
            blobs['AF013-B/R1.fq']
        # ...and the row tags survived into the result file.
        recorded = json.loads(
            (manifest_dir / 'result_0.json').read_text())['results']
        assert sorted(r['row'] for r in recorded) == [0, 1]
        assert all(r['ok'] for r in recorded)
        assert repo.closed

    def test_never_lists_the_source_repository(
        self, tmp_path, monkeypatch, fs, source
    ):
        """The whole point of shipping resolved tasks.

        A worker that ran ``dvc list`` would put N compute nodes onto the
        shared clone's SQLite state db.
        """
        monkeypatch.chdir(tmp_path)
        odb, _, blobs = source
        tasks = [{'row': 0, 'name': 'AF013-A', 'relpath': 'R1.fq',
                  'md5': md5_of(blobs['AF013-A/R1.fq']), 'size': 400}]
        manifest_dir = self._manifest(tmp_path, source, tasks)

        p1, p2 = s3_patches(fs)
        with p1, p2, \
                patch.object(get_mod, '_open_source_odb',
                             return_value=(FakeRepo(), odb)), \
                patch.object(get_mod, 'list_source_files') as listing:
            get_mod.worker_upload_to_s3(manifest_dir, 0)

        listing.assert_not_called()

    def test_an_empty_partition_still_writes_a_result_file(
        self, tmp_path, monkeypatch, fs, source
    ):
        """Absence of a result file has to mean "did not finish"."""
        monkeypatch.chdir(tmp_path)
        manifest_dir = self._manifest(tmp_path, source, [])

        p1, p2 = s3_patches(fs)
        with p1, p2:
            written, failed = get_mod.worker_upload_to_s3(manifest_dir, 0)

        assert (written, failed) == (0, 0)
        assert json.loads(
            (manifest_dir / 'result_0.json').read_text()) == {'results': []}

    def test_preflights_for_itself(self, tmp_path, monkeypatch, fs, source):
        """--dest-account-id must be enforced on the node doing the writing."""
        monkeypatch.chdir(tmp_path)
        odb, _, blobs = source
        tasks = [{'row': 0, 'name': 'AF013-A', 'relpath': 'R1.fq',
                  'md5': md5_of(blobs['AF013-A/R1.fq']), 'size': 400}]
        manifest_dir = self._manifest(
            tmp_path, source,
            tasks,
            dest_config={'profile': None, 'endpoint_url': None,
                         'region': None, 'account_id': '999999999999'},
        )

        with patch.object(get_dest.S3Dest, 'fs', property(lambda self: fs)), \
                patch.object(get_dest.S3Dest, 'preflight',
                             side_effect=GetError('Destination account '
                                                  'mismatch')), \
                patch.object(get_mod, '_open_source_odb',
                             return_value=(FakeRepo(), odb)):
            with pytest.raises(GetError, match='account mismatch'):
                get_mod.worker_upload_to_s3(manifest_dir, 0)

    def test_submitter_jobs_value_is_authoritative(
        self, tmp_path, monkeypatch, fs, source
    ):
        monkeypatch.chdir(tmp_path)
        odb, _, blobs = source
        tasks = [{'row': 0, 'name': 'A', 'relpath': 'R1.fq',
                  'md5': md5_of(blobs['AF013-A/R1.fq']), 'size': 400}]
        manifest_dir = self._manifest(tmp_path, source, tasks, jobs=5)

        p1, p2 = s3_patches(fs)
        with p1, p2, \
                patch.object(get_mod, '_open_source_odb',
                             return_value=(FakeRepo(), odb)), \
                patch.object(get_mod, '_upload_all',
                             return_value=[('R1.fq', True, 'uploaded')]) as up:
            get_mod.worker_upload_to_s3(manifest_dir, 0)

        assert up.call_args[0][2] == 5


# =============================================================================
# The submitter, end to end through a real manifest
# =============================================================================

class TestDistributedUploadToS3:

    def _root(self, fs):
        dest = get_dest.S3Dest('s3://bucket/fastqs')
        dest._fs = fs
        return dest

    def _run(self, tmp_path, fs, source, targets, num_workers=2,
             submit=run_workers_inline, monitor=True):
        odb, entries, _ = source

        def listing(clone, path, rev=None):
            if path not in entries:
                raise GetError(f'Not found in the source repository: {path}')
            return entries[path]

        p1, p2 = s3_patches(fs)
        with p1, p2, \
                patch.object(get_mod.tmp_mod, 'clone_repo',
                             return_value=tmp_path / 'clone'), \
                patch.object(get_mod, 'list_source_files', side_effect=listing), \
                patch.object(get_mod, '_open_source_odb',
                             return_value=(FakeRepo(), odb)), \
                patch.object(hpc, 'require_qxub'), \
                patch.object(hpc, 'submit_workers', side_effect=submit), \
                patch.object(hpc, 'monitor_jobs', return_value=monitor):
            return get_mod.distributed_upload_to_s3(
                repository='myrepo',
                targets=targets,
                root=self._root(fs),
                num_workers=num_workers,
                jobs=2,
                chunk_size=64,
            )

    def test_round_trip_places_every_object_and_reports_per_row(
        self, tmp_path, monkeypatch, fs, source
    ):
        monkeypatch.chdir(tmp_path)
        _, _, blobs = source

        job_ids, manifest_dir, results = self._run(
            tmp_path, fs, source,
            [('data/fq/AF013-A', 'AF013-A'), ('data/fq/AF013-B', 'AF013-B')],
        )

        assert len(job_ids) >= 1
        assert [r[0] for r in results] == \
            ['data/fq/AF013-A', 'data/fq/AF013-B']
        assert all(ok for _, ok, _ in results), results
        assert set(fs.objects) >= {
            's3://bucket/fastqs/AF013-A/R1.fq',
            's3://bucket/fastqs/AF013-A/R2.fq',
            's3://bucket/fastqs/AF013-B/R1.fq',
        }
        for name, data in blobs.items():
            assert fs.objects[f's3://bucket/fastqs/{name}'] == data

    def test_partitions_by_size_not_by_row(
        self, tmp_path, monkeypatch, fs, source
    ):
        """Row A holds 500 bytes and row B holds 50, over two workers.

        A per-row split would give one worker 10x the other's bytes; a per-file
        split puts A's 400-byte file alone and the other two together.
        """
        monkeypatch.chdir(tmp_path)
        _, _, blobs = source

        _, manifest_dir, _ = self._run(
            tmp_path, fs, source,
            [('data/fq/AF013-A', 'AF013-A'), ('data/fq/AF013-B', 'AF013-B')],
        )

        loads = []
        for worker_id in (0, 1):
            _, tasks = hpc.load_worker_partition(manifest_dir, worker_id)
            loads.append(sum(t['size'] for t in tasks))
        assert sorted(loads) == [150, 400]

    def test_a_single_path_with_no_trailing_slash_lands_on_the_root(
        self, tmp_path, monkeypatch, fs, source
    ):
        """``-o s3://b/p`` uses the prefix verbatim, so the row has no name.

        That None has to survive a JSON round trip through the manifest and
        come back as "use the root itself" rather than a prefix literally
        called "None".
        """
        monkeypatch.chdir(tmp_path)
        _, _, blobs = source

        _, _, results = self._run(
            tmp_path, fs, source, [('data/fq/AF013-B', None)],
        )

        assert results == [('data/fq/AF013-B', True,
                            '1 file -> s3://bucket/fastqs')]
        assert fs.objects['s3://bucket/fastqs/R1.fq'] == blobs['AF013-B/R1.fq']

    def test_a_row_that_does_not_resolve_is_reported_not_fatal(
        self, tmp_path, monkeypatch, fs, source
    ):
        monkeypatch.chdir(tmp_path)

        _, _, results = self._run(
            tmp_path, fs, source,
            [('data/fq/AF013-A', 'AF013-A'), ('data/fq/nope', 'nope')],
        )

        assert results[0][1] is True
        assert results[1][1] is False
        assert 'Not found' in results[1][2]

    def test_nothing_resolving_at_all_is_an_error(
        self, tmp_path, monkeypatch, fs, source
    ):
        monkeypatch.chdir(tmp_path)

        with pytest.raises(GetError, match='No row resolved'):
            self._run(tmp_path, fs, source, [('data/fq/nope', 'nope')])

    def test_no_wait_returns_before_collating(
        self, tmp_path, monkeypatch, fs, source
    ):
        monkeypatch.chdir(tmp_path)
        odb, entries, _ = source

        p1, p2 = s3_patches(fs)
        with p1, p2, \
                patch.object(get_mod.tmp_mod, 'clone_repo',
                             return_value=tmp_path / 'clone'), \
                patch.object(get_mod, 'list_source_files',
                             side_effect=lambda c, p, rev=None: entries[p]), \
                patch.object(hpc, 'require_qxub'), \
                patch.object(hpc, 'submit_workers', return_value=['1.pbs']), \
                patch.object(hpc, 'monitor_jobs') as monitor:
            job_ids, manifest_dir, results = get_mod.distributed_upload_to_s3(
                repository='myrepo',
                targets=[('data/fq/AF013-A', 'AF013-A')],
                root=self._root(fs),
                num_workers=2,
                wait=False,
            )

        assert job_ids == ['1.pbs']
        assert results is None
        assert manifest_dir.exists()
        monitor.assert_not_called()

    def test_failing_to_submit_anything_names_the_manifest(
        self, tmp_path, monkeypatch, fs, source
    ):
        monkeypatch.chdir(tmp_path)

        with pytest.raises(GetError, match='No worker jobs were submitted'):
            self._run(tmp_path, fs, source,
                      [('data/fq/AF013-A', 'AF013-A')],
                      submit=lambda *a, **k: [])

    def test_missing_qxub_is_a_get_error(self, tmp_path, monkeypatch, fs, source):
        monkeypatch.chdir(tmp_path)

        with patch.object(hpc, 'require_qxub',
                          side_effect=hpc.HPCError('qxub not found')):
            with pytest.raises(GetError, match='qxub not found'):
                get_mod.distributed_upload_to_s3(
                    repository='myrepo',
                    targets=[('data/fq/AF013-A', 'AF013-A')],
                    root=self._root(fs),
                    num_workers=2,
                )

    def test_worker_results_survive_a_reported_job_failure(
        self, tmp_path, monkeypatch, fs, source
    ):
        """A non-zero PBS exit must not discard what the workers did place."""
        monkeypatch.chdir(tmp_path)

        _, _, results = self._run(
            tmp_path, fs, source,
            [('data/fq/AF013-A', 'AF013-A')],
            monitor=False,
        )

        assert results[0][1] is True

    def test_the_manifest_carries_no_secrets(
        self, tmp_path, monkeypatch, fs, source
    ):
        """Profile name, endpoint and region travel; key material never does."""
        monkeypatch.chdir(tmp_path)
        odb, entries, _ = source
        root = get_dest.S3Dest(
            's3://bucket/fastqs',
            get_dest.S3DestConfig(profile='collab-aws', region='ap-southeast-2'),
        )
        root._fs = fs

        p1, p2 = s3_patches(fs)
        with p1, p2, \
                patch.object(get_mod.tmp_mod, 'clone_repo',
                             return_value=tmp_path / 'clone'), \
                patch.object(get_mod, 'list_source_files',
                             side_effect=lambda c, p, rev=None: entries[p]), \
                patch.object(hpc, 'require_qxub'), \
                patch.object(hpc, 'submit_workers', return_value=['1.pbs']), \
                patch.object(hpc, 'monitor_jobs', return_value=True):
            _, manifest_dir, _ = get_mod.distributed_upload_to_s3(
                repository='myrepo',
                targets=[('data/fq/AF013-A', 'AF013-A')],
                root=root,
                num_workers=1,
                wait=False,
            )

        raw = (manifest_dir / 'manifest.json').read_text()
        metadata = json.loads(raw)
        assert metadata['dest_config'] == {
            'profile': 'collab-aws', 'endpoint_url': None,
            'region': 'ap-southeast-2', 'account_id': None,
        }
        assert 'aws_secret_access_key' not in raw.lower()
        assert 'secret' not in raw.lower()
