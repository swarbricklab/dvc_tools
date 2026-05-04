# CI workflows

This directory contains the GitHub Actions workflows for `dvc_tools`.

| Workflow | Trigger | Purpose |
| --- | --- | --- |
| [`version-check.yml`](version-check.yml) | PR to `main` | Block merges where `pyproject.toml` and `dt/__init__.py` disagree, or where the version has not been bumped relative to the merge target. |
| [`deploy.yml`](deploy.yml) | Merge to `main` (or `workflow_dispatch`) | Build the `dt` conda environment as an OCI image (`ghcr.io/swarbricklab/dt:<version>`) on a GCP remote builder via [absconda](https://github.com/swarbricklab/absconda), then deploy that image to NCI as a Singularity environment module via [qxub](https://github.com/swarbricklab/qxub). |

## Architecture (deploy.yml)

1. **`publish` job** — runs on a self-hosted `qxub-runner` at NCI.
   - Authenticates to GCP via Workload Identity Federation (org-level secrets `WIF_PROVIDER`, `SERVICE_ACCOUNT`, `PROJECT_ID`).
   - Resolves the runner's OS Login POSIX username (different SA → different username).
   - Installs absconda from `main` and configures it via `absconda config set` (no committed `absconda.yaml`).
   - Calls `absconda publish --file conda/dt.yaml ...`. Absconda generates a Dockerfile internally from the conda env spec, uploads the build context to the GCE builder VM (`absconda-builder`) over an IAP tunnel, runs `docker build`, and pushes to GHCR.
   - Full build log is uploaded as the `publish-log-<version>` artifact.
   - Full build log is uploaded as the `publish-log-<version>` artifact.
2. **`deploy` job** — also runs on the qxub-runner; pulls the image on a NCI compute node via `qxub exec --mod absconda -- absconda deploy`, which materialises the conda env into `/opt/conda/envs/dt` and writes Singularity shim wrappers + a TCL modulefile.

## Operational notes / learnings

These are non-obvious things we hit while bringing the pipeline up. Keep this list short — only record items that are likely to bite again.

- **Org-level secrets.** `WIF_PROVIDER`, `SERVICE_ACCOUNT`, `PROJECT_ID` are organisation-level (`swarbricklab`) and shared with the `absconda` repo. Both repos therefore authenticate as the same SA and share builder permissions.
- **Two GCP identities are involved.** The runner's *attached* SA (used for `gcloud auth list` defaults on the runner VM) is distinct from the *WIF-impersonated* SA the workflow runs as. Only the WIF SA needs IAM bindings on the builder; ignore the attached one when debugging permissions.
- **IAP tunnel race.** `gcloud compute instances start ... done` returns when the GCE control plane sees the VM as `RUNNING`, but `sshd` inside the guest is not yet listening. The first IAP tunnel attempt right after a cold start fails with `ssh: exit 255` / `4003: failed to connect to backend`. The publish job mitigates this with a "Pre-warm builder SSH" step that polls `gcloud compute ssh ... --command=true` for up to ~2.5 min before invoking absconda.
- **Builder workspace permissions.** Absconda uploads its build context to `<workspace>/<name>-<timestamp>-<rand>.tar.gz` on the builder. The workspace dir (`/var/lib/absconda` on `absconda-builder`) must be writable by **every** OS Login user that publishes from CI. We set it to `chmod 1777` (sticky, world-writable). Filenames are timestamp-randomised so collisions are not a concern at our team size.
- **`env/` is a default Python `.gitignore` entry.** The conda env spec lives in `conda/dt.yaml` for that reason — never use `env/`.
- **`absconda generate > Dockerfile` corrupts long lines on absconda < 0.2.6.** `Console.print` hard-wraps at the detected terminal width when stdout is redirected, snapping long `RUN` lines mid-shell. We sidestep this entirely by using `absconda publish --file conda/dt.yaml` (no Dockerfile is committed). (See [absconda#50](https://github.com/swarbricklab/absconda/pull/50).)
- **`pip install --user` and PATH.** Self-hosted runners do not have `~/.local/bin` on `$PATH`. After `pip install --user`, append the user-base bin dir to `$GITHUB_PATH` explicitly.
- **Publish log artifact.** The build is verbose (~1.5k lines). It is captured to `build-logs/publish-<version>.log`, wrapped in an Actions log group, and uploaded as a workflow artifact for 30 days — pull it from the run page when diagnosing build failures.
- **pip wheel cache traps unpinned `git+https://` deps.** The conda env spec installs `dvc_tools` and `qxub` from git URLs. pip's wheel cache keys on URL only, so on a warm builder every subsequent build silently reuses whichever SHA was built first (we hit this when 0.5.9 still reported `dt --version` 0.5.3). The "Pin git deps to commit SHAs" step rewrites both URLs to include `@<sha>` before passing the spec to absconda — this both invalidates the cache and guarantees the image contains the same code as the workflow run.

## Open questions

None outstanding. Build is YAML-driven (`conda/dt.yaml`); no Dockerfile is committed.
