# CI workflows

This directory contains the GitHub Actions workflows for `dvc_tools`.

| Workflow | Trigger | Purpose |
| --- | --- | --- |
| [`version-check.yml`](version-check.yml) | PR to `main` | Block merges where `pyproject.toml` and `dt/__init__.py` disagree, or where the version has not been bumped relative to the merge target. |
| [`deploy.yml`](deploy.yml) | Merge to `main` (or `workflow_dispatch`) | Build the `dt` conda environment as an OCI image (`ghcr.io/swarbricklab/dt:<version>`) on a GCP remote builder via [absconda](https://github.com/swarbricklab/absconda), then deploy that image to NCI as a Singularity environment module via [qxub](https://github.com/swarbricklab/qxub). |

## Architecture (deploy.yml)

1. **`publish` job** — runs on a GitHub-hosted runner (`ubuntu-latest`); it needs nothing from NCI.
   - Authenticates to GCP via Workload Identity Federation (org-level secrets `WIF_PROVIDER`, `SERVICE_ACCOUNT`, `PROJECT_ID`).
   - Resolves the runner's OS Login POSIX username (different SA → different username).
   - Installs absconda from `main` and configures it via `absconda config set` (no committed `absconda.yaml`).
   - Calls `absconda publish --file conda/dt.yaml ...`. Absconda generates a Dockerfile internally from the conda env spec, uploads the build context to a GCE builder VM over an IAP tunnel, runs `docker build`, and pushes to GHCR.
   - Build output is shown in a collapsed group in the job log.
2. **`deploy` job** — runs on the self-hosted runner (label from the `RUNNER_LABEL` repository variable), which reaches NCI over SSH; pulls the image on a NCI compute node via `qxub exec --mod absconda -- absconda deploy`, which materialises the conda env into `/opt/conda/envs/dt` and writes Singularity shim wrappers + a TCL modulefile.

## Configuration

Deployment-specific values are repository secrets so they are masked in public run logs: `REMOTE_BUILDER_NAME`, `REMOTE_BUILDER_HOST`, `REMOTE_BUILDER_WORKSPACE`, `GCP_REGION`, `GCP_ZONE`, `QXUB_VENV`, and `GHCR_TOKEN_SECRET` / `GHCR_USERNAME_SECRET` (the Secret Manager secret names holding the GHCR PAT and its username), and `NCI_SCRATCH_ROOT` (the project's `/scratch/<project>` directory, used for the Singularity cache) (plus the org-level `WIF_PROVIDER`, `SERVICE_ACCOUNT`, `PROJECT_ID`, and `USER_NAME`). The runner label is the repository variable `RUNNER_LABEL` (`runs-on` cannot read secrets).

## Operational notes / learnings

These are non-obvious things we hit while bringing the pipeline up. Keep this list short — only record items that are likely to bite again.

- **Org-level secrets.** `WIF_PROVIDER`, `SERVICE_ACCOUNT`, `PROJECT_ID` are organisation-level (`swarbricklab`) and shared with the `absconda` repo. Both repos therefore authenticate as the same SA and share builder permissions.
- **Two GCP identities are involved.** The runner's *attached* SA (used for `gcloud auth list` defaults on the runner VM) is distinct from the *WIF-impersonated* SA the workflow runs as. Only the WIF SA needs IAM bindings on the builder; ignore the attached one when debugging permissions.
- **IAP tunnel race.** `gcloud compute instances start ... done` returns when the GCE control plane sees the VM as `RUNNING`, but `sshd` inside the guest is not yet listening. The first IAP tunnel attempt right after a cold start fails with `ssh: exit 255` / `4003: failed to connect to backend`. The publish job mitigates this with a "Pre-warm builder SSH" step that polls `gcloud compute ssh ... --command=true` for up to ~2.5 min before invoking absconda.
- **Builder workspace permissions.** The builder's workspace dir must be writable by every OS Login user that publishes from CI.
- **`env/` is a default Python `.gitignore` entry.** The conda env spec lives in `conda/dt.yaml` for that reason — never use `env/`.
- **`absconda generate > Dockerfile` corrupts long lines on absconda < 0.2.6.** `Console.print` hard-wraps at the detected terminal width when stdout is redirected, snapping long `RUN` lines mid-shell. We sidestep this entirely by using `absconda publish --file conda/dt.yaml` (no Dockerfile is committed). (See [absconda#50](https://github.com/swarbricklab/absconda/pull/50).)
- **`pip install --user` and PATH.** The runner does not have `~/.local/bin` on `$PATH`, and its system Python refuses `--user` installs (PEP 668), so `publish` uses `actions/setup-python` first. After `pip install --user`, append the user-base bin dir to `$GITHUB_PATH` explicitly.
- **Publish log.** The build is verbose (~1.5k lines), so it is wrapped in a collapsed Actions log group. It is not uploaded as an artifact: secret masking does not apply to artifact contents.
- **pip wheel cache traps unpinned `git+https://` deps.** The conda env spec installs `dvc_tools` and `qxub` from git URLs. pip's wheel cache keys on URL only, so on a warm builder every subsequent build silently reuses whichever SHA was built first (we hit this when 0.5.9 still reported `dt --version` 0.5.3). The "Pin git deps to commit SHAs" step rewrites both URLs to include `@<sha>` before passing the spec to absconda — this both invalidates the cache and guarantees the image contains the same code as the workflow run.
- **Singularity GHCR auth requires persistent credentials, not just env vars.** `absconda deploy` calls `singularity pull docker://ghcr.io/...` without explicit `--docker-username`/`--docker-password` flags, relying instead on `SINGULARITY_DOCKER_*` env vars. In practice, NCI's singularity ignores these in favour of credentials saved by `singularity remote login`. The runner is **not** an NCI host (it reaches NCI over SSH), so the deploy job:
  1. fetches the GHCR PAT from Secret Manager and stages it as a short-lived, owner-only file;
  2. copies it to NCI over the same SSH route qxub uses;
  3. runs `singularity remote login docker://ghcr.io` inside `qxub exec --mod singularity` on a compute node; and
  4. removes the staged files in an `if: always()` cleanup step.

  The token never appears on the `qsub` command line or in `qstat -f` output. To rotate the PAT, update the Secret Manager secrets named by `GHCR_TOKEN_SECRET` and `GHCR_USERNAME_SECRET` — no manual `singularity remote login` needed.
- **GHCR username is a bot user, not the org name.** `SINGULARITY_DOCKER_USERNAME` must be a real GitHub user account that owns the PAT, not the org name. The username is stored in the Secret Manager secret named by `GHCR_USERNAME_SECRET`. That account must have Write access on the `ghcr.io/swarbricklab/dt` package (Packages → dt → Package settings → Manage access).

## Open questions

None outstanding. Build is YAML-driven (`conda/dt.yaml`); no Dockerfile is committed.
