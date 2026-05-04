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
   - Calls `absconda publish` which uploads the build context to the GCE builder VM (`absconda-builder`) over an IAP tunnel, runs `docker build`, and pushes to GHCR.
   - Full build log is uploaded as the `publish-log-<version>` artifact.
2. **`deploy` job** — also runs on the qxub-runner; pulls the image on a NCI compute node via `qxub exec --mod absconda -- absconda deploy`, which materialises the conda env into `/opt/conda/envs/dt` and writes Singularity shim wrappers + a TCL modulefile.

## Operational notes / learnings

These are non-obvious things we hit while bringing the pipeline up. Keep this list short — only record items that are likely to bite again.

- **Org-level secrets.** `WIF_PROVIDER`, `SERVICE_ACCOUNT`, `PROJECT_ID` are organisation-level (`swarbricklab`) and shared with the `absconda` repo. Both repos therefore authenticate as the same SA and share builder permissions.
- **Two GCP identities are involved.** The runner's *attached* SA (used for `gcloud auth list` defaults on the runner VM) is distinct from the *WIF-impersonated* SA the workflow runs as. Only the WIF SA needs IAM bindings on the builder; ignore the attached one when debugging permissions.
- **IAP tunnel race.** `gcloud compute instances start ... done` returns when the GCE control plane sees the VM as `RUNNING`, but `sshd` inside the guest is not yet listening. The first IAP tunnel attempt right after a cold start can fail with `4003: failed to connect to backend`. Retrying (or pre-warming with a throwaway tunnel attempt) clears it. Absconda's own `start_command` flow is generally fine because absconda retries the SSH probe.
- **Builder workspace permissions.** Absconda uploads its build context to `<workspace>/<name>-<timestamp>-<rand>.tar.gz` on the builder. The workspace dir (`/var/lib/absconda` on `absconda-builder`) must be writable by **every** OS Login user that publishes from CI. We set it to `chmod 1777` (sticky, world-writable). Filenames are timestamp-randomised so collisions are not a concern at our team size.
- **`env/` is a default Python `.gitignore` entry.** Keep the Dockerfile in `docker/`, never `env/`, otherwise it silently disappears in CI checkouts.
- **`absconda generate > Dockerfile` corrupts long lines on absconda < 0.2.6.** `Console.print` hard-wraps at the detected terminal width when stdout is redirected, snapping long `RUN` lines mid-shell. Use `absconda generate --output ...` on older versions, or upgrade. (See [absconda#50](https://github.com/swarbricklab/absconda/pull/50).)
- **`pip install --user` and PATH.** Self-hosted runners do not have `~/.local/bin` on `$PATH`. After `pip install --user`, append the user-base bin dir to `$GITHUB_PATH` explicitly.
- **Publish log artifact.** The build is verbose (~1.5k lines). It is captured to `build-logs/publish-<version>.log`, wrapped in an Actions log group, and uploaded as a workflow artifact for 30 days — pull it from the run page when diagnosing build failures.

## Open questions

### a) Build from the conda env definition (YAML) instead of a Dockerfile

`absconda` accepts an `environment.yaml` directly (`absconda publish --file env.yaml ...`), in which case it generates the Dockerfile internally. **Pros:**

- One source of truth; the env file is human-readable and is the same artifact you would `conda env create` from locally.
- We get whatever Dockerfile improvements newer absconda versions ship for free, no manual regeneration.
- Sidesteps the `generate > Dockerfile` corruption bug entirely.

**Cons:**

- We lose any hand-tuned Dockerfile modifications (currently we have none worth keeping).
- Build context becomes implicit — anything currently `COPY`ied from `docker/` would need to move or be inlined.
- Less obvious how to pin the runtime base image; absconda picks the default.

**Recommendation:** Switch to YAML-driven builds. The Dockerfile we currently commit was generated by absconda anyway and we've never modified it intentionally — every "edit" so far has been to undo absconda bugs.

### b) Don't commit the Dockerfile at all

This is the natural follow-on from (a). The Dockerfile becomes a build artifact, not a source artifact:

- Delete `docker/Dockerfile`, keep `docker/dt.yaml` (the env spec) as the canonical source.
- `deploy.yml` calls `absconda publish --file docker/dt.yaml ...`.
- Optionally upload the rendered Dockerfile to the same artifact bundle as the build log for forensic purposes.

**Pros:** removes a class of drift bugs (Dockerfile vs YAML out of sync; manual fixes lost on next `generate`); shrinks the diff noise on every absconda upgrade; "version bump = bump version + maybe bump a dep in the YAML" instead of "remember to regenerate the Dockerfile".

**Cons:** local `docker build` (without absconda installed) is no longer one command — but nobody on the team does that, the builder VM does. Reviewers can no longer eyeball the Dockerfile in PRs — but they can read the YAML, which is shorter and clearer.

**Recommendation:** Do this immediately after (a). The combination is strictly better than the status quo.
