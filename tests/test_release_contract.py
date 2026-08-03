"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Guards on the release contract: declared dependency floors, the packs publish
gate, and pack version bumps.

None of this is runtime behaviour, which is exactly why it rots silently. A
dependency floor that is a lie only shows up in the one environment nobody
tests (an operator's pre-existing venv, a `--resolution lowest` build), and a
pack whose version is not bumped is simply never published, so the bug looks
like "my fix did nothing". Both are cheap to assert statically.

The cross-repository checks skip when the sibling checkout is absent, so this
module still runs in a core-only clone.
"""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - 3.10 path
    import tomli as tomllib

import yaml
from packaging.requirements import Requirement
from packaging.version import Version

CORE_ROOT = Path(__file__).resolve().parent.parent
WORKSPACE = CORE_ROOT.parent
CLI_ROOT = WORKSPACE / "cli"
PACKS_ROOT = WORKSPACE / "packs"


def _lower_bound(pyproject: Path, package: str) -> Version:
    """Return the `>=` floor declared for ``package`` in ``pyproject``."""
    data = tomllib.loads(pyproject.read_text(encoding="utf-8"))
    for raw in data["project"]["dependencies"]:
        req = Requirement(raw)
        if req.name.replace("_", "-").lower() != package:
            continue
        floors = [
            Version(spec.version)
            for spec in req.specifier
            if spec.operator in (">=", "==")
        ]
        assert floors, f"{package} declared without a lower bound: {raw!r}"
        return max(floors)
    raise AssertionError(f"{package} is not a dependency of {pyproject}")


class TestPolarsFloors:
    """The declared polars range must not contain versions that cannot work.

    Below 1.25 polars' ``collect()`` swallows unknown keywords into
    ``**kwargs``, so ``engine="streaming"`` is silently ignored and the query
    runs in memory — no error, just the OOM this migration exists to
    prevent. 1.25-1.29 crash in
    the parquet/partitioned-sink paths core relies on, and 1.25-1.33 hard-panic
    (pyo3 ``PanicException``, uncatchable by ``except Exception``) in the CLI's
    ``scan_json``.
    """

    def test_core_floor_is_at_least_1_30(self) -> None:
        floor = _lower_bound(CORE_ROOT / "pyproject.toml", "polars")
        assert floor >= Version("1.30.0"), (
            "qalita-core uses collect(engine='streaming') and "
            "pl.PartitionMaxSize(file_path=...); its own streaming/analytics "
            f"suites only pass from polars 1.30.0, but the floor is {floor}"
        )

    def test_cli_floor_is_at_least_1_34(self) -> None:
        pyproject = CLI_ROOT / "pyproject.toml"
        if not pyproject.is_file():
            pytest.skip("cli checkout not present next to core")
        floor = _lower_bound(pyproject, "polars")
        assert floor >= Version("1.34.0"), (
            "the CLI data path scans JSON sources, which panics in polars "
            f"1.25-1.33; the first good version is 1.34.0 but the floor is "
            f"{floor}"
        )


def _publish_workflow() -> dict:
    workflow = PACKS_ROOT / ".github" / "workflows" / "publish.yml"
    if not workflow.is_file():
        pytest.skip("packs checkout not present next to core")
    return yaml.safe_load(workflow.read_text(encoding="utf-8"))


class TestPacksPublishGate:
    """Publishing a pack that cannot be installed takes every job down.

    Packs pin a `qalita-core` floor and `scripts/run.sh` runs `uv lock` on the
    worker for every job, so pushing packs before the matching core release is
    on PyPI makes every analysis fail with "No solution found". The workflow
    has to prove resolvability before it pushes anything, and it has to do so
    on the pull request too — the push trigger only fires after the merge.
    """

    def test_runs_on_pull_requests(self) -> None:
        # PyYAML parses the bare `on:` key as the boolean True.
        triggers = _publish_workflow()[True]
        assert "pull_request" in triggers, (
            "the gate must run on PRs; on push-to-main it can only stop the "
            "publish, not the merge that caused it"
        )

    def test_a_resolution_job_gates_the_publish_job(self) -> None:
        jobs = _publish_workflow()["jobs"]
        publish = jobs["publish"]
        needs = publish.get("needs")
        needs = [needs] if isinstance(needs, str) else list(needs or [])
        assert needs, "publish must depend on a resolution gate job"
        for gate in needs:
            steps = jobs[gate]["steps"]
            script = "\n".join(s.get("run", "") for s in steps)
            if "uv lock" in script:
                break
        else:
            raise AssertionError(
                "no job in publish's `needs` resolves dependencies with "
                "`uv lock`; a guard that does not resolve proves nothing"
            )

    def test_publish_job_never_runs_on_pull_requests(self) -> None:
        # The publish job holds the platform credentials; adding the PR
        # trigger must not turn every PR into a release.
        publish = _publish_workflow()["jobs"]["publish"]
        assert "github.event_name == 'push'" in publish.get("if", "")

    def test_gate_runs_before_any_push(self) -> None:
        # The push loop publishes as it iterates, so a mid-loop failure leaves
        # a half-published set. Resolution therefore lives in its own job, not
        # in an earlier step of the publishing job.
        jobs = _publish_workflow()["jobs"]
        publish_script = "\n".join(
            step.get("run", "") for step in jobs["publish"]["steps"]
        )
        assert "uv lock" not in publish_script


def _git(*args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(PACKS_ROOT), *args],
        capture_output=True,
        text=True,
        check=True,
    ).stdout


class TestPackVersionBumps:
    """A pack whose content changed but whose version did not is never shipped.

    `qalita pack push` keys on the `version` in properties.yaml, so an edited
    pack that keeps its old version is uploaded and then ignored: the fix looks
    applied in git and is absent everywhere else.
    """

    def test_every_changed_pack_bumps_properties_version(self) -> None:
        if not (PACKS_ROOT / ".git").exists():
            pytest.skip("packs checkout is not a git repository")
        try:
            base = _git("merge-base", "main", "HEAD").strip()
        except subprocess.CalledProcessError:
            pytest.skip("packs has no `main` ref to diff against")
        if not base:
            pytest.skip("no merge base with main")

        changed = set(_git("diff", "--name-only", base, "HEAD").split())
        stale = []
        for conf in sorted(PACKS_ROOT.glob("*/pack_conf.json")):
            pack = conf.parent
            prefix = f"{pack.name}/"
            touched = {
                path
                for path in changed
                if path.startswith(prefix) and not path.endswith("/uv.lock")
            }
            if not touched:
                continue
            head = yaml.safe_load(
                (pack / "properties.yaml").read_text(encoding="utf-8")
            )["version"]
            try:
                previous = yaml.safe_load(
                    _git("show", f"{base}:{pack.name}/properties.yaml")
                )["version"]
            except subprocess.CalledProcessError:
                continue  # brand new pack: nothing to bump from
            if Version(str(head)) <= Version(str(previous)):
                stale.append(f"{pack.name} (still {head})")

        assert not stale, (
            "changed but not version-bumped, so the change will never be "
            "published: " + ", ".join(stale)
        )
