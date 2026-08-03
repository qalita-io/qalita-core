"""
# QALITA (c) COPYRIGHT 2025 - ALL RIGHTS RESERVED -

Static guard against the constructs that put a whole dataset in RAM.

A memory ceiling that is only asserted at runtime is one un-exercised code path
away from being wrong. This module reads the source instead: an eager read is a
syntactic fact, so it can be rejected before anything runs, on every file,
including the branches no test covers.

It is deliberately AST-based rather than grep-based. ``pl.read_parquet`` written
in a docstring explaining why not to call it, or in a comment, is not a call —
a regex cannot tell the difference and would make the documentation the reason
the build is red.

Every rule can be waived in place with a trailing marker::

    df = pl.read_parquet(tiny_manifest)  # streaming-ok: 12-row config manifest

Waivers are reported with their reason, so ``--show-waivers`` gives the list of
places the project has consciously stepped outside the streaming path.

Usable as a library, as a pytest module, and as a CLI so the packs repository
can reuse it::

    python tests/test_no_eager_reads.py qalita_core ../packs
"""

from __future__ import annotations

import ast
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Sequence

# Optional on purpose: the CLI has to run in a bare interpreter, so a packs
# pipeline can gate on this file without installing the core test extras.
try:
    import pytest
except ImportError:  # pragma: no cover - exercised only in the bare CLI
    pytest = None

__all__ = [
    "Violation",
    "scan_source",
    "scan_file",
    "scan_paths",
    "format_report",
    "main",
]


REPO_ROOT = Path(__file__).resolve().parent.parent

# Eager module-level readers. Each one returns a fully materialized frame, so
# the memory cost is the dataset, not the query.
_EAGER_READER_MODULES = {"pd", "pandas", "pl", "polars"}
_EAGER_READERS = {"read_parquet"}

# Methods that convert a bounded, lazy or streaming structure into an unbounded
# in-memory one, or that walk it a row at a time in Python.
_METHOD_RULES = {
    "to_pandas": (
        "to-pandas",
        "materializes the whole frame as pandas; keep the LazyFrame and use "
        "qalita_core.analytics (agg / sample / failures) instead",
    ),
    "iterrows": (
        "row-iteration",
        "iterates the frame row by row in Python; use analytics.agg() for "
        "statistics or analytics.failures() for bounded row evidence",
    ),
    "itertuples": (
        "row-iteration",
        "iterates the frame row by row in Python; use analytics.agg() for "
        "statistics or analytics.failures() for bounded row evidence",
    ),
    "apply": (
        "python-apply",
        "runs a Python callback per row/group and forces materialization; "
        "express the computation as a polars expression inside analytics.agg()",
    ),
}

# `collect` is not a dataframe method everywhere. `gc.collect()` is the one that
# actually occurs in this codebase; anything else keeps needing a waiver, since
# guessing more broadly would start hiding real eager collects.
_COLLECT_SAFE_OWNERS = {"gc"}

_COLLECT_RULE = (
    "eager-collect",
    'collect() without engine="streaming" uses the in-memory engine; call it '
    'through qalita_core.analytics, or pass engine="streaming" explicitly',
)

_EAGER_READ_MESSAGE = (
    "reads the entire file into memory; use pl.scan_parquet() (or "
    "Pack.scan()) and qalita_core.analytics instead"
)

# `# streaming-ok: <reason>` anywhere on a line covered by the offending call.
_WAIVER = re.compile(r"#\s*streaming-ok\s*:?\s*(?P<reason>.*)$")

_SKIP_DIRS = {
    ".git",
    ".venv",
    "venv",
    "__pycache__",
    ".pytest_cache",
    ".mypy_cache",
    "node_modules",
    "build",
    "dist",
    ".tox",
}


@dataclass(frozen=True)
class Violation:
    """One eager construct, or one waived one."""

    path: str
    line: int
    column: int
    rule: str
    source: str
    message: str
    waiver: str | None = None

    @property
    def waived(self) -> bool:
        return self.waiver is not None

    def format(self) -> str:
        where = f"{self.path}:{self.line}:{self.column}"
        if self.waived:
            reason = self.waiver or "(no reason given)"
            return f"{where}: [waived] {self.rule}: {self.source} -- {reason}"
        return f"{where}: {self.rule}: {self.source}\n    {self.message}"


def _dotted(node: "ast.AST") -> str:
    """Best-effort dotted name for an expression used as a call target."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        prefix = _dotted(node.value)
        return f"{prefix}.{node.attr}" if prefix else node.attr
    return ""


def _snippet(lines: Sequence[str], node: "ast.AST") -> str:
    line = getattr(node, "lineno", 0)
    if not 1 <= line <= len(lines):
        return ""
    return lines[line - 1].strip()


def _waiver_for(lines: Sequence[str], node: "ast.AST") -> str | None:
    """Reason from a ``# streaming-ok`` marker covering ``node``, if any.

    The marker is accepted anywhere between the first and last line of the call
    so a waiver still works on a call split across several lines by Black.
    """
    start = getattr(node, "lineno", 0)
    end = getattr(node, "end_lineno", start) or start
    for index in range(start, min(end, len(lines)) + 1):
        if index < 1:
            continue
        match = _WAIVER.search(lines[index - 1])
        if match:
            return match.group("reason").strip()
    return None


def _has_streaming_engine(node: "ast.Call") -> bool:
    for keyword in node.keywords:
        if keyword.arg != "engine":
            continue
        value = keyword.value
        return isinstance(value, ast.Constant) and value.value == "streaming"
    return False


def scan_source(source: str, path: str = "<string>") -> list[Violation]:
    """Every eager construct in one Python source string.

    A file that does not parse is reported as a violation rather than skipped:
    silently ignoring it would be a hole in the guard.
    """
    try:
        tree = ast.parse(source, filename=path)
    except SyntaxError as exc:
        return [
            Violation(
                path=path,
                line=exc.lineno or 0,
                column=exc.offset or 0,
                rule="unparsable",
                source=(exc.text or "").strip(),
                message=f"file could not be parsed, so it cannot be checked: {exc.msg}",
            )
        ]

    lines = source.splitlines()
    found: list[Violation] = []

    def record(node: "ast.Call", rule: str, message: str) -> None:
        found.append(
            Violation(
                path=path,
                line=node.lineno,
                column=node.col_offset,
                rule=rule,
                source=_snippet(lines, node),
                message=message,
                waiver=_waiver_for(lines, node),
            )
        )

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if not isinstance(func, ast.Attribute):
            continue

        attr = func.attr
        owner = _dotted(func.value)

        if attr in _EAGER_READERS and owner in _EAGER_READER_MODULES:
            record(node, "eager-read", _EAGER_READ_MESSAGE)
            continue

        if attr in _METHOD_RULES:
            rule, message = _METHOD_RULES[attr]
            record(node, rule, message)
            continue

        if (
            attr == "collect"
            and owner not in _COLLECT_SAFE_OWNERS
            and not _has_streaming_engine(node)
        ):
            record(node, _COLLECT_RULE[0], _COLLECT_RULE[1])

    return found


def scan_file(path: "str | os.PathLike[str]") -> list[Violation]:
    target = Path(path)
    return scan_source(
        target.read_text(encoding="utf-8", errors="replace"), str(target)
    )


def _python_files(root: Path) -> Iterator[Path]:
    if root.is_file():
        yield root
        return
    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in _SKIP_DIRS]
        for name in sorted(filenames):
            if name.endswith(".py"):
                yield Path(dirpath) / name


def scan_paths(
    paths: Iterable["str | os.PathLike[str]"],
    *,
    include_waived: bool = False,
) -> list[Violation]:
    """Every violation under ``paths``, waived ones filtered out by default."""
    violations: list[Violation] = []
    for path in paths:
        for file in _python_files(Path(path)):
            violations.extend(scan_file(file))
    if include_waived:
        return violations
    return [v for v in violations if not v.waived]


def format_report(violations: Sequence[Violation]) -> str:
    if not violations:
        return "no eager reads found"
    body = "\n".join(v.format() for v in violations)
    return f"{len(violations)} eager construct(s) found:\n{body}"


# --------------------------------------------------------------------------
# pytest
# --------------------------------------------------------------------------

# The modules that define the streaming contract. These are held to zero
# violations unconditionally: if the primitives themselves read eagerly, every
# ceiling asserted anywhere else is meaningless.
CONTRACT_MODULES = (
    REPO_ROOT / "qalita_core" / "analytics.py",
    REPO_ROOT / "qalita_core" / "profiling.py",
    REPO_ROOT / "qalita_core" / "pack.py",
)


def test_contract_modules_have_no_eager_reads():
    violations = scan_paths(CONTRACT_MODULES)
    assert not violations, format_report(violations)


def test_checker_flags_every_eager_construct(tmp_path):
    source = tmp_path / "offender.py"
    source.write_text(
        "\n".join(
            [
                "import pandas as pd",
                "import polars as pl",
                "",
                "def go(path, lf, df):",
                "    a = pd.read_parquet(path)",
                "    b = pl.read_parquet(path)",
                "    c = lf.collect()",
                "    d = df.to_pandas()",
                "    for row in df.iterrows():",
                "        pass",
                "    for row in df.itertuples():",
                "        pass",
                "    e = df.apply(lambda r: r)",
                "    return a, b, c, d, e",
            ]
        ),
        encoding="utf-8",
    )
    rules = sorted({v.rule for v in scan_paths([source])})
    assert rules == [
        "eager-collect",
        "eager-read",
        "python-apply",
        "row-iteration",
        "to-pandas",
    ]
    assert len(scan_paths([source])) == 7


def test_streaming_collect_is_allowed(tmp_path):
    source = tmp_path / "good.py"
    source.write_text(
        "def go(lf):\n"
        '    return lf.collect(engine="streaming")\n'
        "\n"
        "def schema(lf):\n"
        "    return lf.collect_schema()\n",
        encoding="utf-8",
    )
    assert scan_paths([source]) == []


def test_gc_collect_is_not_a_dataframe_collect(tmp_path):
    source = tmp_path / "cleanup.py"
    source.write_text(
        "import gc\n\n\ndef go(lf):\n    gc.collect()\n"
        '    return lf.collect(engine="streaming")\n',
        encoding="utf-8",
    )
    assert scan_paths([source]) == []


def test_non_literal_engine_is_not_trusted(tmp_path):
    """``engine=ENGINE`` may be anything at runtime, including in-memory."""
    source = tmp_path / "indirect.py"
    source.write_text(
        "ENGINE = 'in-memory'\n\n\ndef go(lf):\n    return lf.collect(engine=ENGINE)\n",
        encoding="utf-8",
    )
    violations = scan_paths([source])
    assert [v.rule for v in violations] == ["eager-collect"]


def test_docstrings_and_comments_are_not_calls(tmp_path):
    source = tmp_path / "prose.py"
    source.write_text(
        '"""Never call pd.read_parquet( or df.to_pandas() here."""\n'
        "\n"
        "# lf.collect() is what this module replaces.\n"
        "MESSAGE = 'do not use pl.read_parquet(path) on a big source'\n",
        encoding="utf-8",
    )
    assert scan_paths([source]) == []


def test_waiver_suppresses_and_records_the_reason(tmp_path):
    source = tmp_path / "waived.py"
    source.write_text(
        "import polars as pl\n"
        "\n"
        "def go(path):\n"
        "    # streaming-ok: 12-row manifest, bounded by construction\n"
        "    return pl.read_parquet(path)  "
        "# streaming-ok: 12-row manifest, bounded by construction\n",
        encoding="utf-8",
    )
    assert scan_paths([source]) == []

    waived = scan_paths([source], include_waived=True)
    assert len(waived) == 1
    assert waived[0].waived
    assert "12-row manifest" in (waived[0].waiver or "")


def test_multiline_call_accepts_a_waiver_on_any_of_its_lines(tmp_path):
    source = tmp_path / "multiline.py"
    source.write_text(
        "import polars as pl\n"
        "\n"
        "def go(path):\n"
        "    return pl.read_parquet(  # streaming-ok: schema probe only\n"
        "        path,\n"
        "        columns=['a'],\n"
        "    )\n",
        encoding="utf-8",
    )
    assert scan_paths([source]) == []


def test_unparsable_file_is_reported_not_skipped(tmp_path):
    source = tmp_path / "broken.py"
    source.write_text("def go(:\n    pass\n", encoding="utf-8")
    violations = scan_paths([source])
    assert [v.rule for v in violations] == ["unparsable"]


def test_report_names_file_line_and_replacement(tmp_path):
    source = tmp_path / "offender.py"
    source.write_text(
        "def go(lf):\n    return lf.collect()\n", encoding="utf-8"
    )
    report = format_report(scan_paths([source]))
    assert "offender.py:2:" in report
    assert 'engine="streaming"' in report


def test_package_scan_is_reportable():
    """The whole package must at least be scannable end to end.

    Kept separate from the contract-module assertion because the migration to
    the streaming API is still landing across ``qalita_core``; this asserts the
    tool works over the package, ``main()`` is what gates a full-package run
    (see the ``streaming-check`` job in .github/workflows/ci.yml).
    """
    violations = scan_paths([REPO_ROOT / "qalita_core"], include_waived=True)
    assert all(v.rule != "unparsable" for v in violations), format_report(
        [v for v in violations if v.rule == "unparsable"]
    )


# --------------------------------------------------------------------------
# CLI
# --------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="test_no_eager_reads",
        description="Fail on constructs that read a dataset into memory.",
    )
    parser.add_argument(
        "paths",
        nargs="*",
        default=[str(REPO_ROOT / "qalita_core")],
        help="files or directories to check (default: qalita_core/)",
    )
    parser.add_argument(
        "--show-waivers",
        action="store_true",
        help="also list the '# streaming-ok:' waivers and their reasons",
    )
    args = parser.parse_args(argv)

    paths = args.paths or [str(REPO_ROOT / "qalita_core")]
    violations = scan_paths(paths)
    print(format_report(violations))

    if args.show_waivers:
        waived = [
            v for v in scan_paths(paths, include_waived=True) if v.waived
        ]
        print(f"\n{len(waived)} waiver(s):")
        for violation in waived:
            print(violation.format())

    return 1 if violations else 0


if __name__ == "__main__":  # pragma: no cover - CLI entry point
    sys.exit(main(sys.argv[1:]))
