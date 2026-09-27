"""Read the literal assignments of a settings file WITHOUT importing it.

Settings files (``base_settings.py``, ``project_settings.py``,
``strategy_settings.py``) are plain modules of ``NAME = literal`` lines. A
tool that only needs the values — the control panel, the generator, the
migration — must never import one: importing binds nothing here but would
run arbitrary code, and the engines' own binding happens by import order.
"""
from __future__ import annotations

import ast
from pathlib import Path
from typing import Any, Optional

_MISSING = object()


def read_literals(path: Path | str, *, names: Optional[set[str]] = None) -> dict[str, Any]:
    """``{NAME: value}`` for every top-level ``NAME = <literal>`` (and
    annotated ``NAME: T = <literal>``) in the file. Non-literal values are
    skipped; a missing or unparsable file gives ``{}``. *names* restricts
    the result."""
    try:
        tree = ast.parse(Path(path).read_text(encoding="utf-8"))
    except (OSError, SyntaxError, ValueError):
        return {}
    out: dict[str, Any] = {}
    for node in tree.body:
        targets: list[ast.expr] = []
        value: Optional[ast.expr] = None
        if isinstance(node, ast.Assign):
            targets, value = node.targets, node.value
        elif isinstance(node, ast.AnnAssign) and node.value is not None:
            targets, value = [node.target], node.value
        if value is None:
            continue
        for t in targets:
            if not isinstance(t, ast.Name):
                continue
            if names is not None and t.id not in names:
                continue
            try:
                out[t.id] = ast.literal_eval(value)
            except (ValueError, TypeError, SyntaxError):
                continue
    return out


def read_literal(path: Path | str, name: str, default: Any = None) -> Any:
    """One value, or *default* when the name is absent or not a literal."""
    return read_literals(path, names={name}).get(name, default)
