#!/usr/bin/env python3
"""Check that every `from mmapq import X` in a README code block resolves
against the actual mmapq.__all__, parsed statically from __init__.py.

Catches the Phase 21 defect class: a README referencing Python names that
don't exist in the package. Does NOT catch:
  - wrong method calls on real classes
  - module-attribute access (mmapq.SomeClass)
  - wrong argument signatures

Usage: check_py_readme_imports.py <block.py> <path/to/mmapq/__init__.py>
Exit 0 if all mmapq imports resolve, 1 otherwise.
"""

import ast
import sys


def parse_all(init_path: str) -> set[str]:
    """Extract __all__ as a set of string literals from __init__.py."""
    with open(init_path) as f:
        tree = ast.parse(f.read())
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if isinstance(target, ast.Name) and target.id == "__all__":
                    if isinstance(node.value, ast.List):
                        return {
                            elt.value for elt in node.value.elts
                            if isinstance(elt, ast.Constant) and isinstance(elt.value, str)
                        }
    return set()


def check_block(block_path: str, all_names: set[str]) -> int:
    """Check all `from mmapq import ...` lines against all_names.
    Returns the count of failures (0 = pass).
    """
    with open(block_path) as f:
        tree = ast.parse(f.read())

    failures = 0
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module == "mmapq":
            for alias in node.names:
                name = alias.asname or alias.name
                if name not in all_names:
                    print(
                        f"  FAIL: 'from mmapq import {name}' — "
                        f"{name!r} is not in mmapq.__all__",
                        file=sys.stderr,
                    )
                    failures += 1
    return failures


def main():
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <block.py> <mmapq/__init__.py>", file=sys.stderr)
        sys.exit(2)

    block_path = sys.argv[1]
    init_path = sys.argv[2]

    all_names = parse_all(init_path)
    if not all_names:
        print("  FAIL: could not parse __all__ from {init_path}", file=sys.stderr)
        sys.exit(1)

    failures = check_block(block_path, all_names)
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
