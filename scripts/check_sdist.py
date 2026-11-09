"""Check an mmapq sdist: contents whitelist, C/Python source sets, metadata.

usage: python3 -I scripts/check_sdist.py <sdist.tar.gz> <repo-root>

Prints one PASS/FAIL line per step; exits 1 if any step failed (all steps
run). stdlib only; never imports the package.
"""
import re
import subprocess
import sys
import tarfile

ALLOWED = (
    r"^(PKG-INFO|setup\.py|setup\.cfg|pyproject\.toml|README\.md|LICENSE|MANIFEST\.in)$",
    r"^src/mmapq/[^/]+\.(py|c)$",
    r"^src/mmapq\.egg-info/[^/]+$",
    r"^tests/[^/]+\.py$",
    r"^c/src/.+\.(c|h)$",
    r"^c/include/.+\.h$",
)

results = []


def record(step, ok, detail=""):
    results.append(ok)
    if ok:
        print("PASS " + step)
    else:
        print("FAIL {}: {}".format(step, detail))


def git(repo, *args):
    return subprocess.run(
        ["git", "-C", repo, *args], check=True, capture_output=True, text=True
    ).stdout.splitlines()


def main():
    sdist, repo = sys.argv[1], sys.argv[2]
    with tarfile.open(sdist) as tf:
        entries = tf.getmembers()
        top = entries[0].name.split("/", 1)[0]
        members = [
            m.name.split("/", 1)[1]
            for m in entries
            if not m.isdir() and "/" in m.name
        ]
        pkg_info = tf.extractfile(top + "/PKG-INFO").read().decode()

    # 1. allowed: every sdist member is whitelisted
    offenders = [
        m for m in members
        if not any(re.match(p, m) for p in ALLOWED)
    ]
    if offenders:
        record("allowed", False, "files outside the whitelist:\n" + "\n".join(offenders))
    else:
        record("allowed", True)

    # 2. c-sources: sdist vendored C == files git tracks in c/
    c_git = sorted("c/" + f for f in git(repo, "-C", "c", "ls-files",
                                         "--", "src/*.c", "src/*.h", "include/*.h"))
    c_pkg = sorted(m for m in members if m.startswith("c/"))
    if c_git != c_pkg:
        detail = "sdist C files != git-tracked C files\n"
        detail += "only in git: " + ", ".join(set(c_git) - set(c_pkg)) + "\n"
        detail += "only in sdist: " + ", ".join(set(c_pkg) - set(c_git))
        record("c-sources", False, detail)
    else:
        record("c-sources", True)

    # 3. py-sources: sdist package sources == working tree (tracked + untracked)
    py_git = sorted(git(repo, "ls-files", "--cached", "--others",
                        "--exclude-standard", "--", "src/mmapq/*"))
    py_pkg = sorted(m for m in members if m.startswith("src/mmapq/")
                    and not m.startswith("src/mmapq.egg-info"))
    if py_git != py_pkg:
        detail = "sdist package files != working-tree package files\n"
        detail += "only in tree: " + ", ".join(set(py_git) - set(py_pkg)) + "\n"
        detail += "only in sdist: " + ", ".join(set(py_pkg) - set(py_git))
        record("py-sources", False, detail)
    else:
        record("py-sources", True)

    # 4. metadata: PKG-INFO agrees with _version.py and pyproject.toml
    version_py = re.search(
        r'__version__ = "([^"]+)"',
        open(repo + "/src/mmapq/_version.py").read(),
    ).group(1)
    fields = {}
    requires = []
    for line in pkg_info.splitlines():
        if line.startswith("Requires-Dist:"):
            requires.append(line[len("Requires-Dist:"):].strip())
        elif ": " in line:
            key, value = line.split(": ", 1)
            fields.setdefault(key, value)
    problems = []
    if fields.get("Version") != version_py:
        problems.append("Version {} != _version.py {}".format(fields.get("Version"), version_py))
    if fields.get("License-Expression") != "BSD-3-Clause":
        problems.append("License-Expression {} != BSD-3-Clause".format(fields.get("License-Expression")))
    if not any("cffi>=1.17.1" in r for r in requires):
        problems.append("Requires-Dist has no cffi>=1.17.1: {}".format(requires))
    if problems:
        record("metadata", False, "; ".join(problems))
    else:
        record("metadata", True)

    if not all(results):
        sys.exit(1)


if __name__ == "__main__":
    main()
