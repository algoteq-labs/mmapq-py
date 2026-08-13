#!/usr/bin/env bash
# check_enum_sync.sh — verify that Python IntEnum members (types.py) match
# C header enums in name, order, and value.
# Hard gate: exits 0 on sync, non-zero on divergence.
#
# C enum members have prefixes (POLL_, NEXT_, BATCH_ENTRY_, CONFLICT_);
# Python IntEnum members are bare. The script strips C prefixes before
# comparing NAME=VALUE pairs in declaration order.
#
# Usage:
#   check_enum_sync.sh            # normal check
#   check_enum_sync.sh --self-test  # mutation self-test
set -euo pipefail

cd "$(dirname "$0")/.."

HAD_ERROR=0

# ── Extract C enum NAME=VALUE pairs for a given type ─────────────────────
# Usage: extract_c_enum <header> <type_name>
# Outputs "NAME=VALUE" lines. Implicit values (no = N) are resolved by
# position starting from 0, or continuing from the last explicit value.
extract_c_enum() {
    local header="$1"
    local type_name="$2"
    awk -v type="$type_name" '
    /^typedef enum[[:space:]]*\{/ { buf = ""; inbuf = 1; next }
    inbuf && /^\}/ {
        rest = substr($0, 2);
        gsub(/^[[:space:]]+/, "", rest);
        if (rest == type ";") {
            n = split(buf, lines, "\n");
            val = -1;
            for (i = 1; i <= n; i++) {
                line = lines[i];
                gsub(/^[[:space:]]+/, "", line);
                gsub(/,.*$/, "", line);
                if (line != "" && line !~ /^[/*]/ && line !~ /^\*/) {
                    name = line;
                    gsub(/[[:space:]]*=.*$/, "", name);
                    if (line ~ /=[[:space:]]*[0-9]+/) {
                        match(line, /=/);
                        restval = substr(line, RSTART + 1);
                        gsub(/^[[:space:]]+/, "", restval);
                        val = restval + 0;
                    } else {
                        val++;
                    }
                    print name "=" val;
                }
            }
        }
        inbuf = 0; buf = "";
        next;
    }
    inbuf { buf = buf $0 "\n"; next }
    ' "$header"
}

# ── Extract Python IntEnum NAME=VALUE pairs for a given class ────────────
# Usage: extract_py_enum <class_name>
extract_py_enum() {
    local class_name="$1"
    local pyfile="$2"
    awk -v cls="$class_name" '
    $0 ~ "^class[[:space:]]+" cls "[[:space:]]*\\(enum\\.IntEnum\\)" {
        collecting = 1;
        next;
    }
    collecting && /^class[[:space:]]+/ {
        collecting = 0;
        next;
    }
    collecting && /^[[:space:]]+[A-Z_][A-Z_0-9]*[[:space:]]*=/ {
        line = $0;
        gsub(/^[[:space:]]+/, "", line);
        gsub(/,.*$/, "", line);
        gsub(/[[:space:]]*#.*$/, "", line);
        if (line != "" && line !~ /^#/) {
            split(line, a, /[[:space:]]*=[[:space:]]*/);
            gsub(/^[[:space:]]+/, "", a[1]);
            gsub(/[[:space:]]+$/, "", a[1]);
            gsub(/^[[:space:]]+/, "", a[2]);
            gsub(/[[:space:]]+$/, "", a[2]);
            if (a[2] ~ /^[0-9]+$/) {
                print a[1] "=" a[2];
            } else {
                print a[1] "=BAD";
            }
        }
        next;
    }
    ' "$pyfile"
}

# ── Compare C enum NAME=VALUE pairs against Python ───────────────────────
# C prefixes are stripped before comparison. Both name and value must match.
check_enum() {
    local header="$1"
    local type_name="$2"
    local prefix="$3"
    local py_class="$4"
    local pyfile="$5"

    mapfile -t c_pairs < <(extract_c_enum "$header" "$type_name" | sed "s/^$prefix//")
    mapfile -t py_pairs < <(extract_py_enum "$py_class" "$pyfile")

    local c_count="${#c_pairs[@]}"
    local py_count="${#py_pairs[@]}"

    if [ "$c_count" -ne "$py_count" ] || [ "${c_pairs[*]}" != "${py_pairs[*]}" ]; then
        echo "MISMATCH: $py_class ($header/$type_name, prefix=$prefix)"
        echo "  C (stripped): ${c_pairs[*]}"
        echo "  Python:       ${py_pairs[*]}"
        HAD_ERROR=1
    fi
}

# ── Self-test: verify both positive and negative directions ───────────────
if [ "${1:-}" = "--self-test" ]; then
    TMP=$(mktemp -d)
    trap 'rm -rf "$TMP"' EXIT
    C_INCLUDE="${MMAPQ_C_DIR:-}" 2>/dev/null || C_INCLUDE=""
    if [ -z "$C_INCLUDE" ]; then
        for d in "../mmapq/include" "c/include" "../mmapq/include"; do
            if [ -f "$d/poller.h" ]; then
                C_INCLUDE="$d"
                break
            fi
        done
    fi
    if [ -z "$C_INCLUDE" ] || [ ! -f "$C_INCLUDE/poller.h" ]; then
        if [ -n "${MMAPQ_SKIP_C_GATES:-}" ]; then
            echo "WARNING: check_enum_sync.sh --self-test skipped (MMAPQ_SKIP_C_GATES set)" >&2
            exit 0
        fi
        echo "ERROR: check_enum_sync.sh --self-test: C headers not found." >&2
        echo "  Set MMAPQ_C_DIR to the mmapq/include directory, or run from the" >&2
        echo "  cqueue workspace root where ../mmapq/include resolves." >&2
        exit 1
    fi

    # Positive: correct tree must exit 0
    HAD_ERROR=0
    check_enum "$C_INCLUDE/poller.h" "MmapqPollResult" "MMAPQ_POLL_" "PollResult" "src/mmapq/types.py"
    if [ "$HAD_ERROR" -ne 0 ]; then
        echo "SELF-TEST FAILED: correct tree reported mismatch"
        exit 1
    fi

    # Negative: mutated value must exit 1
    cat > "$TMP/types.py" << 'TESTEOF'
from __future__ import annotations
import enum
class PollResult(enum.IntEnum):
    END_OF_STREAM = 0
    ADVANCED = 1
    STOPPED = 2
    RETREATED = 3
    ARCHIVED = 4
    ERROR = 5
    CORRUPT = 99
TESTEOF
    HAD_ERROR=0
    check_enum "$C_INCLUDE/poller.h" "MmapqPollResult" "MMAPQ_POLL_" "PollResult" "$TMP/types.py"
    if [ "$HAD_ERROR" -eq 0 ]; then
        echo "SELF-TEST FAILED: value mismatch not detected"
        exit 1
    fi

    echo "self-test passed"
    exit 0
fi

# ── Normal path ────────────────────────────────────────────────────────────
C_INCLUDE="${MMAPQ_C_DIR:-}"
if [ -z "$C_INCLUDE" ]; then
    for d in "c/include" "../mmapq/include"; do
        if [ -d "$d" ]; then
            C_INCLUDE="$d"
            break
        fi
    done
fi
if [ -z "$C_INCLUDE" ] || [ ! -d "$C_INCLUDE" ]; then
    if [ -n "${MMAPQ_SKIP_C_GATES:-}" ]; then
        echo "WARNING: check_enum_sync.sh: C headers not found (MMAPQ_SKIP_C_GATES set)" >&2
        exit 0
    fi
    echo "ERROR: check_enum_sync.sh: C headers not found at $C_INCLUDE." >&2
    echo "  Set MMAPQ_C_DIR to the mmapq/include directory, or run from the" >&2
    echo "  cqueue workspace root." >&2
    exit 1
fi

TYPES_PY="src/mmapq/types.py"
if [ ! -f "$TYPES_PY" ]; then
    echo "ERROR: check_enum_sync.sh: types.py not found at $TYPES_PY" >&2
    exit 1
fi

# ── Enum mapping table ────────────────────────────────────────────────────
# Format: header_path  C_type_name  C_prefix  Python_class  types_py_path
check_enum "$C_INCLUDE/poller.h"                "MmapqNextMove"         "MMAPQ_NEXT_"         "NextMove"         "$TYPES_PY"
check_enum "$C_INCLUDE/poller.h"                "MmapqPollResult"       "MMAPQ_POLL_"         "PollResult"       "$TYPES_PY"
check_enum "$C_INCLUDE/indexed_reader.h"        "MmapqEntryStatus"      "MMAPQ_ENTRY_"        "EntryStatus"      "$TYPES_PY"
check_enum "$C_INCLUDE/indexed_batch_reader.h"  "MmapqBatchEntryStatus" "MMAPQ_BATCH_ENTRY_"  "BatchEntryStatus" "$TYPES_PY"
check_enum "$C_INCLUDE/queue_transfer.h"        "MmapqConflictBehavior" "MMAPQ_CONFLICT_"     "ConflictBehavior" "$TYPES_PY"

if [ "$HAD_ERROR" -ne 0 ]; then
    echo "FAIL: enum sync check found mismatches"
    exit 1
fi

# ── Completeness check ────────────────────────────────────────────────────
# Verify every enum.IntEnum subclass in src/mmapq/ is covered by a check_enum
# invocation above. This closes the class rather than the instance (P1).
CHECKED_ENUMS="NextMove PollResult EntryStatus BatchEntryStatus ConflictBehavior"
ALL_ENUMS=$(grep -rn 'class .*(enum\.IntEnum)' src/mmapq/ 2>/dev/null \
    | sed 's/.*class \([A-Za-z0-9_]*\)(enum\.IntEnum).*/\1/' | sort -u)
for cls in $ALL_ENUMS; do
    found=0
    for checked in $CHECKED_ENUMS; do
        if [ "$cls" = "$checked" ]; then found=1; break; fi
    done
    if [ "$found" -eq 0 ]; then
        echo "ERROR: enum $cls is defined in src/mmapq/ but not covered by check_enum"
        HAD_ERROR=1
    fi
done

if [ "$HAD_ERROR" -ne 0 ]; then
    echo "FAIL: enum sync check found mismatches"
    exit 1
fi

echo "enum mirrors in sync"
