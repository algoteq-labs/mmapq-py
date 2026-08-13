#!/usr/bin/env bash
# check_no_layout_mirrors.sh — verify that setup.py's cdef does not
# declare fields for any type that is opaque in the C public headers.
#
# The P1 rule: a hand-maintained mirror of a private C struct layout is
# a defect class. After removing one (Phase 9a), ensure the class cannot
# return through a different door.
#
# Usage:
#   bash check_no_layout_mirrors.sh           # normal check
#   bash check_no_layout_mirrors.sh --self-test  # self-test mode
#
# Exit code: 0 = no layout mirrors found
#            1 = one or more mirrors found, listed on stderr
#            2 = self-test failed

set -euo pipefail

HERE="$(cd "$(dirname "$0")/.." && pwd)"
CDEF="$HERE/setup.py"
C_INCLUDE="${MMAPQ_C_DIR:-}"
if [ -z "$C_INCLUDE" ]; then
    for d in "$HERE/c/include" "$HERE/../mmapq/include"; do
        if [ -d "$d" ]; then
            C_INCLUDE="$d"
            break
        fi
    done
fi
if [ -z "$C_INCLUDE" ] || [ ! -d "$C_INCLUDE" ]; then
    if [ -n "${MMAPQ_SKIP_C_GATES:-}" ]; then
        echo "WARNING: check_no_layout_mirrors.sh: C headers not found (MMAPQ_SKIP_C_GATES set)" >&2
        exit 0
    fi
    echo "ERROR: check_no_layout_mirrors.sh: C headers not found." >&2
    echo "  Set MMAPQ_C_DIR to the mmapq/include directory." >&2
    exit 1
fi
INCLUDE_DIR="$C_INCLUDE"
SELF_TEST_DIR="/tmp/mmapq-self-test-cdef"

# Strip C comments from a file
strip_comments() {
    # Remove // comments and /* */ comments (basic, handles single-line only)
    sed 's|//.*||g' | sed 's|/\*.*\*/||g' | sed '/\/\*/,/\*\//d'
}

# Extract struct names from cdef that have field declarations (not opaque)
# Looks for: typedef struct { ... } NAME;
list_structs_with_fields() {
    # Normalize multi-line typedefs to single lines, then extract names
    awk '
    /typedef struct \{/ {
        # Found start of a struct typedef with fields
        braces = 1
        while (braces > 0 && getline line > 0) {
            if (line ~ /\{/) braces++
            if (line ~ /\}/) braces--
        }
        # After closing brace, extract the NAME from: } NAME;
        if (match($0, /}[[:space:]]+[A-Za-z_][A-Za-z0-9_]*;/)) {
            rest = substr($0, RSTART, RLENGTH)
            sub(/^[[:space:]}]*[[:space:]]*/, "", rest)
            sub(/;.*/, "", rest)
            print rest
        }
        if (match(line, /}[[:space:]]+[A-Za-z_][A-Za-z0-9_]*;/)) {
            rest = substr(line, RSTART, RLENGTH)
            sub(/^[[:space:]}]*[[:space:]]*/, "", rest)
            sub(/;.*/, "", rest)
            print rest
        }
    }'
}

# Check if a type is opaque in C headers (forward-declared as typedef struct NAME NAME;)
is_opaque_in_c() {
    local type_name="$1"
    for hdr in "$INCLUDE_DIR"/*.h; do
        if grep -q "typedef struct ${type_name}[[:space:]]\+${type_name};" "$hdr" 2>/dev/null; then
            return 0
        fi
    done
    return 1
}

main() {
    local exit_code=0

    # Fail-closed: the cdef is the whole point of this gate. If setup.py is
    # absent, `strip_comments < "$CDEF"` would read empty input and report
    # "no mirrors found" — a vacuous pass. Hard FAIL instead (P5).
    if [ ! -f "$CDEF" ]; then
        echo "ERROR: check_no_layout_mirrors.sh: cdef not found at $CDEF" >&2
        exit 1
    fi

    # Read cdef, strip comments, find field-bearing structs
    while IFS= read -r struct_name; do
        if is_opaque_in_c "$struct_name"; then
            echo "ERROR: cdef declares fields for opaque C type '$struct_name'" >&2
            echo "  The C header has: typedef struct $struct_name $struct_name;" >&2
            echo "  setup.py must use the same opaque typedef, not field declarations." >&2
            exit_code=1
        fi
    done < <(strip_comments < "$CDEF" | list_structs_with_fields)

    if [ "$exit_code" -eq 0 ]; then
        echo "OK: no layout mirrors found in cdef"
    fi
    return "$exit_code"
}

self_test() {
    local test_dir="$SELF_TEST_DIR"
    rm -rf "$test_dir"
    mkdir -p "$test_dir"

    # Create fake include dir with opaque type
    mkdir -p "$test_dir/include"
    cat > "$test_dir/include/test_opaque.h" << 'EOF'
typedef struct TestOpaque TestOpaque;
EOF

    # Create a cdef copy with an opaque that stays opaque (should pass)
    cat > "$test_dir/pass_setup.py" << 'EOF'
"""cdef with opaque typedef only"""
ffi.cdef("""
    typedef struct TestOpaque TestOpaque;
""")
EOF

    # Create a cdef copy with field declarations for an opaque type (should fail)
    cat > "$test_dir/fail_setup.py" << 'EOF'
"""cdef with illegal field declarations"""
ffi.cdef("""
    typedef struct {
        int x;
        int y;
    } TestOpaque;
""")
EOF

    local pass_result=0 fail_result=0

    # Test pass case
    CDEF="$test_dir/pass_setup.py" INCLUDE_DIR="$test_dir/include" main 2>/dev/null && pass_result=0 || pass_result=1

    # Test fail case
    CDEF="$test_dir/fail_setup.py" INCLUDE_DIR="$test_dir/include" main 2>/dev/null && fail_result=0 || fail_result=1

    rm -rf "$test_dir"

    echo "  self-test pass case: $( [ "$pass_result" -eq 0 ] && echo 'OK' || echo 'FAIL' )"
    echo "  self-test fail case: $( [ "$fail_result" -eq 1 ] && echo 'OK' || echo 'FAIL' )"

    if [ "$pass_result" -eq 0 ] && [ "$fail_result" -eq 1 ]; then
        echo "self-test: PASS"
        return 0
    else
        echo "self-test: FAIL" >&2
        return 2
    fi
}

case "${1:-}" in
    --self-test)
        self_test
        ;;
    *)
        main
        ;;
esac
