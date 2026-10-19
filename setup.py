"""cffi API-mode build for mmapq.

Compiles the mmapq C library into a Python C extension.
"""

import glob
import os
from pathlib import Path

from setuptools import Extension, setup
from cffi import FFI

ffi = FFI()

# ── C declarations (must match C headers exactly) ──────────────────────────

ffi.cdef("""
    /* ── Enums ──────────────────────────────────────────────────────────── */
    typedef enum { MMAPQ_NEXT_ADVANCE = 0, MMAPQ_NEXT_STOP = 1, MMAPQ_NEXT_RETREAT = 2 } MmapqNextMove;
    typedef enum { MMAPQ_POLL_END_OF_STREAM, MMAPQ_POLL_ADVANCED, MMAPQ_POLL_STOPPED,
                   MMAPQ_POLL_RETREATED, MMAPQ_POLL_ARCHIVED, MMAPQ_POLL_ERROR,
                   MMAPQ_POLL_CORRUPT } MmapqPollResult;

    /* ── Opaque types ────────────────────────────────────────────────────── */
    typedef struct MmapqIndexedQueue MmapqIndexedQueue;
    typedef struct MmapqIndexedAppender MmapqIndexedAppender;
    typedef struct MmapqIndexedPoller MmapqIndexedPoller;
    typedef struct MmapqIndexedBatchQueue MmapqIndexedBatchQueue;
    typedef struct MmapqIndexedBatchPoller MmapqIndexedBatchPoller;
    typedef struct MmapqIndexedBatchAppender MmapqIndexedBatchAppender;
    typedef struct MmapqMutableBatch MmapqMutableBatch;
    typedef struct MmapqRuntime MmapqRuntime;

    /* MmapqBatchPayloadIterator is opaque — struct definition is private to C.
       Access through mmapq_batch_iterator_advance(), mmapq_batch_iterator_current_data_ffi(),
       etc. The typedef is needed for pointer declarations in MmapqBatchReadingContext. */
    typedef struct MmapqIndexedReader MmapqIndexedReader;
    typedef struct MmapqIndexedBatchReader MmapqIndexedBatchReader;
    typedef struct MmapqIndexedArchiver MmapqIndexedArchiver;
    typedef struct MmapqIndexedBatchArchiver MmapqIndexedBatchArchiver;
    typedef struct MmapqBatchPayloadIterator MmapqBatchPayloadIterator;
    typedef struct MmapqUncommittedPayloadIterator MmapqUncommittedPayloadIterator;
    typedef struct MmapqUncommittedBatchIterator MmapqUncommittedBatchIterator;

    /* ── MmapqBatchReadingContext ──────────────────────────────────────────────── */
    typedef enum { MMAPQ_BATCH_ENTRY_AVAILABLE, MMAPQ_BATCH_ENTRY_END_OF_STREAM,
                   MMAPQ_BATCH_ENTRY_ARCHIVED, MMAPQ_BATCH_ENTRY_ERROR } MmapqBatchEntryStatus;

    typedef struct {
        int64_t               index;
        MmapqBatchEntryStatus      status;
        int64_t               batch_offset;
        int32_t               message_count;
        uint8_t               appender_id;
        uint8_t               reserved[3];
        MmapqBatchPayloadIterator *iterator;
    } MmapqBatchReadingContext;

    /* ── Handler callback ─────────────────────────────────────────────────── */
    typedef MmapqNextMove (*MmapqBatchHandler)(int64_t, MmapqBatchReadingContext*, void*);

    /* ── MmapqRuntime ──────────────────────────────────────────────────────────── */
    int  mmapq_runtime_region_mapper(MmapqRuntime**);
    int  mmapq_runtime_set_affinity(MmapqRuntime*, int cpu_core);
    void mmapq_runtime_destroy(MmapqRuntime*);

    /* ── MmapqReadingContext (single-message result struct) ────────────────────── */
    typedef struct {
        int64_t  index;
        uint8_t  appender_id;
        int64_t  msg_offset;
        void    *data;
        int32_t  length;
        int      status;
    } MmapqReadingContext;

    /* ── MmapqMessageHandler (single-message poller) ────────────────────────────── */
    typedef MmapqNextMove (*MmapqMessageHandler)(int64_t, uint8_t, const void*, int32_t, void*);

    /* ── Single-message queue factory ──────────────────────────────────────── */
    int    mmapq_indexed_queue_create(
               const char*, const char*, int32_t, int, int,
               int64_t, int64_t, uint64_t, int,
               MmapqRuntime*, MmapqRuntime*, MmapqIndexedQueue**);
    int    mmapq_indexed_queue_create_default(
               const char*, const char*, MmapqRuntime*, MmapqRuntime*, MmapqIndexedQueue**);
    int    mmapq_indexed_queue_create_appender(MmapqIndexedQueue*, MmapqIndexedAppender**);
    int    mmapq_indexed_queue_create_appender_with_id(MmapqIndexedQueue*, uint8_t, MmapqIndexedAppender**);
    int    mmapq_indexed_queue_create_poller(MmapqIndexedQueue*, MmapqIndexedPoller**);
    void             mmapq_indexed_queue_close(MmapqIndexedQueue*);
    int64_t  mmapq_indexed_appender_append(MmapqIndexedAppender*, const void*, int32_t);
    int64_t  mmapq_indexed_appender_append_uncommitted(
                 MmapqIndexedAppender*, const void*, int32_t);
    int64_t  mmapq_indexed_appender_commit(MmapqIndexedAppender*, int64_t);
    /* start_offset/end_offset take -1 for "not set" — the sentinel
       MMAPQ_PAYLOAD_ITERATOR_NOT_SET (indexed_appender.h) does not cross
       the FFI; Python passes -1. */
    int      mmapq_indexed_appender_uncommitted_iterator_checked(
                 MmapqIndexedAppender*, int64_t, int64_t,
                 MmapqUncommittedPayloadIterator**);
    int      mmapq_uncommitted_iterator_next(
                 MmapqUncommittedPayloadIterator*, const void**, int32_t*, int64_t*);
    int64_t  mmapq_indexed_appender_current_index(const MmapqIndexedAppender*);
    uint8_t  mmapq_indexed_appender_id(const MmapqIndexedAppender*);
    void     mmapq_indexed_appender_close(MmapqIndexedAppender*);

    /* ── Single-message poller ─────────────────────────────────────────────── */
    int mmapq_indexed_poller_create(const char*, const char*, int, uint64_t, MmapqRuntime*, MmapqIndexedPoller**);
    MmapqPollResult     mmapq_indexed_poller_poll(MmapqIndexedPoller*, MmapqMessageHandler, void*);
    int            mmapq_indexed_poller_move_to_index(MmapqIndexedPoller*, int64_t);
    int            mmapq_indexed_poller_move_to_end(MmapqIndexedPoller*);
    int            mmapq_indexed_poller_move_to_last(MmapqIndexedPoller*);
    int            mmapq_indexed_poller_move_to_start(MmapqIndexedPoller*);
    int64_t        mmapq_indexed_poller_current_index(const MmapqIndexedPoller*);
    int64_t        mmapq_indexed_poller_start_index(MmapqIndexedPoller*);
    int            mmapq_indexed_poller_exists(MmapqIndexedPoller*, int64_t);
    int            mmapq_indexed_poller_last_error(const MmapqIndexedPoller*);
    void           mmapq_indexed_poller_close(MmapqIndexedPoller*);

    /* ── Batch queue factory ───────────────────────────────────────────────── */
    int mmapq_indexed_batch_queue_create(
        const char*, const char*, int32_t, int, int,
        int64_t, int64_t, uint64_t, int,
        MmapqRuntime*, MmapqRuntime*, MmapqIndexedBatchQueue**);
    int mmapq_indexed_batch_queue_create_default(
        const char*, const char*, MmapqRuntime*, MmapqRuntime*, MmapqIndexedBatchQueue**);
    int mmapq_indexed_batch_queue_create_poller(
        MmapqIndexedBatchQueue*, MmapqIndexedBatchPoller**);
    int mmapq_indexed_batch_queue_create_appender(
        MmapqIndexedBatchQueue*, MmapqIndexedBatchAppender**);
    int mmapq_indexed_batch_queue_create_appender_with_id(
        MmapqIndexedBatchQueue*, uint8_t, MmapqIndexedBatchAppender**);
    int mmapq_indexed_batch_queue_create_reader(
        MmapqIndexedBatchQueue*, MmapqIndexedBatchReader**);
    int mmapq_indexed_batch_queue_create_archiver(
        MmapqIndexedBatchQueue*, MmapqIndexedBatchArchiver**);
    void mmapq_indexed_batch_queue_close(MmapqIndexedBatchQueue*);

    /* ── Batch poller ─────────────────────────────────────────────────────── */
    int mmapq_indexed_batch_poller_create(
        const char*, const char*, int, uint64_t, MmapqRuntime*, MmapqIndexedBatchPoller**);
    MmapqPollResult          mmapq_indexed_batch_poller_poll(
        MmapqIndexedBatchPoller*, MmapqBatchHandler, void*);
    int                 mmapq_indexed_batch_poller_move_to_index(
        MmapqIndexedBatchPoller*, int64_t);
    int                 mmapq_indexed_batch_poller_move_to_end(
        MmapqIndexedBatchPoller*);
    int                 mmapq_indexed_batch_poller_move_to_last_existing_batch(
        MmapqIndexedBatchPoller*);
    int                 mmapq_indexed_batch_poller_move_to_start(
        MmapqIndexedBatchPoller*);
    int64_t             mmapq_indexed_batch_poller_current_batch_index(
        const MmapqIndexedBatchPoller*);
    int64_t             mmapq_indexed_batch_poller_start_index(
        MmapqIndexedBatchPoller*);
    int                 mmapq_indexed_batch_poller_exists(
        MmapqIndexedBatchPoller*, int64_t);
    int                 mmapq_indexed_batch_poller_last_error(
        const MmapqIndexedBatchPoller*);
    void                mmapq_indexed_batch_poller_close(
        MmapqIndexedBatchPoller*);

    /* ── Batch payload iterator ──────────────────────────────────────────── */
    int      mmapq_batch_iterator_advance(MmapqBatchPayloadIterator*);
    /* Checked step (1 message / 0 exhausted / negative MMAPQ_E*) — the
       recovery walk uses this; a mid-batch error must raise, not truncate. */
    int      mmapq_batch_iterator_next(
                 MmapqBatchPayloadIterator*, const void**, int32_t*);
    int32_t  mmapq_batch_iterator_current_length_ffi(const MmapqBatchPayloadIterator*);
    const void* mmapq_batch_iterator_current_data_ffi(const MmapqBatchPayloadIterator*);
    int32_t  mmapq_batch_iterator_read_message_count(MmapqBatchPayloadIterator*, int64_t);
    MmapqBatchPayloadIterator* mmapq_batch_iterator_init(
        MmapqBatchPayloadIterator*, int64_t, int32_t, int32_t);

    /* ── Batch appender ──────────────────────────────────────────────────── */
    MmapqMutableBatch* mmapq_indexed_batch_appender_start_batch(MmapqIndexedBatchAppender*);
    int64_t       mmapq_mutable_batch_complete(MmapqMutableBatch*);
    int64_t       mmapq_mutable_batch_complete_uncommitted(MmapqMutableBatch*);
    int           mmapq_mutable_batch_discard(MmapqMutableBatch*);
    int64_t       mmapq_indexed_batch_appender_commit_batch(
                      MmapqIndexedBatchAppender*, int64_t);
    int           mmapq_indexed_batch_appender_uncommitted_batches_checked(
                      MmapqIndexedBatchAppender*, MmapqUncommittedBatchIterator**);
    int           mmapq_uncommitted_batch_next(
                      MmapqUncommittedBatchIterator*, int64_t*, int32_t*,
                      MmapqBatchPayloadIterator**);
    int           mmapq_indexed_batch_appender_join_completed_open_batch(
                      MmapqIndexedBatchAppender*, int64_t*);
    int32_t       mmapq_mutable_batch_append(MmapqMutableBatch*, const void*, int32_t);
    int32_t       mmapq_mutable_batch_append_remaining_from(
                      MmapqMutableBatch*, MmapqBatchPayloadIterator*);
    uint8_t       mmapq_indexed_batch_appender_id(const MmapqIndexedBatchAppender*);
    void          mmapq_indexed_batch_appender_close(MmapqIndexedBatchAppender*);

    /* ── MmapqEntryStatus (indexed_reader.h:42) ───────────────────────────────────── */
    typedef enum {
        MMAPQ_ENTRY_AVAILABLE,
        MMAPQ_ENTRY_END_OF_STREAM,
        MMAPQ_ENTRY_ARCHIVED,
        MMAPQ_ENTRY_ERROR,
    } MmapqEntryStatus;

    /* ── MmapqIndexedReader (indexed_reader.h:68) ──────────────────────────────────── */
    typedef struct MmapqIndexedReader MmapqIndexedReader;

    int mmapq_indexed_reader_create(
        const char*, const char*, int, uint64_t, MmapqRuntime*, MmapqIndexedReader**);
    int mmapq_indexed_queue_create_reader(MmapqIndexedQueue*, MmapqIndexedReader**);
    MmapqReadingContext *mmapq_indexed_reader_read(
        MmapqIndexedReader*, int64_t, MmapqReadingContext*);
    MmapqReadingContext *mmapq_indexed_reader_read_first(
        MmapqIndexedReader*, MmapqReadingContext*);
    MmapqReadingContext *mmapq_indexed_reader_read_last(
        MmapqIndexedReader*, MmapqReadingContext*);
    int64_t mmapq_indexed_reader_last_index(MmapqIndexedReader*);
    int64_t mmapq_indexed_reader_start_index(MmapqIndexedReader*);
    MmapqEntryStatus mmapq_indexed_reader_entry_status(MmapqIndexedReader*, int64_t);
    int     mmapq_indexed_reader_last_error(const MmapqIndexedReader*);
    void    mmapq_indexed_reader_close(MmapqIndexedReader*);

    /* ── MmapqIndexedBatchReader (indexed_batch_reader.h:35) ───────────────────────── */
    typedef struct MmapqIndexedBatchReader MmapqIndexedBatchReader;

    int mmapq_indexed_batch_reader_create(
        const char*, const char*, int, uint64_t, MmapqRuntime*, MmapqIndexedBatchReader**);
    MmapqBatchReadingContext *mmapq_indexed_batch_reader_read(
        MmapqIndexedBatchReader*, int64_t, MmapqBatchReadingContext*);
    MmapqBatchReadingContext *mmapq_indexed_batch_reader_read_first(
        MmapqIndexedBatchReader*, MmapqBatchReadingContext*);
    MmapqBatchReadingContext *mmapq_indexed_batch_reader_read_last(
        MmapqIndexedBatchReader*, MmapqBatchReadingContext*);
    int64_t mmapq_indexed_batch_reader_last_index(MmapqIndexedBatchReader*);
    int64_t mmapq_indexed_batch_reader_start_index(MmapqIndexedBatchReader*);
    int     mmapq_indexed_batch_reader_entry_status(MmapqIndexedBatchReader*, int64_t);
    int     mmapq_indexed_batch_reader_last_error(const MmapqIndexedBatchReader*);
    void    mmapq_indexed_batch_reader_close(MmapqIndexedBatchReader*);

    /* ── libc helpers for FILE* I/O ───────────────────────────────────────────── */
    void* fopen(const char*, const char*);
    int   fclose(void*);

    /* ── Transfer (queue_transfer.h) ──────────────────────────────────────────── */
    typedef int (*MmapqQueueFilterFn)(int64_t, const void*, int32_t, void*);
    typedef int (*MmapqBatchQueueFilterFn)(int64_t, int32_t, int32_t, const void*, int32_t, void*);

    #define MMAPQ_APPENDER_ID_AUTO -1
    typedef int32_t (*MmapqAppenderIdMapper)(uint16_t, void*);

    int64_t mmapq_queue_transfer_export(
        MmapqIndexedReader*, void*, int64_t, int64_t, int,
        MmapqQueueFilterFn, MmapqQueueFilterFn, void*);
    int64_t mmapq_queue_transfer_import(
        void*, const char*, const char*, int,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);
    int64_t mmapq_queue_transfer_import_batch_stream(
        void*, const char*, const char*, int,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);
    int64_t mmapq_queue_transfer_copy(
        MmapqIndexedReader*, const char*, const char*,
        int64_t, int64_t, int,
        MmapqQueueFilterFn, MmapqQueueFilterFn, void*,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);
    int64_t mmapq_queue_transfer_copy_batch_to_normal(
        MmapqIndexedBatchReader*, const char*, const char*,
        int64_t, int64_t, int,
        MmapqBatchQueueFilterFn, MmapqBatchQueueFilterFn, void*,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);

    /* ── Batch transfer (batch_queue_transfer.h) ──────────────────────────────── */
    int64_t mmapq_batch_queue_transfer_export(
        MmapqIndexedBatchReader*, void*, int64_t, int64_t, int,
        MmapqBatchQueueFilterFn, MmapqBatchQueueFilterFn, void*);
    int64_t mmapq_batch_queue_transfer_import(
        void*, const char*, const char*, int,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);
    int64_t mmapq_batch_queue_transfer_import_flat_stream(
        void*, const char*, const char*, int,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);
    int64_t mmapq_batch_queue_transfer_copy(
        MmapqIndexedBatchReader*, const char*, const char*,
        int64_t, int64_t, int,
        MmapqBatchQueueFilterFn, MmapqBatchQueueFilterFn, void*,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);
    int64_t mmapq_batch_queue_transfer_copy_normal_to_batch(
        MmapqIndexedReader*, const char*, const char*,
        int64_t, int64_t, int,
        MmapqBatchQueueFilterFn, MmapqBatchQueueFilterFn, void*,
        MmapqRuntime*, MmapqRuntime*, int,
        MmapqAppenderIdMapper, void*, int);

    /* ── Archiver (indexed_archiver.h:30,48,62,90,97,122,136,145) ────────────── */
    typedef struct MmapqIndexedArchiver MmapqIndexedArchiver;

    int mmapq_indexed_archiver_create(const char*, const char*, int, MmapqRuntime*, MmapqIndexedArchiver**);
    int mmapq_indexed_queue_create_archiver(MmapqIndexedQueue*, MmapqIndexedArchiver**);
    int              mmapq_indexed_archiver_archivable(MmapqIndexedArchiver*);
    int              mmapq_indexed_archiver_archive_up_to_index(MmapqIndexedArchiver*, int64_t);
    int              mmapq_indexed_archiver_archivable_files(
                         MmapqIndexedArchiver*, char**, int);
    int64_t          mmapq_indexed_archiver_start_index(MmapqIndexedArchiver*);
    int              mmapq_indexed_archiver_print_summary(MmapqIndexedArchiver*, char*, int);
    void             mmapq_indexed_archiver_close(MmapqIndexedArchiver*);

    /* ── Batch archiver (indexed_batch_archiver.h) ────────────────────────────── */
    int  mmapq_indexed_batch_archiver_archivable(MmapqIndexedBatchArchiver*);
    int  mmapq_indexed_batch_archiver_archive_up_to_index(MmapqIndexedBatchArchiver*, int64_t);
    int  mmapq_indexed_batch_archiver_archivable_files(MmapqIndexedBatchArchiver*, char**, int);
    int64_t mmapq_indexed_batch_archiver_start_index(MmapqIndexedBatchArchiver*);
    int  mmapq_indexed_batch_archiver_start_offset(MmapqIndexedBatchArchiver*, uint8_t, int64_t*);
     void mmapq_indexed_batch_archiver_close(MmapqIndexedBatchArchiver*);

     /* ── Error codes (mmap_platform.h) ─────────────────────────────────────── */
     const char* mmapq_strerror(int rc);
""")

# ── C source files ─────────────────────────────────────────────────────────
#
# Paths must be relative to setup.py for setuptools compatibility.
# Expected layout:
#   mmapq-py/setup.py
#   mmapq/           (sibling — the C library)
#
# Fallback: look inside a c/ submodule at the same level.

import glob  # noqa: F811

_script_dir = Path(__file__).resolve().parent
# Prefer the vendored c/ submodule (shipped in the sdist, always self-contained),
# falling back to the sibling mmapq/ directory in the development workspace.
# Verify the src/ subtree exists, not just the parent dir — a stray sibling dir
# with no C sources must not shadow the vendored copy.
_c_dir = _script_dir / "c"
if not (_c_dir / "src").exists():
    _c_dir = _script_dir.parent / "mmapq"
if not (_c_dir / "src").exists():
    raise SystemExit(
        "ERROR: no C sources found. Check out the mmapq c/ submodule "
        "(git submodule update --init) or place the mmapq/ sibling next to setup.py."
    )

_c_src_dir = _c_dir / "src"
_c_include_dir = _c_dir / "include"

# Compute relative path from setup.py to the C sources
_rel_src = Path(os.path.relpath(_c_src_dir, _script_dir))
_rel_inc = Path(os.path.relpath(_c_include_dir, _script_dir))

c_sources = sorted(str(p) for p in _c_src_dir.glob("*.c"))
if not c_sources:
    raise SystemExit(
        f"ERROR: no C sources found in {_c_src_dir}. "
        "Make sure the mmapq submodule is checked out."
    )

# Convert absolute paths to relative for setuptools compatibility.
# os.path.relpath works even when sources are outside the setup.py tree.
_rel_sources = [os.path.relpath(p, _script_dir) for p in c_sources]

ffi.set_source(
    "mmapq._cffi",
    """
    #include "mmap_platform.h"
    #include "batch_payload_iterator.h"
    #include "indexed_batch_queue.h"
    #include "indexed_batch_poller.h"
    #include "indexed_batch_appender.h"
    #include "indexed_batch_reader.h"
    #include "runtime.h"
    #include "indexed_queue.h"
    #include "indexed_archiver.h"
    #include "queue_transfer.h"
    #include "batch_queue_transfer.h"
    """,
    sources=_rel_sources,
    include_dirs=[str(_rel_inc), str(_rel_src)],
    libraries=["rt", "pthread", "m"],
    extra_compile_args=["-std=gnu11", "-O2", "-D_GNU_SOURCE", "-DNDEBUG",
                        "-Wno-unused-parameter", "-Wno-attributes"],
)

setup(
    name="mmapq",
    version="0.1.0",
    description="Python bindings for mmapq — low-latency memory-mapped queue",
    packages=["mmapq"],
    package_dir={"": "src"},
    ext_modules=[ffi.distutils_extension(), Extension(
        "mmapq._borrowed_buffer", ["src/mmapq/_borrowed_buffer.c"],
        extra_compile_args=["-std=c11", "-Wall", "-Wextra"],
    )],
    python_requires=">=3.10",
)
