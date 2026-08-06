"""cffi API-mode build for mmapq.

Compiles the mmapq C library into a Python C extension.
"""

import glob
import os
from pathlib import Path

from setuptools import setup
from cffi import FFI

ffi = FFI()

# ── C declarations (must match C headers exactly) ──────────────────────────

ffi.cdef("""
    /* ── Enums ──────────────────────────────────────────────────────────── */
    typedef enum { NEXT_ADVANCE = 0, NEXT_STOP = 1, NEXT_RETREAT = 2 } NextMove;
    typedef enum { POLL_END_OF_STREAM, POLL_ADVANCED, POLL_STOPPED,
                   POLL_RETREATED, POLL_ARCHIVED, POLL_ERROR } PollResult;

    /* ── Opaque types ────────────────────────────────────────────────────── */
    typedef struct IndexedQueue IndexedQueue;
    typedef struct IndexedAppender IndexedAppender;
    typedef struct IndexedPoller IndexedPoller;
    typedef struct IndexedBatchQueue IndexedBatchQueue;
    typedef struct IndexedBatchPoller IndexedBatchPoller;
    typedef struct IndexedBatchAppender IndexedBatchAppender;
    typedef struct MutableBatch MutableBatch;
    typedef struct Runtime Runtime;

    /* Buffer — embedded in BatchPayloadIterator */
    typedef struct {
        unsigned char *data;
        int32_t        capacity;
    } Buffer;

    /* BatchPayloadIterator — embedded by value in BatchReadingContext.
       Full field declaration needed for correct parent struct layout.
       Field access from Python is through C functions, not directly. */
    typedef struct {
        void     *queue_name;
        void     *payload_acc;
        Buffer    payload_buf;
        int64_t   batch_offset;
        int32_t   message_count;
        int32_t   current_msg_index;
        int64_t   current_msg_offset;
        int32_t   current_msg_length;
        int64_t   next_message_position;
        int       exhausted;
    } BatchPayloadIterator;

    /* ── BatchReadingContext ──────────────────────────────────────────────── */
    typedef enum { BATCH_ENTRY_AVAILABLE, BATCH_ENTRY_END_OF_STREAM,
                   BATCH_ENTRY_ARCHIVED, BATCH_ENTRY_ERROR } BatchEntryStatus;

    typedef struct {
        int64_t              index;
        BatchEntryStatus     status;
        int64_t              batch_offset;
        int32_t              message_count;
        uint8_t              appender_id;
        uint8_t              reserved[3];
        BatchPayloadIterator batch_iterator;
    } BatchReadingContext;

    /* ── Handler callback ─────────────────────────────────────────────────── */
    typedef NextMove (*BatchHandler)(int64_t, BatchReadingContext*, void*);

    /* ── Runtime ──────────────────────────────────────────────────────────── */
    Runtime* runtime_region_mapper(void);
    int      runtime_set_affinity(Runtime*, int cpu_core);
    void     runtime_destroy(Runtime*);

    /* ── ReadingContext (single-message result struct) ────────────────────── */
    typedef struct {
        int64_t  index;
        uint8_t  appender_id;
        int64_t  msg_offset;
        void    *data;
        int32_t  length;
        int      status;
    } ReadingContext;

    /* ── MessageHandler (single-message poller) ────────────────────────────── */
    typedef NextMove (*MessageHandler)(int64_t, uint8_t, const void*, int32_t, void*);

    /* ── Single-message queue factory ──────────────────────────────────────── */
    IndexedQueue*    indexed_queue_create(
                         const char*, const char*, int32_t, int, int,
                         int64_t, int64_t, uint64_t, int,
                         Runtime*, Runtime*);
    IndexedQueue*    indexed_queue_create_default(
                         const char*, const char*, Runtime*, Runtime*);
    IndexedAppender* indexed_queue_create_appender(IndexedQueue*);
    IndexedAppender* indexed_queue_create_appender_with_id(IndexedQueue*, uint8_t);
    IndexedPoller*   indexed_queue_create_poller(IndexedQueue*);
    void             indexed_queue_close(IndexedQueue*);
    int64_t  indexed_appender_append(IndexedAppender*, const void*, int32_t);
    int64_t  indexed_appender_append_uncommitted(
                 IndexedAppender*, const void*, int32_t);
    int64_t  indexed_appender_commit(IndexedAppender*, int64_t);
    int64_t  indexed_appender_current_index(const IndexedAppender*);
    uint8_t  indexed_appender_id(const IndexedAppender*);
    void     indexed_appender_close(IndexedAppender*);

    /* ── Single-message poller ─────────────────────────────────────────────── */
    IndexedPoller* indexed_poller_create(const char*, const char*, int, uint64_t, Runtime*);
    PollResult     indexed_poller_poll(IndexedPoller*, MessageHandler, void*);
    int            indexed_poller_move_to_index(IndexedPoller*, int64_t);
    int            indexed_poller_move_to_end(IndexedPoller*);
    int            indexed_poller_move_to_last(IndexedPoller*);
    int            indexed_poller_move_to_start(IndexedPoller*);
    int64_t        indexed_poller_current_index(const IndexedPoller*);
    int64_t        indexed_poller_start_index(IndexedPoller*);
    int            indexed_poller_exists(IndexedPoller*, int64_t);
    void           indexed_poller_close(IndexedPoller*);

    /* ── Batch queue factory ───────────────────────────────────────────────── */
    IndexedBatchQueue*    indexed_batch_queue_create(
                              const char*, const char*, int32_t, int, int,
                              int64_t, int64_t, uint64_t, int,
                              Runtime*, Runtime*);
    IndexedBatchQueue*    indexed_batch_queue_create_default(
                              const char*, const char*, Runtime*, Runtime*);
    IndexedBatchPoller*   indexed_batch_queue_create_poller(IndexedBatchQueue*);
    IndexedBatchAppender* indexed_batch_queue_create_appender(IndexedBatchQueue*);
    void                  indexed_batch_queue_close(IndexedBatchQueue*);

    /* ── Batch poller ─────────────────────────────────────────────────────── */
    IndexedBatchPoller* indexed_batch_poller_create(
        const char*, const char*, int, uint64_t, Runtime*);
    PollResult          indexed_batch_poller_poll(
        IndexedBatchPoller*, BatchHandler, void*);
    int                 indexed_batch_poller_move_to_index(
        IndexedBatchPoller*, int64_t);
    int                 indexed_batch_poller_move_to_end(
        IndexedBatchPoller*);
    int                 indexed_batch_poller_move_to_last_existing_batch(
        IndexedBatchPoller*);
    int                 indexed_batch_poller_move_to_start(
        IndexedBatchPoller*);
    int64_t             indexed_batch_poller_current_batch_index(
        const IndexedBatchPoller*);
    int64_t             indexed_batch_poller_start_index(
        IndexedBatchPoller*);
    int                 indexed_batch_poller_exists(
        IndexedBatchPoller*, int64_t);
    void                indexed_batch_poller_close(
        IndexedBatchPoller*);

    /* ── Batch payload iterator ──────────────────────────────────────────── */
    int      batch_iterator_advance(BatchPayloadIterator*);
    int32_t  batch_iterator_current_length_ffi(const BatchPayloadIterator*);
    const void* batch_iterator_current_data_ffi(const BatchPayloadIterator*);
    int32_t  batch_iterator_read_message_count(BatchPayloadIterator*, int64_t);
    BatchPayloadIterator* batch_iterator_init(
        BatchPayloadIterator*, int64_t, int32_t, int32_t);

    /* ── Batch appender ──────────────────────────────────────────────────── */
    MutableBatch* indexed_batch_appender_start_batch(IndexedBatchAppender*);
    int64_t       mutable_batch_complete(MutableBatch*);
    int64_t       indexed_batch_appender_commit_batch(
                      IndexedBatchAppender*, int64_t);
    int32_t       mutable_batch_append(MutableBatch*, const void*, int32_t);
    int32_t       mutable_batch_append_remaining_from(
                      MutableBatch*, BatchPayloadIterator*);
    void          indexed_batch_appender_discard_batch(
                      IndexedBatchAppender*, int64_t);
    void          indexed_batch_appender_close(IndexedBatchAppender*);

    /* ── EntryStatus (indexed_reader.h:42) ───────────────────────────────────── */
    typedef enum {
        ENTRY_AVAILABLE,
        ENTRY_END_OF_STREAM,
        ENTRY_ARCHIVED,
        ENTRY_ERROR,
    } EntryStatus;

    /* ── IndexedReader (indexed_reader.h:68) ──────────────────────────────────── */
    typedef struct IndexedReader IndexedReader;

    IndexedReader *indexed_reader_create(
        const char*, const char*, int, uint64_t, Runtime*);
    ReadingContext *indexed_reader_read(
        IndexedReader*, int64_t, ReadingContext*);
    ReadingContext *indexed_reader_read_first(
        IndexedReader*, ReadingContext*);
    ReadingContext *indexed_reader_read_last(
        IndexedReader*, ReadingContext*);
    int64_t indexed_reader_last_index(IndexedReader*);
    int64_t indexed_reader_start_index(IndexedReader*);
    EntryStatus indexed_reader_entry_status(IndexedReader*, int64_t);
    void    indexed_reader_close(IndexedReader*);

    /* ── IndexedBatchReader (indexed_batch_reader.h:35) ───────────────────────── */
    typedef struct IndexedBatchReader IndexedBatchReader;

    IndexedBatchReader *indexed_batch_reader_create(
        const char*, const char*, int, uint64_t, Runtime*);
    BatchReadingContext *indexed_batch_reader_read(
        IndexedBatchReader*, int64_t, BatchReadingContext*);
    BatchReadingContext *indexed_batch_reader_read_first(
        IndexedBatchReader*, BatchReadingContext*);
    BatchReadingContext *indexed_batch_reader_read_last(
        IndexedBatchReader*, BatchReadingContext*);
    int64_t indexed_batch_reader_last_index(IndexedBatchReader*);
    int64_t indexed_batch_reader_start_index(IndexedBatchReader*);
    int     indexed_batch_reader_entry_status(IndexedBatchReader*, int64_t);
    void    indexed_batch_reader_close(IndexedBatchReader*);

    /* ── libc helpers for FILE* I/O ───────────────────────────────────────────── */
    void* fopen(const char*, const char*);
    int   fclose(void*);

    /* ── Transfer (queue_transfer.h) ──────────────────────────────────────────── */
    typedef int (*QueueFilterFn)(int64_t, const void*, int32_t, void*);
    typedef int (*BatchQueueFilterFn)(int64_t, int32_t, int32_t, const void*, int32_t, void*);

    #define APPENDER_ID_AUTO -1
    typedef int32_t (*AppenderIdMapper)(uint16_t, void*);

    int64_t queue_transfer_export(
        IndexedReader*, void*, int64_t, int64_t, int,
        QueueFilterFn, QueueFilterFn, void*);
    int64_t queue_transfer_import(
        void*, const char*, const char*, int,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);
    int64_t queue_transfer_import_batch_stream(
        void*, const char*, const char*, int,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);
    int64_t queue_transfer_copy(
        IndexedReader*, const char*, const char*,
        int64_t, int64_t, int,
        QueueFilterFn, QueueFilterFn, void*,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);
    int64_t queue_transfer_copy_batch_to_normal(
        IndexedBatchReader*, const char*, const char*,
        int64_t, int64_t, int,
        BatchQueueFilterFn, BatchQueueFilterFn, void*,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);

    /* ── Batch transfer (batch_queue_transfer.h) ──────────────────────────────── */
    int64_t batch_queue_transfer_export(
        IndexedBatchReader*, void*, int64_t, int64_t, int,
        BatchQueueFilterFn, BatchQueueFilterFn, void*);
    int64_t batch_queue_transfer_import(
        void*, const char*, const char*, int,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);
    int64_t batch_queue_transfer_import_flat_stream(
        void*, const char*, const char*, int,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);
    int64_t batch_queue_transfer_copy(
        IndexedBatchReader*, const char*, const char*,
        int64_t, int64_t, int,
        BatchQueueFilterFn, BatchQueueFilterFn, void*,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);
    int64_t batch_queue_transfer_copy_normal_to_batch(
        IndexedReader*, const char*, const char*,
        int64_t, int64_t, int,
        BatchQueueFilterFn, BatchQueueFilterFn, void*,
        Runtime*, Runtime*, int,
        AppenderIdMapper, void*, int);

    /* ── Archiver (indexed_archiver.h:30,48,62,90,97,122,136,145) ────────────── */
    typedef struct IndexedArchiver IndexedArchiver;

    IndexedArchiver *indexed_archiver_create(const char*, const char*, int, Runtime*);
    int              indexed_archiver_archivable(IndexedArchiver*);
    int              indexed_archiver_archive_up_to_index(IndexedArchiver*, int64_t);
    int              indexed_archiver_archivable_files(
                         IndexedArchiver*, char**, int);
    int64_t          indexed_archiver_start_index(IndexedArchiver*);
    int              indexed_archiver_print_summary(IndexedArchiver*, char*, int);
    void             indexed_archiver_close(IndexedArchiver*);
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
_c_dir = _script_dir.parent / "mmapq"
if not _c_dir.exists():
    _c_dir = _script_dir / "c"

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
    #include "header_codec.h"
    #include "batch_payload_iterator.h"
    #include "indexed_batch_queue.h"
    #include "indexed_batch_poller.h"
    #include "indexed_batch_appender.h"
    #include "indexed_batch_reader.h"
    #include "runtime.h"
    #include "indexed_queue.h"
    #include "indexed_archiver.h"
    #include "queue_transfer.h"
    """,
    sources=_rel_sources,
    include_dirs=[str(_rel_inc)],
    libraries=["rt", "pthread", "m"],
    extra_compile_args=["-std=gnu11", "-O2", "-D_GNU_SOURCE", "-DNDEBUG",
                        "-Wno-unused-parameter", "-Wno-attributes"],
)

setup(
    name="mmapq",
    version="0.1.0",
    description="Python bindings for mmapq — low-latency memory-mapped queue",
    license="BSD 2-Clause",
    packages=["mmapq"],
    package_dir={"": "src"},
    ext_modules=[ffi.distutils_extension()],
    python_requires=">=3.10",
)
