"""Runtime — async region mapper for background mapping."""

from __future__ import annotations

import weakref

from . import _cffi as _cffi_mod
from ._cdata import require_cdata
from ._errors import check_rc

ffi = _cffi_mod.ffi
lib = _cffi_mod.lib


class Runtime:
    """Async region mapper runtime.

    Pass to queue factories to enable background region mapping.
    Default (None) uses synchronous mapping on the hot path.
    """

    def __init__(self, inner):
        require_cdata(inner, "struct MmapqRuntime *", "Runtime")
        self._inner = inner
        self._finalizer = weakref.finalize(self, _destroy, inner)

    @classmethod
    def region_mapper(cls) -> Runtime:
        """Create the process-wide shared mapper runtime.

        The queue and its participants hold a reference to the runtime;
        it will not be finalized while they are alive.
        """
        ptr = ffi.new("MmapqRuntime **")
        rc = lib.mmapq_runtime_region_mapper(ptr)
        check_rc(rc, "mmapq_runtime_region_mapper")
        return cls(ptr[0])

    def close(self) -> None:
        """Close the runtime, stopping background mapping. Idempotent."""
        if self._inner is not None:
            self._finalizer.detach()
            _destroy(self._inner)
            self._inner = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def _destroy(inner):
    if inner is not None:
        lib.mmapq_runtime_destroy(inner)
