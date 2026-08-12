"""CData pointer validation for participant constructors."""
from __future__ import annotations

from . import _cffi as _cffi_mod

ffi = _cffi_mod.ffi


def require_cdata(inner, expected_cname, clsname):
    if not isinstance(inner, ffi.CData):
        raise TypeError(
            f"{clsname}() requires a cffi CData pointer, got "
            f"{type(inner).__name__}. "
            f"Use {clsname}.create() instead."
        )
    if inner == ffi.NULL:
        raise ValueError(f"{clsname}() received a NULL pointer")
    actual = ffi.typeof(inner).cname
    if actual != expected_cname:
        raise TypeError(
            f"{clsname}() expects {expected_cname}, got {actual}. "
            f"Use {clsname}.create() instead."
        )
