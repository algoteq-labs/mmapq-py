#define PY_SSIZE_T_CLEAN
#include <Python.h>
#include <stddef.h>

/* A snapshot of one reader result. Python's buffer consumers retain this
 * exporter (including the managed buffer shared by memoryview slices).
 * The reader keeps only a weak reference back, so a live export keeps the
 * reader alive without creating a cycle in the ordinary read_view path.
 * All fields are accessed with the GIL held; no queue calls occur here. */
typedef struct {
    PyObject_HEAD
    Py_buffer source;
    PyObject *owner;
    PyObject *weakrefs;
    Py_ssize_t exports;
    int released;
} BorrowedBuffer;

static int borrowed_clear(BorrowedBuffer *self)
{
    /* Invalidate before decref: a destructor can reenter Python. Once the
     * last export is released, even a saved view.obj cannot reopen this
     * snapshot after the reader has advanced or closed. */
    self->released = 1;
    if (self->source.obj != NULL) PyBuffer_Release(&self->source);
    Py_CLEAR(self->owner);
    return 0;
}

static int borrowed_traverse(BorrowedBuffer *self, visitproc visit, void *arg)
{
    Py_VISIT(self->source.obj);
    Py_VISIT(self->owner);
    return 0;
}

static void borrowed_dealloc(BorrowedBuffer *self)
{
    PyObject_GC_UnTrack(self);
    if (self->weakrefs != NULL) PyObject_ClearWeakRefs((PyObject *)self);
    borrowed_clear(self);
    Py_TYPE(self)->tp_free((PyObject *)self);
}

static int borrowed_getbuffer(PyObject *object, Py_buffer *view, int flags)
{
    BorrowedBuffer *self = (BorrowedBuffer *)object;
    if (self->released) {
        PyErr_SetString(PyExc_BufferError, "reader view has been released");
        return -1;
    }
    /* Read-only even when a consumer reexports view.obj directly. A
     * memoryview.toreadonly() wrapper alone would not enforce that. */
    if (PyBuffer_FillInfo(view, object, self->source.buf,
                          self->source.len, 1, flags) < 0) return -1;
    self->exports++;
    return 0;
}

static void borrowed_releasebuffer(PyObject *object, Py_buffer *view)
{
    BorrowedBuffer *self = (BorrowedBuffer *)object;
    (void)view;
    self->exports--;
    if (self->exports == 0) borrowed_clear(self);
}

static PyObject *borrowed_active(BorrowedBuffer *self, void *closure)
{
    (void)closure;
    return PyBool_FromLong(self->exports != 0 && !self->released);
}

static PyGetSetDef borrowed_getset[] = {
    {"active", (getter)borrowed_active, NULL, NULL, NULL},
    {NULL, NULL, NULL, NULL, NULL}
};

static PyBufferProcs borrowed_buffer = {
    .bf_getbuffer = borrowed_getbuffer,
    .bf_releasebuffer = borrowed_releasebuffer,
};

static PyTypeObject BorrowedBufferType = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "mmapq._borrowed_buffer.BorrowedBuffer",
    .tp_basicsize = sizeof(BorrowedBuffer),
    .tp_dealloc = (destructor)borrowed_dealloc,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)borrowed_traverse,
    .tp_clear = (inquiry)borrowed_clear,
    .tp_as_buffer = &borrowed_buffer,
    .tp_getset = borrowed_getset,
    .tp_weaklistoffset = offsetof(BorrowedBuffer, weakrefs),
};

static PyObject *make_view(PyObject *module, PyObject *args)
{
    PyObject *source, *owner;
    (void)module;
    if (!PyArg_ParseTuple(args, "OO", &source, &owner)) return NULL;
    BorrowedBuffer *self = PyObject_GC_New(BorrowedBuffer, &BorrowedBufferType);
    if (self == NULL) return NULL;
    self->source = (Py_buffer){0};
    self->owner = NULL;
    self->weakrefs = NULL;
    self->exports = 0;
    self->released = 0;
    if (PyObject_GetBuffer(source, &self->source, PyBUF_SIMPLE) < 0) {
        PyObject_GC_Del(self);
        return NULL;
    }
    Py_INCREF(owner);
    self->owner = owner;
    PyObject_GC_Track(self);
    PyObject *result = PyMemoryView_FromObject((PyObject *)self);
    Py_DECREF(self);
    return result;
}

static PyMethodDef methods[] = {
    {"make_view", make_view, METH_VARARGS, NULL},
    {NULL, NULL, 0, NULL}
};

static struct PyModuleDef module = {
    PyModuleDef_HEAD_INIT,
    .m_name = "_borrowed_buffer",
    .m_size = -1,
    .m_methods = methods,
};

PyMODINIT_FUNC PyInit__borrowed_buffer(void)
{
    if (PyType_Ready(&BorrowedBufferType) < 0) return NULL;
    return PyModule_Create(&module);
}
