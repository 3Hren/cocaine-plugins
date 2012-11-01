/*
    Copyright (c) 2011-2012 Andrey Sibiryov <me@kobology.ru>
    Copyright (c) 2011-2012 Other contributors as noted in the AUTHORS file.

    This file is part of Cocaine.

    Cocaine is free software; you can redistribute it and/or modify
    it under the terms of the GNU Lesser General Public License as published by
    the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    Cocaine is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
    GNU Lesser General Public License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program. If not, see <http://www.gnu.org/licenses/>. 
*/

#ifndef COCAINE_PYTHON_SANDBOX_OBJECTS_HPP
#define COCAINE_PYTHON_SANDBOX_OBJECTS_HPP

#include "log.hpp"
#include "io.hpp"

namespace cocaine { namespace sandbox {

static
PyMethodDef
log_object_methods[] = {
    { "debug", (PyCFunction)log_object_t::debug, METH_VARARGS,
        "Logs a message with a Debug priority" },
    { "info", (PyCFunction)log_object_t::info, METH_VARARGS,
        "Logs a message with an Information priority" },
    { "warning", (PyCFunction)log_object_t::warning, METH_VARARGS,
        "Logs a message with a Warning priority" },
    { "error", (PyCFunction)log_object_t::error, METH_VARARGS,
        "Logs a message with an Error priority" },
    { "write", (PyCFunction)log_object_t::write, METH_VARARGS,
        "Writes a message to the error stream" },
    { "writelines", (PyCFunction)log_object_t::writelines, METH_VARARGS,
        "Writes messages from the iterable to the error stream" },
    { "flush", (PyCFunction)log_object_t::flush, METH_NOARGS,
        "Flushes the error stream" },
    { NULL, NULL, 0, NULL }
};

static
PyTypeObject
log_object_type = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /* ob_size */
    "cocaine.context.Log",                      /* tp_name */
    sizeof(log_object_t),                       /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)log_object_t::destructor,       /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                         /* tp_flags */
    "Log Proxy",                                /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    0,                                          /* tp_iter */
    0,                                          /* tp_iternext */
    log_object_methods,                         /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)log_object_t::constructor,        /* tp_init */
    0,                                          /* tp_alloc */
    PyType_GenericNew                           /* tp_new */
};

static
PyMethodDef
python_io_object_methods[] = {
    { "read", (PyCFunction)python_io_t::read,
        METH_KEYWORDS, "Pulls in a request chunk from the engine" },
    { "write", (PyCFunction)python_io_t::write,
        METH_VARARGS, "Pushes a response chunk to the engine" },
    // { "delegate", (PyCFunction)python_io_t::delegate,
    //     METH_KEYWORDS, "Delegate a part of the workload to a new job" },
    { "readline", (PyCFunction)python_io_t::readline,
        METH_KEYWORDS, "Pulls in a request line from the engine" },
    { "readlines", (PyCFunction)python_io_t::readlines,
        METH_KEYWORDS, "Pulls in all available request lines from the engine" },
    { NULL }
};

static
PyTypeObject
python_io_object_type = {
    PyObject_HEAD_INIT(NULL)
    0,                                          /* ob_size */
    "IO",                                       /* tp_name */
    sizeof(python_io_t),                        /* tp_basicsize */
    0,                                          /* tp_itemsize */
    (destructor)python_io_t::destructor,        /* tp_dealloc */
    0,                                          /* tp_print */
    0,                                          /* tp_getattr */
    0,                                          /* tp_setattr */
    0,                                          /* tp_compare */
    0,                                          /* tp_repr */
    0,                                          /* tp_as_number */
    0,                                          /* tp_as_sequence */
    0,                                          /* tp_as_mapping */
    0,                                          /* tp_hash */
    0,                                          /* tp_call */
    0,                                          /* tp_str */
    0,                                          /* tp_getattro */
    0,                                          /* tp_setattro */
    0,                                          /* tp_as_buffer */
    Py_TPFLAGS_DEFAULT,                         /* tp_flags */
    "I/O Proxy",                                /* tp_doc */
    0,                                          /* tp_traverse */
    0,                                          /* tp_clear */
    0,                                          /* tp_richcompare */
    0,                                          /* tp_weaklistoffset */
    PyObject_SelfIter,                          /* tp_iter */
    (iternextfunc)python_io_t::iter_next,       /* tp_iternext */
    python_io_object_methods,                   /* tp_methods */
    0,                                          /* tp_members */
    0,                                          /* tp_getset */
    0,                                          /* tp_base */
    0,                                          /* tp_dict */
    0,                                          /* tp_descr_get */
    0,                                          /* tp_descr_set */
    0,                                          /* tp_dictoffset */
    (initproc)python_io_t::constructor,         /* tp_init */
    0,                                          /* tp_alloc */
    PyType_GenericNew                           /* tp_new */
};

}}

#endif
