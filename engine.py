#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
dependency engine similar to MXNET
"""
import threading
from threading.Queue import Queue


class Op(object):
    def __init__(self, fn, params, read_vars, write_vars):
        self.fn = fn
        self.params = params
        self.read_vars = read_vars
        self.write_vars = write_vars
        self.num_wait_vars = len(read_vars) + len(write_vars)

    def dec_wait(self):
        self.num_wait_vars -= 1
        return self.num_wait_vars

    def finish(self):
        for v in self.read_vars:
            v.comp_read()
        for v in self.write_vars:
            v.comp_write()


class VarOp(object):
    def __init__(self, op, is_write):
        self.op = op
        self.is_write = is_write

    def is_read(self):
        return self.is_write is False

    def is_write(self):
        return self.is_write

    def dec_wait(self):
        return self.op.dec_wait()


class Var(object):
    def __init__(self):
        self.queue = []
        self.num_pending_read = 0
        self.pending_write = False

    def append_read(self, op):
        var_op = VarOp(op, self in op.write_vars)
        last = self.queue[-1]
        if isinstance(last, list):
            last.append(var_op)
        else:
            self.queue.append([var_op, ])

    def append_write(self, op):
        var_op = VarOp(op, self in op.write_vars)
        self.queue.append(var_op)

    def comp_read(self, dispatcher):
        assert self.pending_write is False
        assert self.num_pending_read > 0

        self.num_pending_read -= 1
        if len(self.queue) == 0:
            return
        first = self.queue[0]
        if isinstance(first, list):
            for var_op in first:
                self.num_pending_read += 1
                if var_op.dec_wait() == 0:
                    dispatcher(var_op)
            self.queue = self.queue[1:]
        else:
            if self.num_pending_read == 0:
                self.pending_write = True
                if first.dec_wait() == 0:
                    dispatcher(first)
                self.queue = self.queue[1:]

    def comp_write(self, dispatcher):
        assert self.pending_write is True
        assert self.num_pending_read == 0
        if len(self.queue):
            return
        first = self.queue[0]
        if isinstance(first, list):
            for var_op in first:
                self.num_pending_read += 1
                if var_op.dec_wait() == 0:
                    dispatcher(var_op)
            self.queue = self.queue[1:]
        else:
            if self.num_pending_read == 0:
                self.pending_write = True
                if first.dec_wait() == 0:
                    dispatcher(first)
                self.queue = self.queue[1:]


class Engine(object):
    def __init__(self):
        pass

    def new_var(self):
        return Var()

    def new_op(self, fn, params, read_vars, write_vars):
        return Op(fn, params, read_vars, write_vars)

    def push(self, op):
        for v in op.read_vars:
            v.append_read(op)
        for v in op.write_vars:
            v.append_write(op)
        if op.dec_wait() == 0:
            self.execute(op)

    def execute(self, op):
        pass


def spawn_thread(target, *args, **kw):
    t = threading.Thread(target=target, name=target.__name__, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t


class ThreadedEngine(Engine):
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.task_queue = Queue
        for i in range(num_threads):
            spawn_thread(self.thread_func)

    def thread_func(self):
        while True:
            task = self.task_queue.get()
            self.do_task(task)

    def do_task(task):
        pass

    def execute(self, op):
        pass


class ProcessEngine(Engine):
    def __init__(self, num_process):
        self.num_process = num_process

    def execute(self, op):
        pass
