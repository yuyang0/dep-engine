#!/usr/bin/env python
# -*- coding:utf-8 -*-

"""
dependency engine similar to MXNET
"""
import signal
import time
import threading
from Queue import Queue, Empty


class Op(object):
    def __init__(self, fn, params, read_vars, write_vars):
        self.fn = fn
        self.params = params
        self.read_vars = read_vars
        self.write_vars = write_vars
        self.num_wait_vars = len(read_vars) + len(write_vars)

    def inc_wait(self):
        self.num_wait_vars += 1
        return self.num_wait_vars

    def dec_wait(self):
        self.num_wait_vars -= 1
        return self.num_wait_vars

    def execute(self):
        self.fn(self.params)

    def finish(self, dispatcher):
        for v in self.read_vars:
            v.comp_read(dispatcher)
        for v in self.write_vars:
            v.comp_write(dispatcher)


class VarOp(object):
    def __init__(self, op, is_write):
        self.op = op
        self.is_write = is_write

    def is_read(self):
        return self.is_write is False

    def is_write(self):
        return self.is_write

    def inc_wait(self):
        return self.op.inc_wait()

    def dec_wait(self):
        return self.op.dec_wait()

    def execute(self):
        self.op.execute()


class ReadSeq(list):
    pass


class Var(object):
    _IDX = 0

    def __init__(self):
        self.idx = Var._IDX
        Var._IDX += 1

        self.queue = []
        self.num_pending_read = 0
        self.pending_write = False
        self.lck = threading.Lock()

    def __str__(self):
        return '<Var: %s>' % self.idx

    def append_read(self, op):
        var_op = VarOp(op, self in op.write_vars)
        if len(self.queue) == 0:
            if self.pending_write is False:
                self.num_pending_read += 1
                op.dec_wait()
                return
            else:
                self.queue.append(ReadSeq([var_op, ]))
        else:
            last = self.queue[-1]
            if isinstance(last, ReadSeq):
                last.append(var_op)
            else:
                self.queue.append(ReadSeq([var_op, ]))

    def append_write(self, op):
        if len(self.queue) == 0 and \
           self.num_pending_read == 0 and \
           self.pending_write is False:

            self.pending_write = True
            op.dec_wait()
            return
        var_op = VarOp(op, self in op.write_vars)
        self.queue.append(var_op)

    def comp_read(self, dispatcher):
        assert self.pending_write is False
        assert self.num_pending_read > 0

        self.num_pending_read -= 1
        if len(self.queue) == 0:
            return
        first = self.queue[0]
        if isinstance(first, ReadSeq):
            for var_op in first:
                self.num_pending_read += 1
                if var_op.dec_wait() == 0:
                    dispatcher(var_op.op)
            self.queue = self.queue[1:]
        else:
            if self.num_pending_read == 0:
                self.pending_write = True
                if first.dec_wait() == 0:
                    dispatcher(first.op)
                self.queue = self.queue[1:]

    def comp_write(self, dispatcher):
        assert self.pending_write is True
        assert self.num_pending_read == 0

        self.pending_write = False
        if len(self.queue) == 0:
            return
        first = self.queue[0]
        if isinstance(first, ReadSeq):
            for var_op in first:
                self.num_pending_read += 1
                if var_op.dec_wait() == 0:
                    dispatcher(var_op.op)
            self.queue = self.queue[1:]
        else:
            if self.num_pending_read == 0:
                self.pending_write = True
                if first.dec_wait() == 0:
                    dispatcher(first.op)
                self.queue = self.queue[1:]


class Engine(object):
    def __init__(self):
        pass

    def new_var(self):
        return Var()

    def new_op(self, fn, params, read_vars, write_vars):
        return Op(fn, params, read_vars, write_vars)

    def push(self, op):
        op.inc_wait()
        for v in op.read_vars:
            v.append_read(op)
        for v in op.write_vars:
            v.append_write(op)
        if op.dec_wait() == 0:
            self.execute(op)

    def execute(self, op):
        raise NotImplementedError()


def spawn_thread(target, *args, **kw):
    t = threading.Thread(target=target, name=target.__name__, args=args, kwargs=kw)
    t.daemon = True
    t.start()
    return t


class ThreadedEngine(Engine):
    def __init__(self, num_threads):
        self.num_threads = num_threads
        self.task_queue = Queue()
        self.comp_queue = Queue()
        for i in range(num_threads):
            spawn_thread(self.thread_func)
        self.need_stop = False

    def thread_func(self):
        while True:
            op = self.task_queue.get()
            # self.do_task(task)
            op.execute()
            self.comp_queue.put(op)

    def do_task(op):
        op.execute()
        op.finish()

    def execute(self, op):
        self.task_queue.put(op)

    def finish(self):
        while not self.need_stop:
            try:
                op = self.comp_queue.get(True, 2)
                op.finish(self.execute)
            except Empty:
                pass

    def wait_all(self):
        time.sleep(500)

    def stop(self):
        self.need_stop = True


if __name__ == '__main__':
    def common_fn(params, read_vars, write_vars):
        # print "read vars: %s, write_vars: %s" % (read_vars, write_vars)
        print "start time: %s" % time.time()
        time.sleep(5)
        print "end time: %s" % time.time()
        print "----------------------------"

    engine = ThreadedEngine(8)
    signal.signal(signal.SIGINT, lambda *args: engine.stop())

    var_a = engine.new_var()
    var_b = engine.new_var()
    var_c = engine.new_var()
    op1 = engine.new_op(lambda params: common_fn(params, (var_a, var_b), ()), (), (var_a, var_b), ())
    op2 = engine.new_op(lambda params: common_fn(params, (var_a, var_b), ()), (), (var_a, var_b), ())
    op3 = engine.new_op(lambda params: common_fn(params, (var_b), (var_a, )), (), (var_b, ), (var_a, ))
    engine.push(op1)
    engine.push(op2)
    engine.push(op3)
    engine.finish()
