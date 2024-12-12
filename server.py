#!/usr/bin/env python3


# Author: kokurewm (400551006)
# Description:  4DS3 Final Portfolio, Part 1
# Synopsis:  Priority-based task spooler demonstrating data structures and algorithms from the course being applied. Server portion.


import heapq
import collections
import contextlib
import threading
import subprocess
import shlex
import os
import argparse
import sys
import signal
import logging
import atexit
import json
import time
import uuid
import hashlib
import math
import zmq


class WorkerPool:
    def __init__(self, max_threads):
        """Custom class for implementing a worker pool, limits the number of running threads at a time to max_threads."""
        self.max_threads = (
            max_threads  # Max number of threads that can run at the same time
        )
        self.active_threads = 0  # Counter for active threads
        self.lock = (
            threading.Lock()
        )  # Lock to synchronize thread access to `active_threads`

    def start_thread(self, thread):
        """Starts the passed thread object if the active thread count is less than max_threads."""
        with (
            self.lock
        ):  # Ensure that only one thread can access `active_threads` at a time
            if (
                self.active_threads < self.max_threads
            ):  # Check if there is room to start a new thread
                self.active_threads += 1  # Increment the active thread count
                thread.start()  # Start the provided thread

    def thread_finished(self):
        """Call this method to decrement active_threads when a thread finishes its work."""
        with self.lock:
            self.active_threads -= (
                1  # Decrement active thread count when thread finishes
            )

    def check_if_free(self):
        """Checks if there are any free threads (not all being used)"""
        return self.max_threads > self.active_threads


class SubprocessThread(threading.Thread):
    def __init__(self, priority, arrival_time, job_id, hash_table, *args, **kwargs):
        """Subclass of threading.Thread that adds itself to some defined hash table, adds extra attributes, and an extra helper method"""
        super().__init__(*args, **kwargs)
        self.priority = priority
        self.arrival_time = arrival_time
        self.job_id = job_id
        self.hash_table = hash_table
        self._is_active = threading.Event()

        self.hash_table[self.job_id] = self
        self.process = None

    def run(self, *args, **kwargs):
        self._is_active.set()
        super().run(*args, **kwargs)
        self._is_active.clear()
        self.hash_table.pop(self.job_id, None)

    def is_active(self):
        return self._is_active.is_set()


class RealTimeLogger(threading.Thread):
    def __init__(self, name, handler, level, *args, **kwargs):
        """Subclass of threading.Thread that implements a logger to run in realtime with subprocess."""
        threading.Thread.__init__(self)
        self.daemon = True
        self.fdRead, self.fdWrite = os.pipe()
        self.pipeReader = os.fdopen(self.fdRead)

        self.level = level
        self.logger = logging.getLogger(name)
        self.logger.setLevel(self.level)
        self.logger.addHandler(handler)

        self.start()

    def fileno(self):
        return self.fdWrite

    def run(self):
        """Logs every line of output."""
        for line in iter(self.pipeReader.readline, ""):
            self.logger.log(self.level, line.strip())

        self.pipeReader.close()

    def close(self):
        os.close(self.fdWrite)


def worker(
    cmd,
    stdout,
    stderr,
    controller,
    timeout,
    kill_signal,
    user=None,
    group=None,
    executable=None,
):
    """Main worker function that runs a child process."""
    try:
        with subprocess.Popen(
            shlex.split(cmd),
            shell=False,
            stdout=stdout,
            stderr=stderr,
            user=user,
            group=group,
            executable=executable,
            universal_newlines=True,
            start_new_session=True,
            close_fds=True,
        ) as process:
            threading.current_thread().process = process
            try:
                process.communicate(timeout=timeout)
            except subprocess.TimeoutExpired:
                process.send_signal(kill_signal)

    except PermissionError as err:
        stderr.logger.error(f"{err} - trying to run '{cmd}'")
    finally:
        controller.thread_finished()


class DispatchTimer(threading.Timer):
    def __init__(self, *args, **kwargs):
        """Subclass of threading.Timer that repeats at a periodic interval."""
        super().__init__(*args, **kwargs)
        self.daemon = True

    def run(self):
        while not self.finished.wait(self.interval):
            self.function(*self.args, **self.kwargs)


def threads_enumerated_heapsort():
    """Custom heapsort, loops through the currently running threads and creates a heap, O(nlgn) running time."""
    h = list()
    for thread in threading.enumerate():
        if priority := getattr(thread, "priority", None):
            heapq.heappush(h, (priority, thread.arrival_time, thread))
    return [heapq.heappop(h) for i in range(len(h))]


def is_thread_running(key, ht):
    """Checks if is_active() is true or not on the thread object."""
    if process := ht.get(key, None):
        match process.is_active():
            case True:
                return (True, process)
            case _:
                return (False, process)


def list_jobs(pq, ht):
    """List jobs and return string in JSON format."""
    report = collections.defaultdict(dict)

    # Show running/preparing to run processes - Run heapsort on iterable threading.enumerate(), takes O(nlgn) to sort
    for _thread in threads_enumerated_heapsort():
        *_, thread = _thread
        report["running"].update(
            {thread.job_id: {"command": thread.name, "priority": thread.priority}}
        )

    # Show queued processes - O(nlgn) to sort heap items
    for job in heapq.nsmallest(len(pq), pq):
        prio, _, cmd, key = job
        if lookup := is_thread_running(key, ht):
            started, thread = lookup
            if not started:
                report["queued"].update({key: {"command": cmd, "priority": prio}})

    return json.dumps(report, indent=4)


def delete_job(ht, key, signal):
    """Stop a job by doing a hash table lookup and popping (removing the value)
    If the job is running as a process, send the defined kill signal to it."""
    deleted = False
    # O(1) to lookup job in hash table, delete job
    if pcs := ht.get(key, None):
        if pcs.is_active() and pcs.process:
            pcs.process.send_signal(signal)

        ht.pop(key, None)
        deleted = True

    if deleted:
        return key


def process_notification(
    mq,
    value,
    priority,
    pq,
    wp,
    ht,
    stdout,
    stderr,
    timeout,
    signal,
    user=None,
    group=None,
    executable=None,
):
    """Handler for when message queue receives a message."""
    mutex = threading.Lock()
    match priority:
        case _ if priority == math.inf:
            op, _, remainder = value.partition(":")
            match op:
                case "status":
                    mutex.acquire()
                    mq.send_string(list_jobs(pq, ht))
                    mutex.release()

                case "rm":
                    mutex.acquire()
                    deleted = delete_job(ht, remainder, signal)
                    if deleted:
                        mq.send_string(f"deleted: {deleted}")
                    else:
                        mq.send_string("no matching jobs, nothing deleted.")
                    mutex.release()
        case _:
            mutex.acquire()

            command = value
            arrival_time = time.time()
            job_id = hashlib.sha256(uuid.uuid4().bytes).hexdigest()[:10]

            thr = SubprocessThread(
                target=worker,
                args=(command, stdout, stderr, wp, timeout, signal),
                kwargs={"user": user, "group": group, "executable": executable},
                priority=priority,
                arrival_time=arrival_time,
                job_id=job_id,
                hash_table=ht,
                name=f"'{command}'",
            )

            # Push newly added job onto the heap (priority queue) - O(log n) operation.
            heapq.heappush(pq, (thr.priority, arrival_time, command, job_id))
            mq.send_string(job_id)

            mutex.release()


def dispatcher(pq, wp, ht):
    """Pop job off heap, start in thread"""
    while len(pq) > 0 and wp.check_if_free():
        *_, key = heapq.heappop(pq)  # Pop off heap, O(log n) operation
        if lookup := is_thread_running(key, ht):
            started, thread = lookup
            if not started:
                wp.start_thread(thread)


@contextlib.contextmanager
def message_queue(path):
    """FIFO message queue for interaction"""
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://{path}")
    try:
        yield socket
    finally:
        socket.close()
        context.term()


def cli(argv):
    """CLI args with argparse."""
    parser = argparse.ArgumentParser(argv)
    parser.add_argument(
        "-e", "--endpoint", type=str, default="*:15411", help="host:port to bind to."
    )
    parser.add_argument(
        "-n",
        "--num-workers",
        type=int,
        default=1,
        help="the maximum number of jobs that should run in parallel.",
    )
    parser.add_argument(
        "-f",
        "--frequency",
        type=int,
        default=15,
        help="how often (in seconds) to periodically run queued jobs.",
    )
    parser.add_argument(
        "-t",
        "--timeout",
        type=int,
        default=300,
        help="maximum number of seconds a job can run before being timed out.",
    )
    parser.add_argument(
        "-s",
        "--signal",
        type=str.upper,
        default="SIGTERM",
        choices=("SIGINT", "SIGTERM"),
        help="what kill signal to send a job if deleted/timed out.",
    )
    parser.add_argument(
        "-u", "--user", type=str, default=None, help="user to run jobs as."
    )
    parser.add_argument(
        "-g", "--group", type=str, default=None, help="group to run jobs as."
    )
    parser.add_argument(
        "-x",
        "--executable",
        type=str,
        default=None,
        help="executable (e.g. shell) to run jobs against.",
    )
    return parser.parse_args()


def main():
    formatter = logging.Formatter(
        "%(asctime)s - [%(name)s]  %(message)s", datefmt="%Y-%m-%d %H:%M:%S"
    )
    handler_stdout = logging.StreamHandler(sys.stdout)
    handler_stderr = logging.StreamHandler(sys.stderr)

    handler_stdout.setFormatter(formatter)
    handler_stderr.setFormatter(formatter)

    STDOUT_LOGGER = RealTimeLogger(
        name="STDOUT", handler=handler_stdout, level=logging.INFO
    )
    STDERR_LOGGER = RealTimeLogger(
        name="STDERR", handler=handler_stderr, level=logging.ERROR
    )

    PRIORITY_QUEUE = list()
    HASH_TABLE = dict()

    args = cli(sys.argv[1:])

    WORKER_POOL = WorkerPool(max_threads=args.num_workers)
    DISPATCHER_FREQUENCY = args.frequency
    TIMEOUT = args.timeout
    SIGNAL = getattr(signal, args.signal)
    USER = args.user
    GROUP = args.group
    EXECUTABLE = args.executable

    @atexit.register
    def cleanup():
        STDOUT_LOGGER.close()
        STDERR_LOGGER.close()

    DispatchTimer(
        DISPATCHER_FREQUENCY, dispatcher, args=(PRIORITY_QUEUE, WORKER_POOL, HASH_TABLE)
    ).start()
    try:
        with message_queue(args.endpoint) as mq:
            while True:
                try:
                    value, priority = mq.recv_json()
                except ValueError:
                    mq.send(b"")  # Got json, but not in right format
                except zmq.error.ZMQError:
                    pass
                else:
                    process_notification(
                        mq,
                        value=value,
                        priority=priority,
                        pq=PRIORITY_QUEUE,
                        wp=WORKER_POOL,
                        ht=HASH_TABLE,
                        stdout=STDOUT_LOGGER,
                        stderr=STDERR_LOGGER,
                        signal=SIGNAL,
                        timeout=TIMEOUT,
                        user=USER,
                        group=GROUP,
                        executable=EXECUTABLE,
                    )
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
