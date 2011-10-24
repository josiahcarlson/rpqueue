
'''
rpqueue (Redis Priority Queue)

Written July 5, 2011 by Josiah Carlson
Released under the GNU GPL v2
available: http://www.gnu.org/licenses/gpl-2.0.html

Other licenses may be available upon request.
'''

import datetime
try:
    import simplejson as json
except ImportError:
    import json
import multiprocessing
import os
import random
import signal
import sys
import time
import threading
import traceback
import uuid

try:
    from crontab import CronTab
except ImportError:
    class CronTab(object):
        __slots__ = ()

import redis

QUEUES_KNOWN = 'queues:all'
QUEUE_KEY = 'queue:'
RQUEUE_KEY = 'rqueue:'
MESSAGES_KEY = 'messages:'
SEEN_KEY = 'seen:'
REGISTRY = {}
NEVER_SKIP = set()
SHOULD_QUIT = multiprocessing.Array('i', (0,), lock=False)
REENTRY_RETRY = 5
MINIMUM_DELAY = 1

REDIS_CONNECTION_SETTINGS = {}
POOL = None
PID = None

def _log(message, *args):
    '''
    Logs messages to stderr.
    '''
    try:
        message = message % args
    except TypeError as err:
        print >>sys.stderr, repr(err)
    print >>sys.stderr, message

def log(message, *args):
    return

def _assert(condition, message, *args):
    if not condition:
        raise ValueError(message%args)

def set_redis_connection_settings(host='localhost', port=6379, db=0,
    password=None, socket_timeout=30):#, unix_socket_path=None):
    '''
    Sets the global redis connection settings for the queue. If not called
    before use, will connect to localhost:6379 with no password and db 0.
    '''
    global REDIS_CONNECTION_SETTINGS, POOL
    REDIS_CONNECTION_SETTINGS = locals()
    POOL = threading.local()

def get_connection():
    global PID
    if PID != os.getpid():
        # handle fork() after connection, and initial connection
        PID = os.getpid()
        set_redis_connection_settings(**REDIS_CONNECTION_SETTINGS)
    if not getattr(POOL, 'conn', None):
        POOL.conn = redis.Redis(**REDIS_CONNECTION_SETTINGS)
    return POOL.conn

def _enqueue_call(conn, queue, fname, args, kwargs, delay=0, taskid=None):
    '''
    Internal implementation detail of call queueing. You shouldn't need to
    call this function directly.
    '''
    pipeline = conn.pipeline(True)
    # prepare the message
    qkey = QUEUE_KEY + queue
    rqkey = RQUEUE_KEY + queue
    ikey = MESSAGES_KEY + queue

    delay = max(delay, 0)
    taskid = taskid or str(uuid.uuid4())
    ts = time.time() + delay

    if taskid and taskid == fname and delay > 0:
        pipeline.zscore(rqkey, taskid)
        pipeline.zscore(qkey, taskid)
        last, current = pipeline.execute()
        if current or (last and time.time()-last < REENTRY_RETRY):
            log("SKIPPED: %s %s", taskid, fname)
            return taskid

    message = json.dumps([taskid, fname, args, kwargs])
    # enqueue it
    pipeline.hset(ikey, taskid, message)
    if taskid == fname:
        pipeline.zadd(rqkey, taskid, ts)
    pipeline.zadd(qkey, taskid, ts)
    pipeline.sadd(QUEUES_KNOWN, queue)
    pipeline.hincrby(SEEN_KEY, queue, 1)
    pipeline.execute()
    if delay:
        log("SENT: %s %s for %r", taskid, fname, datetime.datetime.utcfromtimestamp(ts))
    else:
        log("SENT: %s %s", taskid, fname)
    # return the taskid, to determine whether the task has been started or not
    return taskid

def _get_work(conn, queues=None, timeout=1):
    '''
    Internal implementation detail of call queue removal. You shouldn't need
    to call this function directly
    '''
    # find the set of queues for processing
    pipeline = conn.pipeline(True)
    if not queues:
        queues = conn.smembers(QUEUES_KNOWN)
    queues = list(sorted(queues))
    if not queues:
        time.sleep(timeout)
        return
    # cache the full strings
    iqueues = [MESSAGES_KEY + q for q in queues]
    queues = [QUEUE_KEY + q for q in queues]
    to_end = time.time() + timeout
    i = 0
    while time.time() <= to_end:
        # look for a work item
        item = conn.zrange(queues[i], 0, 0, withscores=True)
        if not item or item[0][1] > time.time():
            # try the next queue
            i += 1
        else:
            # we've got a potential work item
            item_id, scheduled = item[0]
            pipeline.hget(iqueues[i], item_id)
            pipeline.hdel(iqueues[i], item_id)
            pipeline.zrem(queues[i], item_id)
            message, _ignore, should_use = pipeline.execute()
            if should_use:
                # return the work item
                work = json.loads(message)
                work.append(scheduled)
                if work[0] == work[1]:
                    # periodic or cron task, re-schedule it
                    sch = scheduled if item_id in NEVER_SKIP else time.time()
                    delay = REGISTRY[item_id].next(sch)
                    if delay is not None:
                        # it can be scheduled again, do it
                        pipeline.zadd(queues[i], item_id, sch + delay)
                    # re-add the call arguments
                    pipeline.hset(iqueues[i], item_id, message)
                    pipeline.execute()
                return work
            # the item was already taken by another processor, try again
            # immediately
            continue
        if i >= len(queues):
            # we tried all of the queues and didn't find any work, wait for a
            # bit and try again
            i = 0
            time.sleep(timeout / 100.0)
    return None

class _Task(object):
    '''
    An object that represents a task to be executed. These will replace
    functions in modules.
    '''
    __slots__ = 'queue', 'name', 'function', 'delay', 'attempts', 'retry_delay'
    def __init__(self, queue, name, function, delay=None, never_skip=False, attempts=1, retry_delay=30):
        self.queue = queue
        self.name = name
        self.function = function
        self.attempts = int(max(attempts, 1))
        self.retry_delay = max(retry_delay, 0)
        if delay is None:
            pass
        elif isinstance(delay, (int, long, float)):
            _assert(delay >= MINIMUM_DELAY,
                "periodic delay must be at least %r seconds, you provided %r",
                MINIMUM_DELAY, delay)
        elif isinstance(delay, str) and CronTab.__slots__:
            delay = CronTab(delay)
        elif isinstance(delay, CronTab) and CronTab.__slots__:
            pass
        else:
            _assert(False,
                "Provided run_every or crontab argument value of %r not supported",
                delay)
        self.delay = delay
        if never_skip:
            NEVER_SKIP.add(name)
        else:
            NEVER_SKIP.discard(name)
        REGISTRY[name] = self
        if self.delay:
            # periodic or cron task
            execute_delay = self.next()
            if execute_delay is not None:
                self.execute(delay=execute_delay, taskid=self.name)
    def __call__(self, taskid=None, nowarn=False):
        '''
        This wraps the function in an _ExecutingTask so as to offer reasonable
        introspection upon potential exception.
        '''
        if not taskid and not nowarn:
            log("You probably intended to call the function: %s, you are half-way there", self.name)
        return _ExecutingTask(self, taskid)
    def next(self, now=None):
        if self.delay is not None:
            if isinstance(self.delay, CronTab):
                return self.delay.next(now)
            else:
                return self.delay
        return 0
    def execute(self, *args, **kwargs):
        '''
        Invoke this task with the given arguments inside a task processor.
        '''
        delay = kwargs.pop('delay', None) or 0
        taskid = kwargs.pop('taskid', None)
        if kwargs.pop('execute_inline_now', None):
            # allow for testing/debugging to execute the task immediately
            _execute_task([None, self.name, args, kwargs])
            return
        if self.attempts > 1 and '_attempts' not in kwargs:
            kwargs['_attempts'] = self.attempts
        conn = get_connection()
        taskid = _enqueue_call(conn, self.queue, self.name, args, kwargs, delay, taskid=taskid)
        return _EnqueuedTask(self.name, taskid, self.queue)
    def retry(self, *args, **kwargs):
        '''
        Invoke this task as a retry with the given arguments inside a task
        processor.
        '''
        attempts = max(kwargs.pop('_attempts', 0), 0) - 1
        if attempts < 1:
            return
        kwargs['_attempts'] = attempts
        if self.retry_delay > 0 and 'delay' not in kwargs:
            kwargs['delay'] = self.retry_delay
        return self.execute(*args, **kwargs)
    def __repr__(self):
        return "<Task function=%s>"%(self.task.name,)

class _ExecutingTask(object):
    '''
    An object that offers introspection into running tasks.
    '''
    __slots__ = 'task', 'taskid', 'args', 'kwargs'
    def __init__(self, task, taskid):
        self.task = task
        self.taskid = taskid
        self.args = None
        self.kwargs = None
    def __call__(self, *args, **kwargs):
        '''
        Actually invoke the task.
        '''
        self.args = args
        self.kwargs = kwargs
        self.task.function(*args, **kwargs)
    def __repr__(self):
        return "<ExecutingTask taskid=%s function=%s args=%r kwargs=%r>"%(
            self.taskid, self.task.name, self.args, self.kwargs)

class _EnqueuedTask(object):
    '''
    An object that allows for simple status checks on tasks to be executed.
    '''
    __slots__ = 'name', 'taskid', 'queue'
    def __init__(self, name, taskid, queue):
        self.name = name
        self.taskid = taskid
        self.queue = queue
    @property
    def args(self):
        '''
        Get the arguments that were passed to this task.
        '''
        queue = MESSAGES_KEY + self.queue
        conn = get_connection()
        args = conn.hget(queue, self.taskid)
        if not args:
            return "<already executed>"
        return args
    @property
    def status(self):
        '''
        Get the status of this task.
        '''
        queue = QUEUE_KEY + self.queue
        conn = get_connection()
        eta = conn.zscore(queue, self.taskid)
        if eta is None:
            return "done"
        elif eta <= time.time():
            return "late"
        else:
            return "early"
    def __repr__(self):
        return "<EnqueuedTask taskid=%s queue=%s function=%s status=%s>"%(
            self.taskid, self.queue, self.name, self.status)

def task(*args, **kwargs):
    '''
    Decorator to allow the transparent execution of a function as a task. Used
    via:

    @task(queue='bar')
    def function1(arg1, arg2, ...):
        #will execute from within the 'bar' queue.

    @task
    def function2(arg1, arg2, ...):
        # will execute from within the 'default' queue.
    '''
    queue = kwargs.pop('queue', None) or 'default'
    attempts = kwargs.pop('attempts', None) or 1
    retry_delay = max(kwargs.pop('retry_delay', 30), 0)
    assert isinstance(queue, str)
    def decorate(function):
        name = '%s.%s'%(function.__module__, function.__name__)
        return _Task(queue, name, function, attempts=attempts, retry_delay=retry_delay)
    if args:
        return decorate(args[0])
    return decorate

_to_seconds = lambda td: td.days * 86400 + td.seconds + td.microseconds / 1000000.

def periodic_task(run_every, queue='default', never_skip=False, attempts=1, retry_delay=30):
    '''
    Decorator to allow the automatic repeated execution of a function every
    run_every seconds, which can be provided via int, long, float, or via a
    datetime.timedelta instance. Run from the context of the given queue. Used
    via:

    @periodic_task(25)
    def function1():
        # will be executed every 25 seconds from within the 'default' queue.

    @periodic_task(timedelta(minutes=5), queue='bar')
    def function2():
        # will be executed every 5 minutes from within the 'bar' queue.

    If never_skip is provided and is considered True, if you have a periodic
    task that is defined as...

    @periodic_task(60, never_skip=True)
    def function3():
        pass

    ... and the function was scheduled to be executed at 4:15PM and 5 seconds,
    but actually executed at 4:25PM and 13 seconds, then prior to execution,
    it will be rescheduled to execute at 4:16PM and 5 seconds, which is 60
    seconds after the earlier scheduled time (it never skips a scheduled
    time). If you instead had the periodic task defined as...

    @periodic_task(60, never_skip=True)
    def function4():
        pass

    ... and the function was scheduled to be executed at 4:15PM and 5 seconds,
    but actually executed at 4:25PM and 13 seconds, then prior to execution,
    it will be rescheduled to execute at 4:26PM and 13 seconds, which is 60
    seconds after the current time (it skips any missed scheduled time).

    '''
    _assert(isinstance(queue, str),
        "queue name provided must be a string, not %r", queue)
    _assert(isinstance(run_every, (int, long, float, datetime.timedelta)),
        "run_every provided must be an int, long, float, or timedelta, not %r",
        run_every)
    if isinstance(run_every, datetime.timedelta):
        run_every = _to_seconds(run_every)
    _assert(run_every >= MINIMUM_DELAY,
        "periodic execution timer must be at least %r, you provided %r",
        MINIMUM_DELAY, run_every)
    def decorate(function):
        name = '%s.%s'%(function.__module__, function.__name__)
        return _Task(queue, name, function, delay=run_every, never_skip=never_skip,
                     attempts=attempts, retry_delay=retry_delay)
    return decorate

if CronTab.__slots__:
    def cron_task(crontab, queue='default', never_skip=False, attempts=1, retry_delay=30):
        '''
        Decorator to allow the automatic repeated execution of a function
        on a schedule with a crontab syntax. Crontab syntax provided by the
        'crontab' Python module: http://pypi.python.org/pypi/crontab/

        Similar in use to the @periodic_task decorator:

        @cron_task('* * * * *')
        def function1():
            # will be executed every minute

        @cron_task('*/5 * * * *', queue='bar')
        def function2():
            # will be executed every 5 minutes from within the 'bar' queue.

        If never_skip is provided and is considered True, it will attempt to
        never skip a scheduled task, just like the @periodic_task decorator.

        Please see the crontab documentation for more information.
        '''
        _assert(isinstance(queue, str),
            "queue name provided must be a string, not %r", queue)
        _assert(isinstance(crontab, str),
            "crontab provided must be a string, not %r", crontab)
        crontab = CronTab(crontab)
        def decorate(function):
            name = '%s.%s'%(function.__module__, function.__name__)
            return _Task(queue, name, function, delay=crontab, never_skip=never_skip,
                         attempts=attempts, retry_delay=retry_delay)
        return decorate

def execute_tasks(queues=None, threads_per_process=1, processes=1, wait_per_thread=1):
    '''
    Will execute tasks from the (optionally) provided queues until the first
    value in the global SHOULD_QUIT is considered true.
    '''
    threads_per_process = max(threads_per_process, 1)
    processes = max(processes, 1)
    sp = []
    st = []
    for p in xrange(processes-1):
        pp = multiprocessing.Process(target=execute_tasks, args=(queues, threads_per_process, 1))
        pp.daemon = True
        pp.start()
        sp.append(pp)
    for t in xrange(threads_per_process):
        tt = threading.Thread(target=_execute_tasks, args=(queues,))
        tt.daemon = True
        tt.start()
        st.append(tt)
    _execute_tasks()
    for t in st:
        t.join(wait_per)
    if sp:
        sp[0].join(wait_per)
    # We have waited at least wait_per_thread * threads_per_process, which is
    # all we were supposed to wait. Because every sub-process/thread is
    # daemonic, falling off here will cause exit for any survivors.
    
def _execute_tasks(queues=None):
    '''
    Internal implementation detail to execute multiple tasks.
    '''
    conn = get_connection()
    while not SHOULD_QUIT[0]:
        work = _get_work(conn, queues)
        if not work:
            time.sleep(.05)
            continue
        _execute_task(work)

def _execute_task(work):
    '''
    Internal implementation detail to execute a single task.
    '''
    try:
        taskid, fname, args, kwargs, scheduled = work
    except ValueError as err:
        log(err)
        return

    now = datetime.datetime.utcnow()
    jitter = now - datetime.datetime.utcfromtimestamp(scheduled)
    log("RECEIVED: %s %s at %r (%r late)", taskid, fname, now, jitter)

    if fname not in REGISTRY:
        log("ERROR: Missing function %s in registry", fname)
        return

    to_execute = REGISTRY[fname](taskid, True)

    try:
        to_execute(*args, **kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        log("ERROR: Exception in task %r: %s", to_execute, traceback.format_exc().rstrip())
    else:
        log("SUCCESS: Task completed: %s %s", taskid, fname)

def known_queues(conn=None):
    '''
    Get a list of all known queues.
    '''
    conn = conn or get_connection()
    return list(sorted(conn.smembers(QUEUES_KNOWN), key=lambda x:x.lower()))

def queue_sizes(conn=None):
    '''
    Return a list of all known queues, their sizes, and the number of items
    that have been seen in the queue.
    '''
    conn = conn or get_connection()
    queues = known_queues(conn)

    pipeline = conn.pipeline(False)
    for queue in queues:
        pipeline.zcard(QUEUE_KEY + queue)
    for queue in queues:
        pipeline.hget(SEEN_KEY, queue)
    items = pipeline.execute()
    return zip(queues, items[:len(queues)], items[len(queues):])

def clear_queue(queue, conn=None, delete=False):
    '''
    Delete all items in a given queue, optionally deleting the queue itself.
    '''
    conn = conn or get_connection()
    pipeline = conn.pipeline(True)
    qkey = QUEUE_KEY + queue
    pipeline.zcard(qkey)
    pipeline.delete(qkey, MESSAGES_KEY + queue)
    if delete:
        pipeline.srem(QUEUES_KNOWN, queue)
    return pipeline.execute()[0]

if __name__ == '__main__':
    from optparse import OptionParser
    parser = OptionParser()
    parser.add_option('--clear', dest='queue', action='store', default=None,
        help='Remove all items from the given queue.')
    parser.add_option('--host', dest='host', action='store', default='localhost',
        help='The host that Redis is running on.')
    parser.add_option('--port', dest='port', action='store', type='int', default=6379,
        help='The port that Redis is listening on.')
    parser.add_option('--db', dest='db', action='store', type='int', default=0,
        help='The database that the queues are in.')
    parser.add_option('--password', dest='password', action='store', default=None,
        help='The password to connect to Redis with.')
    parser.add_option('--pwprompt', dest='pwprompt', action='store_true', default=False,
        help='Will prompt the user for a password before trying to connect.')
    ## parser.add_option('--unixpath', dest='unixpath', action='store', default=None,
        ## help='The unix path to connect to Redis with (use unix path OR host/port, not both.')
    parser.add_option('--timeout', dest='timeout', action='store', type='int', default=30,
        help='How long to wait for a connection or command to succeed or fail.')
    options, args = parser.parse_args()
    if options.pwprompt:
        if options.password:
            print "You must pass either --password OR --pwprompt not both."
            raise SystemExit
        import getpass
        options.password = getpass.getpass()

    set_redis_connection_settings(options.host, options.port, options.db,
        options.password, options.timeout)#, options.unixpath)

    if options.queue:
        items = clear_queue(options.queue)
        print "Queue %r cleared of %i items."%(options.queue, items)
    else:
        known = queue_sizes()
        known.insert(0, ('Queues', 'Sizes', 'Seen'))
        ml = [max(len(str(i)) for i in s) for s in zip(*known)]
        known.insert(1, [m*'-' for m in ml])
        fmt = '  '.join("%%-%is"%m for m in ml)
        for name, size, seen in known:
            print fmt%(name, size, seen)
