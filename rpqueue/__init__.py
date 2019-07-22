
'''
rpqueue (Redis Priority Queue)

Originally written July 5, 2011
Copyright 2011-2019 Josiah Carlson
Released under the GNU LGPL v2.1
available: http://www.gnu.org/licenses/old-licenses/lgpl-2.1.html

Other licenses may be available upon request.
'''

from __future__ import print_function
import datetime
from hashlib import sha1
import imp
import itertools
try:
    import simplejson as json
except ImportError:
    import json
import logging
import multiprocessing
import os
import signal
import sys
import time
import threading
import traceback
import types
import uuid
import warnings

try:
    from crontab import CronTab
except ImportError:
    class CronTab(object):
        __slots__ = ()

import redis
MED_CLIENT = redis.__version__ >= '2.6.'
NEW_CLIENT = redis.__version__ >= '3.'

def _zadd(conn, key, data):
    if NEW_CLIENT:
        return conn.zadd(key, data)
    return conn.zadd(key, **data)

def _setex(conn, key, value, time):
    if MED_CLIENT:
        return conn.setex(key, time, value)
    return conn.setex(key, value, time)

if list(map(int, redis.__version__.split('.'))) < [2, 4, 12]:
    raise Exception("Upgrade your Redis client to version 2.4.12 or later")

VERSION = '0.30.0'

RPQUEUE_CONFIGS = {}

__all__ = ['VERSION', 'new_rpqueue', 'set_key_prefix',
    'set_redis_connection_settings', 'set_redis_connection',
    'task', 'periodic_task', 'cron_task', 'Task', 'Data',
    'get_task', 'result', 'results',
    'set_priority', 'known_queues', 'queue_sizes', 'clear_queue',
    'execute_tasks', 'SimpleLock', 'NoLock', 'script_load']
__all__.sort()

def new_rpqueue(name, pfix=None):
    '''
    Creates a new rpqueue state for running separate rpqueue task systems in the
    same codebase. This simplifies configuration for multiple Redis servers, and
    allows for using the same Redis server without using queue names (provide a
    'prefix' for all RPqueue-related keys).

    If an rpqueue with the same name already exists, and has the same prefix,
    return that rpqueue module.
    '''
    # I had planned on building a class to embed state, etc., then I decided to
    # use the hack I'd already advised someone else to do. Warning: here there
    # be dragons. Yes, the module gets reloaded. I'm using that like class state.
    # :P

    # copy the original rpqueue out, that's our registry
    root = sys.modules[__name__]
    configs = root.RPQUEUE_CONFIGS
    pfix = pfix or b''
    if name in configs:
        if pfix != configs[name].KEY_PREFIX:
            raise Exception("Rpqueue config name %r already exists with a different prefix!"%(name,))
        return configs[name]

    # make a new module
    nr = types.ModuleType(__name__)
    # copy attributes over and set the sys.modules so the reload() works right
    nr.__dict__.update(root.__dict__)
    sys.modules[__name__] = nr
    imp.reload(nr) if PY3 else reload(nr)

    # put the registry back
    sys.modules[__name__] = root
    if pfix:
        # update the prefix, as necessary
        nr.set_key_prefix(pfix)
    configs[name] = nr
    return nr

# Also super lazy, but I am being lazy tonight.
_NAME_KEYS = dict(
    QUEUES_KNOWN = b'queues:all',
    QUEUES_PRIORITY = b'queues:wall',
    QUEUE_KEY = b'queue:',
    NOW_KEY = b'nqueue:',
    PNOW_KEY = b'pqueue:',
    RQUEUE_KEY = b'rqueue:',
    MESSAGES_KEY = b'messages:',
    SEEN_KEY = b'seen:',
    LOCK_KEY = b'locks:',
    RESULT_KEY = b'result:',
    FINISHED_KEY = b'finished:',
    VISIBILITY_KEY = b'invisible:',
    DATA_KEY = b'dataq:',
    DATAM_KEY = b'dataqm:',
    DATAV_KEY = b'dataqv:',

)
# silence Pyflakes
QUEUES_KNOWN = _NAME_KEYS['QUEUES_KNOWN']
QUEUES_PRIORITY = _NAME_KEYS['QUEUES_PRIORITY']
QUEUE_KEY = _NAME_KEYS['QUEUE_KEY']
NOW_KEY = _NAME_KEYS['NOW_KEY']
PNOW_KEY = _NAME_KEYS['PNOW_KEY']
RQUEUE_KEY = _NAME_KEYS['RQUEUE_KEY']
MESSAGES_KEY = _NAME_KEYS['MESSAGES_KEY']
SEEN_KEY = _NAME_KEYS['SEEN_KEY']
LOCK_KEY = _NAME_KEYS['LOCK_KEY']
RESULT_KEY = _NAME_KEYS['RESULT_KEY']
FINISHED_KEY = _NAME_KEYS['FINISHED_KEY']
VISIBILITY_KEY = _NAME_KEYS['VISIBILITY_KEY']
DATA_KEY = _NAME_KEYS['DATA_KEY']
DATAM_KEY = _NAME_KEYS['DATAM_KEY']
DATAV_KEY = _NAME_KEYS['DATAV_KEY']
KEY_PREFIX = b''

def set_key_prefix(pfix):
    '''
    Run before starting any tasks or task runners; will set the prefix on keys
    in Redis, allowing for multiple parallel rpqueue executions in the same
    Redis without worrying about queue names.
    '''
    if PY3 and isinstance(pfix, str):
        pfix = pfix.encode('latin-1')
    globals().update((k, pfix+v) for k, v in _NAME_KEYS.items())
    global KEY_PREFIX
    KEY_PREFIX = pfix

PY3 = sys.version_info >= (3, 0)
_SB = (str, bytes) if PY3 else (str, unicode)
REGISTRY = {}
NEVER_SKIP = set()
SHOULD_QUIT = multiprocessing.Array('i', (0,), lock=False)
REENTRY_RETRY = 5
DEADLETTER_QUEUE = b'DEADLETTER_QUEUE'
MINIMUM_DELAY = 1
EXECUTE_TASKS = False
QUEUE_ITEMS_PER_PASS = 100

REDIS_CONNECTION_SETTINGS = {}
POOL = None

LOG_LEVELS = dict((v, getattr(logging, v)) for v in ['DEBUG', 'INFO', 'WARNING', 'ERROR'])
LOG_LEVEL = 'debug'
logging.basicConfig()
log_handler = logging.root

SUCCESS_LOG_LEVEL = 'debug'
AFTER_FORK = None

CURRENT_TASK = threading.local()

bstr = ((lambda v: (v.encode('utf-8') if type(v) is str else v)) if PY3
   else (lambda v: (v.encode('utf-8') if type(v) is unicode else v)))

def _assert(condition, message, *args):
    if not condition:
        raise ValueError(message%args)

def script_load(script):
    '''
    Borrowed and updated from my book, Redis in Action:
    https://github.com/josiahcarlson/redis-in-action/blob/master/python/ch11_listing_source.py
    '''
    tt = str if PY3 else unicode
    script = script.encode('latin-1') if isinstance(script, tt) else script
    sha = [None, sha1(script).hexdigest()]
    def call(conn, keys=[], args=[], force_eval=False):
        if not force_eval:
            if not sha[0]:
                try:
                    return conn.execute_command("EVAL", script, len(keys), *(keys+args))
                finally:
                    del sha[:-1]

            try:
                return conn.execute_command("EVALSHA", sha[0], len(keys), *(keys+args))

            except redis.exceptions.ResponseError as msg:
                if not msg.args[0].startswith("NOSCRIPT"):
                    raise

        return conn.execute_command("EVAL", script, len(keys), *(keys+args))

    return call

def set_redis_connection_settings(host='localhost', port=6379, db=0,
    password=None, socket_timeout=30, unix_socket_path=None, ssl=False, ssl_ca_certs=None):
    '''
    Sets the global redis connection settings for the queue. If not called
    before use, will connect to localhost:6379 with no password and db 0.
    '''
    global REDIS_CONNECTION_SETTINGS, POOL
    REDIS_CONNECTION_SETTINGS = locals()
    if redis.__version__ < '2.4':
        REDIS_CONNECTION_SETTINGS.pop('unix_socket_path', None)
    POOL = None

def set_redis_connection(conn):
    '''
    Sets the global pooled connection to the provided connection object. Useful
    for environments where additional pooling or other options are desired or
    required.
    '''
    global POOL
    POOL = conn

def get_connection():
    global POOL
    if not POOL:
        POOL = redis.Redis(**REDIS_CONNECTION_SETTINGS)
    return POOL

def _enqueue_call(conn, queue, fname, args, kwargs, delay=0, taskid=None, vis_timeout=0, use_dead=0):
    '''
    Internal implementation detail of call queueing. You shouldn't need to
    call this function directly.
    '''
    pipeline = conn.pipeline(True)
    # prepare the message
    qkey = QUEUE_KEY + queue
    nkey = NOW_KEY + queue
    rqkey = RQUEUE_KEY + queue
    ikey = MESSAGES_KEY + queue

    delay = max(delay, 0)
    taskid = taskid or str(uuid.uuid4())
    ts = time.time()

    if taskid == fname and delay > 0:
        pipeline.zscore(rqkey, taskid)
        pipeline.zscore(qkey, taskid)
        last, current = pipeline.execute()
        if current or (last and ts+delay-last < REENTRY_RETRY):
            log_handler.debug("SKIPPED: %s %s", taskid, fname)
            return taskid

    if vis_timeout or use_dead:
        kwargs = dict(kwargs)
        if vis_timeout:
            kwargs['_vis_timeout'] = vis_timeout
        if use_dead:
            kwargs['_use_dead'] = use_dead

    message = json.dumps([taskid, fname, args, kwargs, ts + delay])
    # enqueue it
    pipeline.hset(ikey, taskid, message)
    if taskid == fname:
        _zadd(pipeline, rqkey, {taskid: ts + delay})
    if delay > 0:
        _zadd(pipeline, qkey, {taskid: ts + delay})
    else:
        pipeline.rpush(nkey, taskid)
    pipeline.sadd(QUEUES_KNOWN, queue)
    pipeline.hincrby(SEEN_KEY, queue, 1)
    pipeline.zrem(VISIBILITY_KEY + queue, taskid)
    pipeline.execute()
    if delay:
        log_handler.debug("SENT: %s %s for %r", taskid, fname, datetime.datetime.utcfromtimestamp(ts + delay))
    else:
        log_handler.debug("SENT: %s %s", taskid, fname)
    # return the taskid, to determine whether the task has been started or not
    return taskid

_work_step_lua = script_load('''
-- KEYS {MESSAGE_KEY, VIS_KEY}
-- ARGV {id}
-- Make sure that the queue item isn't already being executed / already done.

redis.replicate_commands()
local id = ARGV[1]
if redis.call('zscore', KEYS[2], id) then
    return ''
end
local item = redis.call('hget', KEYS[1], id)
local future = 0
if item then
    local itemd = cjson.decode(item)
    if itemd[4]['_vis_timeout'] then
        future = redis.call('time')
        future = tonumber(future[1]) + tonumber(future[2]) / 1000000
        future = future + tonumber(itemd[4]['_vis_timeout'])
        redis.call('zadd', KEYS[2], future, id)
    else
        redis.call('hdel', KEYS[1], id)
    end
end
return {item or '', string.format('%.6f', future)}
''')

_finish_step_lua = script_load('''
-- KEYS {MESSAGE_KEY, VIS_KEY}
-- ARGV {id, t}

redis.replicate_commands()
local v = redis.call('zscore', KEYS[2], ARGV[1]) or '0'
v = string.format("%.6f", tonumber(v))
if v == ARGV[2] then
    redis.call('hdel', KEYS[1], ARGV[1])
    redis.call('zrem', KEYS[2], ARGV[1])
    return 1
end
return 0
''')


def _get_work(conn, queues=None, timeout=1):
    '''
    Internal implementation detail of call queue removal. You shouldn't need
    to call this function directly.
    '''
    lkp = len(KEY_PREFIX)
    # find the set of queues for processing
    pipeline = conn.pipeline(True)
    if not queues:
        queues = known_queues(conn)
    if not queues:
        time.sleep(timeout)
        return
    # cache the full strings
    _queues = []
    for q in queues:
        if q == DEADLETTER_QUEUE:
            continue
        _queues.append(PNOW_KEY + q)
        _queues.append(NOW_KEY + q)
    queues = _queues

    to_end = time.time() + max(int(timeout), 0) + .01
    while time.time() < to_end and not SHOULD_QUIT[0]:
        remain = to_end - time.time()
        iremain = int(remain)
        if iremain < 1:
            # If we have less than a second left, exploit MULTI/EXEC blocking
            # pop behavior to get a timeout of 0.
            pipeline.blpop(queues, 1)
            item = pipeline.execute()[0]
            if not item and remain > .05:
                time.sleep(.05)
        else:
            item = conn.blpop(queues, iremain)

        if not item:
            # couldn't find work.
            continue

        q, id = item
        queue = q[lkp:].partition(b':')[2]
        message, t = _work_step_lua(conn,
            keys=[MESSAGES_KEY + queue, VISIBILITY_KEY + queue], args=[id])

        if not message:
            # item was cancelled
            continue

        # return the work item
        if PY3:
            message = message.decode('latin-1')
        work = json.loads(message)
        item_id = work[0]
        # some annoying legacy stuff...
        if len(work) == 4:
            work.append(time.time())
        if item_id == work[1]:
            # periodic or cron task, re-schedule it
            if item_id not in REGISTRY:
                log_handler.debug("ERROR: Missing function %s in registry", item_id)
                continue
            sch = work[-1] if item_id in NEVER_SKIP else time.time()
            delay = REGISTRY[item_id].next(sch)
            if delay is not None:
                # it can be scheduled again, do it
                if delay > 0:
                    _zadd(pipeline, QUEUE_KEY + queue, {item_id: sch + delay})
                else:
                    pipeline.rpush(NOW_KEY + queue, item_id)
                # re-add the call arguments
                pipeline.hset(MESSAGES_KEY + queue, item_id, json.dumps(work[:4] + [ts]))
                pipeline.execute()
        if t != '0':
            work.append(t)

        return work

_handle_delayed_lua = script_load('''
local run_again = false
local limit = tonumber(ARGV[1] or '100')

local t1 = 0
local t2 = 0
local t3 = {}
redis.replicate_commands()
local cutoff

if #ARGV == 1 then
    cutoff = redis.call('time')
    cutoff = cutoff[1] .. '.' .. cutoff[2]
else
    cutoff = ARGV[2]
end

for i = 1, #KEYS, 5 do
    local queue = KEYS[i]
    local qnow = KEYS[i+1]
    local messages = KEYS[i+2]
    local vis = KEYS[i+3]
    local dead = KEYS[i+4]

    -- handle delayed items
    local items = redis.call('zrangebyscore', queue, '-inf', '(' .. cutoff, 'limit', 0, limit)

    local isize = #items
    if isize > 0 then
        redis.call('zremrangebyrank', queue, 0, isize-1)
        redis.call('rpush', qnow, unpack(items))
        redis.call('zrem', vis, unpack(items))
        run_again = run_again or isize >= limit
    end

    -- handle visibility timeouts and deadletter queue
    items = redis.call('zrangebyscore', vis, '-inf','(' .. cutoff, 'limit', 0, limit)
    isize = #items
    if isize > 0 then
        for i, id in ipairs(items) do
            local odata = redis.call('hget', messages, id)
            redis.call('zrem', vis, id)
            redis.call('zrem', queue, id)
            if odata then
                local data = cjson.decode(odata)
                local retries = tonumber(data[4]['_attempts'] or '0')

                if retries >= 1 then
                    data[4]['_attempts'] = retries - 1
                    redis.call('hset', messages, id, cjson.encode(data))
                    redis.call('rpush', qnow, id)
                else
                    -- that was the last try
                    redis.call('hdel', messages, id)
                    if data[4]['_use_dead'] then
                        redis.call('rpush', dead, odata)
                    end
                end
            end
        end
        run_again = run_again or isize >= limit
    end
end

return run_again and 1 or 0
''')

def handle_delayed_lua_(conn, queues, timeout):
    '''
    Internal implementation detail of handling delayed requests. You shouldn't
    need to call this function directly.
    '''
    # This changes the delay semantics to basically be:
    # Until we run out of time/quit:
    #     If any queue had >= QUEUE_ITEMS_PER_PASS entries processed, retry immediately.
    #     Otherwise wait 50 milliseconds and go again.

    to_end = time.time() + timeout
    first = True
    _queues = []
    for q in queues:
        _queues.extend([
            QUEUE_KEY + q,
            PNOW_KEY + q,
            MESSAGES_KEY + q,
            VISIBILITY_KEY + q,
            NOW_KEY + DEADLETTER_QUEUE,
        ])
    while not SHOULD_QUIT[0] and (time.time() <= to_end or first):
        first = False
        run_again = _handle_delayed_lua(conn, keys=_queues, args=[QUEUE_ITEMS_PER_PASS, time.time()])
        if not run_again:
            time.sleep(.05)

def _handle_delayed(conn, queues=None, timeout=1):
    # find the set of queues for processing
    if not conn:
        conn = get_connection()

    queues = queues or []
    if not queues:
        queues = known_queues(conn)
    if not queues:
        time.sleep(timeout)
        return

    handle_delayed_lua_(conn, queues, timeout)

def _gt(conn):
    t = conn.time()
    if not isinstance(t, (list, tuple)):
        t = conn.execute()[-1]
    return t[0] + t[1] / 1000000

_get_data_lua = script_load('''
-- KEYS {LIST_QUEUE, MESSAGES_KEY, VISIBILITY_KEY, DEADLETTER_KEY}
-- ARGV {wanted, vis_timeout_, attempts_, use_dead_}
-- ARGV attempts and use_dead override defaults if not provided originally
-- during insertion. Vis_timeout is caller decided.

redis.replicate_commands()
local now = redis.call('time')
now = tonumber(now[1]) + tonumber(now[2]) / 1000000
local vis_timeout = tonumber(ARGV[2])
local items = {}
local items_ = redis.call('lrange', KEYS[1], 0, tonumber(ARGV[1]) - 1)
if #items_ > 0 then
    redis.call('ltrim', KEYS[1], #items_, -1)
    items = redis.call('hmget', KEYS[2], unpack(items_))
    if vis_timeout == 0 then
        redis.call('hdel', KEYS[2], unpack(items_))
    end
    items_ = nil
end
local attempts_ = tonumber(ARGV[3])
local use_dead_ = tonumber(ARGV[4])
local avail = 0
for i, val in ipairs(items) do
    if val then
        local dval = cjson.decode(val)
        local available = dval[4]['_attempts'] or attempts_
        if vis_timeout >= 0 and available >= 1 then
            if vis_timeout > 0 then
                dval[4]['_attempts'] = available - 1
                redis.call('hset', KEYS[2], dval[1], cjson.encode(dval))
                redis.call('zadd', KEYS[3], now + vis_timeout, dval[1])
            end
        else
            -- out of tries, or 0/1 removal
            redis.call('hdel', KEYS[2], dval[1])
            redis.call('zrem', KEYS[3], dval[1])
            if vis_timeout > 0 then
                -- out of tries
                local ud = dval[4]['_use_dead'] or use_dead_
                if ud > 0 then
                    redis.call('rpush', KEYS[4], val)
                end
                items[i] = ''
            end
        end
    else
        items[i] = ''
    end
end

return {string.format("%.6f", now), items}
''')


_refresh_lua = script_load('''
-- KEYS {VISIBILITY_KEY}
-- ARGV {dt, uuid, uuid, uuid, ...}
-- returns: list of uuids whose timeouts have been updated

redis.replicate_commands()
local out = {}
local new_t = redis.call('time')
new_t = tonumber(ARGV[1]) + tonumber(new_t[1]) + tonumber(new_t[2]) / 1000000
local big = new_t + 1
for i, item in ipairs(ARGV, 2) do
    if tonumber(redis.call('zscore', KEYS[1], item) or big) < new_t then
        redis.call('zadd', KEYS[1], ARGV[1], item)
        out[#out + 1] = item
    end
end

return out
''')

class Data(object):
    '''
    An object that represents an abstract data queue with 0/1 or 1+ removal
    semantics. Internally works much the same way as tasks, just different keys,
    and no explicit associated task.

    For named queues with arbitrary names, with / without default timeouts and
    deadletter queues::

        # 0/1 with no retries is the default behavior
        data_queue = rpqueue.Data("queue_name")

        # 0/1 or 1+ queue, with up to 5 times visible, visibility controlled by
        # the receiver, no deadletter queue
        data_queue = rpqueue.Data("queue_name", attempts=5)
        data_queue.get_data() # 0/1 queue
        data_queue.get_data(vis_timeout=5) # 1+ queue with vis_timeout > 0

        # 0/1 or 1+ queue with default visibility timeout, and automatic
        # deadletter queue insertion on failure with 1+ queue operation
        data_queue = rpqueue.Data("queue_name", attempts=5, vis_timeout=5, use_dead=True, is_tasks=False)
        data_queue.get_data(vis_timeout=0) # 0/1 queue
        data_queue.get_data(vis_timeout=5) # 1+ queue with vis_timeout > 0

    If you find that you need access to the various deadletter queues (because
    you are using them and want to clean them out)...

    For the Data deadletter Queue::

        data_dlq = rpqueue.Data(rpqueue.DEADLETTER_QUEUE)

    For the Tasks deadletter Queue, with this interface::

        tasks_dlq = rpqueue.Data(rpqueue.DEADLETTER_QUEUE, is_tasks=True)

    You can only pull data from these queues in a 0/1 fashion with this

    '''
    __slots__ = 'queue', 'attempts', 'vis_timeout', 'use_dead', 'is_tasks', 'is_dead', 'dlq'
    def __init__(self, queue, attempts=1, vis_timeout=0, use_dead=None, is_tasks=False, is_dead=False):
        '''
        - ``queue`` - name of the queue
        - ``attempts`` - how many times to try/retry reading the data
        - ``vis_timeout`` - how long should items be hidden between tries (see ``refresh_data()`` and ``done_data()``)
        - ``use_dead`` - should we use a deadletter queue for items that can't be successfully processed?
        - ``is_tasks`` - is this a task queue that we're trying to access?
        - ``is_dead`` - is this actually a deadletter queue (default true for the global default deadletter queue name)

        If use_dead is true-ish, and this is not a deadletter queue itself,
        attempts to ``get_data()`` on an item more than ``attempts`` times with
        a >0 ``vis_timeout`` will cause the item to be placed into the
        associated deadletter queue.

        '''
        self.queue = queue
        self.attempts = max(int(attempts), 1)
        # negative timeout would default all reads to "peeking"
        self.vis_timeout = max(float(vis_timeout), 0)
        self.use_dead = int(bool(use_dead))
        self.is_tasks = is_tasks
        self.is_dead = is_dead
        if is_dead:
            self.dlq = queue
        elif use_dead and isinstance(use_dead, bytes):
            self.dlq = use_dead
        else:
            self.dlq = DEADLETTER_QUEUE

    @property
    def qkey(self):
        if self.is_tasks:
            return PNOW_KEY + self.queue
        return DATA_KEY + self.queue

    @property
    def mkey(self):
        if self.is_tasks:
            return MESSAGES_KEY + self.queue
        return DATAM_KEY + self.queue

    @property
    def vkey(self):
        if self.is_tasks:
            return VISIBILITY_KEY + self.queue
        return DATAV_KEY + self.queue

    @property
    def dkey(self):
        if self.is_tasks:
            return PNOW_KEY + self.dlq
        return DATA_KEY + self.dlq

    def currently_available(self):
        '''
        Returns the number of (available, invisible) items in the queue.
        '''
        pipe = get_connection().pipeline(True)
        pipe.hlen(self.mkey)
        pipe.zcard(self.vkey)
        im, iv = pipe.execute()
        return (im - iv, iv)

    def put_data(self, data, use_dead=None, is_one=False, chunksize=512):
        '''
        Puts data items into the queue. Data is assumed to be a list of data
        items, each of which will be assigned a new UUID as an identifier before
        being placed into queues / mappings as necessary.

         - ``data`` - list of json-encodable data items
         - ``use_dead`` - if provided and true-ish, will spew attempts exceeded
           data items into data DEADLETTER_QUEUE (queue has a default, can be
           overridden here to enable on this inserted data)
         - ``is_one`` - if you provide a list to ``data``, but just want that to be
           one item, you can ensure that with ``is_one=True``
         - ``chunksize`` - the number of items to insert per pipeline flush

        '''
        if is_one:
            data = [data]
        use_dead = int(bool(self.use_dead if use_dead is None else use_dead))
        proto = {}
        if self.attempts > 1:
            proto['_attempts'] = self.attempts
        if use_dead:
            proto['_use_dead'] = use_dead
        proto = json.dumps(proto)
        conn = get_connection()
        t = _gt(conn)
        pipe = conn.pipeline(True)
        lq = DATA_KEY + self.queue
        hq = DATAM_KEY + self.queue
        chunksize = max(chunksize, 1)
        added = {}
        add = {}
        for i, d in enumerate(data):
            tid = str(uuid.uuid4())
            add[tid] = '["%s","",%s,%s,%.6f]'%(tid, json.dumps(d), proto, t)
            added[tid] = d
            if i and not i % chunksize:
                if self.queue == DEADLETTER_QUEUE or self.is_dead:
                    pipe.rpush(lq, *list(add.values()))
                else:
                    pipe.hmset(hq, add)
                    pipe.rpush(lq, *list(add))
                t = _gt(pipe)
                add = {}
        if add:
            if self.queue == DEADLETTER_QUEUE or self.is_dead:
                pipe.rpush(lq, *list(add.values()))
            else:
                pipe.hmset(hq, add)
                pipe.rpush(lq, *list(add))
                pipe.execute()

        return added

    def get_data(self, items=1, vis_timeout=None, get_timeout=None):
        '''
        Gets 1-1000 data items from the queue.

         - ``items`` - number of items to get from the queue, 1 to 1000
         - ``vis_timeout``

           - If 0 or None, will pull items in 0/1 fashion.

           - If >0, will pull items in a 1+ fashion with the visibility timeout as provided (you must call ``done_data(items)`` to 'finish' with queue items, or `refresh_data(items, vis_timeout)` to refresh the visibility timeout on certain tasks)

           - If <0, will peek items in the queue without pulling them

         - `get_timeout` - how long to wait for at least 1 item

        Note: if you are using the DEADLETTER_QUEUE, all item removals are 0/1

        If this is a data queue, will return::

            {<uuid>: <data_item>, <uuid>: <data_item>}

        If this is a data deadletter queue, will return::

            {<uuid>: [<uuid>, '', <your data>, <extra fields>, <insert time>], ...}

        If this is a task queue or task deadletter queue, will return::

            {<uuid>: [<uuid>, <task_function_name>, <args>, <kwargs>, <scheduled time>], ...}
            # to access the function by <task_function_name>, see: rpqueue.REGISTRY

        Use::

            dq = Data('queue_name', attempts=3)
            inserted = dq.put_data([1,2,3,4])
            removed = dq.get_data(2, vis_timeout=30) # get the data with a timeout
            dq.refresh_data(removed, vis_timeout=45) # to update / refresh the lease
            dq.done_data(removed) # when done processing


        '''
        conn = get_connection()
        items = max(1, min(items, 1000))
        vis_timeout = self.vis_timeout if vis_timeout is None else vis_timeout
        load = json.loads
        if self.queue == DEADLETTER_QUEUE or self.is_dead:
            pipe = conn.pipeline(True)
            qk = self.qkey
            pipe.lrange(qk, 0, items-1)
            if vis_timeout is None or vis_timeout >= 0:
                pipe.ltrim(qk, items, -1)
            data = pipe.execute()[0]
            return {d[0]: d for d in map(load, data)}

        out = {}
        keys = [
            self.vkey,
            self.qkey,
            self.mkey,
            self.vkey,
            self.dkey,
        ]
        keys2 = keys[1:]
        more = 1

        data_items = None
        ts = _gt(conn)
        end = ts + max((0 if get_timeout is None else get_timeout), .000001)
        while len(out) < items and ts < end:
            if not more and not data_items:
                remain = max(end - time.time(), 0)
                if remain >= 1:
                    # will cycle the item in the queue, but will at least wait
                    # to wake us until something should be ready
                    conn.brpoplpush(keys[1], keys[1], 1)
                else:
                    time.sleep(min(.05, remain))
            more = _handle_delayed_lua(conn, keys=keys,
                args=[max(QUEUE_ITEMS_PER_PASS, items)])

            wanted = items - len(out)
            ts, data_items = _get_data_lua(conn, keys=keys2, args=[
                wanted, vis_timeout, self.attempts, self.use_dead])
            ts = float(ts)

            for item in data_items:
                if item:
                    item = load(item)
                    if self.is_tasks:
                        out[item[0]] = item
                    else:
                        out[item[0]] = item[2]

        return out

    def refresh_data(self, items, vis_timeout=None):
        '''
        Refreshes the vis_timeout on your provided item uuids, if available.

        Returns: List of item uuids refreshed with the provided vis_timeout.

        '''
        if not isinstance(items, (list, tuple)):
            items = list(items)
        if items:
            vis_timeout = self.vis_timeout if vis_timeout is None else vis_timeout
            vis_timeout = 0 if vis_timeout is None else max(vis_timeout, 0)
            return _refresh_lua(get_connection(), keys=[self.vkey], args=[vis_timeout] + items)
        return []

    def done_data(self, items):
        '''
        Call when you are done with data that has a visibility timeout > 0.
        '''
        if not isinstance(items, (list, tuple)):
            items = list(items)
        if items:
            pipe = get_connection().pipeline(True)
            pipe.hdel(self.mkey, *items)
            pipe.zrem(self.vkey, *items)
            return pipe.execute()[0]
        return 0

    def delete_all(self):
        '''
        Deletes all data in the queue. If you want to delete data from the
        DEADLETTER_QUEUE, you should::

            Data(DEADLETTER_QUEUE).delete_all()

        To clear out the Tasks DEADLETTER_QUEUE::

            Data(DEADLETTER_QUEUE, is_tasks=True).delete_all()

        For all other queues::

            Data(<name>, is_tasks=<true or false>).delete_all()

        '''
        get_connection().delete(self.qkey, self.mkey, self.vkey)



class Task(object):
    '''
    An object that represents a task to be executed. These will replace
    functions when any of the ``@task``, ``@periodic_task``, or ``@cron_task``
    decorators have been applied to a function.
    '''
    __slots__ = 'queue', 'name', 'function', 'delay', 'attempts', 'retry_delay', \
                'save_results', 'vis_timeout', 'use_dead'
    def __init__(self, queue, name, function, delay=None, never_skip=False,
                 attempts=1, retry_delay=30, low_delay_okay=False, save_results=0,
                 vis_timeout=0, use_dead=None):
        '''
        These Task objects are automatically created by the @task,
        @periodic_task, and @cron_task decorators. You shouldn't need to
        call this directly.
        '''
        self.queue = queue
        self.name = name
        self.function = function
        self.attempts = int(max(attempts, 1))
        self.retry_delay = max(retry_delay, 0)
        self.save_results = max(save_results, 0)
        self.vis_timeout = vis_timeout
        self.use_dead = use_dead
        if delay is None:
            pass
        elif isinstance(delay, _number_types):
            if not low_delay_okay:
                _assert(delay >= MINIMUM_DELAY,
                    "periodic delay must be at least %r seconds, you provided %r",
                    MINIMUM_DELAY, delay)
            else:
                delay = max(delay, 0)
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

    def __call__(self, taskid=None, nowarn=False, exp='0'):
        '''
        This wraps the function in an _ExecutingTask so as to offer reasonable
        introspection upon potential exception. If you want to execute the
        original function inline, use: ``my_task.function(...)`` .
        '''
        if not taskid and not nowarn:
            log_handler.debug("You probably intended to call the function: %s, you are half-way there", self.name)
        return _ExecutingTask(self, taskid, exp)

    def next(self, now=None):
        '''
        Calculates the next run time of recurring tasks.
        '''
        if not EXECUTE_TASKS:
            return None

        if isinstance(self.delay, CronTab):
            return self.delay.next(now)
        return self.delay

    def execute(self, *args, **kwargs):
        '''
        Invoke this task with the given arguments inside a task processor.

        Optional arguments:

            * *delay* - how long to delay the execution of this task for, in
              seconds
            * *taskid* - override the taskid on this call, can be used to choose
              a destination key for the results (be careful!)
            * *_queue* - override the queue to be used in this call, which can
              be used to alter priorities of individual calls when coupled with
              queue priorities
        '''
        delay = kwargs.pop('delay', None) or 0
        taskid = kwargs.pop('taskid', None)
        _queue = kwargs.pop('_queue', None) or self.queue
        if self.attempts > 1 and '_attempts' not in kwargs:
            kwargs['_attempts'] = self.attempts

        rpq = None
        if '_rpqueue_module' in kwargs:
            rpq = kwargs.pop('_rpqueue_module')
            ec = rpq._enqueue_call
            conn = rpq.get_connection()
            et = rpq._execute_task
            enq = rpq._EnqueuedTask
        else:
            ec = _enqueue_call
            conn = get_connection()
            et = _execute_task
            enq = _EnqueuedTask

        if kwargs.pop('execute_inline_now', None):
            # allow for testing/debugging to execute the task immediately
            et([None, self.name, args, kwargs, 0], conn)
            return

        taskid = ec(conn, _queue , self.name, args, kwargs, delay, taskid=taskid,
                    vis_timeout=self.vis_timeout, use_dead=self.use_dead)
        return enq(self.name, taskid, _queue, self)

    def retry(self, *args, **kwargs):
        '''
        Invoke this task as a retry with the given arguments inside a task
        processor.

        To retry, the task must accept ``_attempts`` as a parameter, either
        directly or via ``**kwargs``.
        '''
        attempts = max(kwargs.pop('_attempts', 0), 0) - 1
        if attempts < 1:
            return
        kwargs['_attempts'] = attempts
        if self.retry_delay > 0 and 'delay' not in kwargs:
            kwargs['delay'] = self.retry_delay
        return self.execute(*args, **kwargs)

    def __repr__(self):
        return "<Task function=%s>"%(self.name,)

_Task = Task

class _ExecutingTask(object):
    '''
    An object that offers introspection into running tasks.
    '''
    __slots__ = 'task', 'taskid', 'args', 'kwargs', 'exp'
    def __init__(self, task, taskid, exp):
        self.task = task
        self.taskid = taskid.encode('latin-1') if PY3 else taskid
        self.args = None
        self.kwargs = None
        self.exp = exp

    def __call__(self, *args, **kwargs):
        self.args = args
        self.kwargs = kwargs
        CURRENT_TASK.task = self
        k = RESULT_KEY + self.taskid
        want_status = self.taskid not in REGISTRY
        conn = get_connection()
        if want_status:
            _setex(conn, k, '', 60)

        vt = max(kwargs.get('_vis_timeout', 0), 0)

        try:
            result = self.task.function(*args, **kwargs)
        except:
            if want_status:
                conn.delete(k)
            raise
        del CURRENT_TASK.task
        pipe = conn.pipeline(False)
        if vt > 0:
            _finish_step_lua(pipe,
                keys=[MESSAGES_KEY + self.task.queue, VISIBILITY_KEY + self.task.queue],
                args=[self.taskid, self.exp or '0'])

        if self.task.save_results > 0:
            _setex(pipe, k,
                json.dumps(result),
                self.task.save_results,
            )
        elif want_status:
            pipe.delete(k)
        pipe.execute()

    def __repr__(self):
        return "<ExecutingTask taskid=%s function=%s args=%r kwargs=%r>"%(
            self.taskid, self.task.name, self.args, self.kwargs)

class _EnqueuedTask(object):
    '''
    An object that allows for simple status checks on tasks to be executed.
    '''
    __slots__ = 'name', 'taskid', 'queue', 'task'
    def __init__(self, name, taskid, queue, task=None):
        self.name = name
        self.taskid = taskid
        self.queue = queue
        self.task = task if task else REGISTRY.get(name)

    @property
    def args(self):
        '''
        Get the arguments that were passed to this task.
        '''
        args = get_connection().hget(MESSAGES_KEY + self.queue, self.taskid)
        if not args:
            return "<already executed>"
        return args

    @property
    def status(self):
        '''
        Get the status of this task.
        '''
        conn = get_connection()
        pipe = conn.pipeline(True)
        eta = conn.zscore(QUEUE_KEY + self.queue, self.taskid)
        if eta is None:
            if conn.hexists(MESSAGES_KEY + self.queue, self.taskid):
                return "waiting"

            tid = self.taskid
            if PY3 and isinstance(tid, str):
                tid = tid.encode('latin-1')

            exp = self.task.save_results if self.task and self.task.save_results > 0 else 60
            k = RESULT_KEY + tid
            if pipe.expire(k, exp).get(k).execute()[-1] == '':
                return "started"

            return "done"
        elif eta <= time.time():
            return "late"
        return "early"

    def cancel(self):
        '''
        Cancel the task, if it has not been executed yet.

        Returns True if the cancellation was successful.
        Returns False if no information about task cancellation is known.
        Returns -1 if the task executed, saved a result, and the result was
        deleted.
        '''
        return delete_item(self.queue, self.taskid)

    def wait(self, timeout=30):
        '''
        Will wait up to the specified timeout for the task to start execution,
        returning whether the task started execution.
        '''
        end = time.time() + max(timeout, .01)
        while self.status != 'done' and end > time.time():
            time.sleep(.01)
        return self.status == 'done'

    @property
    def result(self):
        '''
        Get the value returned by the task. Requires task.save_results > 0.

        See :py:function:``result(taskid)``
        '''
        return results([self.taskid])[0]

    def __repr__(self):
        return "<EnqueuedTask taskid=%s queue=%s function=%s status=%s>"%(
            self.taskid, self.queue, self.name, self.status)

class NoLock(Exception):
    "Raised when a lock cannot be acquired"

class SimpleLock(object):
    '''
    This lock is dirt simple. You shouldn't use it for anything unless you
    want it to fail fast when the lock is already held.

    If Redis had a "setnxex key value ttl" that set the 'key' to 'value' if it
    wasn't already set, and also set the expiration to 'ttl', this lock
    wouldn't exist.

    (Redis now has this functionality, but we need to support legacy)
    '''
    def __init__(self, conn, name, duration=1):
        self.conn = conn
        self.name = name
        self.duration = duration
    def __enter__(self):
        pipe = self.conn.pipeline(True)
        pipe.zremrangebyscore(LOCK_KEY, 0, time.time() - 15)
        pipe.zscore(LOCK_KEY, self.name)
        if pipe.execute()[-1]:
            # lock is already held, :(
            raise NoLock()
        if not _zadd(self.conn, LOCK_KEY, {self.name: time.time() + self.duration}):
            # we just refreshed the other lock, no big deal, it didn't exist
            # one round-trip ago
            raise NoLock()
        return self
    def __exit__(self, type, value, traceback):
        if type is None:
            self.conn.zrem(LOCK_KEY, self.name)
    def refresh(self):
        'Refreshes a lock'
        _zadd(self.conn, LOCK_KEY, {self.name: time.time() + self.duration})

def task(args=None, queue=b'default', attempts=1, retry_delay=30, save_results=0, vis_timeout=0, use_dead=False, **kwargs):
    '''
    Decorator to allow the transparent execution of a function as a task.

    Used like::

        @task(queue='bar')
        def function1(arg1, arg2, ...):
            'will execute from within the 'bar' queue.'

        @task
        def function2(arg1, arg2, ...):
            'will execute from within the 'default' queue.'
    '''
    queue = queue or b'default'
    attempts = max(1, attempts)
    retry_delay = max(retry_delay, 0)
    vis_timeout = max(vis_timeout, 0)
    use_dead = bool(use_dead)
    save_results = max(save_results, 0)
    name = kwargs.pop('name', None)
    assert isinstance(queue, bytes)
    def decorate(function):
        _name = name or '%s.%s'%(function.__module__, function.__name__)
        return _Task(queue, _name, function, attempts=attempts, retry_delay=retry_delay,
            save_results=save_results, vis_timeout=vis_timeout, use_dead=use_dead)
    if args:
        return decorate(args[0])
    return decorate

_to_seconds = lambda td: td.days * 86400 + td.seconds + td.microseconds / 1000000.
_number_types = (int, float, )
if not PY3:
    _number_types += (long,)
_number_types_plus = _number_types + (datetime.timedelta,)

def periodic_task(run_every, queue=b'default', never_skip=False, attempts=1, retry_delay=30, low_delay_okay=False, save_results=0, name=None):
    '''
    Decorator to allow the automatic repeated execution of a function every
    run_every seconds, which can be provided via int, long, float, or via a
    datetime.timedelta instance. Run from the context of the given queue. Used
    via::

        @periodic_task(25)
        def function1():
            'will be executed every 25 seconds from within the 'default' queue.'

        @periodic_task(timedelta(minutes=5), queue='bar')
        def function2():
            'will be executed every 5 minutes from within the 'bar' queue.'

    If never_skip is provided and is considered True like::

        @periodic_task(60, never_skip=True)
        def function3():
            pass

    ... and the function was scheduled to be executed at 4:15PM and 5 seconds,
    but actually executed at 4:25PM and 13 seconds, then prior to execution,
    it will be rescheduled to execute at 4:16PM and 5 seconds, which is 60
    seconds after the earlier scheduled time (it never skips a scheduled
    time). If you instead had the periodic task defined as::

        @periodic_task(60, never_skip=False)
        def function4():
            pass

    ... and the function was scheduled to be executed at 4:15PM and 5 seconds,
    but actually executed at 4:25PM and 13 seconds, then prior to execution,
    it will be rescheduled to execute at 4:26PM and 13 seconds, which is 60
    seconds after the current time (it skips any missed scheduled time).

    '''
    _assert(isinstance(queue, bytes),
        "queue name provided must be a string, not %r", queue)
    _assert(isinstance(run_every, _number_types_plus),
        "run_every provided must be an int, long, float, or timedelta, not %r",
        run_every)
    if isinstance(run_every, datetime.timedelta):
        run_every = _to_seconds(run_every)
    if not low_delay_okay:
        _assert(run_every >= MINIMUM_DELAY,
            "periodic execution timer must be at least %r, you provided %r",
            MINIMUM_DELAY, run_every)
    else:
        run_every = max(run_every, 0)
    def decorate(function):
        _name = name or '%s.%s'%(function.__module__, function.__name__)
        return _Task(queue, _name, function, delay=run_every, never_skip=never_skip,
            attempts=attempts, retry_delay=retry_delay, low_delay_okay=low_delay_okay,
            save_results=save_results)
    return decorate

def cron_task(crontab, queue=b'default', never_skip=False, attempts=1, retry_delay=30, save_results=0, name=None):
    '''
    Decorator to allow the automatic repeated execution of a function
    on a schedule with a crontab syntax. Crontab syntax provided by the
    'crontab' Python module: http://pypi.python.org/pypi/crontab/
    Which must also be installed to use this decorator.

    Similar in use to the @periodic_task decorator::

        @cron_task('* * * * *')
        def function1():
            'will be executed every minute'

        @cron_task('*/5 * * * *', queue='bar')
        def function2():
            'will be executed every 5 minutes from within the 'bar' queue.'

    If never_skip is provided and is considered True, it will attempt to
    never skip a scheduled task, just like the @periodic_task decorator.

    Please see the crontab package documentation or the crontab Wikipedia
    page for more information on the meaning of the schedule.
    '''
    if not CronTab.__slots__:
        raise Exception("You must have the 'crontab' module installed to use @cron_task")

    _assert(isinstance(queue, str),
        "queue name provided must be a string, not %r", queue)
    _assert(isinstance(crontab, str),
        "crontab provided must be a string, not %r", crontab)
    crontab = CronTab(crontab)
    def decorate(function):
        _name = name or '%s.%s'%(function.__module__, function.__name__)
        return _Task(queue, _name, function, delay=crontab, never_skip=never_skip,
                     attempts=attempts, retry_delay=retry_delay, save_results=save_results)
    return decorate

PREVIOUS = None

def quit_on_signal(signum, frame):
    SHOULD_QUIT[0] = 1

SUCCESS_LOG = None

def execute_tasks(queues=None, threads_per_process=1, processes=1, wait_per_thread=1, module=None):
    '''
    Will execute tasks from the (optionally) provided queues until the first
    value in the global SHOULD_QUIT is considered false.
    '''
    global EXECUTE_TASKS
    EXECUTE_TASKS = True
    signal.signal(signal.SIGUSR1, quit_on_signal)
    signal.signal(signal.SIGTERM, quit_on_signal)
    log_handler.setLevel(LOG_LEVELS[LOG_LEVEL.upper()])
    sp = []
    def _signal_subprocesses(signum, frame):
        for pp in sp:
            if pp.is_alive():
                # this won't actually kill the other process, just print its stack.
                os.kill(pp.pid, signal.SIGUSR2)
        _print_stackframes_on_signal(signum, frame)
    signal.signal(signal.SIGUSR2, _signal_subprocesses)

    threads_per_process = max(threads_per_process, 1)
    processes = max(processes, 1)
    __import__(module) # for any connection modification side-effects
    log_handler.info("Starting %i subprocesses", processes)
    for p in range(processes):
        pp = multiprocessing.Process(target=execute_task_threads, args=(queues, threads_per_process, 1, module))
        pp.daemon = True
        pp.start()
        sp.append(pp)
    while not SHOULD_QUIT[0]:
        _handle_delayed(get_connection(), queues, 1)

    log_handler.info("Waiting for %i subprocesses to shutdown", len(sp))
    # wait some time for processes to die...
    end_time = threads_per_process * wait_per_thread + time.time()
    while end_time > time.time() and sp:
        for i in range(len(sp)-1, -1, -1):
            sp[i].join(.01)
            if not sp[i].is_alive():
                del sp[i]
        time.sleep(.05)
    # at this point we have waited long enough, they will die when we die.
    log_handler.info("Letting %i processes die", len(sp))

def _print_stackframes_on_signal(signum, frame):
    pid = os.getpid()
    for tid, frame in sys._current_frames().items():
        log_handler.critical('PID: %s THREAD: %s\n%s' % (pid, tid, ''.join(traceback.format_stack(frame))))

def execute_task_threads(queues=None, threads=1, wait_per_thread=1, module=None):
    signal.signal(signal.SIGUSR1, quit_on_signal)
    signal.signal(signal.SIGTERM, quit_on_signal)
    signal.signal(signal.SIGUSR2, _print_stackframes_on_signal)
    if module:
        __import__(module)
    if AFTER_FORK:
        try:
            AFTER_FORK()
        except:
            log_handler.exception("ERROR: Exception in AFTER_FORK function: %s", traceback.format_exc().rstrip())
    st = []
    log_handler.info("PID %i executing tasks in %i threads %s", os.getpid(), threads, MESSAGES_KEY)
    for t in range(threads-1):
        tt = threading.Thread(target=_execute_tasks, args=(queues,))
        tt.daemon = True
        tt.start()
        st.append(tt)
    _execute_tasks(queues)

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

        _execute_task(work, conn)

def _execute_task(work, conn):
    '''
    Internal implementation detail to execute a single task.
    '''
    exp = '0'
    try:
        if len(work) == 6:
            exp = work.pop()
        taskid, fname, args, kwargs, scheduled = work
    except ValueError as err:
        log_handler.exception(err)
        return

    global SUCCESS_LOG
    if SUCCESS_LOG is None:
        SUCCESS_LOG = getattr(log_handler, SUCCESS_LOG_LEVEL.lower(), log_handler.debug)

    now = datetime.datetime.utcnow()
    jitter = time.time() - scheduled
    log_handler.debug("RECEIVED: %s %s at %r (%r late)", taskid, fname, now, jitter)

    if fname not in REGISTRY:
        log_handler.debug("ERROR: Missing function %s in registry", fname)
        return

    to_execute = REGISTRY[fname](taskid, True, exp)

    try:
        to_execute(*args, **kwargs)
    except (KeyboardInterrupt, SystemExit):
        raise
    except:
        log_handler.exception("ERROR: Exception in task %r: %s", to_execute, traceback.format_exc().rstrip())
    else:
        SUCCESS_LOG("SUCCESS: Task completed: %s %s", taskid, fname)

def set_priority(queue, qpri, conn=None):
    '''
    Set the priority of a queue. Lower values means higher priorities.
    Queues with priorities come before queues without priorities.
    '''
    conn = conn or get_connection()
    _zadd(conn, QUEUES_PRIORITY, {queue:qpri})

def known_queues(conn=None):
    '''
    Get a list of all known queues.
    '''
    conn = conn or get_connection()
    q, pq, dq = conn.pipeline(True) \
        .smembers(QUEUES_KNOWN) \
        .zrange(QUEUES_PRIORITY, 0, -1) \
        .exists(NOW_KEY + DEADLETTER_QUEUE) \
        .execute()
    q = list(sorted(q, key=lambda x:x.lower()))
    if dq:
        pq.append(DEADLETTER_QUEUE)
    return pq + [qi for qi in q if qi not in pq]

def read_deadletter(conn=None, delete=False, limit=100, is_data=False):
    '''
    Read and optionally delete contents of the deadletter queue.
    '''
    conn = conn or get_connection()
    limit = max(limit, 1)
    k = (DATA_KEY if is_data else NOW_KEY) + DEADLETTER_QUEUE
    if delete:
        pipe = conn.pipeline(True)
        pipe.lrange(k, 0, limit)
        pipe.lrem(k, 0, limit)
        return pipe.execute()[0]
    return conn.lrange(k, 0, limit)

def _window(size, seq):
    iterators = []
    for i in range(size):
        iterators.append(itertools.islice(seq, i, len(seq), size))
    return itertools.izip(*iterators)

def queue_sizes(conn=None):
    '''
    Return a list of all known queues, their sizes, and the number of items
    that have been seen in the queue.
    '''
    conn = conn or get_connection()
    queues = known_queues(conn)

    pipeline = conn.pipeline(False)
    for queue in queues:
        pipeline.llen(NOW_KEY + queue)
        pipeline.llen(PNOW_KEY + queue)
        pipeline.zcard(QUEUE_KEY + queue)
    for queue in queues:
        pipeline.hget(SEEN_KEY, queue)
    items = pipeline.execute()
    return zip(queues, map(sum, _window(3, items[:-len(queues)])), items[-len(queues):])

def clear_queue(queue, conn=None, delete=False, is_data=False):
    '''
    Delete all items in a given queue, optionally deleting the queue itself.
    '''
    conn = conn or get_connection()
    pipeline = conn.pipeline(True)
    if not is_data:
        nkey = NOW_KEY + queue
        pkey = PNOW_KEY + queue
        qkey = QUEUE_KEY + queue
    else:
        nkey = DATAM_KEY + queue
        pkey = DATA_KEY + queue
        qkey = DATAV_KEY + queue
    keys = [nkey, pkey, qkey]
    if not is_data:
        keys.extend((MESSAGES_KEY + queue, VISIBILITY_KEY + queue))
    pipeline.llen(pkey)
    pipeline.delete(*keys)
    if delete:
        pipeline.srem(QUEUES_KNOWN, queue)
        pipeline.zrem(QUEUES_PRIORITY, queue)
    return pipeline.execute()[0]

def get_task(name):
    '''Get a task dynamically by name. The task's module must be loaded first.'''
    return REGISTRY.get(name)

def result(taskid, conn=None):
    '''
    Get the results of remotely executing tasks from one or more taskids.

    If a task is configured with save_results>0, any remote execution of that task
    will save its return value to expire after that many seconds.

    These two ways of fetching the result are equivalent::

        >>> remote = task.execute()
        >>> # some concurrent logic
        >>> result = remote.result

        >>> taskid = task.execute().taskid
        >>> # some concurrent logic
        >>> result = rpqueue.result(taskid)

    .. note:: If you have more than one taskid whose results you want to fetch,
        check out ``rpqueue.results()`` below.
    '''
    return results([taskid], conn=conn)[0]

def results(taskids, conn=None):
    '''Get the results of remotely executing tasks from one or more taskids.

    If a task is configured with save_results>0, any remote execution of that task
    will save its return value to expire after that many seconds.

    These two ways of fetching the result are equivalent::

        >>> remote = [t.execute() for t in tasks]
        >>> # some concurrent logic
        >>> results = [r.result for r in remote]

        >>> taskids = [t.execute().taskid for t in tasks]
        >>> # some concurrent logic
        >>> results = rpqueue.results(taskids)

    '''
    conn = conn or get_connection()
    if PY3:
        taskids = [tid.encode('latin-1') for tid in taskids]

    results = conn.mget([RESULT_KEY + tid for tid in taskids])
    return [(json.loads(r.decode('latin-1')) if r is not None else None) for r in results]

_BAD_ITEM = (None, '<unknown>', '<unknown>', '<unknown>')
_DONE_ITEM = (None, '<done>', '<done>', '<done>')
def get_page(queue, page, per_page=50, conn=None):
    '''
    Get information about a sequence of tasks in the queue.
    '''
    conn = conn or get_connection()
    page = max(0, page-1)
    start = page * per_page
    end = start + per_page - 1
    tasks = conn.lrange(PNOW_KEY + queue, start, end)
    lt = conn.llen(PNOW_KEY + queue)
    if len(tasks) < per_page:
        start = start - lt + len(tasks)
        end = start + per_page - 1 - len(tasks)
        tasks.extend(conn.lrange(NOW_KEY + queue, start, end))
        lt += conn.llen(NOW_KEY + queue)
    if len(tasks) < per_page:
        start = start - lt + len(tasks)
        end = start + per_page - 1 - len(tasks)
        tasks.extend(conn.zrange(QUEUE_KEY + queue, start, end, withscores=True))
    if queue == DEADLETTER_QUEUE:
        return tasks

    stasks = [task if isinstance(task, str) else task[0] for task in tasks]
    messages = conn.hmget(MESSAGES_KEY + queue, stasks) if tasks else []
    out = []
    for tid, msg in itertools.izip(tasks, messages):
        if isinstance(tid, str):
            ts = '<now>'
        else:
            tid, ts = tid
        try:
            if msg:
                msg = json.loads(msg)
                ts = ts if len(msg) == 4 else msg[4]
            else:
                msg = _DONE_ITEM
        except:
            msg = _BAD_ITEM
        out.append([tid, ts, msg[1], msg[2], msg[3]])
    return out

def delete_item(queue, taskid, conn=None):
    if queue == DEADLETTER_QUEUE:
        warnings.warn("Can't delete individual items from the deadletter queue")
        return False
    pipeline = (conn or get_connection()).pipeline(True)
    pipeline.hdel(MESSAGES_KEY + queue, taskid)
    pipeline.zrem(RQUEUE_KEY + queue, taskid)
    pipeline.zrem(QUEUE_KEY + queue, taskid)
    k = RESULT_KEY + taskid.encode('utf-8')
    pipeline.get(k) # only count if was non-empty
    pipeline.delete(k) # also delete its results, don't count
    r = pipeline.execute()
    return True if any(r[:3]) else (-1 if r[3] else False)

def print_rows(rows, use_header):
    if not rows:
        return
    if use_header:
        ml = [max(len(str(i)) for i in s) for s in zip(*rows)]
        rows.insert(1, [m*'-' for m in ml])
        fmt = '  '.join("%%-%is"%m for m in ml)
    else:
        fmt = '  '.join('%s' for i in range(len(rows[0])))
    for row in rows:
        print(fmt%tuple(row))

from optparse import OptionGroup, OptionParser
parser = OptionParser(usage='''
    %prog [connection options]
        -> general queue information

    %prog [connection options] --queue <queue> [read options]
        -> more specific information about a queue and it's tasks

    %prog [connection options] --queue <queue> --clear
        -> clear out a queue

    %prog [connection options] --queue <queue> --delete <itemid>
        -> delete a specific item from a given queue''')

cgroup = OptionGroup(parser, "Connection Options")
cgroup.add_option('--host', dest='host', action='store', default='localhost',
    help='The host that Redis is running on')
cgroup.add_option('--port', dest='port', action='store', type='int', default=6379,
    help='The port that Redis is listening on')
cgroup.add_option('--db', dest='db', action='store', type='int', default=0,
    help='The database that the queues are in')
cgroup.add_option('--password', dest='passwd', action='store', default=None,
    help='The password to connect to Redis with')
cgroup.add_option('--pwprompt', dest='pwprompt', action='store_true', default=False,
    help='Will prompt the user for a password before trying to connect')
if redis.__version__ >= '2.4':
    cgroup.add_option('--unixpath', dest='unixpath', action='store', default=None,
        help='The unix path to connect to Redis with (use unix path OR host/port, not both')
cgroup.add_option('--timeout', dest='timeout', action='store', type='int', default=30,
    help='How long to wait for a connection or command to succeed or fail')
cgroup.add_option('--prefix', default=None,
    help=('Provide a prefix to keys, allowing for multiple rpqueue runtimes in '
          'the same Redis db without worrying about queue names'))

if __name__ == '__main__':

    vgroup = OptionGroup(parser, "Read Options")
    vgroup.add_option('--queue', dest='queue', action='store',
        help='Which queue to operate on')
    vgroup.add_option('--page', dest='page', action='store', type='int', default=0,
        help='What page of items to show with --queue')
    vgroup.add_option('--skip-header', dest='skip', action='store_true', default=False,
        help='Skip header and auto-spacing when showing queue/item information')
    vgroup.add_option('--show-args', dest='sargs', action='store_true', default=False,
        help='Pass to show the args/kwargs of queue items displayed in the queue')

    dgroup = OptionGroup(parser, "Delete Options")
    dgroup.add_option('--clear', dest='clear', action='store_true', default=False,
        help='Remove all items from the given queue')
    dgroup.add_option('--delete', dest='delete', action='store', default=None,
        help='The item id of the queue item that you want to delete')

    # this piece is not quite done yet, so it is commented for now
    ## igroup = OptionGroup(parser, "Info Options (requires bottle.py)")
    ## igroup.add_option('--info', dest='info', action='store_true', default=False,
        ## help='Run an informational web server on the provided lhost and lport')
    ## igroup.add_option('--lhost', dest='lhost', action='store', default='localhost',
        ## help='The host/ip that the informational web server should listen on')
    ## igroup.add_option('--lport', dest='lport', action='store', type='int', default=6300,
        ## help='The port that the informational web server should listen on')

    parser.add_option_group(cgroup)
    parser.add_option_group(vgroup)
    parser.add_option_group(dgroup)
    options, args = parser.parse_args()
    if options.pwprompt:
        if options.passwd:
            print("You must pass either --password OR --pwprompt not both.")
            sys.exit(1)
        import getpass
        options.passwd = getpass.getpass()

    if options.prefix:
        set_key_prefix(options.prefix)
    set_redis_connection_settings(options.host, options.port, options.db,
        options.passwd, options.timeout, getattr(options, 'unixpath', None))

    if (options.clear or options.page or options.delete) and not options.queue:
        print("You must provide '--queue <queuename>' with that command")
        sys.exit(1)
    if (bool(options.clear) + bool(options.page) + bool(options.delete)) > 1:
        print("You can choose at most one of --clear, --page, or --delete")
        sys.exit(1)

    if options.clear:
        items = clear_queue(options.queue)
        print("Queue %r cleared of %i items."%(options.queue, items))
    elif options.queue and not options.delete:
        items = get_page(options.queue, options.page)
        out = []
        if not options.skip:
            out.append(['name', 'scheduled', 'taskid'])
            if options.sargs:
                out[0].extend(['args', 'kwargs'])

        for tid, ts, name, args, kwargs in items:
            out.append([name, datetime.datetime.utcfromtimestamp(ts).isoformat(), tid])
            if options.sargs:
                out[-1].extend([args, kwargs])

        print_rows(out, not options.skip)
    elif options.delete:
        if delete_item(options.queue, options.delete):
            print("Deleted item:", options.delete)
        else:
            print("Item not found")
            sys.exit(1)
    else:
        known = queue_sizes()
        if not options.skip:
            known.insert(0, ('Queue', 'Size', 'Seen'))
        print_rows(known, not options.skip)
