
from __future__ import print_function
import json
import multiprocessing
import threading
import time
import unittest

import rpqueue

# make sure we're not hiding errors during tests
rpq2 = rpqueue.new_rpqueue('test', b'tpfix')
rpqueue.log_handler.setLevel(rpqueue.logging.DEBUG)

rpqueue.DEFAULT_QUEUE = queue = b'TEST_QUEUE'
rpqueue.DEADLETTER_QUEUE = b'TEST_DEADLETTER_QUEUE'

if __name__ == '__main__':
    import os
    _host = os.environ.get('REDIS_HOST', 'localhost')
    _port = int(os.environ.get('REDIS_PORT', '6379'))
    _db = int(os.environ.get('REDIS_DB', '15'))
    rpqueue.set_redis_connection_settings(host=_host, port=_port, db=_db)
    rpq2.set_redis_connection_settings(host=_host, port=_port, db=_db)
    rpqueue.SHOULD_QUIT[0] = 0
    rpq2.SHOULD_QUIT[0] = 0
    print("set connection setings!")

def wrap(f):
    def call(*args):
        rpqueue.EXECUTE_TASKS = True
        f(*args)
    return call

def _start_task_processor(rpqueue=rpqueue):
    t = multiprocessing.Process(target=wrap(rpqueue.execute_task_threads), args=([queue],))
    t.daemon = True
    t.start()
    t2 = multiprocessing.Process(target=wrap(rpqueue._handle_delayed), args=(None, None, 60))
    t2.daemon = True
    t2.start()
    return t, t2

saw = multiprocessing.Array('i', (0,))

@rpqueue.task(queue=queue)
def task1(a):
    saw[0] = a

@rpqueue.task(queue=queue, attempts=5, retry_delay=0)
def taskr(v, **kwargs):
    saw[0] = v
    if v < 5:
        taskr.retry(v + 1, **kwargs)

@rpqueue.task(queue=queue, attempts=2, retry_delay=0)
def taskr2(v1, v2, **kwargs):
    if v1 != 0:
        taskr2.retry(v1 - 1, v2, **kwargs)
    else:
        saw[0] = v2

class ExpectedException(Exception):
    pass

@rpqueue.task(queue=queue)
def taske():
    raise ExpectedException("ignore me during tests, I should be logging")

@rpqueue.task(queue=queue)
def speed():
    saw[0] += 1

@rpqueue.task(queue=queue, retry_delay=0)
def speed2(**kwargs):
    saw[0] += 1
    speed2.retry(**kwargs)

@rpqueue.task(queue=queue, retry_delay=.000001)
def speed3(**kwargs):
    saw[0] += 1
    speed3.retry(**kwargs)

@rpqueue.periodic_task(0, queue=queue, low_delay_okay=True)
def periodic_task():
    saw[0] += 1

@rpqueue.task(queue=queue, save_results=30)
def wait_test(n):
    time.sleep(n)
    return n

@rpqueue.task(queue=queue, save_results=30)
def result_test(n):
    return n

@rpqueue.task(queue=queue, attempts=3, vis_timeout=.1, use_dead=True)
def vis_test1(i, **kwargs):
    time.sleep(.2)

@rpqueue.task(queue=queue, attempts=3, vis_timeout=.1, use_dead=False)
def vis_test2(i, **kwargs):
    time.sleep(.2)

@rpqueue.task(queue=queue, attempts=3, vis_timeout=.1, use_dead=True, save_results=30)
def vis_test3(i, **kwargs):
    return i

@rpqueue.task
def simple_task(a):
    saw[0] = a

@rpqueue.periodic_task(.001, queue=queue, low_delay_okay=True)
def periodic_task2(**kwargs):
    saw[0] += 1

@rpqueue.task(save_results=5)
def simple_task2(a):
    saw[0] = a

global_wait_test = wait_test

scale = 1000

class TestRPQueue(unittest.TestCase):
    def _common(self):
        saw[0] = 0
        rpqueue.clear_queue(queue)
        rpqueue.clear_queue(queue + b'2')
        rpqueue.clear_queue(rpqueue.DEADLETTER_QUEUE)
        rpqueue.clear_queue(queue + b'1', is_data=True)
        rpqueue.clear_queue(queue + b'2', is_data=True)
        rpqueue.clear_queue(queue + b'3', is_data=True)
        rpqueue.clear_queue(rpqueue.DEADLETTER_QUEUE, is_data=True)
        rpq2.clear_queue(queue, delete=True)

    def setUp(self):
        self._common()
        rpqueue.SHOULD_QUIT[0] = 0
        self.t, self.t2 = _start_task_processor()

    def tearDown(self):
        rpqueue.SHOULD_QUIT[0] = 1
        if self.t.is_alive():
            self.t.terminate()
        if self.t2.is_alive():
            self.t2.terminate()
        self._common()
        rpqueue.clear_queue(queue, delete=True)
        rpqueue.clear_queue(rpqueue.DEADLETTER_QUEUE, delete=True)
        task1.execute(1)
        speed.execute()
        speed.execute(delay=5)

    def test_simple_task(self):
        task1.execute(1)
        time.sleep(.25)

        self.assertEqual(saw[0], 1)

    def test_delayed_task(self):
        saw[0] = 0
        task1.execute(2, delay=3)

        time.sleep(2.75)
        self.assertEqual(saw[0], 0)
        time.sleep(.5)
        self.assertEqual(saw[0], 2)

    def test_retry_task(self):
        saw[0] = -1
        taskr.execute(0)
        time.sleep(.5)
        self.assertEqual(saw[0], 4)

        saw[0] = -1
        taskr.execute(1)
        time.sleep(.5)
        self.assertEqual(saw[0], 5)

    def test_retry_task2(self):
        taskr2.execute(0, 1)
        time.sleep(.5)
        self.assertEqual(saw[0], 1)

        taskr.execute(1)
        time.sleep(.5)
        self.assertEqual(saw[0], 5)

    def test_exception_no_kill(self):
        taske.execute()
        time.sleep(.001)
        task1.execute(6)

        time.sleep(1)
        self.assertEqual(saw[0], 6)

    def test_periodic_task(self):
        rpqueue.EXECUTE_TASKS = True
        periodic_task.execute(taskid=periodic_task.name)
        time.sleep(2)
        x = saw[0]
        self.assertTrue(x > 0, x)
        print("\n%.1f tasks/second periodic tasks"%(x/2.0,))

    def test_periodic_task2(self):
        saw[0] = 0
        periodic_task2.execute(taskid=periodic_task2.name)
        time.sleep(5)
        x = saw[0]
        self.assertGreater(x, 1)

    def test_z_performance1(self):
        saw[0] = 0
        t = time.time()
        for i in range(scale):
            speed.execute()
        while saw[0] < scale:
            time.sleep(.05)
        s = saw[0]
        dt = time.time() - t
        print("\n%.1f tasks/second injection/running"%(s/dt,))

    def test_z_performance2(self):
        saw[0] = 0
        t = time.time()
        speed2.execute(_attempts=scale)
        while saw[0] < scale:
            time.sleep(.05)
        s = saw[0]
        dt2 = time.time() - t
        print("%.1f tasks/second sequential retries"%(s/dt2,))

    def test_z_performance3(self):
        saw[0] = 0
        _scale = scale / 4
        t = time.time()
        speed3.execute(_attempts=_scale)
        while saw[0] < _scale:
            time.sleep(.05)
        s = saw[0]
        dt3 = time.time() - t
        print("%.1f tasks/second delayed retries"%(s/dt3,))

    def test_wait(self):
        wt = wait_test.execute(.01, delay=1)
        self.assertTrue(wt.wait(2))
        time.sleep(1)
        self.assertEqual(wt.result, .01)

        wt = wait_test.execute(2, delay=5)
        self.assertFalse(wt.wait(4))
        self.assertTrue(wt.cancel())

        wt = wait_test.execute(2, delay=5)
        wt.wait(6)
        self.assertFalse(wt.cancel())
        time.sleep(2.1)
        self.assertEqual(wt.result, 2)

    def test_instance(self):
        rpq2.log_handler.setLevel(rpqueue.logging.CRITICAL)
        @rpq2.task(queue=queue, save_results=30)
        def wait_test(arg):
            # doesn't wait, alternate implementation, for prefix/instance
            # testing :P
            return arg

        if rpqueue.CronTab.__slots__:
            saw[0] = 0
            # we'll use per-second cron tasks :P
            @rpq2.cron_task('* * * * * *', queue=queue)
            def increment():
                saw[0] += 1

        wt = global_wait_test.execute(0)
        time.sleep(1)
        self.assertEqual(wt.result, 0)

        wt2 = wait_test.execute(0)
        time.sleep(1)
        # no task runner!
        self.assertEqual(wt2.result, None)
        t1, t2 = _start_task_processor(rpq2)
        time.sleep(1)
        self.assertEqual(wt2.result, 0)
        rpq2.SHOULD_QUIT[0] = 1
        # wait for the runner to quit
        t1.join()
        t2.join()
        if rpq2.CronTab.__slots__:
            self.assertGreater(saw[0], 2)

    def test_queue_override(self):
        t = result_test.execute(5)
        time.sleep(1)
        self.assertEqual(t.result, 5)

        t = result_test.execute(6, _queue=queue + b'2')
        time.sleep(1)
        self.assertEqual(t.result, None)

    def test_visibility_timeout(self):
        t1 = vis_test1.execute(5)
        t2 = vis_test2.execute(10)
        t3 = vis_test3.execute(15)
        # should be long enough for the tasks to execute / fail
        time.sleep(3)
        items = rpqueue.get_page(rpqueue.DEADLETTER_QUEUE, 0, per_page=3)
        self.assertEqual(len(items), 1)
        item0 = json.loads(rpqueue._preload(items[0]))
        self.assertEqual(item0[1], "__main__.vis_test1")
        self.assertEqual(item0[2], [5])
        self.assertEqual(t3.result, 15)

    def test_data_queue(self):
        data = list(range(5))
        dead = rpqueue.Data(rpqueue.DEADLETTER_QUEUE)
        dead.get_data(1000)
        dq1 = rpqueue.Data(queue + b'1', attempts=1, vis_timeout=0, use_dead=False)
        dq2 = rpqueue.Data(queue + b'2', attempts=3, vis_timeout=.1, use_dead=True)
        dq3 = rpqueue.Data(queue + b'3', attempts=3, vis_timeout=.1, use_dead=False)

        # put data in queue
        # dq1: get data, verify only once
        dq1.put_data(data)
        dq1.put_data(data, is_one=True)
        d = dq1.get_data(5)
        self.assertEqual(len(d), 5)
        self.assertEqual(list(sorted(d.values())), [0,1,2,3,4])
        self.assertEqual(list(dq1.get_data().values())[0], [0,1,2,3,4])

        # dq3: get data, sleep, get data, sleep, get data, sleep, get data (fail)
        dq3.put_data(data)
        dq3.put_data(data, is_one=True)
        d = dq3.get_data(5)
        self.assertEqual(list(sorted(d.values())), [0,1,2,3,4])
        d2 = dq3.get_data()
        self.assertEqual(list(d2.values())[0], [0,1,2,3,4])
        dq3.done_data(d2)
        time.sleep(.2)
        self.assertEqual(d, dq3.get_data(5))
        time.sleep(.2)
        self.assertEqual(d, dq3.get_data(5))
        time.sleep(.2)
        self.assertEqual({}, dq3.get_data(5))

        # dq2: get data, sleep, get data, sleep, get data, sleep, get data (fail)
        dq2.put_data(data)
        dq2.put_data(data, is_one=True)
        d = dq2.get_data(5)
        self.assertEqual(list(sorted(d.values())), [0,1,2,3,4])
        d2 = dq2.get_data()
        self.assertEqual(list(d2.values())[0], [0,1,2,3,4])
        dq2.done_data(d2)
        time.sleep(.2)
        self.assertEqual(d, dq2.get_data(5))
        time.sleep(.2)
        self.assertEqual(d, dq2.get_data(5))
        time.sleep(.2)
        self.assertEqual({}, dq2.get_data(5))

        # verify only dq2 items are in the dead queue
        di = dead.get_data(5)
        di = {k: v[2] for k,v in di.items()}
        self.assertEqual(di, d)

    def test_simple_task2(self):
        saw[0] = 0
        simple_task.execute(113)
        time.sleep(.1)
        self.assertEqual(saw[0], 113)

    def test_eil(self):
        saw[0] = 0
        simple_task.execute(112, execute_inline_now=True)
        self.assertEqual(saw[0], 112)

    def test_call_via_registry(self):
        saw[0] = 0
        t = rpqueue.call_task(simple_task2.name, None, 45)
        t.wait(1)
        time.sleep(.1)
        self.assertEqual(saw[0], 45)

        t = rpqueue.known_tasks[simple_task2.name].execute(46)
        t.wait(1)
        time.sleep(.1)
        self.assertEqual(saw[0], 46)

        self.assertRaises(ValueError, lambda: rpqueue.call_task('does_not_exist', None, verify=True))
        self.assertRaises(KeyError, lambda: rpqueue.known_tasks['does_not_exist'])

    def test_data_delay(self):
        data = list(range(1,6))
        dq1 = rpqueue.Data(queue + b'1', attempts=1, vis_timeout=0, use_dead=False)
        self.assertRaises(ValueError, lambda: dq1.put_data(data, delay=True))
        dq1 = rpqueue.Data(queue + b'1', attempts=1, vis_timeout=1, use_dead=False)
        dq1.put_data(data, delay=.25)
        self.assertFalse(dq1.get_data(1))
        time.sleep(.25)
        self.assertTrue(dq1.get_data(1))

    def test_flush_tasks(self):
        saw[0] = 0
        t = time.time()
        task1.execute(5, delay=100)
        a, b, c = rpqueue.flush_tasks(complete=True, force_delayed=1)
        d = time.time() - t
        self.assertFalse(c)
        self.assertFalse(b)
        self.assertTrue(a)

        # if this fails, we are broken
        self.assertLess(d, 99)
        # if this fails, we are slow
        self.assertLess(d, .25)


if __name__ == '__main__':
    unittest.main()
