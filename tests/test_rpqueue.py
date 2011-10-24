
import multiprocessing
import threading
import time
import unittest

import rpqueue
olog = rpqueue._log
rpqueue.log = olog

queue = 'TEST_QUEUE'

def _start_task_processor():
    t = multiprocessing.Process(target=rpqueue._execute_tasks, args=([queue],))
    t.daemon = True
    t.start()
    return t

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
        taskr2.retry(v1 - 1, **kwargs)
    else:
        saw[0] = v2

@rpqueue.task(queue=queue)
def taske():
    raise Exception

@rpqueue.task(queue=queue)
def speed():
    saw[0] += 1

@rpqueue.task(queue=queue, retry_delay=0)
def speed2(**kwargs):
    saw[0] += 1
    speed2.retry(**kwargs)

@rpqueue.task(queue=queue)
def periodic_task():
    saw[0] += 1

class TestRPQueue(unittest.TestCase):
    def setUp(self):
        saw[0] = 0
        rpqueue.clear_queue(queue)
        rpqueue.SHOULD_QUIT[0] = 0
        rpqueue.log = lambda *args, **kwargs: None
        self.t = _start_task_processor()

    def tearDown(self):
        rpqueue.SHOULD_QUIT[0] = 1
        if self.t.is_alive():
            self.t.terminate()
        rpqueue.clear_queue(queue, delete=True)

    def test_simple_task(self):
        task1.execute(1)
        time.sleep(.25)

        self.assertEquals(saw[0], 1)

    def test_delayed_task(self):
        task1.execute(2, delay=3)

        time.sleep(2.75)
        self.assertEquals(saw[0], 0)
        time.sleep(.5)
        self.assertEquals(saw[0], 2)

    def test_retry_task(self):
        saw[0] = -1
        taskr.execute(0)
        time.sleep(.5)
        self.assertEquals(saw[0], 4)

        saw[0] = -1
        taskr.execute(1)
        time.sleep(.5)
        self.assertEquals(saw[0], 5)

    def test_retry_task2(self):
        taskr2.execute(0)
        time.sleep(.5)
        self.assertEquals(saw[0], 0)
    
        taskr.execute(1)
        time.sleep(.5)
        self.assertEquals(saw[0], 5)

    def test_exception_no_kill(self):
        taske.execute()
        time.sleep(.001)
        task1.execute(6)

        time.sleep(1)
        self.assertEquals(saw[0], 6)

    def test_periodic_task(self):
        # this will cause the task to be enqueued immediately
        @rpqueue.periodic_task(1, queue=queue)
        def periodic_task(self):
            # The body doesn't matter, because the process will be executing
            # using the earlier code. Also, the execute delay is ignored,
            # so this will execute as fast as possible.
            pass
        time.sleep(2)
        x = saw[0]
        self.assertTrue(x > 10, x)

    def test_z_performance(self):
        scale = 1000

        saw[0] = 0
        t = time.time()
        for i in xrange(scale):
            speed.execute()
        while saw[0] < scale:
            time.sleep(.1)
        dt = time.time() - t
        print "\n%.1f tasks/second injection/running"%(scale/dt,)

        saw[0] = 0
        t = time.time()
        speed2.execute(_attempts=scale)
        while saw[0] < scale:
            time.sleep(.1)
        dt2 = time.time() -t
        print "%.1f tasks/second retries"%(scale/dt2,)

if __name__ == '__main__':
    unittest.main()
