
Description
===========

This package intends to offer a priority-based remote task queue solution
using Redis as the transport and persistence layer, and JSON for a common
interchange format.

Semantically, this module implements a 0/1 queue with optional retries. That
is, it attempts to execute every task once. If the task raises an exception,
it will not automatically retry, but you can manually retry the task and
specify the maximum attempts. See the `Retries`_ section below.


Getting started
===============

In order to execute tasks, you must ensure that rpqueue knows about your
tasks that can be executed, you must configure rpqueue to connect to your
Redis server, then you must start the task execution daemon::

    from mytasks import usertasks1, usertasks2, ...
    import rpqueue

    rpqueue.set_redis_connection_settings(host, port, db)
    rpqueue.execute_tasks()


Example uses
============

Say that you have a module ``usertasks1`` with a task to be executed called
``echo_to_stdout``. Your module may look like the following::

    from rpqueue import task

    @task
    def echo_to_stdout(message):
        print message

To call the above task, you would use::

    echo_to_stdout.execute(...)
    echo_to_stdout.execute(..., delay=delay_in_seconds)

You can also schedule a task to be repeatedly executed with the
``periodic_task`` decorator::

    @periodic_task(25, queue="low")
    def function1():
        # Will be executed every 25 seconds from within the 'low' queue.
        pass


Retries
=======

Tasks may be provided an optional ``attempts`` argument, which specifies the
total number of times the task will try to be executed before failing. By
default, all tasks have ``attempts`` set at 1, unless otherwise specified::

    @task(attempts=3)
    def fail_until_zero(value, **kwargs):
        try:
            if value != 0:
                value -= 1
                raise Exception
        except:
            fail_until_zero.retry(value, **kwargs)
        else:
            print "succeeded"

If passed the value ``3``, "succeeded" will never be printed. Why? The first
try has value=3, attempts=3, and fails. The second pass has value=2,
attempts=2, and fails. The third pass has value=1, attempts=1, fails, and the
retry returns without retrying. The ``attempts`` value is the total number of
attempts, including the first, and all retries.


Waiting for task execution
==========================

As of version .19, RPQueue offers the ability to wait on a task until it
begins execution::

    @task
    def my_task(args):
        # do something

    executing_task = my_task.execute()
    if executing_task.wait(5):
        # task is either being executed, or it is done
    else:
        # task has not started execution yet

With the ability to wait for a task to complete, you can have the ability to
add deadlines by inserting a call to ``executing_task.cancel()`` in the else
block above.


Automatically storing results of tasks
======================================

As of version .19, RPQueue offers the ability to store the result returned by
a task as it completes::

    @task(save_results=30)
    def task_with_results():
        return 5

    etask = task_with_results.execute()
    if etask.wait(5):
        print etask.result # should print 5

The ``save_results`` argument can be passed to tasks, periodic tasks, and even
cron tasks (described below). The value passed will be how long the result is
stored in Redis, in seconds. All results must be json-encodable.


Additional features
===================

Support for cron_tasks using a crontab-like syntax requires the Python crontab
module: http://pypi.python.org/pypi/crontab/ , allowing for::

    @cron_task('0 5 tue * *')
    def function2():
        # Will be executed every Tuesday at 5AM.
        pass
