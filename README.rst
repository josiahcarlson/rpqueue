
Description
===========

This package intends to offer a priority-based remote task queue solution
using Redis as the transport and persistence layer, and JSON for a common
interchange format.

Semantically, this module implements a 0/1 or 1+ queue with optional retries.
That is, it attempts to execute every task once by default, or >1 manually, or
>1 automatically with 'visibility timeouts'.

If a 'manual' retry task raises an exception, it will not automatically retry,
but you can manually retry the task and specify the maximum attempts. Similarly,
for tasks with visibility timeouts, if the task rasises an exception or doesn't
complete, it will be retried up to the limit of retries provided.

See the `Retries`_ section below.

Full documentation is available: `https://josiahcarlson.github.io/rpqueue/
<https://josiahcarlson.github.io/rpqueue/>`_

Getting started
===============

In order to execute tasks, you must ensure that rpqueue knows about your
tasks that can be executed, you must configure rpqueue to connect to your
Redis server, then you must start the task execution daemon::

    from mytasks import usertasks1, usertasks2, ...
    import rpqueue

    rpqueue.set_redis_connection_settings(host, port, db)
    rpqueue.execute_tasks()

Alternatively, rpqueue offers a command-line interface to do the same, though
you must provide the name of a module or package that imports all modules or
packages that define tasks that you want to run. For example::

    # tasks.py
    from tasks import accounting, cleanup, ...
    # any other imports or configuration necessary, put them here

    # run from the command-line
    python -m rpqueue.run --module=tasks --host=... --port=... --db=...


Example uses
============

Say that you have a module ``usertasks1`` with a task to be executed called
``echo_to_stdout``. Your module may look like the following::

    from rpqueue import task

    @task
    def echo_to_stdout(message):
        print(message)

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

Automatic retries with vis_timeout
----------------------------------

Included with rpqueue 0.30.0 or later, you can give tasks (and now data queues)
a visibility timeout, which is (per Amazon SQS-style semantics) a time for how
long the task has to execute correctly before being automatically re-entered
into the queue.::

    @task(attempts=20, vis_timeout=5, use_dead=False)
    def usually_eventually_succeed(**kwargs):
        # (4/5)**20  is ~ 0.0115, so call chain fails about 1% of the time
        if not random.randrange(5):
            return "done!"

        time.sleep(6) # fail silently

Deadletter task queue
---------------------

If you would like to know which tasks failed, failed calls can be automatically
entered into a deadletter queue.::

        @rpqueue.task(attempts=5, vis_timeout=5, use_dead=True)
        def fails_to_dead(**kwargs):
            # (4/5)**5  is 0.32768, so call chain fails about 33% of the time
            if not random.randrange(5):
                return "done!"

            time.sleep(6) # fail silently

        task_deadletter = rpqueue.Data(rpqueue.DEADLETTER_QUEUE, is_tasks=True)
        dead_tasks = task_deadletter.get_data(items=5)


See ``help(rpqueue.Data)`` for more.

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

Crontab
-------

Support for cron_tasks using a crontab-like syntax requires the Python crontab
module: http://pypi.python.org/pypi/crontab/ , allowing for::

    @cron_task('0 5 tue * *')
    def function2():
        # Will be executed every Tuesday at 5AM.
        pass

Data queues
-----------

Put data in queues, not tasks. I mean, should have probably been here from the
start, but it's here now.

Convenient features:
 * 1-1000 data items per read, at your discretion
 * ``vis_timeout``
 * ``attempts``
 * ``use_dead``
 * refresh data if you want to keep working on it (we don't identify the reader, so you should use an explicit lock if you want guaranteed exclusivity)

A few examples::

    # 0/1 queue
    dq = rpqueue.Data('best_effort')
    dq.put_data([item1, item2, item3, ...])
    items = dq.get_data(2) # {<uuid>: <item>, ...}

    # Up to 5 deliveries, with 5 second delay before re-insertion
    dq5 = rpqueue.Data('retry_processing', attempts=5, vis_timeout=5)
    dq5.put_data([item1, item2, item3, ...])
    items = dq5.get_data(2) # {<uuid>: <item>, ...}
    items2 = dq5.get_data(2, vis_timeout=20) # override timeout on read
    refreshed = set(dq5.refresh_data(items, vis_timeout=7)) # refresh our lock
    items = {k:v for k,v in items if k in refreshed}
    dq5.done_data(items)
    dq5.done_data(items2)

    # Up to 1 try with a 5 second delay before insertion into deadletter queue
    dqd = rpqueue.Data('retry_processing', attempts=1, vis_timeout=5, use_dead=True)
    dqd.put_data([item1, item2, item3, ...])
    items = dqd.get_data(2) # {<uuid>: <item>, ...}
    items2 = dqd.get_data(2, vis_timeout=20) # override timeout on read
    refreshed = set(dqd.refresh_data(items, vis_timeout=7)) # refresh our lock
    items = {k:v for k,v in items if k in refreshed}
    dqd.done_data(items)
    time.sleep(20)
    # items2 are now "dead"
    dead = rpqueue.Data(rpqueue.DEADLETTER_QUEUE)
    dead_items = dead.get_data(2) # these have a different format, see docs!

A longer example closer to what would be seen in practice::

    aggregate_queue = rpqueue.Data("aggregate_stats", vis_timeout=30, use_dead=False)

    @rpqueue.periodic_task(60)
    def aggregate():
        # If vis_timeout is not provided, will use the queue default.
        # If vis_timeout is <= 0, will act as a 0/1 queue, and later "done data"
        # calling is unnecessary.
        data = aggregate_queue.get_data(items=100, vis_timeout=5)
        # data is a dictionary: {<uuid>: <item>, <uuid>: <item>, ...}
        # do something with data
        done_with = []
        for id, value in data.items():
            # do something with value
            done_with.append(id)

        aggregate_queue.refresh_data(data) # still working!

        # You can pass any iterator that naturally iterates over the uuids you
        # want to be "done" with.
        aggregate_queue.done_data(done_with)
        # also okay:
        # aggregate_queue.done_data(data)
        # aggregate_queue.done_data(tuple(data))
        # aggregate_queue.done_data(list(data))

Sponsors
========

Don't like LGPL? Sponsor the project and get almost any license you want.

This project has been partly sponsored by structd.com and hCaptcha.com, both of
whom received licenses that match their needs appropriately. Historically,
rpqueue has been used to help support the delivery of millions of food orders at
chownow.com, billions of ad impressions for system1.com, and billions of
captchas for hCaptcha.com.

Thank you to our sponsors and those who have consumed our services.

You are welcome for the good service.

Your company link here.
