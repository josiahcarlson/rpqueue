
from __future__ import print_function
import sys

import rpqueue

if __name__ == '__main__':
    from optparse import OptionGroup
    rpqueue.parser.usage = '''
    %prog [connection options] --module <module> [run options] [queues]
        -> start a queue processor with the given options'''
    rgroup = OptionGroup(rpqueue.parser, "Run Options")
    rgroup.add_option('--module', dest='run', action='store', default=None,
        help='Run a task queue processor after importing the given module that ' \
             'defines all of your tasks, with the given number of processes and ' \
             'threads')
    rgroup.add_option('--threads', dest='threads', action='store', type='int', default=1,
        help='The number of threads per process')
    rgroup.add_option('--processes', dest='processes', action='store', type='int', default=1,
        help='The number of processes to spawn for task processing')
    rgroup.add_option('--wait', dest='wait', action='store', type='float', default=1,
        help='The number of seconds to wait per running thread when killed with USR1 '\
             'before letting them die a horrible death (maximum total time is wait * '\
             'processes * threads)')
    rgroup.add_option('--loglevel', dest='loglevel', action='store', type='string',
        default='DEBUG', help='Set the default log level for logging')
    rgroup.add_option('--successlevel', dest='successlevel', action='store', type='string',
        default='DEBUG', help='Set the log level for logging "Task X finished executing successfully" results')
    rpqueue.parser.add_option_group(rpqueue.cgroup)
    rpqueue.parser.add_option_group(rgroup)
    options, args = rpqueue.parser.parse_args()
    if options.prefix:
        rpqueue.set_key_prefix(options.prefix)

    rpqueue.set_redis_connection_settings(options.host, options.port, options.db,
        options.passwd, options.timeout, getattr(options, 'unixpath', None))

    if not options.run:
        print("You must pass --module")
        sys.exit(1)
    if options.threads < 1:
        print("You must have at least 1 thread, you gave %s"%(options.threads,))
        sys.exit(1)
    if options.processes < 1:
        print("You must have at least 1 process, you gave %s"%(options.threads,))
        sys.exit(1)
    if options.wait < 0:
        print("You must provide a non-negative wait, you provided %r"%(options.wait,))
        sys.exit(1)
    LOG_LEVELS = rpqueue.LOG_LEVELS
    if options.loglevel.upper() not in LOG_LEVELS:
        print("You must choose a valid log level from one of: %s"%(list(sorted(LOG_LEVELS, key=lambda x:x[2:])),))
        sys.exit(1)
    if options.successlevel.upper() not in LOG_LEVELS:
        print("You must choose a valid success level from one of: %s"%(list(sorted(LOG_LEVELS, key=lambda x:x[2:])),))
        sys.exit(1)
    import imp
    # used for the side-effect if it can't be found
    imp.find_module(options.run)
    # set the log level
    rpqueue.LOG_LEVEL = options.loglevel.upper()
    rpqueue.SUCCESS_LOG_LEVEL = options.successlevel.lower()
    rpqueue.execute_tasks((args or None), options.threads, options.processes, options.wait, module=options.run)
