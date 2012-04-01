#!/usr/bin/env python
"""Mongo Stress Test
"""
import datetime
import logging
from optparse import OptionParser
import os
import random
import socket
import sys
import time

import pymongo

my_hostname = socket.getfqdn()
my_pid = os.getpid()

log = logging.getLogger(__name__)

STRESS_DB = 'stress'
STRESS_COLL = 'data'

REPORT_DB = STRESS_DB
REPORT_COLL = 'report'

wait_for_it = 0
net_timeout = 3600 # default to 1 hr
g_conn = None # global connection

def stress_test(ndocs=1, db_name=None, coll_name=None):
    """Run a stress test.
    Returns: duration, in seconds
    """
    collection = g_conn[db_name][coll_name]
    
    t0 = time.time()
    if wait_for_it > 0:
        wait_sleep = int(max(0,wait_for_it - t0))
        log.debug("client.wait.start sec={0:d}".format(wait_sleep))
        time.sleep(wait_sleep)
        log.debug("client.wait.end sec={0:d}".format(wait_sleep))
    message = "I am legend"
    log.debug("loop.start n={n}".format(n=ndocs))
    t0 = time.time()
    for i in xrange(ndocs):
        doc = {'doc_num': i, 'created_at': datetime.datetime.now(), 'message': message}
        collection.insert(doc)
    dur = time.time() - t0
    log.debug("loop.end n={n} dur={d:f}".format(n=ndocs, d=dur))
    return dur

def report(run, delta_time, **kw):
    """Report time for a client.
    """
    doc = {"host":my_hostname, "pid":my_pid, "run":run, "dt":delta_time}
    doc.update(kw)
    coll = g_conn[REPORT_DB][REPORT_COLL]
    coll.insert(doc)

def print_results(coll): 
    hdr = None
    for rec in coll.find():
        if not hdr:
            hdr = filter(lambda x: x[0] != "_", rec.keys())
            print(",".join(hdr))
        values = [str(rec.get(key, "")) for key in hdr]
        print(",".join(values))

def main():
    """Program entry point.
    """
    global wait_for_it, g_conn

    # command-line
    parser = OptionParser()
    parser.add_option('-c', '--clear', dest='do_clear', help='Clear collection first', 
                      action="store_true", default=False)
    parser.add_option('-d', '--ndocs', dest='ndocs', metavar="NUM", type='int', default= 1000,
                      help='Insert NUM docs per client (default=%default)')
    parser.add_option('-r', '--results', dest='do_check', help="Print results and exit. "
                      "With -c/--clear also clears results.",
                      action="store_true")
    parser.add_option("-R", "--run", dest="runid", metavar="ID", help="Run identifier", default=None)
    parser.add_option('-s', '--server', dest='host', help='MongoDB server host (required)')
    parser.add_option('-p', '--port', dest='port', type='int', default=27018,
                      help='Connect to MongoDB server on PORT (default=%default)')
    parser.add_option('-q', '--quiet', dest='quiet', action='store_true', help="No logging")
    parser.add_option('-v', '--verbose', dest='vb', action='store_true', help="More logging")
    parser.add_option('-w', '--when', dest='when', type='int', default=0,
                      help="Start at future time SEC seconds since 1/1/1970 (default=now)",
                      metavar="SEC")
    (options, args) = parser.parse_args()
    if options.host is None:
        parser.error("-s/--server is required")
        return 1

    # set run identifier string
    if options.runid is None:
        if options.when > 0:
            tm = options.when
        else:
            tm = int(time.time())
        run_id = "{0:d}".format(tm)
    else:
        run_id = options.runid

    # init logging
    hdlr = logging.StreamHandler()
    formatter = logging.Formatter("{r} %(asctime)s {h} {p:d} "
        "%(levelname)s %(message)s".format(r=run_id, h=my_hostname, p=my_pid))
    hdlr.setFormatter(formatter)
    log.addHandler(hdlr)
    if options.quiet:
       log.setLevel(logging.ERROR)
    elif options.vb:
        log.setLevel(logging.DEBUG)
    else:
        log.setLevel(logging.INFO)

    # get vars from options
    db_name, coll_name = STRESS_DB, STRESS_COLL
    ndocs, host, port, wait_for_it = (options.ndocs, options.host, options.port, options.when)

    # start
    log.info("run.start docs={m} server={h}:{p:d} "
             "db={db} collection={coll}".format(
             r=run_id, m=ndocs, h=host, p=port, db=db_name, coll=coll_name))

    log.debug("pre.start")
    # connect
    g_conn = pymongo.Connection(host, port)
    # check mode, just print results and stop
    if options.do_check:
        coll = g_conn[db_name][REPORT_COLL]
        print_results(coll)
        if options.do_clear:
            g_conn[db_name][REPORT_COLL].remove()
        log.debug("pre.end status=0")
        return 0
    # with clear flag, empty db first
    if options.do_clear:
        g_conn[db_name][coll_name].remove()
    log.debug("pre.end status=0")

    log.debug("main.start")
    dur = stress_test(ndocs=ndocs, db_name=db_name, coll_name=coll_name)
    log.debug("main.end status=0")

    log.debug("post.start")
    # re-use a previous connection
    report(run_id, dur, ndocs=ndocs)
    log.debug("post.end status=0")

    # done
    log.info("run.end status=0")
    return 0

if __name__ == '__main__':
    sys.exit(main())
