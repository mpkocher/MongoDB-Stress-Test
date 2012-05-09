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
try:
    import mongate
except ImportError:
    mongate = None

TRACE = logging.DEBUG / 2
def trace(x, msg, *args, **kwargs):
    x.log(TRACE, msg, *args, **kwargs)

my_hostname = socket.getfqdn()
my_pid = os.getpid()

log = logging.getLogger(__name__)

STRESS_DB = 'stress'
STRESS_COLL = 'data'

REPORT_DB = STRESS_DB
REPORT_COLL = 'report'

wait_for_it = 0
net_timeout = 3600 # default to 1 hr

def stress_test(connections, ndocs=None, db_name=None,
                coll_name=None, pause=0):
    """Run a stress test.
    Returns: duration, in seconds
    """
    tracing = log.isEnabledFor(TRACE)
    collections = [x[db_name][coll_name] for x in connections]
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
        doc = {'doc_num': i, 'message': message}
        for collection in collections:
            collection.insert(doc)
        if pause > 0:
            time.sleep(pause)
        if tracing:
            trace(log, "inserted {0:d}".format(i))
    dur = time.time() - t0
    log.debug("loop.end n={n} dur={d:f}".format(n=ndocs, d=dur))
    return dur

def report(conn, run, delta_time, **kw):
    """Report time for a client.
    """
    doc = {"host":my_hostname, "pid":my_pid, "run":run, "dt":delta_time}
    doc.update(kw)
    log.debug("reporting {}".format(doc))
    coll = conn[REPORT_DB][REPORT_COLL]
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
    global wait_for_it

    # command-line
    parser = OptionParser()
    parser.add_option('-c', '--clear', dest='do_clear', help='Clear collection first', 
                      action="store_true", default=False)
    parser.add_option('-d', '--ndocs', dest='ndocs', metavar="NUM", type='int', default= 1000,
                      help='Insert NUM docs per client (default=%default)')
    parser.add_option('-m', '--mongoose', dest='mongoose', action='store_true',
                      help="Use sleepy mongoose REST api instead of mongodb native protocol")
    parser.add_option('-n', '--nclients', dest='nclients', metavar="NUM", type='int', default=1,
                      help="Number of clients to connect to server (default=%default)")
    parser.add_option('-p', '--port', dest='port', type='int', default=27018,
                      help='Connect to MongoDB server on PORT (default=%default)')
    parser.add_option('-P', '--pause', dest='pause', type='int', default=0,
                      help="Pause MS milliseconds between each write (default=%default)",
                      metavar="MS")
    parser.add_option('-q', '--quiet', dest='quiet', action='store_true', help="No logging")
    parser.add_option('-r', '--results', dest='do_check', help="Print results and exit. "
                      "With -c/--clear also clears results.",
                      action="store_true")
    parser.add_option("-R", "--run", dest="runid", metavar="ID", help="Run identifier", default=None)
    parser.add_option('-s', '--server', dest='host', help='MongoDB server host (required)')
    parser.add_option('-v', '--verbose', dest='vb', action='count', help="More logging")
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
    elif options.vb > 1:
        log.setLevel(TRACE)
    elif options.vb == 1:
        log.setLevel(logging.DEBUG)
    else:
        if options.do_check:
            log.setLevel(logging.WARN)
        else:
            log.setLevel(logging.INFO)

    # get vars from options
    db_name, coll_name = STRESS_DB, STRESS_COLL
    ndocs, host, port, wait_for_it, ncli = (options.ndocs, options.host,
                                     options.port, options.when, options.nclients)
    pause = options.pause / 1000.0

    # start
    log.info("run.start docs={m} clients={c} server={h}:{p:d} "
             "db={db} collection={coll}".format(
             r=run_id, m=ndocs, h=host, p=port, db=db_name,
             coll=coll_name, c=ncli))

    log.debug("pre.start")
    # connect (once for each client)
    if options.do_check:
        ncli = 1
    if options.mongoose:
        # Assume sleepymongoose is listening on port 27080
        # and connecting via localhost to the mongodb server.
        if mongate is None:
            parser.error("-m/--mongoose option requires 'mongate' Python "
                    "module, which was not found.")
            return 2
        from mongate import connection
        try:
            connections = [connection.Connection(host, 27080)
                       for _ in xrange(ncli)]
        except socket.error, err:
            log.critical("mongoose.connection.error msg={}".format(err))
            return -1
        for conn in connections:
            conn.connect_to_mongo("mongodb://localhost", port)
    else:
        connections = [pymongo.Connection(host, port)
                      for _ in xrange(ncli)]
    # check mode, just print results and stop
    conn = connections[0]
    if options.do_check:
        coll = conn[db_name][REPORT_COLL]
        print_results(coll)
        if options.do_clear:
            conn[db_name][REPORT_COLL].remove()
        log.debug("pre.end status=0")
        return 0
    # with clear flag, empty db first
    if options.do_clear:
        conn[db_name][coll_name].remove()
    log.debug("pre.end status=0")

    log.debug("main.start")
    dur = stress_test(connections, ndocs=ndocs, db_name=db_name,
                      coll_name=coll_name, pause=pause)
    log.debug("main.end status=0")

    log.debug("post.start")
    # re-use a previous connection
    report(conn, run_id, dur, docs=ndocs, clients=ncli)
    log.debug("post.end status=0")

    # done
    log.info("run.end status=0")
    return 0

if __name__ == '__main__':
    sys.exit(main())
