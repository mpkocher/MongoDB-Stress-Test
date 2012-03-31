#!/usr/bin/env python
""" Mongo Stress Test
"""
import datetime
import logging
from optparse import OptionParser
import os
import socket
import sys
import time
import threading

import pymongo

_name = 'stress_test'
_file_name = '_'.join([_name, datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f") + '.log'])

log = logging.getLogger(__name__)
#hdlr = logging.FileHandler(_file_name)
hdlr = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
log.addHandler(hdlr)
log.setLevel(logging.DEBUG)

STRESS_DB = 'stress'
STRESS_COLL = 'data'

REPORT_DB = STRESS_DB
REPORT_COLL = 'report'

my_hostname = socket.getfqdn()
wait_for_it = 0

def stress_test(ndocs=1, host="localhost", port=27017, db_name=STRESS_DB,
                coll_name=STRESS_COLL, result=None):
    """Run a stress test.
    Duration (result) is stored in `result` argument.
    Returns: None
    """
    conn = pymongo.Connection(host, port)
    collection = conn[db_name][coll_name]
    
    t0 = time.time()
    if wait_for_it > 0:
        wait_sleep = int(max(0,wait_for_it - t0))
        log.info("wait sec={0:d}".format(wait_sleep))
        time.sleep(wait_sleep)
    message = "I am legend"
    log.info("loop.start n={n}".format(n=ndocs))
    t0 = time.time()
    for n in xrange(ndocs):
        doc = {'doc_num': n, 'created_at': datetime.datetime.now(), 'message': message}
        collection.insert(doc)
    result.dur = time.time() - t0
    log.info("loop.end n={n} dur={d:f}".format(n=ndocs, d=result.dur))

def report(conn, run, delta_time, num, **kw):
    """Report time for a client.
    """
    doc = {"host":my_hostname, "pid":os.getpid(),
           "client": num, "run":run, "dt":delta_time}
    doc.update(kw)
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

class Result:
    def __init__(self):
        self.dur = 0
           
def main():
    """Program entry point.
    """
    global wait_for_it

    parser = OptionParser()
    parser.add_option('-c', '--clear', dest='do_clear', help='Clear collection first', 
                      action="store_true", default=False)
    parser.add_option('-n', '--ndocs', dest='ndocs', type='int', default= 1000,
                      help='number of docs to insert per client into the db (default=%default)')
    parser.add_option('-r', '--results', dest='do_check', help="Print results and exit. "
                      "With -c/--clear also clears results.",
                      action="store_true")
    parser.add_option('-s', '--host', dest='host', help='MongoDB server host (required)')
    parser.add_option('-t', '--nthreads', dest='nclients', type='int', default= 1)
    parser.add_option('-p', '--port', dest='port', type='int', default=27018,
                      help='MongoDB server port to connect to (default=%default)')
    parser.add_option('-w', '--when', dest='when', type='int', default=0,
                      help="Start at future time SEC seconds since 1/1/1970 (default=now)",
                      metavar="SEC")
    (options, args) = parser.parse_args()
    
    if options.host is None:
        parser.error("Please provide --host option")
        return 1

    db_name, coll_name = STRESS_DB, STRESS_COLL
    ndocs, nclients, host, port, wait_for_it = (options.ndocs, options.nclients,
                 options.host, options.port, options.when)
    log.info("run.start docs={m} clients={n} server={h}:{p:d} "
             "db={db} collection={coll}".format(
             m=ndocs, n=nclients, h=host, p=port, db=db_name, coll=coll_name))

    run_id = int(time.time())
    #run_id = "{t:d}-{m}-{n}".format(m=ndocs, n=nclients, t=run_sec)

    try:
        conn =  pymongo.Connection(host, port, network_timeout=2)
        conn.server_info() # test conn
    except Exception, err:
        print("connect error: {e}".format(e=err))
        return -1

    if options.do_check:
        coll = conn[db_name][REPORT_COLL]
        print_results(coll)
        if options.do_clear:
            conn[db_name][REPORT_COLL].remove()
        return 0

    if options.do_clear:
        conn[db_name][coll_name].remove()

    conn.end_request()

    threads, results = [ ], [ ]
    docs_per_thread = ndocs
    test_kw = { "ndocs": docs_per_thread, "host":host, "port":port,
                "db_name":db_name, "coll_name":coll_name }
    if nclients > 1:
        for i in xrange(nclients):
            kw = { "result" : Result() }
            kw.update(test_kw)
            thr = threading.Thread(target=stress_test, kwargs=kw)
            threads.append(thr)
            results.append(kw["result"])
        for i in xrange(nclients):
            threads[i].start()
        for i in xrange(nclients):
            threads[i].join()
    else:
        test_kw["result"] = Result()
        stress_test(**test_kw)
        results = [test_kw["result"]]

    try:
        conn =  pymongo.Connection(host, port)
    except Exception, err:
        print("after_run.connect error: {e}".format(e=err))
        return -1
    for i, r in enumerate(results):
        report(conn, run_id, r.dur, i, ndocs=docs_per_thread, nclients=nclients)
    log.info("run.end")
    return 0

if __name__ == '__main__':
    sys.exit(main())
