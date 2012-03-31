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
hdlr = logging.FileHandler(_file_name)
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
log.addHandler(hdlr)
log.setLevel(logging.DEBUG)


STRESS_DB = 'stress'
STRESS_COLL = 'data'

REPORT_DB = STRESS_DB
REPORT_COLL = 'report'

my_hostname = socket.getfqdn()

def stress_test(ndocs=1, host="localhost", port=27017, db_name=STRESS_DB,
                coll_name=STRESS_COLL, wait_for_it=None, result=None):
    """Run a stress test.
    Duration (result) is stored in input list, result
    Returns: None
    """
    conn = pymongo.Connection(host, port)
    collection = conn[db_name][coll_name]
    
    t0 = time.time()
    if wait_for_it:
        time.sleep(wait_for_it - t0)
    message = "I am legend"
    for n in xrange(ndocs):
        doc = {'doc_num': n, 'created_at': datetime.datetime.now(), 'message': message}
        collection.insert(doc)
    result[0] = time.time() - t0

def report(conn, delta_time):
    """Report time for a client.
    """
    client_id = "{pid:d}:{thread}".format(pid=os.getpid(),
                thread=threading.current_thread().name)
    coll = conn[REPORT_DB][REPORT_COLL]
    coll.insert({"client":client_id, "dt":delta_time})
 
def main():
    """Program entry point.
    """
    parser = OptionParser()
    parser.add_option('-c', '--clear', dest='do_clear', help='Clear collection first', 
                      action="store_true", default=False)
    parser.add_option('-n', '--ndocs', dest='ndocs', type='int', default= 1000,
                      help='number of docs to insert per client into the db (default=%default)')
    parser.add_option('-s', '--host', dest='host', help='MongoDB server host (required)')
    parser.add_option('-t', '--nthreads', dest='nclients', type='int', default= 1)
    parser.add_option('-p', '--port', dest='port', type='int', default=27017,
                      help='MongoDB server port to connect to (default=%default)')
    (options, args) = parser.parse_args()
    
    if options.host is None:
        parser.error("Provide --host option")
        return 1

    db_name, coll_name = STRESS_DB, STRESS_COLL
    ndocs, nclients, host, port = (options.ndocs, options.nclients,
                 options.host, options.port)
    log.info("run.start docs={m} server={h}:{p:d} "
             "db={db} collection={coll}".format(
             m=ndocs, h=host, p=port, db=db_name, coll=coll_name))

    try:
        conn =  pymongo.Connection(host, port)
    except Exception, err:
        print("connect error: {}".format(err))
        return -1
    if options.do_clear:
        conn[db_name][coll_name].remove()
    threads, results = [ ], [ ]
    for _ in xrange(nclients):
        r = [ ] # store result here
        thr = threading.Thread(target=stress_test,
		     kwargs=dict(ndocs=ndocs, host=host,
				 port=port, db_name=db_name,
				 coll_name=coll_name,
                                 result=r))
        threads.append(thr)
        results.append(r)
    for _ in xrange(nclients):
        threads[i].start()
    for _ in xrange(nclients):
        threads[i].join()
    for r in results:
        report(conn, r[0])
    log.info("run.end")
    return 0

if __name__ == '__main__':
    sys.exit(main())
