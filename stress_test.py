#!/usr/bin/env python
" Mongo Stress Test"
import sys
import os
import time
import datetime
import logging
import socket
import pymongo
from optparse import OptionParser


_name = 'stressor'
_file_name = '_'.join([_name, datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S-%f") + '.log'])

log = logging.getLogger(__name__)
hdlr = logging.FileHandler(os.path.join(os.environ['HOME'], 'logs', _file_name))
formatter = logging.Formatter('%(asctime)s %(levelname)s %(message)s')
hdlr.setFormatter(formatter)
log.addHandler(hdlr)
log.setLevel(logging.DEBUG)


DB_NAME = 'db_flex'
COLLECTION_NAME = 'flexers'

def mongo_inserter(cid, ndocs, host, port):
    """
    cid (int) Client id
    ndoc (int) Number of docs to import
    host (str) mongo hostname
    port (int) mongo port to connect to
    """
    s = socket.getfqdn()
    log.info("inserter.start client={i} node={n} server={h}:{p}".format(
             i=cid, n=s, h=host, p=port))
    conn = pymongo.Connection(host, port)
    db = conn[DB_NAME]
    collection = db[COLLECTION_NAME]
    t0 = time.time()
    for n in xrange(ndocs):
        message = "updating mongodb with value {v} from client {i}".format(v=n, i = cid)
        #log.info(message)
        doc = {'doc_id': n, 'created_at': datetime.datetime.now(), 'message': message}
        collection.insert(doc)

    tf = time.time()
    dt = tf - t0
    ops = ndocs / dt
    m = "Client {i} took {dt} with op/s {o:2f}".format(i=cid, dt=dt, o=ops)
    log.info(m)
    #print m
    log.info("Client {i} completed".format(i=cid))


def main(nclients, ndocs, host, port):
    " Main entry point"
    log.info("Starting Main with {n} clients and inserting {m} docs".format(n=nclients, m=ndocs))

    pids = {}
    for n in xrange(nclients):
        pid = os.fork()
        pids[n] = pid

        if pid == 0:
            log.debug("forking process {n}:{i}".format(n=n, i=pid))
            mongo_inserter(n, ndocs, host, port)
            os._exit(0)

    # wait for children to complete
    os.waitpid(-1, 0)
    print "completed"
    return 0

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-n', '--nclients', type='int', dest='nclients', help='Number of clients to start up')
    parser.add_option('-d', '--ndocs', dest='ndocs', type='int', default= 1000, help='number of docs to import per client into the db')
    parser.add_option('-H', '--host', dest='host', help='db hostname')
    parser.add_option('-p', '--port', dest='port', type='int', default=27017, help='db port to connect to')
    (options, args) = parser.parse_args()
    
    # nclients, host must be given!
    if options.nclients is not None and options.host is not None:
        sys.exit(main(options.nclients, options.ndocs, options.host, options.port))
    else:
        print "Provide --nclients and --host options"
        sys.exit(0)
