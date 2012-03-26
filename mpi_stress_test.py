#!/usr/bin/env python
" Mongo Stress Test using MPI"
import sys
import time
import datetime
import logging
import socket
from optparse import OptionParser

import pymongo
from mpi4py import MPI

DB_NAME = 'db_flex'
COLLECTION_NAME = 'flexers'

log = logging.getLogger(__name__)

comm = MPI.COMM_WORLD
rank = comm.Get_rank()
size = comm.Get_size()

log.info("MPI size {n}".format(n=size))


def mongo_inserter(cid, ndocs, host, port):
    """
    cid (int) Client id
    ndoc (int) Number of docs to import
    host (str) mongo hostname
    port (int) mongo port to connect to
    """
    s = socket.getfqdn()
    log.info("start client {i} on node {n} connecting to {h}:{p} at {d}".format(i=cid, n=s, h=host, p=port, d=datetime.datetime.now()))
    conn = pymongo.Connection(host, port)
    db = conn[DB_NAME]
    collection = db[COLLECTION_NAME]
    t0 = time.time()
    for n in xrange(ndocs):
        message = "updating mongodb with value {v} from client {i}".format(v=n, i=cid)
        log.info(message)
        doc = {'doc_id': n, 'created_at': datetime.datetime.now(), 'message': message, 'client_id': cid}
        collection.insert(doc)

    tf = time.time()
    dt = tf - t0
    ops = ndocs / dt
    m = "Client {i} on node {s} took {dt} with op/s {o:2f}".format(i=cid, s=s, dt=dt, o=ops)
    print m
    log.info(m)
    log.info("Client {i} completed".format(i=cid))


def main(nclients, ndocs, host, port):
    sname = socket.getfqdn()
    if rank == 0:
        #data could be the db config params
        #comm.send(config, tag=11)
        print "starting up rank {r} on {s}".format(r=rank, s=sname)
    else:
        print "starting up rank {r} on {s}".format(r=rank, s=sname)
        #data = comm.recv(source=0, tag=11)
        mongo_inserter(rank, ndocs, host, port)
        sys.exit(0)
    return 0


if __name__ == '__main__':
    parser = OptionParser()
    # nclients isn't necessary now. MPI determines the number from PBS script
    parser.add_option('-n', '--nclients', type='int', dest='nclients', help='Number of clients to start up')
    parser.add_option('-d', '--ndocs', dest='ndocs', type='int', default=1000, help='number of docs to import per client into the db')
    parser.add_option('-H', '--host', dest='host', help='db hostname')
    parser.add_option('-p', '--port', dest='port', type='int', default=27017, help='db port to connect to')
    (options, args) = parser.parse_args()
    # nclients, host must be given!
    if options.nclients is not None and options.host is not None:
        sys.exit(main(options.nclients, options.ndocs, options.host, options.port))
    else:
        print "Provide --nclients and --host options"
        sys.exit(0)
