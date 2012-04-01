#!/usr/bin/env python
"""Generate PBS run scripts.

Usage: pbstache.py <input>.stache from:to:by
  Output is <input>_<procs>_<docs>.pbs,
  where <procs> is number of clients and <docs>
  is the total number of documents.
"""
import pystache
import sys

def usage():
    print(__doc__)
    sys.exit(1)

if len(sys.argv) < 2 or sys.argv[1] == "-h" or len(sys.argv) != 3:
    usage()

infile = sys.argv[1]
if not infile.endswith(".stache"):
    print("Input file format is <name>.stache")
    usage()
try:
    data = file(infile).read()
except IOError, err:
    print("Cannot read file {f}".format(f=infile))
    usage()
base_infile = infile[:-7]

try:
    p_from, p_to, p_step = map(int, sys.argv[2].split(':'))
except ValueError:
    print("Client count range must be 3 numbers a:b:c")
    usage()

total_docs = 1000000
v = {clear:True, server:"128.55.57.13"}

for procs in xrange(p_from, p_to, p_step):
    v['docs'] = total_docs / procs
    v['run'] = "{f}_{p:d}_{t:d}".format(base_infile, procs, total_docs)
    script = pystache.render(data, v)
    ofile = "{run}.pbs".format(**v)
    file(ofile, "w").write(script)

