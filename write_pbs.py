import sys
import os
from optparse import OptionParser


name = 'stress_test.py'
EXE = os.path.join(os.path.abspath(os.path.dirname(__file__)), name)


def write_file(file_name, nclients, ndocs, host, port, walltime):
    """
    ncores (int) to use
    file_name (str) to write pbs to
    walltime (int) walltime (in min) used
    """
    outs = []
    outs.append("#!/bin/bash")
    outs.append("")
    outs.append("#PBS -q debug")
    outs.append("#PBS -N test")
    # add one for the driving process?
    outs.append("#PBS -l mppwidth={n}".format(n=nclients + 1))
    outs.append("#PBS -l walltime=00:0{i}:00".format(i=walltime))
    outs.append("#PBS -o {f}/RUN.out".format(f=os.getcwd()))
    outs.append("#PBS -e {f}/RUN.error".format(f=os.getcwd()))
    outs.append("")
    outs.append("module load python/2.7.1")
    outs.append("")
    outs.append("cd {d}".format(d=os.getcwd()))
    outs.append("aprun -n {n} python {e} --nclient {n} --port {p} --ndocs {ndocs} --host {h}".format(e=EXE, n=nclients, h=host, p=port, ndocs=ndocs))
    outs.append("")

    with open(file_name, 'w+') as f:
        f.write("\n".join(outs))


def main(output_file, nclients, ndocs, host, port, walltime):
    write_file(output_file, nclients, ndocs, host, port, walltime)
    return 0

if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-n', '--nclients', type='int', dest='nclients', help='Number of clients to start up')
    parser.add_option('-d', '--ndocs', dest='ndocs', type='int', default= 1000, help='number of docs to import per client into the db')
    parser.add_option('-H', '--host', dest='host', help='db hostname')
    parser.add_option('-o', '--out', dest='output_file', default='run.pbs', help='Output file to write to (e.g, "my_file.pbs"')
    parser.add_option('-p', '--port', dest='port', type='int', default=27017, help='db port to connect to')
    # 30 min is the max walltime allowed in the Hopper Debug queue
    parser.add_option('-t', '--walltime', dest='walltime', type='int', default=29, help='pbs walltime')
    (options, args) = parser.parse_args()
    
    # nclients, host must be given!
    if options.nclients is not None and options.host is not None:
        sys.exit(main(options.output_file, options.nclients, options.ndocs, options.host, options.port, options.walltime))
    else:
        print "Provide --nclients and --host options"
        sys.exit(0)
