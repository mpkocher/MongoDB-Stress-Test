#!/bin/bash

#PBS -q debug
#PBS -N test_mpi4py
#PBS -l mppwidth=216
## Number of processes on each node
##PBS -l mppnppn=1
#PBS -l walltime=00:10:00
#PBS -o $PBS_O_WORKDIR/job.out
#PBS -e $PBS_O_WORKDIR/job.error

module load python/2.7.1

cd $PBS_O_WORKDIR
# run 216 tasks
aprun -n 216 python ./mpi_stress_test.py --host='X.X.X.X' --port=27018 --nclients=216
# run 4 tasks with 1 task (MPI process) per node
#aprun -n 4 -N 1 python ./test_01.py
