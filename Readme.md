MongoDB Stress Test
====================

MPK version
------------

* Running

	$> cd MongoDB-Stress-Test
	$> python write_pbs.py --nclients 192 --host 'myhost.domain.com' --port 27018
	$> qsub run.pbs

Dang version
-------------
* Files:
  - w.py: Main program
  - w_run.pbs: PBS script

Example of running w.py

        # Pick a time 100 seconds in the future
        future=`python -c "import time; print(int(time.time()) + 100)"`
        # Run 1 client at that time
        ./w.py --host=128.55.57.13 --ndocs=100 --when=$future

* Util files
  - sharded-mongo  :  Run simple localhost setup with 2 shards
