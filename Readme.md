# MongoDB Stress Test

### Running

	$> cd MongoDB-Stress-Test
	$> python write_pbs.py --nclients 192 --host 'myhost.domain.com' --port 27018

	$> qsub run.pbs