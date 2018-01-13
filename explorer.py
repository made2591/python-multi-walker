import itertools, multiprocessing, sys, os, json
from multiprocessing import Pool
from contextlib import contextmanager

@contextmanager
def terminating(thing):
	try:
		yield thing
	finally:
		thing.terminate()

def worker(filename):
	stats = os.stat(filename)
	ext   = filename.split(".")[-1] if len(filename.split(".")) > 1 else "Unknown"
	return {
		"name":  filename,
		"ext":   ext,
		"mode":  stats[0],
		"ino":   stats[1],
		"dev":   stats[2],
		"nlink": stats[3],
		"uid":   stats[4],
		"gid":   stats[5],
		"size":  stats[6],
		"atime": stats[7],
		"mtime": stats[8],
		"ctime": stats[9]
	}

def explorer(path = ".", process = 1, output = "files.json"):
	with terminating(Pool(processes=process)) as pool:

		walk = os.walk(path)
		fn_gen = itertools.chain.from_iterable((os.path.join(root, file) for file in files) for root, dirs, files in walk)
		results_of_work = pool.map(worker, fn_gen)

	with open(output, "w") as fp:
		for result_of_work in range(0, len(results_of_work)):
			fp.write("{\"index\":{\"_id\":\""+str(results_of_work[result_of_work]["ino"])+"\"}}\n")
			fp.write(json.dumps(results_of_work[result_of_work])+"\n")

explorer(path = "/", process = 1, output = "files.json")