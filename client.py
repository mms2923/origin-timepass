import json
import sys

import alluxio
from alluxio import option


def colorize(code):
    def _(text, bold=False):
        c = code
        if bold:
            c = '1;%s' % c
        return '\033[%sm%s\033[0m' % (c, text)
    return _

green = colorize('32')


def info(s):
    print green(s)


def pretty_json(obj):
    return json.dumps(obj, indent=2)


def stress_test(client, id, timer):
    py_test_root_dir = '/py-test-dir' + str(id)
    py_test_nested_dir = py_test_root_dir + '/nested'
    py_test = py_test_nested_dir + '/py-test'
    py_test_renamed = py_test_root_dir + '/py-test-renamed'
	start = time.time()
    info("creating directory %s" % py_test_nested_dir)
    opt = option.CreateDirectory(recursive=True)
    client.create_directory(py_test_nested_dir, opt)
    info("done")

    info("writing to %s" % py_test)
    with client.open(py_test, 'w') as f:
        f.write('Alluxio works with Python!\n')
        with open(sys.argv[0]) as this_file:
            f.write(this_file)
    info("done")

    info("getting status of %s" % py_test)
    stat = client.get_status(py_test)
    print pretty_json(stat.json())
    info("done")

    info("renaming %s to %s" % (py_test, py_test_renamed))
    client.rename(py_test, py_test_renamed)
    info("done")

    info("getting status of %s" % py_test_renamed)
    stat = client.get_status(py_test_renamed)
    print pretty_json(stat.json())
    info("done")

    info("reading %s" % py_test_renamed)
    with client.open(py_test_renamed, 'r') as f:
        print f.read()
    info("done")

    info("listing status of paths under /")
    root_stats = client.list_status('/')
    for stat in root_stats:
        print pretty_json(stat.json())
    info("done")

    info("deleting %s" % py_test_root_dir)
    opt = option.Delete(recursive=True)
    client.delete(py_test_root_dir, opt)
    info("done")

    info("asserting that %s is deleted" % py_test_root_dir)
    assert not client.exists(py_test_root_dir)
    info("done")
	
	alluxio_read_time = time.time() - start
	with timer.get_lock():
        timer.value += alluxio_read_time
    return alluxio_read_time
	
def run_stress_test(process_id, timer, iteration_num):
	client = alluxio.Client('localhost', 39999)
    for iteration in range(iteration_num):
        print('process {}, iteration {} ... '.format(process_id, iteration), end='')
        t = stress_test(client, process_id, timer)
        print('{} seconds'.format(t))
        sys.stdout.flush()


def print_stats(iteration_num, average_time_per_process, processes_num):
    src_bytes = 64000000
    average_time_per_iteration_per_process = average_time_per_process / iteration_num
    average_throughput_per_client = src_bytes / average_time_per_iteration_per_process
    average_throughput_per_node = src_bytes * processes_num * iteration_num / average_time_per_process

    print('Number of iterations: %d' % iteration_num)
    print('Number of processes per iteration: %d' % processes_num)
    print('File size: %d bytes' % src_bytes)
    print('Average time for each process: %f seconds' % average_time_per_process)
    print('Average time for each iteration of a process: %f seconds' % average_time_per_iteration_per_process)
    print('Average read throughput of each client: %f bytes/second' % average_throughput_per_client)
    print('Average read throughput per node: %f bytes/second' % average_throughput_per_node)
	
def main():
    timer = Value('d', 0)
    processes = []
	iteration_num = 20
	processes_num = 3
    for process_id in range(processes_num):
        p = Process(target=run_stress_test, args=(process_id, timer, iteration_num))
        processes.append(p)
    for p in processes:
        p.start()
    for p in processes:
        p.join()
    average_time_per_process = timer.value / len(processes)
    print_stats(iteration_num, average_time_per_process)
	
if __name__ == '__main__':
    main()