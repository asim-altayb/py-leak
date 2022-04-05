import queue
import sqlite3
import threading
import multiprocessing
import os,uuid,time
import numpy as np
from typing import List

DB_NAME = "powned"

q = queue.Queue()
start_t = time.time()


def consumer():
    con = sqlite3.connect(DB_NAME, isolation_level=None)
    con.execute('PRAGMA journal_mode = OFF;')
    con.execute('PRAGMA synchronous = 0;')
    con.execute('PRAGMA cache_size = 30000000;')  # give it a 30 GB
    con.execute('PRAGMA locking_mode = EXCLUSIVE;')
    con.execute('PRAGMA temp_store = MEMORY;')

    while True:
        item = q.get()
        stmt, batch = item
        # print(len(batch),stmt)
        print ("\n start Time Taken for "+str(len(batch))+" record: %.3f sec" % (time.time()-start_t))
        con.execute('BEGIN')
        con.executemany(stmt, batch)
        con.commit()
        q.task_done()
        print ("\n finish Time Taken for"+str(len(batch))+"  record: %.3f sec" % (time.time()-start_t))
        n_estimate = con.execute("SELECT COUNT() FROM pwn").fetchone()[0]
        print("successfully store ",n_estimate," record in database")


def producer(count: int, batches, p_id):
    print(batches[p_id])
    min_batch_size = 50
    current_batch = []
    counter = 0

    for file_path in batches[p_id]:
        input_file = open(file_path,"r")
        with open(file_path,"r") as input_file:
            lines = input_file.read().splitlines()
            print("start file "+file_path+ " => process "+str(p_id))
            for line in lines: 
                split = ''
                semi = line.find(';')
                dots = line.find(':')
                if(semi == -1):
                    split = line.split(':',1)
                elif(dots == -1):
                    split = line.split(';',1)
                if(semi < dots):
                    split = line.split(';',1)
                if(semi < dots):
                    split = line.split(':',1)
                #print(split)
                if(len(split) ==2):
                    email = split[0]
                    password = split[1].split("\n")[0] or split[1]
                    current_batch.append((str(uuid.uuid4()), email, password))
                    counter +=1
                if(counter % min_batch_size == 1):
                    q.put(('INSERT INTO pwn (uuid, email, password) VALUES(?, ?, ?)', current_batch))

def path_splitter(producers_count):
    reader_path = '/home/asim/Downloads/Programming/Python lab/splitted'
    pathes = []
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".txt"):
                start_t = time.time()
                pathes.append(os.path.join(path, file))
    batches = np.array_split(pathes,producers_count)
    return batches


def main():
    
    total_rows = 1_000
    # start the consumer
    threading.Thread(target=consumer, daemon=True).start()

    # we would want to launch as many as producers, so we will take the max CPU value
    # and launch as many. We keep two threads, one for main and one for consumer.
    max_producers = multiprocessing.cpu_count() - 2
    batches = path_splitter(max_producers)
    print(max_producers, 'multiprocessing used')

    # how many rows each producer should produce
    each_producer_count = int(total_rows / max_producers)

    producer_threads: List[threading.Thread] = [threading.Thread(
        target=producer, args=(each_producer_count,batches,i)) for i in range(max_producers)]

    for p in producer_threads:
        p.start()

    for p in producer_threads:
        p.join()

    q.join()
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()