import queue
import sqlite3
import threading
import multiprocessing
import os,uuid,time
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
        print(len(batch),stmt)
        print ("\n start Time Taken for "+str(len(batch))+" record: %.3f sec" % (time.time()-start_t))
        con.execute('BEGIN')
        con.executemany(stmt, batch)
        con.commit()
        q.task_done()
        print ("\n finish Time Taken for"+str(len(batch))+"  record: %.3f sec" % (time.time()-start_t))
        n_estimate = con.execute("SELECT COUNT() FROM pwn").fetchone()[0]
        print("successfully store ",n_estimate," record in database")


def producer(count: int):
    min_batch_size = 1_000_000
    for _ in range(int(count / min_batch_size)):
        current_batch = []
        for _ in range(min_batch_size):
                current_batch.append((str(uuid.uuid4()), 'email@em.com', 'password@123'))
        q.put(('INSERT INTO pwn (uuid, email, password) VALUES(?, ?, ?)', current_batch))


def main():
    
    total_rows = 100_000_000
    # start the consumer
    threading.Thread(target=consumer, daemon=True).start()

    # we would want to launch as many as producers, so we will take the max CPU value
    # and launch as many. We keep two threads, one for main and one for consumer.
    max_producers = multiprocessing.cpu_count() - 2
    print(max_producers, 'multiprocessing used')

    # how many rows each producer should produce
    each_producer_count = int(total_rows / max_producers)

    producer_threads: List[threading.Thread] = [threading.Thread(
        target=producer, args=(each_producer_count,)) for _ in range(max_producers)]

    for p in producer_threads:
        p.start()

    for p in producer_threads:
        p.join()

    q.join()
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()