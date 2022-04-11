import threading
import multiprocessing
import numpy as np
import os,time
from pymongo import MongoClient
 
 
myclient = MongoClient("mongodb://localhost:27017/")
 
# database
db = myclient["powned"]
 
# Created or Switched to collection
# names: GeeksForGeeks
collection = db["users"]

start_t = time.time()
TOTAL_ROWS = 0
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def inserter(pathes,P_ID):
    global TOTAL_FILES
    global TOTAL_ROWS
    global INSERTED_FILES
    global INSERTED_ROWS
    min_batch_size = 50
    for file_path in pathes[P_ID]:
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        with open(file_path,"r") as input_file:
            lines = input_file.read().splitlines()
            print("\n start file "+file_path+" =>" + str(P_ID))
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
                    current_batch.append({"email":email,"password":password})
            INSERTED_ROWS += len(lines)
        collection.insert_many(current_batch, ordered=False)
        INSERTED_FILES +=1
        print("\n inserted "+len(lines)+" in " + str(time.time()-insert_s_time)+" =>" + str(P_ID))
        print("\n FILES PROGRESS "+str(INSERTED_FILES)+"/"+str(TOTAL_FILES)+" =>" + str(P_ID))
        print("\n ROWS PROGRESS "+str(INSERTED_ROWS)+"/"+str(TOTAL_ROWS)+" =>" + str(P_ID))
        





def path_splitter(producers_count):
    global TOTAL_FILES
    global TOTAL_ROWS
    reader_path = '/home/asim/Downloads/Programming/Python lab/splitted'
    pathes = []
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".txt"):
                TOTAL_FILES +=1
                TOTAL_ROWS += sum(1 for line in open(os.path.join(path, file)))
                start_t = time.time()
                pathes.append(os.path.join(path, file))
    #batches = np.array_split(pathes,producers_count)
    return pathes

def main():
    max_producers = multiprocessing.cpu_count() - 2
    pathes = path_splitter(max_producers)
    print(max_producers, 'multiprocessing used')

    # how many rows each producer should produce
    each_producer_count = int(TOTAL_ROWS / max_producers)
    inserter(TOTAL_ROWS,1)
    # inserter_threads: List[threading.Thread] = [threading.Thread(
    #     target=inserter, args=(pathes,i)) for i in range(multiprocessing.cpu_count() - 2)]


    # for p in inserter_threads:
    #     p.start()

    # for p in inserter_threads:
    #     p.join()

    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()