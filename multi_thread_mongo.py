import threading
import multiprocessing
import numpy as np
import os,time
from pymongo import MongoClient
 
 
myclient = MongoClient("mongodb://localhost:27017/")
 
# database
db = myclient["powned"]
 
collection = db["users"]

start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def split_line(txt):
    delims = [':',';',' ']
    for d in delims:
        result = txt.strip().split(d, maxsplit=1)
        if len(result) == 2: 
            result[0] = result[0].strip()
            result[1] = result[1].strip()
            return result

    return [txt] # If nothing worked, return the input

def delete_inserted_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
    return True


def inserter(pathes,P_ID):
    global TOTAL_FILES
    global INSERTED_FILES
    global INSERTED_ROWS
    for file_path in pathes[P_ID]:
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        with open(file_path,"r") as input_file:
            lines = input_file.read().splitlines()
            print("\n start file "+file_path+" =>" + str(P_ID))
            for line in lines: 
                split = split_line(line)
                if(len(split) ==2):
                    email = split[0]
                    password = split[1].split("\n")[0] or split[1]
                    current_batch.append({"email":email, "password":password, "source":file_path})
                    INSERTED_ROWS +=1
        INSERTED_FILES +=1
        if(len(current_batch) >0):
            try:
                collection.insert_many(current_batch, ordered=False)
                delete_inserted_file(file_path)
                print("\n inserted "+str(len(lines))+" in " + str(time.time()-insert_s_time)+" =>" + str(P_ID))
                print("\n FILES PROGRESS "+str(INSERTED_FILES)+"/"+str(TOTAL_FILES)+" =>" + str(P_ID))
                print("\n ROWS INSERTED "+str(INSERTED_ROWS))
            except:
                print('** File'+file_path+' failed to insert => skip')
        

def path_splitter(producers_count):
    global TOTAL_FILES
    reader_path = '/home/nawaf/splitted'
    pathes = []
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".txt"):
                TOTAL_FILES +=1
                start_t = time.time()
                pathes.append(os.path.join(path, file))
    #batches = np.array_split(pathes,producers_count)
    return pathes

def main():
    max_producers = multiprocessing.cpu_count() - 2
    pathes = path_splitter(max_producers)
    print(max_producers, 'multiprocessing used')
    inserter(pathes,1)
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()