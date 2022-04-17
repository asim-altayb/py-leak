import os,time,re
start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def split_line(txt):
    delims = [',']
    for d in delims:
        result = txt.strip().split(d, maxsplit=1)
        if len(result) > 2: 
            result[0] = result[0].strip()
            result[1] = result[1].strip()
            return result

    return [txt] # If nothing worked, return the input

def delete_inserted_file(file_path):
    if os.path.exists(file_path):
        os.remove(file_path)
    return True


def inserter(pathes,P_ID):
    for file_path in pathes:
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        with open(file_path,"r") as input_file:
            print("\n start file "+file_path+" =>" + str(P_ID))
            results = [[r.strip().replace('""','').replace('\'\'','') for r in line.split(',')] for line in input_file.read().splitlines()]
            results = [[r for r in row if r] for row in results if row]
            lines =  [row for row in results if row]
            
            print(lines)

def path_splitter(producers_count):
    global TOTAL_FILES
    reader_path = '/home/asim/Downloads/Programming/Python lab/splitted/splitters/x'
    pathes = []
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".txt"):
                TOTAL_FILES +=1
                start_t = time.time()
                pathes.append(os.path.join(path, file))
    return pathes

def main():
    pathes = path_splitter(1)
    inserter(pathes,1)
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))


if __name__ == '__main__':
    main()