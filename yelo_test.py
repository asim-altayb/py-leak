import os,time,traceback

start_t = time.time()
TOTAL_FILES = 0
INSERTED_FILES = 0
INSERTED_ROWS = 0

def split_line(txt, source):
    try:
        object_line = eval(txt)
        return {"name":object_line['fullName'].get('displayName',None),
                "phone":object_line['contactInformation'].get('primaryPhone',None),
                "s_phone":object_line['contactInformation'].get('secondaryPhone',None),
                "s_phone":object_line['contactInformation'].get('secondaryPhone',None),
                "email":object_line['contactInformation'].get('email',None),
                "dob":object_line['basicInformation'].get('dateOfBirth',None),
                "addresses":object_line.get('address',None),
                "documents":object_line.get('documents',None),
                "source":source,
                }
    except Exception:
        print(traceback.format_exc())
    
def inserter(pathes,P_ID):
    for file_path in pathes:
        input_file = open(file_path,"r")
        current_batch = []
        insert_s_time = time.time()
        with open(file_path,"r") as input_file:
            try:
                lines = input_file.read().splitlines()
                print("\n start file "+file_path+" =>" + str(P_ID))
                for line in lines:
                    line_ob = eval(line)
                    row = line_ob.get('results',line_ob)
                    if(row != None):
                        split = split_line(row)
                        if(split):
                            current_batch.append(split)
                print(current_batch[0:200])
            except Exception:
                print(traceback.format_exc())
                print('** File'+file_path+' failed to insert => skip')

def path_splitter():
    global TOTAL_FILES
    reader_path = '/home/nawaf/splitted'
    pathes = []
    for path, currentDirectory, files in os.walk(reader_path):
        for file in files:
            if file.endswith(".txt"):
                TOTAL_FILES +=1
                start_t = time.time()
                pathes.append(os.path.join(path, file))
    return pathes

def main():
    pathes = path_splitter()
    inserter(pathes,1)
    print ("\n Time Taken: %.3f sec" % (time.time()-start_t))

if __name__ == '__main__':
    main()