import os,time

lines_per_file = 1000000
smallfile = None
splitted_path = '/home/asim/Downloads/Programming/Python lab/splitted'
t = time.time()
for path, currentDirectory, files in os.walk("/home/asim/Downloads/Programming/Python lab/dataleak"):
    for file in files:
        if file.endswith(".txt"):
            folder_name = os.path.basename(path)
            file_name = os.path.splitext(file)[0]
            with open(os.path.join(path, file)) as bigfile:
                for lineno, line in enumerate(bigfile):
                    if lineno % lines_per_file == 0:
                        if smallfile:
                            smallfile.close()
                        small_filename = file_name+'_{}.txt'.format(lineno + lines_per_file)
                        os.makedirs((splitted_path+'/splitters/'+folder_name+'/'), exist_ok=True)
                        n_path = 'splitters/'+folder_name+'/'+small_filename
                        print(os.path.join(splitted_path, n_path))
                        smallfile = open(os.path.join(splitted_path, n_path), "w")
                    smallfile.write(line)
                if smallfile:
                    smallfile.close()
print ("\n Time Taken: %.3f sec" % (time.time()-t))