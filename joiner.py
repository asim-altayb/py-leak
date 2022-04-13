import os
with open("large.txt", "wb") as outfile:
    for path, currentDirectory, files in os.walk("/home/asim/Downloads/Programming/Python lab/splitted"):
        for file in files:
            if file.endswith(".txt"):
                print(file)
                with open(os.path.join(path, file),'rb') as infile:
                    outfile.write(infile.read())