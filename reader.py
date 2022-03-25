import os,sqlite3,uuid

conn = sqlite3.connect("powned")
c = conn.cursor()
for path, currentDirectory, files in os.walk("/home/asim/Downloads/Programming/"):
    for file in files:
        if file.endswith(".txt"):
            print(os.path.join(path, file))
            email = ""
            password = ""
            count = 0

            print("Starting:",os.path.join(path, file))
            input_file = open(file,"r")
            for line in input_file: 
                split = line.split(":")
                email = split[0]
                password = split[1]

                c.execute("INSERT INTO pwn (email, password) VALUES(?, ?)", (email, password))
                if(count % 1000 == 0):
                    print("+1000 record done")
                count += 1
            print("file :",os.path.join(path, file), count , " record")

    conn.commit()
    conn.close()