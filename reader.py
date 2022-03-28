import os,sqlite3,uuid,re

conn = sqlite3.connect("powned")
c = conn.cursor()
counter = 0
for path, currentDirectory, files in os.walk("/home/asim/Downloads/Programming/Python lab/dataleak"):
    for file in files:
        if file.endswith(".txt"):
            print(os.path.join(path, file))
            email = ""
            password = ""
            count = 0
            with open('log.txt', 'a') as the_file:
                the_file.write("==============")

            print("Starting:",os.path.join(path, file))
            input_file = open(file,"r")
            for line in input_file: 
                split = ''
                semi = line.find(';')
                dots = line.find(':')
                if(semi == -1):
                    split = line.split(':')
                elif(dots == -1):
                    split = line.split(';')
                if(semi < dots):
                    split = line.split(';')
                if(semi < dots):
                    split = line.split(':')
                email = split[0]
                password = split[1].split("\n")[0] or split[1]

                c.execute("INSERT INTO pwn (uuid, email, password) VALUES(?, ?, ?)", (str(uuid.uuid4()), email, password))
                count += 1
                counter += 1
            print("file :",os.path.join(path, file), count , " record")
            with open('log.txt', 'a') as the_file:
                the_file.write("file :",os.path.join(path, file), count , " record")

conn.commit()
conn.close()
print("successfully store ",counter," record in database")
with open('log.txt', 'a') as the_file:
                the_file.write("successfully store ",counter," record in database")
