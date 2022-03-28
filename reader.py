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

            print("Starting:",os.path.join(path, file))
            input_file = open(file,"r")
            for line in input_file: 
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

                    c.execute("INSERT INTO pwn (uuid, email, password) VALUES(?, ?, ?)", (str(uuid.uuid4()), email, password))
                    count += 1
                    counter += 1
            conn.commit()
            print("file :",os.path.join(path, file), count , " record")
            with open('log', 'a') as the_file:
                the_file.write(os.path.join(path, file)+ os.linesep)
            print(counter, " record inserted in db")
            
            

n_estimate = conn.execute("SELECT COUNT() FROM pwn").fetchone()[0]
print("successfully store ",n_estimate," record in database")

conn.close()


