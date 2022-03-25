import sqlite3

conn = sqlite3.connect('powned') 
c = conn.cursor()

c.execute('''
        CREATE TABLE `pwn` ( `uuid` VARCHAR(200) NOT NULL , `f_name` VARCHAR(200) NULL ,`s_name` VARCHAR(200) NULL, `l_name` VARCHAR(200) NULL , `full_name` VARCHAR(255) NULL , `username` VARCHAR(200) NULL , `email` VARCHAR(200) NULL , `password` VARCHAR(255) NULL , `phone1` VARCHAR(100) NULL, `phone2` VARCHAR(100) NULL , `ip` VARCHAR(100) NULL ,`address1` VARCHAR(100) NULL,`address2` VARCHAR(100) NULL,`gender` VARCHAR(100) NULL,`source` VARCHAR(100) NULL,`passport` VARCHAR(100) NULL, PRIMARY KEY (`uuid`));
        ''')
c.execute('''
        CREATE INDEX index_pwn_f_name ON pwn(f_name)
        ''')
c.execute('''
        CREATE INDEX index_pwn_s_name ON pwn(s_name)
        ''')
c.execute('''
        CREATE INDEX index_pwn__l_name ON pwn(l_name)
        ''')
c.execute('''
        CREATE INDEX index_pwn__full_name ON pwn(full_name)
        ''')
c.execute('''
        CREATE INDEX index_pwn_source ON pwn(source)
        ''')
c.execute('''
        CREATE INDEX index_pwn_username ON pwn(username)
        ''')
c.execute('''
        CREATE INDEX index_pwn_email ON pwn(email)
        ''')
c.execute('''
        CREATE INDEX index_pwn_password ON pwn(password)
        ''')
c.execute('''
        CREATE INDEX index_pwn_phone1 ON pwn(phone1)
        ''')
c.execute('''
        CREATE INDEX index_pwn_phone2 ON pwn(phone2)
        ''')
c.execute('''
        CREATE INDEX index_pwn_ip ON pwn(ip)
        ''')
c.execute('''
        CREATE INDEX index_pwn_passport ON pwn(passport)
        ''')
conn.commit()