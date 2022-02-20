'''
db_ascii.py
tabelle mit ascii-zeichen füllen
@RLange
'''

import pymysql


try:
    con = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 #database='obstladen_2021',
                                 database='obstladen',
                                 charset='utf8mb4')

    with con.cursor() as cursor:
        for z in range(33,127):
            sql = "INSERT INTO ascii_table (id, ascii_value) values (%s, %s)"
            val = (z, chr(z))
            cursor.execute(sql, val)
            con.commit()
    print("Fertisch......")
except:
    print("Unexpected error:", sys.exc_info()[1])
finally:
    if con:
        con.close()
    

