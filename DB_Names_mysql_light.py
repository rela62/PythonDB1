"""
Created on Tuesday Apr 06 11:10:12 2021
DB_Names_mysql_light.py
Insert data from names.csv to table 'names'
Id,Name,Year,Gender,State,Count
@author: RLange
"""

import pymysql, sys
from datetime import datetime

# classes
class TimeDelta():
    def __init__(self):
        self.__start = datetime
        self.__stop = datetime
        self.__delta = datetime

    def Start(self):
        self.__start = datetime.now()
            
    def Stop(self):
        self.__stop = datetime.now()

    def Delta(self):
        self.__delta = self.__stop - self.__start
        return self.__delta


# Funktionen
def insert_record(ids,name,year,gender,state,count):
    try:
        sql = "insert into names values (%s, %s, %s, %s, %s, %s)"
        val = (ids,name,year,gender,state,count)
        cursor.execute(sql,val)
        con.commit()   
    except:
        print("Unexpected error:", sys.exc_info()[1])
        
def names_einlesen(max):
    with open("names.csv","r",encoding="utf-8") as file:
        file.readline()
        counter = 1
        for line in file:
            list = line.strip().split(",")
            ids       = list[0]
            name      = list[1]
            year      = list[2]
            gender    = list[3]
            state     = list[4]
            count     = list[5]
            insert_record(ids, name, year, gender, state, count)
            counter += 1
            
            if counter == max + 1:
                print("Records inserted:",counter - 1)
                break
            
# main --------------

# open db

try:
    con = pymysql.connect(host='localhost',
                                 user='root',
                                 password='',
                                 database='obstladen',
                                 charset='utf8mb4')
    cursor = con.cursor()
    delta = TimeDelta()
    print("Connection successfull !")
except:
    print("Unexpected error:", sys.exc_info()[1])
    if con:
        con.close()
    sys.exit()

delta.Start()
names_einlesen(10000)
if con:
    cursor.close()
    con.close()
    print("Connection disconnected")
    
delta.Stop()
print("Time:",delta.Delta())


