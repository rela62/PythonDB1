"""
DB_Names_to_csv.py
Insert data from database to outputnames.csv
Id,Name,Year,Gender,State,Count
@author: RLange
2022-02-07
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

class DBOutput():
    def __init__(self,dbhost,dbuser,dbpwd,db,dbchar="utf8mb4"):
        try:
            self.__con = pymysql.connect(host=dbhost,
                                 user=dbuser,
                                 password=dbpwd,
                                 database=db,
                                 charset=dbchar)
            self.__cursor = self.__con.cursor()
            print("Connection established ")
        except:
            print("Unexpected error:", sys.exc_info()[1])
            if self.__con:
                self.__con.close()
            return
        
    def SelectRecords(self,sql):
        try:
            self.__cursor.execute(sql)
            return self.__cursor
        except:
            print("Unexpected error:", sys.exc_info()[1])
            return -1
            
    def ConClose(self):
        self.__con.close()
        
# ---------------------------------------------------------------

# Funktionen
        
def writeNames(myFile,records, mode="w"):
    with open(myFile,mode,encoding="utf-8") as file:
        lineCounter = 0
        for record in records:
            file.write(str(record[0]) + "," + \
                       str(record[1]) + "," + \
                       str(record[2]) + "," + \
                       str(record[3]) + "," + \
                       str(record[4]) + "," + \
                       str(record[5]) + "\n")
            lineCounter += 1

        file.close()
        print(lineCounter,"Records stored to file", myFile)
            
# main --------------
delta = TimeDelta()
delta.Start()
dbTest = DBOutput("localhost","root","","obstladen")
sql = "SELECT * FROM names"
myCursor = dbTest.SelectRecords(sql)
if myCursor != -1:
    writeNames("testtesttest.csv",myCursor)
dbTest.ConClose()
delta.Stop()
print("Time:",delta.Delta())
