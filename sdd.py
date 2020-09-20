import pandas as pd
import sqlite3
import os.path
import sys

sys.path.append(".")
from Selector import *

BASE_DIR = '/home/ubuntu/flaskapp'
db_path = os.path.join(BASE_DIR, "ex1.db")

def createTables(db):
    db.execute("DROP TABLE ZIP_SLOT_SIM")
    db.execute("DROP TABLE SLOT_CAPACITY")
    db.execute("CREATE TABLE ZIP_SLOT_SIM(ZIPCODE INT, MARKET TEXT,SLOT INT, CLAIM_CNT INT,SERVED_FLAG INT, DENSITY_PRED INT, DENSITY_SUCC INT)")
    db.execute("CREATE TABLE SLOT_CAPACITY(MARKET TEXT, DAY_OF_WK INT, SLOT INT, CAPACITY, CAPACITY_REMAINING)")

def loadDFs():
    global data
    data = pd.read_csv('data/HackathonInputData.txt', delimiter="|")
    zipInfo = pd.read_csv('data/zip_info_STATIC_v2.csv', delimiter=",")
    slotInfo = pd.read_csv('data/zip_slot_info_STATIC_v1.csv', delimiter=",")
   # print(data.SERVICE_JOB_ID)

def loadDynamicData(db):
    pd.read_csv('data/zip_slot_sim_DYNAMIC_v1.csv', delimiter=",").to_sql("ZIP_SLOT_SIM",db, if_exists='append', index=False)
    displayData(db, "ZIP_SLOT_SIM")
    pd.read_csv('data/slot_capacity_DYNAMIC_full_v1.csv', delimiter=",").to_sql("SLOT_CAPACITY",db, if_exists='append', index=False)
    print("************ SLOT_CAPACITY DATA**************")
    displayData(db, "SLOT_CAPACITY")
    
def displayData(db, tableName):
    cursor = db.execute("select * from {}".format(tableName));
    for row in cursor:
         print(row)

def main():
    loadDFs()
    
    with sqlite3.connect(db_path) as db:
       #createTables(db)
       # insert(db,"pasha",2000)
       loadDynamicData(db)
    db.close()

    slotSelector = SlotSelector()
    slots = {
        '13:00': 1.00,
        '15:00': 4.50,
        '17:00': 11.25,
        '19:00': 20.75,
    }
    availability =  {"13:00": 0.0, "15:00": 0.9, "17:00": 0.0, "19:00": 0.0}
    slot_list = [Slot(k,v) for k, v in slots.items()]
    print("slot_list=",slot_list)
    print(slotSelector.normalize_costs(slot_list))
    print(slotSelector.scale_by_availability(slot_list,availability))
    print(slotSelector.generate_open_slots(slots,availability))
    print(slotSelector.choose_slot(slots,availability))
    print('Done')

def costFunction():
    print("*************")
    print(data)
    print("*************")

def insert(db, val1, val2):
    db.execute("insert into tbl1 values('{}','{}')".format(val1,val2))
    cursor = db.execute("select * from tbl1");
    for row in cursor:
         print(row[0])

if __name__=="__main__": 
    main()
