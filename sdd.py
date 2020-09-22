import pandas as pd
import sqlite3
import os.path
import sys

sys.path.append(".")
from Selector import *
from CostFunction_hackathon_2020_v1 import *

BASE_DIR = '/home/ubuntu/flaskapp'
db_path = os.path.join(BASE_DIR, "ex1.db")

def createTables(db):
    db.execute("DROP TABLE ZIP_SLOT_SIM")
    db.execute("DROP TABLE SLOT_CAPACITY")
    db.execute("CREATE TABLE ZIP_SLOT_SIM(ZIPCODE INT, MARKET TEXT,SLOT INT, CLAIM_CNT INT,SERVED_FLAG INT, DENSITY_PRED INT, DENSITY_SUCC INT)")
    db.execute("CREATE TABLE SLOT_CAPACITY(MARKET TEXT, DAY_OF_WK INT, SLOT INT, CAPACITY, CAPACITY_REMAINING)")

def loadDynamicData(db):
    pd.read_csv('data/zip_slot_sim_DYNAMIC_v3.csv', delimiter=",").to_sql("ZIP_SLOT_SIM",db, if_exists='append', index=False)
    #displayData(db, "ZIP_SLOT_SIM")
    pd.read_csv('data/slot_capacity_DYNAMIC_full_v3.csv', delimiter=",").to_sql("SLOT_CAPACITY",db, if_exists='append', index=False)
    print("************ SLOT_CAPACITY DATA**************")
   # displayData(db, "SLOT_CAPACITY")
    
def displayData(db, tableName):
    cursor = db.execute("select * from {}".format(tableName));
    for row in cursor:
         print(row)

def main():
    inputData = pd.read_csv('data/HackathonInputData.txt', delimiter="|")
    zip_info = pd.read_csv("data/zip_info_STATIC_v3.csv", sep=",")
    zip_slot_info = pd.read_csv("data/zip_slot_info_STATIC_v3.csv", sep=",")
    slot_maxZipCount = pd.read_csv("data/slot_maxZipCount_STATIC_v3.csv", sep=",")

    zip_info.zipcode = zip_info.zipcode.astype(str)
    zip_slot_info.zipcode = zip_slot_info.zipcode.astype(str)
    zip_slot_info.scheduled_service_time = zip_slot_info.scheduled_service_time.astype('int64')
    zip_info.adj_zip_30 = zip_info.adj_zip_30.astype(str)
    zip_info['adj_zip_30'] = zip_info['adj_zip_30'].apply(lambda x: x.replace("[", "").replace("]", "").replace("'", "").split(" "))

    for row in inputData.head(1).itertuples():
      print(row.ZIPCODE, row.SCHEDULED_SERVICE_TIME, row.LOCATION_NAME)
      zipcode = row.ZIPCODE
      market = row.LOCATION_NAME
      slot = row.SCHEDULED_SERVICE_TIME
      day_of_wk = 0
      expert_type = "FT"
      time_of_day = "AM"

      
      zip_info = zip_info[zip_info.market == market]
      zip_slot_info = zip_slot_info[zip_slot_info.market == market]
      slot_maxZipCount = slot_maxZipCount[slot_maxZipCount.market == market]
    
      zipcode = 35620
      #market = 'Nashville TN'
      slot = 1300
      #day_of_wk = 0
      #expert_type = "FT"

      with sqlite3.connect(db_path) as db:
        #createTables(db)
        # insert(db,"pasha",2000)
        #loadDynamicData(db)
        slot_capacity = pd.read_sql_query("select * from SLOT_CAPACITY", db)
        zip_slot_sim = pd.read_sql_query("select * from ZIP_SLOT_SIM", db)
      db.close()
   
      slot_capacity.columns = map(str.lower,slot_capacity.columns)
      zip_slot_sim.columns = map(str.lower,zip_slot_sim.columns)

      slot_sim = slot_capacity[(slot_capacity.market == market)&(slot_capacity.day_of_wk == day_of_wk)]
    
      #print(slot_sim)
    
      zip_slot_sim = zip_slot_sim[zip_slot_sim.market == market]
      zip_slot_sim.zipcode = zip_slot_sim.zipcode.astype(str)
      zip_slot_sim.slot = zip_slot_sim.slot.astype('int64')
    
     # print(zip_info)


      cost_dictionary = slotCosts(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim)
      print("Cost Dictionary ::")
      print(cost_dictionary)
      print("Done")

      slot_Pref = pd.read_csv("data/Selection_Probablility_AEHistoricalDataMay-June2020.csv", sep=",")
      slot_Pref = slot_Pref[slot_Pref.LOCATION_NAME == market]
      slot_Pref = slot_Pref[slot_Pref.AM_PM == time_of_day]
      slot_Pref['1300'] = slot_Pref['1300'].apply((lambda x: float(x.replace("%", ""))/100))

      slot_Pref['1500'] = slot_Pref['1500'].apply((lambda x: float(x.replace("%", ""))/100))
      slot_Pref['1700'] = slot_Pref['1700'].apply((lambda x: float(x.replace("%", ""))/100))
      slot_Pref['1900'] = slot_Pref['1900'].apply((lambda x: float(x.replace("%", ""))/100))

      slotPref =  {"1300": slot_Pref['1300'][2], "1500": slot_Pref['1500'][2], "1700": slot_Pref['1700'][2], "1900": slot_Pref['1900'][2]}

      print(slotPref)
      slotSelector = SlotSelector(slot_preferences = slotPref)

      slot_sim_1300 = slot_sim[slot_sim.slot == 1300]
      capacity_1300 = (slot_sim_1300['capacity'] - slot_sim_1300['capacity_remaining'])/slot_sim_1300['capacity']

      slot_sim_1500 = slot_sim[slot_sim.slot == 1500]
      capacity_1500 = (slot_sim_1500['capacity'] - slot_sim_1500['capacity_remaining'])/slot_sim_1500['capacity']

      slot_sim_1700 = slot_sim[slot_sim.slot == 1700]
      capacity_1700 = (slot_sim_1700['capacity'] - slot_sim_1700['capacity_remaining'])/slot_sim_1700['capacity']

      slot_sim_1900 = slot_sim[slot_sim.slot == 1900]
      capacity_1900 = (slot_sim_1900['capacity'] - slot_sim_1900['capacity_remaining'])/slot_sim_1900['capacity']
      
      #print(capacity)

      availability =  {"1300": capacity_1300[1], "1500": capacity_1500[2], "1700": capacity_1700[3], "1900": capacity_1900[4]}
      print(availability)
      choosenSlot = slotSelector.choose_slot(cost_dictionary, availability)
      print(choosenSlot)

      with sqlite3.connect(db_path) as db:
        updateCapacity(db,choosenSlot.start_time, market)
      db.close()

def updateCapacity(db, slot, market):
    db.execute("update SLOT_CAPACITY set capacity_remaining = capacity_remaining-1 where slot = '{}' and market = '{}'".format(slot,market))

if __name__=="__main__": 
    main()
