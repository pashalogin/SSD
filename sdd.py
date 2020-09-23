import pandas as pd
import sqlite3
import os.path
import sys

import warnings
warnings.filterwarnings("ignore")

sys.path.append(".")
from Selector import *
from CostFunction_hackathon_2020_v1 import *

BASE_DIR = '/home/ubuntu/flaskapp'
db_path = os.path.join(BASE_DIR, "ex1.db")

def createTables(db):
    db.execute("DROP TABLE ZIP_SLOT_SIM")
    db.execute("DROP TABLE SLOT_CAPACITY")
    db.execute("DROP TABLE SLOT_ALLOCATION_OUTPUT")
    db.execute("CREATE TABLE ZIP_SLOT_SIM(ZIPCODE INT, MARKET TEXT,SLOT INT, CLAIM_CNT INT,SERVED_FLAG INT, DENSITY_PRED INT, DENSITY_SUCC INT)")
    db.execute("CREATE TABLE SLOT_CAPACITY(MARKET TEXT, DAY_OF_WK INT, SLOT INT, TIME_OF_DAY TEXT,CAPACITY INT, CAPACITY_REMAINING INT)")
    db.execute("CREATE TABLE SLOT_ALLOCATION_OUTPUT(claimid TEXT, slot_sim INT, cost float, fullAddress text, SERVICE_JOB_ID text)")

def loadDynamicData(db):
    pd.read_csv('data/zip_slot_sim_DYNAMIC_v3.csv', delimiter=",").to_sql("ZIP_SLOT_SIM",db, if_exists='append', index=False)
    #displayData(db, "ZIP_SLOT_SIM")
    pd.read_csv('data/slot_capacity_DYNAMIC_full_v5_1.csv', delimiter=",").to_sql("SLOT_CAPACITY",db, if_exists='append', index=False)
   # displayData(db, "SLOT_CAPACITY")
    
def displayData(db, tableName):
    cursor = db.execute("select * from {}".format(tableName));
    for row in cursor:
         print(row)
def checkLen(x):
    if len(x)==3:
        return '00'+x
    elif len(x)==4:
        return '0'+x
    else:
        return x
def main():
    inputData = pd.read_csv('data/HackathonInputData_v4.csv', sep=",")
    
    zip_info = pd.read_csv("data/zip_info_STATIC_v3.csv", sep=",")
    zip_slot_info = pd.read_csv("data/zip_slot_info_STATIC_v3.csv", sep=",")
    slot_maxZipCount = pd.read_csv("data/slot_maxZipCount_STATIC_v3.csv", sep=",")

    zip_info.zipcode = zip_info.zipcode.astype(str)
    zip_slot_info.zipcode = zip_slot_info.zipcode.astype(str)
    zip_slot_info.scheduled_service_time = zip_slot_info.scheduled_service_time.astype('int64')
    zip_info.adj_zip_30 = zip_info.adj_zip_30.astype(str)
    zip_info['adj_zip_30'] = zip_info['adj_zip_30'].apply(lambda x: x.replace("[", "").replace("]", "").replace("'", "").replace("\n", "").split(" "))

    #print(zip_info.dtypes)
    #print(zip_info.head(2))
    uniqueDateList = inputData['SJ CREATED DATE'].unique()

    with sqlite3.connect(db_path) as db:
         createTables(db)
         loadDynamicData(db)
         slot_capacity = pd.read_sql_query("select * from SLOT_CAPACITY", db)
         zip_slot_sim = pd.read_sql_query("select * from ZIP_SLOT_SIM", db)
    db.close()

    List = [uniqueDateList[0],uniqueDateList[1], uniqueDateList[2],uniqueDateList[3], uniqueDateList[4]] 
    print(List)
    #print(uniqueDateList)
    #for uDate in uniqueDateList:
    for uDate in List:
        inputDataByDate = inputData[inputData['SJ CREATED DATE'] == uDate]
        inputDataByDate = inputDataByDate.sort_values(by=['SJ CREATED TIME'],ascending=True) 
        inputDataByDate = inputData[inputData['SERVICE_JOB_STATUS'] != 'CAN']
        
        for row in inputDataByDate.itertuples():
            inputData = pd.read_csv('data/HackathonInputData_v4.csv', sep=",")

            zip_info = pd.read_csv("data/zip_info_STATIC_v3.csv", sep=",")
            zip_slot_info = pd.read_csv("data/zip_slot_info_STATIC_v3.csv", sep=",")
            slot_maxZipCount = pd.read_csv("data/slot_maxZipCount_STATIC_v3.csv", sep=",")

            zip_info.zipcode = zip_info.zipcode.astype(str)
            zip_slot_info.zipcode = zip_slot_info.zipcode.astype(str)
            zip_slot_info.scheduled_service_time = zip_slot_info.scheduled_service_time.astype('int64')
            zip_info.adj_zip_30 = zip_info.adj_zip_30.astype(str)
            zip_info['adj_zip_30'] = zip_info['adj_zip_30'].apply(lambda x: x.replace("[", "").replace("]", "").replace("'", "").replace("\n", "").split(" "))

            #zip_info['adj_zip_30'] = zip_info['adj_zip_30'].apply(lambda x: x.replace("[", "").replace("]", "").replace("'", "").replace("\n", "").split(" "))
            print(row.ZIPCODE, row.SCHEDULED_SERVICE_TIME, row.LOCATION_NAME, row.time_of_day)
            zipcode = checkLen(str(row.ZIPCODE))
            market = row.LOCATION_NAME
            #slot = int(row.SCHEDULED_SERVICE_TIME)
            slot = 1300
            day_of_wk = row.DAY_OF_WEEK
            expert_type = "FT"
            time_of_day = row.time_of_day
            claim_id = str(row.SERVICE_JOB_ID).replace("SJ", "")
            fullAddress = str(row.ADDRESS_LINE_1) + " " + str(row.ADDRESS_CITY) + " " + str(row.ADDRESS_PROVINCE) + " " + str(row.ZIPCODE)
            
            #print(slot_capacity)
            slot_capacity = slot_capacity[(slot_capacity.TIME_OF_DAY == time_of_day) & (slot_capacity.MARKET == market) & (slot_capacity.DAY_OF_WK == day_of_wk)] 
            zip_info = zip_info[zip_info.market == market]
            zip_slot_info = zip_slot_info[zip_slot_info.market == market]
            slot_maxZipCount = slot_maxZipCount[slot_maxZipCount.market == market]
    
            #zipcode = 35620
            #market = 'Nashville TN'
            #slot = 1300
            #day_of_wk = 0
           # expert_type = "FT"
           # time_of_day = "AM"

            #with sqlite3.connect(db_path) as db:
                #createTables(db)
                #loadDynamicData(db)
                #slot_capacity = pd.read_sql_query("select * from SLOT_CAPACITY", db)
                #zip_slot_sim = pd.read_sql_query("select * from ZIP_SLOT_SIM", db)
            #db.close()
   
            slot_capacity.columns = map(str.lower,slot_capacity.columns)
            zip_slot_sim.columns = map(str.lower,zip_slot_sim.columns)

            slot_sim = slot_capacity[(slot_capacity.market == market)&(slot_capacity.day_of_wk == day_of_wk)]
    
            #print(slot_sim)
    
            zip_slot_sim = zip_slot_sim[zip_slot_sim.market == market]
            zip_slot_sim.zipcode = zip_slot_sim.zipcode.astype(str)
            zip_slot_sim.slot = zip_slot_sim.slot.astype('int64')
    
            # print(zip_info)
            zipList = zip_info.zipcode.to_list()
            print("ZipList::",len(zipList))
            if zipcode in zipList:
                cost_dictionary = slotCosts(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim)
            else:
                print(zipcode)
                print(zipList)
                cost_dictionary = dict({'1300':99999, '1500': 99999, '1700': 99999, '1900': 99999})
            #try:
             #   cost_dictionary = slotCosts(zipcode,slot,expert_type,zip_info,zip_slot_info,slot_maxZipCount, zip_slot_sim)
           # except:
            #    print("Unexpected error:", sys.exc_info())
             #   with sqlite3.connect(db_path) as db:
              #      slot_capacity = pd.read_sql_query("select * from SLOT_CAPACITY", db)
               #     zip_slot_sim = pd.read_sql_query("select * from ZIP_SLOT_SIM", db)
              #  db.close()
               # continue
            print("Cost Dictionary ::", cost_dictionary)
            #print(cost_dictionary)
            #print("Done")

            slot_Pref = pd.read_csv("data/Selection_Probablility_AEHistoricalDataMay-June2020.csv", sep=",")
            slot_Pref = slot_Pref[slot_Pref.LOCATION_NAME == market]
            slot_Pref = slot_Pref[slot_Pref.AM_PM == time_of_day]
            #print(slot_Pref['1300'].apply((lambda x: float(x.replace("%", ""))/100)))

            #print(slot_Pref)
            slot_Pref['1300'] = slot_Pref['1300'].apply((lambda x: float(x.replace("%", ""))/100))
            slot_Pref['1500'] = slot_Pref['1500'].apply((lambda x: float(x.replace("%", ""))/100))
            slot_Pref['1700'] = slot_Pref['1700'].apply((lambda x: float(x.replace("%", ""))/100))
            slot_Pref['1900'] = slot_Pref['1900'].apply((lambda x: float(x.replace("%", ""))/100))

            #print(slot_Pref['1300'].values[0])
            slotPref =  {"1300": slot_Pref['1300'].values[0], "1500": slot_Pref['1500'].values[0], "1700": slot_Pref['1700'].values[0], "1900": slot_Pref['1900'].values[0]}

            print("Slot Preferences ::", slotPref)
            slotSelector = SlotSelector(slot_preferences = slotPref)

            slot_sim_1300 = slot_sim[slot_sim.slot == 1300]
            if slot_sim_1300['capacity_remaining'].values[0] < 1:
                 capacity_1300 = (slot_sim_1300['capacity'].values[0])/slot_sim_1300['capacity'].values[0]
            else:
                capacity_1300 = (slot_sim_1300['capacity'].values[0] - slot_sim_1300['capacity_remaining'].values[0])/slot_sim_1300['capacity'].values[0]

            slot_sim_1500 = slot_sim[slot_sim.slot == 1500]
            if slot_sim_1500['capacity_remaining'].values[0] < 1:
                 capacity_1500 = (slot_sim_1500['capacity'].values[0])/slot_sim_1500['capacity'].values[0]
            else:
                capacity_1500 = (slot_sim_1500['capacity'].values[0] - slot_sim_1500['capacity_remaining'].values[0])/slot_sim_1500['capacity'].values[0]
            
            slot_sim_1700 = slot_sim[slot_sim.slot == 1700]
            if slot_sim_1700['capacity_remaining'].values[0] < 1:
                 capacity_1700 = (slot_sim_1700['capacity'].values[0])/slot_sim_1700['capacity'].values[0]
            else:
                capacity_1700 = (slot_sim_1700['capacity'].values[0] - slot_sim_1700['capacity_remaining'].values[0])/slot_sim_1700['capacity'].values[0]

            slot_sim_1900 = slot_sim[slot_sim.slot == 1900]
            if slot_sim_1900['capacity_remaining'].values[0] < 1:
                 capacity_1900 = (slot_sim_1900['capacity'].values[0])/slot_sim_1900['capacity'].values[0]
            else:
                capacity_1900 = (slot_sim_1900['capacity'].values[0] - slot_sim_1900['capacity_remaining'].values[0])/slot_sim_1900['capacity'].values[0]

            #slot_sim_1500 = slot_sim[slot_sim.slot == 1500]
            #capacity_1500 = (slot_sim_1500['capacity'] - slot_sim_1500['capacity_remaining'])/slot_sim_1500['capacity']

            #slot_sim_1700 = slot_sim[slot_sim.slot == 1700]
            #capacity_1700 = (slot_sim_1700['capacity'] - slot_sim_1700['capacity_remaining'])/slot_sim_1700['capacity']

            #slot_sim_1900 = slot_sim[slot_sim.slot == 1900]
            #capacity_1900 = (slot_sim_1900['capacity'] - slot_sim_1900['capacity_remaining'])/slot_sim_1900['capacity']
      
             #print(capacity)

            availability =  {"1300": capacity_1300, "1500": capacity_1500, "1700": capacity_1700, "1900": capacity_1900}
            print("Availability ::", availability)
            print("day Of Week ::", day_of_wk)
            print("row ::", row)
            choosenSlot = slotSelector.choose_slot(cost_dictionary, availability)
            print("Choosen Slots ::", choosenSlot)

            with sqlite3.connect(db_path) as db:
                updateCapacity(db,choosenSlot.start_time, market, time_of_day, day_of_wk)
                updateZipSlotSim(db,choosenSlot.start_time, market, zipcode)
                slot_capacity = pd.read_sql_query("select * from SLOT_CAPACITY", db)
                zip_slot_sim = pd.read_sql_query("select * from ZIP_SLOT_SIM", db)
                insertResults(db,claim_id,choosenSlot.start_time, choosenSlot.cost,fullAddress, row.SERVICE_JOB_ID)
            db.close()
    
    
    with sqlite3.connect(db_path) as db:
        slot_allocation_output = pd.read_sql_query("select * from SLOT_ALLOCATION_OUTPUT", db)
        slot_allocation_output = slot_allocation_output.merge(inputData, on = 'SERVICE_JOB_ID',how='left')
        slot_allocation_output.to_csv("data/slot_allocation_output.csv", index=False)
    db.close()

def insertResults(db,claim_id, slot, cost, fullAddress, serviceJobID):
    db.execute("insert into SLOT_ALLOCATION_OUTPUT values('{}','{}', '{}', '{}', '{}')".format(claim_id, slot, cost,fullAddress, serviceJobID))
def updateCapacity(db, slot, market, time_of_day, day_of_wk):
    db.execute("update SLOT_CAPACITY set capacity_remaining = capacity_remaining-1 where slot = '{}' and market = '{}' and time_of_day = '{}' and day_of_wk = '{}'".format(slot,market,time_of_day, day_of_wk))
def updateZipSlotSim(db, slot, market, zipcode):
    db.execute("update ZIP_SLOT_SIM set SERVED_FLAG = 1, CLAIM_CNT = CLAIM_CNT+1 where ZIPCODE = '{}' and MARKET = '{}' and SLOT = '{}'".format(zipcode,market,slot))

#ZIP_SLOT_SIM(ZIPCODE INT, MARKET TEXT,SLOT INT, CLAIM_CNT INT,SERVED_FLAG INT, DENSITY_PRED INT, DENSITY_SUCC INT)


if __name__=="__main__": 
    main()
