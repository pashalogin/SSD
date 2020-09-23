import pandas as pd
import sqlite3
import os.path
import sys

BASE_DIR = '/home/ubuntu/flaskapp'
db_path = os.path.join(BASE_DIR, "ex1.db")

with sqlite3.connect(db_path) as db:
    inputData = pd.read_csv('data/HackathonInputData_v4.csv', sep=",")
    slot_allocation_output = pd.read_sql_query("select * from SLOT_ALLOCATION_OUTPUT", db)
    slot_allocation_output = slot_allocation_output.merge(inputData, on = 'SERVICE_JOB_ID',how='left')
    slot_allocation_output.to_csv("data/slot_allocation_output_2.csv", index=False)
    print(slot_allocation_output.head(5))
    print(len(slot_allocation_output))
db.close()
