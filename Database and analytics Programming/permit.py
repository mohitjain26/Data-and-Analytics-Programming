# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 01:52:36 2020

@author: jnmht
"""
# fetch data from api
import requests
import datetime
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["permitdatabase"]
mycol = mydb["customers"]
date = datetime.datetime(2007,1,1)
#store = ''
#m = {}
for i in range(1,11):
         date += datetime.timedelta(days=1)
         #str(date)
         chang = str(date).replace(" 00:00:00","T00:00:00.000")
         url2 = 'https://data.sfgov.org/resource/i98e-djp9.json'
         final_query1 = url2 + '?permit_creation_date=' + chang
         response = requests.get(final_query1)
         api_data = response.json()
         for data in api_data:
             mycol.insert_one(data) # store data in mongo db
#         store = store +str(response.json())
         #m.update(store)
         #print(m)
         #mycol.insert_many(store)
#########
# fetch data from mongodb
#fetch = mycol.find()
#for j in fetch:
#    print(j)
# postgre
    
# Json to DF   
import pandas as pd
import numpy
df = pd.DataFrame(list(mycol.find())) 
df.head()
#del df["_id"]
#df1 = df.replace(numpy.NaN,0)
from sqlalchemy import create_engine
import psycopg2 
import io

 

# Code to create DB
try:
    dbConnection = psycopg2.connect(
        password = "mohitjain267",
        host = "localhost",
        port = "5432",
        user = "postgres")
    
except (Exception , psycopg2.Error) as dbError :
    print("Error while connecting to PostgreSQL", dbError)

 

else:
    dbConnection.set_isolation_level(0)
    dbCursor = dbConnection.cursor()
    dbCursor.execute('CREATE DATABASE building_permit;')
    dbCursor.close()
    print("Connection established")

 

finally:
    if(dbConnection): dbConnection.close()

#test = pd.DataFrame({'numbers': [1, 2, 3], 'colors': ['red', 'white', 'blue']})

# DF to Postgresql
engine = create_engine('postgresql+psycopg2://postgres:mohitjain267@localhost:5432/building_permit')
df.to_sql("test1", engine, if_exists='replace',index=False) #truncates the table
#print(df1)
#df.to_csv(r'C:\Users\jnmht\Desktop\Data Analytics\DAP\Project\permit.csv', index = False)


 

# psotgresql to DF
#import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
 


sql="""SELECT * FROM test1;"""
#sql1= SELECT permit_number FROM permit
try:
    dbConnection = psycopg2.connect(
        password = "mohitjain267",
        host = "localhost",
        port = "5432",
        user = "postgres",
        database="building_permit")
    permit_dataframe = sqlio.read_sql_query(sql, dbConnection)
    print(permit_dataframe)
except (Exception , psycopg2.Error) as dbError :
    print ("Error", dbError)
finally:
    if(dbConnection): dbConnection.close()
"""
# from csv to postgre

try:
    dbConnection = psycopg2.connect(
            user = "postgres",
            password = "mohitjain267",
            host = "localhost",
            port = "5432",
            database = "building_permit")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    dbCursor.execute("""
#CREATE TABLE weather4(
#id varchar(50) PRIMARY KEY,
#permit_number varchar(50),
#permit_type integer,
);

""")
    dbCursor.close()
except (Exception , psycopg2.Error) as dbError :
    print ("Error while connecting to PostgreSQL", dbError)
finally:
    if(dbConnection): dbConnection.close()


import csv
try:
    dbConnection = psycopg2.connect(
            user = "postgres",
            password = "mohitjain267",
            host = "localhost",
            port = "5432",
            database = "building_permit")
    dbConnection.set_isolation_level(0) # AUTOCOMMIT
    dbCursor = dbConnection.cursor()
    insertString = "INSERT INTO weather4 VALUES ('{}',"+"'{}')"
    with open('C:\\Users\\jnmht\\Desktop\\Data Analytics\\DAP\\Project\\permit.csv', 'r') as f:
        reader = csv.reader(f)
        next(reader) # skip the header
        for row in reader:
            dbCursor.execute(insertString.format(*row))
    dbConnection.commit()
    dbCursor.close()
except (Exception , psycopg2.Error) as dbError :
    print ("Error:", dbError)
finally:
    if(dbConnection): dbConnection.close()
"""
#Y = json.dumps(s)
#f= open("data1.txt","w+")
#f.write(store)
#f.close()         
#len(store)



#import pymongo
# create database
#myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#mydb = myclient["permitdatabase"]
#mycol = mydb['Customers"]

# can check which database exist
#print(myclient.list_database_names())
#mycol.insert_many(store.json())
