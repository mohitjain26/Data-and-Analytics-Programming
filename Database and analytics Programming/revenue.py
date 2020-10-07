# -*- coding: utf-8 -*-
"""
Created on Sun Apr 26 00:49:58 2020

@author: jnmht
"""



# fetch data from api
import requests
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["revenue_california"]
mycol = mydb["county"]
url2 = 'https://bythenumbers.sco.ca.gov/resource/ky7j-fsk5.json?$where=fiscal_year > 2002'
response = requests.get(url2)
api_data = response.json()
for data in api_data:
    mycol.insert_one(data)


# Json to DF   
import pandas as pd
import numpy
df1 = pd.DataFrame(list(mycol.find())) 
df1.head()
del df1["_id"]
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
    dbCursor.execute('CREATE DATABASE revenue_per_capita3;')
    dbCursor.close()
    print("Connection established")

 

finally:
    if(dbConnection): dbConnection.close()


# DF to Postgresql
engine = create_engine('postgresql+psycopg2://postgres:mohitjain267@localhost:5432/revenue_per_capita3')
df1.to_sql("test1", engine, if_exists='replace',index=False) #truncates the table
 

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
        database="revenue_per_capita3")
    permit_dataframe = sqlio.read_sql_query(sql, dbConnection)
    print(permit_dataframe)
except (Exception , psycopg2.Error) as dbError :
    print ("Error", dbError)
finally:
    if(dbConnection): dbConnection.close()