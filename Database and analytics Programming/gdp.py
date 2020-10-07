# -*- coding: utf-8 -*-
"""
Created on Sat Apr 25 20:54:26 2020

@author: jnmht
"""

# fetch data from api
import requests
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["gdp_US"]
mycol = mydb["US_county_gdp"]
county = ['Alpine','Butte','Calaveras','Contra Costa','Del Norte','El Dorado','Glenn','Humboldt','Inyo','Kern',
          'Kings','Lake','Lassen','Marin','Mariposa','Mendocino','Nevada','Placer','Plumas','Sacramento',
          'San Bernardino','Shasta','Sierra','Siskiyou','Solano','Stanislaus','Sutter','Trinity','Tuolumne',
          'Ventura','Yolo','Yuba','Alameda','Amador','Colusa','Fresno','Imperial','Los Angeles','Madera',
          'Merced','Monterey','Napa','Orange','Riverside','Sacramento','San Bernardino','San Diego',
          'San Joaquin','San Luis Obispo','San Mateo','Santa Barbara', 'Santa Clara','Santa Cruz','Sonoma',
          'Tehama','Tulare']
type(county)
for i in range(len(county)):
         #date += datetime.timedelta(days=1)
         #str(date)
         #chang = str(date).replace(" 00:00:00","T00:00:00.000")
         url2 = 'https://data.ftb.ca.gov/resource/xf7k-ux8z.json?%24where=taxable_year > 2002'
         #print(urllib.parse.urlencode(url2))
         final_query1 = url2 + '&county=' + county[i]
         response = requests.get(final_query1)
         api_data = response.json()
         for data in api_data:
             mycol.insert_one(data)
             #print(data)

# Json to DF   
import pandas as pd
import numpy
df_gdp = pd.DataFrame(list(mycol.find())) 
df_gdp.head()
del df_gdp["_id"]
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
    dbCursor.execute('CREATE DATABASE gdp_US_county;')
    dbCursor.close()
    print("Connection established")

 

finally:
    if(dbConnection): dbConnection.close()


# DF to Postgresql
engine = create_engine('postgresql+psycopg2://postgres:mohitjain267@localhost:5432/gdp_us_county')
df_gdp.to_sql("county", engine, if_exists='replace',index=False) #truncates the table
 

# psotgresql to DF(GDP)
#import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
 
sql="""SELECT * FROM county;"""
try:
    dbConnection = psycopg2.connect(
        password = "mohitjain267",
        host = "localhost",
        port = "5432",
        user = "postgres",
        database="gdp_us_county")
    gdp_dataframe = sqlio.read_sql_query(sql, dbConnection)
    print(gdp_dataframe)
except (Exception , psycopg2.Error) as dbError :
    print ("Error", dbError)
finally:
    if(dbConnection): dbConnection.close()
###############################################################################################################
# data preprocessing
null_values = gdp_dataframe.isnull().sum() #checking for missing values
summary = gdp_dataframe.describe()
mean_gdp = gdp_dataframe.mean()
# data types
gdp_dataframe.dtypes
# change datatype 
gdp_dataframe['agic_sortid']= gdp_dataframe['agic_sortid'].astype(int)
gdp_dataframe['taxable_year']= gdp_dataframe['taxable_year'].astype(int)
#gdp_dataframe['county']= gdp_dataframe['county'].astype(string)
gdp_dataframe['all_returns']= gdp_dataframe['all_returns'].astype(float)
gdp_dataframe['all_joint_returns']= gdp_dataframe['all_joint_returns'].astype(float)
gdp_dataframe['all_taxable_returns']= gdp_dataframe['all_taxable_returns'].astype(float)
gdp_dataframe['dependents_on_returns']= gdp_dataframe['dependents_on_returns'].astype(float)
gdp_dataframe['tax_assessed']= gdp_dataframe['tax_assessed'].astype(float)
gdp_dataframe['adjusted_gross_income']= gdp_dataframe['adjusted_gross_income'].astype(float)

# median and mean
median_gdp = gdp_dataframe.median()
mean_gdp = gdp_dataframe.mean()
# replacing null with mean
gdp_dataframe["all_returns"] = gdp_dataframe["all_returns"].fillna(median_gdp[2])
gdp_dataframe["all_joint_returns"] = gdp_dataframe["all_joint_returns"].fillna(median_gdp[3])
gdp_dataframe["all_taxable_returns"] = gdp_dataframe["all_taxable_returns"].fillna(median_gdp[4])
gdp_dataframe["dependents_on_returns"] = gdp_dataframe["dependents_on_returns"].fillna(median_gdp[5])
gdp_dataframe.isnull().sum() #checking the null
# visualization
#!pip install matplotlib
import matplotlib.pyplot as plt

# graph creation
average_returns_county = gdp_dataframe[['all_returns','county']].groupby('county').mean()
average_returns_county.plot(kind='bar', title='Returns vs County')

total_returns_year = gdp_dataframe[['all_returns','taxable_year']].groupby('taxable_year').sum()
total_returns_year.plot(kind='bar', title='Returns vs year(in million($))')

total_gross_returns_county = gdp_dataframe[['adjusted_gross_income','county']].groupby('county').sum()
total_gross_returns_county.plot(kind='bar', title='Total Gross Incomes of County(in billion($))')

multi = gdp_dataframe[['all_returns','tax_assessed','county']].groupby('county').sum()
multi.plot(kind='bar', title='return and tax vs county')
