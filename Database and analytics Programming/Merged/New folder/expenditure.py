#extracting data from API
import requests
import pymongo
myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["expenditure_california1"]
mycol = mydb["county"]
url2 = 'https://bythenumbers.sco.ca.gov/resource/miui-wb29.json'
#final_query1 = url2 + '?permit_creation_date=' + chang
response = requests.get(url2)
api_data = response.json()
for data in api_data:
    mycol.insert_one(data)
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
        password = "aditi123",
        host = "localhost",
        port = "5432",
        user = "postgres")
    
except (Exception , psycopg2.Error) as dbError :
    print("Error while connecting to PostgreSQL", dbError)

 

else:
    dbConnection.set_isolation_level(0)
    dbCursor = dbConnection.cursor()
    try:
        dbCursor.execute('CREATE DATABASE expenditure_california1;')
    except Exception as e:
        print(e)
    dbCursor.close()
    print("Connection established")

 

finally:
    if(dbConnection): dbConnection.close()

#test = pd.DataFrame({'numbers': [1, 2, 3], 'colors': ['red', 'white', 'blue']})

# DF to Postgresql
engine = create_engine('postgresql+psycopg2://postgres:aditi123@localhost:5432/expenditure_california1')

#drop _id column
df = df.drop(columns = ['_id'])

df.to_sql("county", engine, if_exists='replace',index=False) #truncates the table
print(df)
#df.to_csv(r'C:\Users\jnmht\Desktop\Data Analytics\DAP\Project\permit.csv', index = False)
# psotgresql to DF
#import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
 


sql="""SELECT * FROM county;"""

try:
    dbConnection = psycopg2.connect(
        password = "aditi123",
        host = "localhost",
        port = "5432",
        user = "postgres",
        database="expenditure_california1")
    expenditure_dataframe = sqlio.read_sql_query(sql, dbConnection)
    print(expenditure_dataframe)
except (Exception , psycopg2.Error) as dbError :
    print ("Error", dbError)
finally:
    if(dbConnection): dbConnection.close()
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
sns.set()
plt.rcParams['figure.figsize'] =(10,8)
expenditure_dataframe['expenditures_per_capita'] = expenditure_dataframe['expenditures_per_capita'].astype(int)
expenditure_dataframe['estimated_population'] = expenditure_dataframe['estimated_population'].astype(int)
sns.distplot(expenditure_dataframe['estimated_population'])
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
sns.set()

plt.rcParams['figure.figsize'] =(10,8)
expenditure_dataframe['expenditures_per_capita'] = expenditure_dataframe['expenditures_per_capita'].astype(int)
sns.distplot(expenditure_dataframe['expenditures_per_capita'])
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
sns.set()

plt.rcParams['figure.figsize'] =(10,8)
expenditure_dataframe['total_expenditures'] = expenditure_dataframe['total_expenditures'].astype(float)

sns.distplot(expenditure_dataframe['total_expenditures'])
import matplotlib.pyplot as plt
import pandas as pd
# a simple line plot
expenditure_dataframe.expenditures_per_capita=pd.to_numeric(expenditure_dataframe.expenditures_per_capita)
expenditure_dataframe.fiscal_year=pd.to_numeric(expenditure_dataframe.fiscal_year)

groups = expenditure_dataframe.groupby("entity_name")

groups.plot(kind='scatter',x='fiscal_year',y='expenditures_per_capita', c = 'red')
import matplotlib.pyplot as plt
import pandas as pd
#expenditure_dataframe=expenditure_dataframe.astype(float)
# a simple line plot
expenditure_dataframe.total_expenditures=pd.to_numeric(expenditure_dataframe.total_expenditures)
expenditure_dataframe.fiscal_year=pd.to_numeric(expenditure_dataframe.fiscal_year)
expenditure_dataframe.plot(kind='scatter',x='fiscal_year',y='total_expenditures')
import matplotlib.pyplot as plt
import pandas as pd
#expenditure_dataframe=expenditure_dataframe.astype(float)
# a simple line plot
expenditure_dataframe.expenditures_per_capita=pd.to_numeric(expenditure_dataframe.expenditures_per_capita)
expenditure_dataframe.fiscal_year=pd.to_numeric(expenditure_dataframe.fiscal_year)
expenditure_dataframe.plot(kind='scatter',x='fiscal_year',y='expenditures_per_capita')
import matplotlib.pyplot as plt
import pandas as pd
#expenditure_dataframe=expenditure_dataframe.astype(float)
# a simple line plot
expenditure_dataframe.estimated_population=pd.to_numeric(expenditure_dataframe.estimated_population)
expenditure_dataframe.fiscal_year=pd.to_numeric(expenditure_dataframe.fiscal_year)
#expenditure_dataframe.plot(kind='scatter',x='expenditures_per_capita',y='fiscal_year')

groups = expenditure_dataframe.groupby("entity_name")

groups.plot(kind='scatter',x='fiscal_year',y='estimated_population', c = 'red')
