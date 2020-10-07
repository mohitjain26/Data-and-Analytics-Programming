# -*- coding: utf-8 -*-
"""
Created on Wed May  6 18:06:12 2020

@author: jnmht
"""
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


# postgresql to DF(expenditure)
#import pandas as pd
import pandas.io.sql as sqlio
import psycopg2
 
sql_exp="""SELECT * FROM county;"""
try:
    dbConnection = psycopg2.connect(
        password = "aditi123",
        host = "192.168.0.122",
        port = "5432",
        user = "postgres",
        database="expenditure_california1")
    expenditure_dataframe = sqlio.read_sql_query(sql_exp, dbConnection)
    print(expenditure_dataframe)
except (Exception , psycopg2.Error) as dbError :
    print ("Error", dbError)
finally:
    if(dbConnection): dbConnection.close()

########
    # postgrl to df(revenue)
sql_rev="""SELECT * FROM revenue_table;"""
try:
    dbConnection = psycopg2.connect(
        password = "aditi123",
        host = "192.168.0.122",
        port = "5432",
        user = "postgres",
        database="dap_cl")
    revenue_dataframe = sqlio.read_sql_query(sql_rev, dbConnection)
    print(revenue_dataframe)
except (Exception , psycopg2.Error) as dbError :
    print ("Error", dbError)
finally:
    if(dbConnection): dbConnection.close()

# expenditure
# merge our dataset
# dump df into CSV
import pandas as pd
revenue_dataframe.to_csv('revenue.csv')
expenditure_dataframe.to_csv('expenditure.csv')
gdp_dataframe.to_csv('gdp_US.csv')
revenue_dataframe.dtypes
gdp_dataframe.dtypes
expenditure_dataframe.dtypes
gdp_dataframe['taxable_year']= gdp_dataframe['taxable_year'].astype(object)
tax_yr = gdp_dataframe["taxable_year"]
tax_yr = tax_yr.astype(str)
gdp_dataframe["id"]= gdp_dataframe["county"].str.cat(tax_yr, sep ="")
expenditure_dataframe["id_exp"]= expenditure_dataframe["entity_name"].str.cat(expenditure_dataframe["fiscal_year"], sep ="")
tax_yr_rev = revenue_dataframe["fiscal_year"]
tax_yr_rev = tax_yr_rev.astype(str)
revenue_dataframe["id_rev"]= revenue_dataframe["entity_name"].str.cat(tax_yr_rev, sep ="")
df_gdp_compres = gdp_dataframe[['all_returns','adjusted_gross_income','all_taxable_returns','id']].groupby('id').sum()
df6_exp_gdp = pd.merge(expenditure_dataframe,df_gdp_compres, how='left', left_on='id_exp', right_on='id')
df6_exp_gdp_rev = pd.merge(df6_exp_gdp,revenue_dataframe, how='left', left_on='id_exp', right_on='id_rev')

# df to postgres (Merged DATA)
#from sqlalchemy import create_engine

engine = create_engine('postgresql+psycopg2://postgres:mohitjain267@localhost:5432/gdp_us_county')
df6_exp_gdp_rev.to_sql("merged_data", engine, if_exists='replace',index=False) #truncates the table

# Implementing machine learning model on merged data 

# cleaning data 
# drop unwanted column
null_values = df6_exp_gdp_rev.isnull().sum()#missing values
df6_exp_gdp_rev = df6_exp_gdp_rev.drop(columns=['entity_name_y','fiscal_year_y','total_revenues','estimated_population_y','revenues_per_capita','id_rev'])
# filling the mean in the missing values
df6_exp_gdp_rev.isnull().sum()
df6_exp_gdp_rev['all_returns'].fillna((df6_exp_gdp_rev['all_returns'].mean()), inplace=True)
df6_exp_gdp_rev['adjusted_gross_income'].fillna((df6_exp_gdp_rev['adjusted_gross_income'].mean()), inplace=True)
df6_exp_gdp_rev['all_taxable_returns'].fillna((df6_exp_gdp_rev['all_taxable_returns'].mean()), inplace=True)
df6_exp_gdp_rev.to_csv('merged.csv')
#  multiple linear regression 
#Splitting X-opt into training and Split test
import sklearn.model_selection
from sklearn.model_selection import train_test_split
use = df6_exp_gdp_rev.drop(columns=['entity_name_x','fiscal_year_x',
                                    'estimated_population_x','id_exp','all_taxable_returns'])
X = use.iloc[:, use.columns != 'expenditures_per_capita'].values
y = use.iloc[:, use.columns =='expenditures_per_capita'].values
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size = 0.2, random_state = 0)
# feature scaling
from sklearn.preprocessing import StandardScaler
sc_X = StandardScaler()
X_train = sc_X.fit_transform(X_train)
X_test = sc_X.transform(X_test)
sc_y = StandardScaler()
y_train = sc_y.fit_transform(y_train)

# Fitting Multiple Linear Regression to the Training set
from sklearn.linear_model import LinearRegression
regressor = LinearRegression()
regressor.fit(X_train, y_train)

# Predicting the Test set results
#We can compare y_pred and y_test to find its accuracy
y_pred = regressor.predict(X_test)
from sklearn.metrics import mean_squared_error
from sklearn.metrics import r2_score

mean_squared_error(y_test, y_pred)
r2_score(y_test, y_pred)
