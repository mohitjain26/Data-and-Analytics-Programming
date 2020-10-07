# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 09:39:26 2020

@author: jnmht
"""


# property tax
import requests
url = 'https://data.sfgov.org/resource/wv5m-vpq2.json'
final_query = url + '?closed_roll_year=2007'
response = requests.get(final_query)
#print(response.status())
#print(response.json())   

f= open("property tax.txt","w+")
f.write(str(response.json()))
f.close()         
   
len(response.json())