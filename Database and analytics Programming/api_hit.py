# -*- coding: utf-8 -*-
"""
Created on Tue Apr 21 01:36:47 2020

@author: jnmht
"""

import requests
response = requests.get("https://data.sfgov.org/resource/5cei-gny5.json")

print(response.status_code)
print(response.json())
vari = response.json()
len(vari)


import requests
response1 = requests.get("https://data.sfgov.org/resource/wv5m-vpq2.json")

print(response1.status_code)
print(response1.json())
vari1 = response1.json()
len(vari1)

response2 = requests.get("https://data.sfgov.org/resource/i98e-djp9.json")

print(response2.status_code)
print(response2.json())
vari2 = response2.json()
len(vari2)

import requests

# property tax
url = 'https://data.sfgov.org/resource/wv5m-vpq2.json'
final_query = url + '?closed_roll_year=2007'
response = requests.get(final_query)
#print(response.status())
print(response.json())      
len(response.json())
#permit
import datetime
date = datetime.datetime(2007,1,1)
store = ''
def apiCall(i, j):
    global store, date
    for i in range(i, j):
         date += datetime.timedelta(days=1)
         #str(date)
         chang = str(date).replace(" 00:00:00","T00:00:00.000")
         url2 = 'https://data.sfgov.org/resource/i98e-djp9.json'
         final_query1 = url2 + '?permit_creation_date=' + chang
         response = requests.get(final_query1)
         store = store +str(response.json())
    return store

s1 = apiCall(1, 101)
s2 = apiCall(101,201)
s3 = apiCall(201,301)
s4 = apiCall(301,401)
s5 = apiCall(401,501)
s6 = apiCall(501,601)
s7 = apiCall(601,701)
s8 = apiCall(701,801)
s9 = apiCall(801,901)
s10 = apiCall(901,1001)
#print(response.status())
print(store)
print(response.json())
print(chang)
      
len(response.json())






















import datetime
url2 = 'https://data.sfgov.org/resource/i98e-djp9.json'
for i in range(1,30):
    for j in range(1,12):
        final_query2 = url2 + '?permit_creation_date' + 
response1 = requests.get(final_query2)
print(response1.json())
#https://data.sfgov.org/resource/wv5m-vpq2.json?closed_roll_year=2007
# =============================================================================
# for data in response.json():
#     print(data['property_location'])
# =============================================================================


'''
for i in range(0,20):
    url = "https://data.sfgov.org/resource/wv5m-vpq2.json"
    final_query = url + '?skip=' + str(i*5)
    
    querystring = {"ReleaseCountries":"US","Providers":"AmazonPrimeVideo"}

 

    headers = {
        'x-rapidapi-host': "ivaee-internet-video-archive-entertainment-v1.p.rapidapi.com",
        'x-rapidapi-key': "3d877f5436mshcb3b5ae1703e080p11c9f7jsnc76c929010b8",
        'content-type': "application/json"
        }

 

    response = requests.request("GET",final_query, headers=headers, params=querystring)
    #print(type(response.json()))
    print(response.json())
'''