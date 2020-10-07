# -*- coding: utf-8 -*-
"""
Created on Wed Apr 22 01:52:36 2020

@author: jnmht
"""
import requests
import datetime
date = datetime.datetime(2007,1,1)
store = ''
for i in range(1,1001):
         date += datetime.timedelta(days=1)
         #str(date)
         chang = str(date).replace(" 00:00:00","T00:00:00.000")
         url2 = 'https://data.sfgov.org/resource/i98e-djp9.json'
         final_query1 = url2 + '?permit_creation_date=' + chang
         response = requests.get(final_query1)
         store = store +str(response.json())
         