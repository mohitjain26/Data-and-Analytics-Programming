
import requests
import datetime
date1 = datetime.datetime(2007,1,1)
response = []
listRes = {}
for i in range(1,1001):
     temp = []
     date1 += datetime.timedelta(days=1)
     #str(date)
     chan = str(date1).replace(" 00:00:00","T00:00:00.000")
     url3 = 'https://data.sfgov.org/resource/5cei-gny5.json'
     final_query2 = url3 + '?file_date=' + chan
    # print(final_query2)
     temp = requests.get(final_query2)
     response += temp.json()
     #listRes += response[0]
     #print()
  #   store1 = store1 +str(response.json())

f= open("eviction.txt","w+")
f.write(str(response))
f.close()         
print(response)
len(response)