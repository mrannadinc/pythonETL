import pgdb
import datetime
import json
import requests
import time
import conf


con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

headers = {"Accept": "application/json",'Content-type':'application/json'}

configbillPresentationMedia = list()

configbillPresentationMedia.append({
    "id": "4b4d7ec4-2981-42f7-9336-d4ec7c29c1f3",
    "description": "This bill presentation media Paper",
    "name": "Paper",
    "@type": "MediaType"
    })
    
configbillPresentationMedia.append({
   "id": "f6602d3d-7f18-4572-90dd-3e239f5a3e57",
   "description": "This bill presentation media Electronic",
   "name": "Electronic",
   "@type": "MediaType"
   })
    
configbillPresentationMedia.append({
    "id": "5c46bde5-18b7-4a5a-bf07-7f0984028970",
    "description": "This bill presentation media E-Bill",
    "name": "E-Bill",
    "@type": "MediaType"
    })
    
b=0
for i in configbillPresentationMedia:
    b = b+1
    print(b)
    print(i)
    r = requests.post("https://dacm-api.sit1.dnext.al.vodafone.com/api/accountManagement/v4/billPresentationMedia", data=json.dumps(i,indent=3,default=str), headers=headers)