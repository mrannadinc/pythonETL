import pgdb
import datetime
import json
import requests
import time
import conf


con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

headers = {"Accept": "application/json",'Content-type':'application/json'}

configPaymentMethod = list()

configPaymentMethod.append({
    "id": "63587117-7800-48e1-94ed-404cf55daba6",
    "name": "BANKNOTE",
    "description": "Payment Method LoV",
    "@type": "Cash"
    })

configPaymentMethod.append({
    "id": "ffd728fc-b94b-4202-ba1a-faff016440c8",
    "name": "CARD",
    "description": "Payment Method LoV",
    "@type": "DigitalWallet"
    })

configPaymentMethod.append({
    "id": "e98feea8-d552-4762-a81b-c4626dd435e2",
    "name": "OTHER",
    "description": "Payment Method LoV",
    "@type": "Other"
    })
    
p=0
for i in configPaymentMethod:
    p = p+1
    print(p)
    print(i)
    r = requests.post(conf.url['paymentmethod'], data=json.dumps(i,indent=3,default=str), headers=headers)

