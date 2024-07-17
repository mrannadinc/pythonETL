import pgdb
import datetime
import json
import requests
import time
import conf

con = con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

headers = {"Accept": "application/json",'Content-type':'application/json'}

roleType = list() 

roleType.append({
    "id": "5d4fba92-c5f9-4f0f-8171-b0627de6f2e0",
    "href": "/api/partyRoleManagement/v4/roleType/5d4fba92-c5f9-4f0f-8171-b0627de6f2e0",
    "name": "Admin"})
    
roleType.append({
    "id": "aba29ba0-4262-4d54-9f79-c669820ec2c5",
    "href": "/api/partyRoleManagement/v4/roleType/aba29ba0-4262-4d54-9f79-c669820ec2c5",
    "name": "Vodafone Employee"})
    
roleType.append({
    "id": "5b2ac4cb-8236-4d0b-93da-360739ff19bf",
    "href": "/api/partyRoleManagement/v4/roleType/5b2ac4cb-8236-4d0b-93da-360739ff19bf",
    "name": "Sales Agent"})

roleType.append({
    "id": "a3af3436-afa9-440a-9d78-ed725766fca7",
    "href": "/api/partyRoleManagement/v4/roleType/a3af3436-afa9-440a-9d78-ed725766fca7",
    "name": "Back Office Agent"})

roleType.append({
    "id": "921f1500-b7c0-42b8-bbe9-f53daada34c0",
    "href": "/api/partyRoleManagement/v4/roleType/921f1500-b7c0-42b8-bbe9-f53daada34c0",
    "name": "Account Manager"})
    
roleType.append({
    "id": "aa68050f-30be-44aa-82d1-846dacb325a4",
    "href": "/api/partyRoleManagement/v4/roleType/aa68050f-30be-44aa-82d1-846dacb325a4",
    "name": "Sales Partner"})

roleType.append({
    "id": "a472796c-918d-4740-a71d-f3cce38d9d67",
    "href": "/api/partyRoleManagement/v4/roleType/a472796c-918d-4740-a71d-f3cce38d9d67",
    "name": "Authorized Person"})
    
roleType.append({
    "id": "c308aef9-8f06-4096-a238-52114df5f95d",
    "href": "/api/partyRoleManagement/v4/roleType/c308aef9-8f06-4096-a238-52114df5f95d",
    "name": "Legal_Custodian"})
    
roleType.append({
    "id": "24f55564-b9a4-4790-82e4-fd2117638987",
    "href": "/api/partyRoleManagement/v4/roleType/24f55564-b9a4-4790-82e4-fd2117638987",
    "name": "Partner_Employee"})


m=0
for i in roleType:
    m = m+1
    print(m)
    print(i)
    r = requests.post("https://dpr-api.sit1.dnext.al.vodafone.com/api/partyRoleManagement/v4/roleType", data=json.dumps(i,indent=3,default=str), headers=headers)
    
    
    
    
