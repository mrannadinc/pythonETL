import pgdb
import datetime
import json
import requests
import time
import conf

con = con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

headers = {"Accept": "application/json",'Content-type':'application/json'}


ChanneList = list() 

ChanneList.append({
    "id": "b6e7416f-1c28-4fab-ad26-babd3a003a7e",
    "name": "Self - Service",
    "href": "/api/referenceManagement/v4/marketSegment/b6e7416f-1c28-4fab-ad26-babd3a003a7e"}
    )
    
ChanneList.append({
    "id": "8231b7b5-f3e0-4900-9d7d-9d8e39d9d434",
    "name": "TLM",
    "href": "/api/referenceManagement/v4/marketSegment/8231b7b5-f3e0-4900-9d7d-9d8e39d9d434"}
    )
    
ChanneList.append({
    "id": "16470582-9989-4c3a-874c-5d185ab77444",
    "name": "Retail",
    "href": "/api/referenceManagement/v4/marketSegment/16470582-9989-4c3a-874c-5d185ab77444"}
    )

ChanneList.append({
    "id": "62385928-64c9-4c9d-adeb-985e3a567099",
    "name": "D2D",
    "href": "/api/referenceManagement/v4/marketSegment/62385928-64c9-4c9d-adeb-985e3a567099"}
    )
    
ChanneList.append({
    "id": "8af7a032-c98f-4464-8f6f-a694f4b29df1",
    "name": "Business Partners",
    "href": "/api/referenceManagement/v4/marketSegment/8af7a032-c98f-4464-8f6f-a694f4b29df1"}
    )
    
ChanneList.append({
    "id": "02bfe21b-f04e-47e7-88dd-4769a9aad210",
    "name": "Business Sales Executives",
    "href": "/api/referenceManagement/v4/marketSegment/02bfe21b-f04e-47e7-88dd-4769a9aad210"}
    )

ChanneList.append({
    "id": "05e6d2f2-bac6-4d86-87a6-8459fdfa4ca1",
    "name": "Business Experts",
    "href": "/api/referenceManagement/v4/marketSegment/05e6d2f2-bac6-4d86-87a6-8459fdfa4ca1"}
    )
    
ChanneList.append({
    "id": "10cc006f-8ca8-438c-a16c-33d0f3a718c8",
    "name": "Business Advisors",
    "href": "/api/referenceManagement/v4/marketSegment/10cc006f-8ca8-438c-a16c-33d0f3a718c8"}
    )
    
ChanneList.append({
    "id": "8941ab38-364e-47b6-8d80-6e30407cb2a9",
    "name": "Digital farmers",
    "href": "/api/referenceManagement/v4/marketSegment/8941ab38-364e-47b6-8d80-6e30407cb2a9"}
    )
    
ChanneList.append({
    "id": "23abb292-6f8d-47b8-8e63-9d95933d2c2d",
    "name": "Pega",
    "href": "/api/referenceManagement/v4/marketSegment/23abb292-6f8d-47b8-8e63-9d95933d2c2d"}
    )
    
ChanneList.append({
    "id": "b52325b2-6477-4431-80f9-aebce878c423",
    "name": "Web",
    "href": "/api/referenceManagement/v4/marketSegment/b52325b2-6477-4431-80f9-aebce878c423"}
    )
    
ChanneList.append({
    "id": "4026c710-5877-434f-b7d1-432844df9f0d",
    "name": "Test",
    "href": "/api/referenceManagement/v4/channel/4026c710-5877-434f-b7d1-432844df9f0d"}
    )
    
ChanneList.append({
    "id": "e774e8ac-4c7a-4e2b-b7db-636e714972b2",
    "name": "TestSheraz",
    "href": "/api/referenceManagement/v4/channel/e774e8ac-4c7a-4e2b-b7db-636e714972b2"}
    )
    
ChanneList.append({
    "id": "48811c2f-1935-467e-a8e3-808a9b1a2ce6",
    "name": "BIR",
    "href": "/api/referenceManagement/v4/channel/48811c2f-1935-467e-a8e3-808a9b1a2ce6"}
    )
    
m=0
for i in ChanneList:
    m = m+1
    print(m)
    print(i)
    r = requests.post("https://reference-management-api.sit2.dnext.al.vodafone.com/api/referenceManagement/v4/channel", data=json.dumps(i,indent=3,default=str), headers=headers)
 
    
