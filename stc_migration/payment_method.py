import pgdb
import datetime
import json
import requests
import time
import conf
from awsglue.utils import getResolvedOptions
import sys
import pymongo
import dns
from pymongo import MongoClient
import pandas as pd
from bson.json_util import dumps
import boto3
import io

uri = "mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority".format(conf.DB_Mongo['username'], conf.DB_Mongo['password'], conf.DB_Mongo['hostname'],  conf.DB_Mongo['database']['product'])
client = MongoClient(uri)
db = client["dpayms_db"]
collection = db.get_collection("paymentMethod")

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

# for active products
def activepaymentMethod(condition):
    successedrecordslist = []
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute("""
    select 
    p.productid,
    p.createdate,
    p.updatedate,
    p.createdby,
    p.lastmodifiedby
    from staging.product p
    inner join staging.customer c on p.customerid = c.customerid
    inner join staging.agreement a on p.agreementid = a.agreementid
    where c.mig_flag = '1' 
        and p.prod_mig_flag = '1'
        and p.status = 'active'
        and {}
        and a.agreementid in (select 
                    	a.agreementid
                    from staging.agreement a
                    	inner join staging.customer c on a.customerid = c.customerid
                    	left join staging.product p on a.agreementid = p.agreementid
                    where c.mig_flag = '1'
                        and p.prod_mig_flag = '1'
                        and cast(a.productcount as int) > 1
                        group by a.agreementid,p.legacyproductofferingid
                        having count(distinct p.status) > 1)
        """.format(condition))
    for i in cursor.fetchall():
        pk = i[0]
        
        try:
            paymentmethod = {
                "_id": "A_" + pk,
                "name":'BANKNOTE',
                "description": "migration-record",
                "ispreferred": True,
                "accessPolicyConstraint":[
                    {
                        "role":"Tenancy",
                        "referredType":"accessRole"
                }],
                "baseType": "PaymentMethod",
                "schemaLocation":"/api/paymentMethod/v4/schema/cash.json",
                "type": "Cash",
                "revision":0,
                "createdDate": stringToTimestamp(i[1]),
                "updatedDate": stringToTimestamp(i[2]),
                "createdBy": i[3],
                "updatedBy": i[4],
                "_class":"com.pia.orbitant.paymentmethod.entity.Cash"
                }
            try:
                x = collection.insert_one(paymentmethod)      
                successedrecordslist.append(pk)
            except Exception as e:
                print(paymentmethod)
                print("Exception CallWhenInsertPaymentMongoDB")
                print(e)
                
        except Exception as e:
            print("Exception WhenCreateActiveProductPaymentMethod for: " + pk)
            print(e)
            pass
    
    print("PaymentMethod(active) Success Records Count")
    print(len(successedrecordslist))
    
# for pendingActive products
def pendingActivepaymentMethod(condition):
    successedrecordslist = []
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute("""
    select 
        p.legacyproductofferingid,
        p.customerid,
        p.agreementid,
        min(p.createdate),
        min(p.updatedate),
        min(p.createdby),
        min(p.lastmodifiedby)
    from staging.product p
    inner join staging.customer c on p.customerid = c.customerid
    inner join staging.agreement a on p.agreementid = a.agreementid
        where c.mig_flag = '1'
        and p.prod_mig_flag = '1'
        and p.status = 'pendingActive'
        and {}
        group by p.legacyproductofferingid,p.customerid,p.agreementid
        """.format(condition))
    for i in cursor.fetchall():
        pk = i[0] + '_' + i[2]
        
        try:
            paymentmethod = {
                "_id": "PA_" + pk,
                "name":'BANKNOTE',
                "description": "migration-record",
                "ispreferred": True,
                "accessPolicyConstraint":[
                    {
                        "role":"Tenancy",
                        "referredType":"accessRole"
                }],
                "baseType": "PaymentMethod",
                "schemaLocation":"/api/paymentMethod/v4/schema/cash.json",
                "type": "Cash",
                "revision":0,
                "createdDate": stringToTimestamp(i[3]),
                "updatedDate": stringToTimestamp(i[4]),
                "createdBy": i[5],
                "updatedBy": i[6],
                "_class":"com.pia.orbitant.paymentmethod.entity.Cash"
                }
            try:
                x = collection.insert_one(paymentmethod)      
                successedrecordslist.append(pk)
            except Exception as e:
                print(paymentmethod)
                print("Exception CallWhenInsertPaymentMongoDB")
                print(e)
                
        except Exception as e:
            print("Exception WhenCreatependingActiveProductPaymentMethod for: " + pk)
            print(e)
            pass

    print("PaymentMethod(pendingActive) Success Records Count")
    print(len(successedrecordslist))
    print(args['condition'])

def stringToTimestamp(val):
    if((val == None) or (val == "")):
        return "1800-01-01T00:00:00Z"
    else:
        val = str(val).replace(' ','T')+'Z'
        return val    
        
activepaymentMethod(args['condition'])
pendingActivepaymentMethod(args['condition'])

con.commit()
cursor.close() 
con.close()