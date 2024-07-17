import pgdb
import datetime
import json
import requests
import time
import conf
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()


def invoiceaccount(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    postpaidsuccessedrecordlist = []
    prepaidsuccessedrecordlist = []
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute("""
    select 
    c.name,
    c.updatedate,
    c.ratingtype,
    c.status,
    c.dnextcustomerid,
    c.dnextpostpaidpaymentid,
    c.currency,
    c.engagedpartyid, 
    cur.contractcurrency,
    c.customerid
    from staging.customer c 
    left join (
    select * from ( 
      select a.customerid, a.contractcurrency,
            RANK () OVER (
                PARTITION BY a.customerid
                ORDER BY a.agreementid
            ) currency_rank
        from staging.agreement a
        ) subq
        where currency_rank = 1
    ) cur on c.customerid = cur.customerid
    left join staging.individual i on c.customerid = i.partyid
    where c.mig_flag = '1' 
    and c.individual_flag = 'individual' 
    and i.role is null
    and {}
        """.format(condition))
    
    for i in cursor.fetchall():
            pk = i[9]
            cyclespecid = "BC-01"
            if i[2] == None:
                ratingtype = ""
            else:
                ratingtype = i[2]
            if i[1] == None:
                lastm = "1800-01-01T00:00:00Z"
            else:
                lastm = i[1]
            if i[3] == None:
                status = ""
            else:
                status = i[3]
    
            if (i[8] == "EUR"):
                currency = "EUR"
            elif (i[8] == "USD"):
                currency = "USD"
            else:
                currency = "ALL"
            
            try:
                
                billing = {
                    'id': "Post_" + pk,
                    'accountType':'InvoiceAccount',
                    'name':i[0],
                    'description':"Migrated Account",
                    'ratingType':'Postpaid',
                    'relatedParty':[{
                        'id':"F" + pk,
                        'name':i[0],
                        'href' : '/api/customerManagement/v4/customer/'+"F" +pk,
                        '@referredType':'Customer'
                    }],
                    'billStructure':{
                        "cycleSpecification": {
                            "id": cyclespecid,
                            "href": "/api/accountManagement/v4/billingCycleSpecification/" + cyclespecid,
                            "isRef": True,
                            "name": cyclespecid
                        },
                        "presentationMedia" : [{
                            "href":"/api/accountManagement/v4/billPresentationMedia/4b4d7ec4-2981-42f7-9336-d4ec7c29c1f3",
                            "id":"4b4d7ec4-2981-42f7-9336-d4ec7c29c1f3",
                            "name":"Paper",
                            "isRef":True
                        }],
                        
                        "preferredBillingCurrency": currency
                    },
                    'externalReference':[{
                        'externalReferenceType' : 'migration',
                        'id':pk,
                        'name':'legacyNumber'
                    }],
                    'contact':[{
                        'contactName':i[0],
                        'contactType':'primary',
                        'partyRoleType':'Customer',
                        'validFor':{
                            'startDateTime':'1800-01-01T00:00:00Z',
                            'endDateTime': '2099-01-01T00:00:00Z'
                            },
                        'contactMedium':[]
                    }],
                    'characteristic' :[{
                        'name' : 'CreditClassId',
                        'value' : 'bab166e9-12c9-429c-a21b-79a74ac7fdb2'
                    }]
                }
                if status == 'Active':
                    billing['state'] = status
                else:
                    billing['state'] = 'Inactive'
                
                
                billing['defaultPaymentMethod'] = {
                    "id": "Post_" + pk,
                    "href": '/api/paymentMethod/v4/paymentMethod/'+"Post_" + pk,
                    'name':'Cash'  
                }
                cursor.execute("""
                select 
                mediumtype,
                preferred,
                city,
                contacttype,
                stateorprovince,
                street1,
                emailaddress,
                phonenumber,
                country,
                street2,
                dnextaddressid 
                from staging.contactmedium 
                where customerid = '{}'""".format(pk))
                for i in cursor.fetchall():
                    if i[5] == None:
                        street1 = ""
                    else:
                        street1 = i[5]
                    if i[9] == None:
                        street2 = "-"
                    else:
                        street2 = i[9]
                    if i[2] == None:
                        city = ""
                    else:
                        city = i[2]
                    if i[1] == 'true':
                        pref = True
                    else:
                        pref = False
            
                    if i[3] == ('DEFAULT_ADDRESS') and (street1 != "") and (i[10] != None):
                        billing['contact'][0]['contactMedium'].append({
                            'mediumType':i[0],
                            'preferred': pref,
                            'characteristic':{
                                "contactType":"DEFAULT_BILLING_ADDRESS",
                                'place':[{
                                    'id': i[10],
                                    'href':"/api/geographicAddressManagement/v4/geographicAddress/"+i[10],
                                    'role':"DEFAULT_BILLING_ADDRESS",
                                    'name': "DEFAULT_BILLING_ADDRESS"
                                }]
                            }
                        })
                        
                
                
                #print("Postpaid InvoiceAccount text : " + "Post_" + pk)
                #print(billing)
                r = requests.post(conf.url['account'], data=json.dumps(billing,indent=3,default=str), headers=headers)
                #print('Postpaid InvoiceAccount Status Code : ',r.status_code)
                
                if r.status_code != 201:
                    cursor.execute("update staging.customer set billingaccountstatuscode = '{}' where customerid = '{}'".format(r.status_code,pk))
                    con.commit()
                
                    print('Postpaid InvoiceAccount Postpaid Error')
                    print(r.text)
                
                else:
                    postpaidsuccessedrecordlist.append(pk)
                
                billing['id'] = "Pre_" + pk
                billing['ratingType'] = 'Prepaid'
                billing['characteristic'][0]['value'] = '5bad203d-a6b1-40d0-8ef4-1c258e429afe'
                billing['billStructure'].pop("cycleSpecification")
                del billing['defaultPaymentMethod']
                
                print("Prepaid InvoiceAccount text : " + "Pre_" + pk)
                print(billing)
                r = requests.post(conf.url['account'], data=json.dumps(billing,indent=3,default=str), headers=headers)
                print("Prepaid InvoiceAccount Status Code : ",r.status_code)
                
                if r.status_code != 201:
                    cursor.execute("update staging.customer set billingaccountstatuscode = '{}' where customerid = '{}'".format(r.status_code,pk))
                    print('Prepaid InvoiceAccount Prepaid Error')
                    print(r.text)
                
                else:
                    prepaidsuccessedrecordlist.append(pk)
            except Exception as e:
                print("Exception WhenCreateInvoiceAccountPayload for:" + pk)
                print(e)
                cursor.execute("update staging.customer set billingaccountstatuscode = '434' where customerid = '{}'".format(pk))
                con.commit()
                pass
            
    
    print("Postpaid InvoiceAccount Success Records Count")
    print(len(postpaidsuccessedrecordlist))
    print("Prepaid InvoiceAccount Success Records Count")
    print(len(prepaidsuccessedrecordlist))
    print(args['condition'])
    

invoiceaccount(args['condition'])


con.commit()
cursor.close() 
con.close()