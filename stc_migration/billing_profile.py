import pgdb
import datetime
import json
import requests
import time
import conf
import uuid
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def billingprofile(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    successedrecordlist = []
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
    
    cursor.execute("""
    select 
    c.name,
    a.updatedate,
    a.ratingtype,
    c.status,
    c.customerid,
    a.agreementid,
    a.subscriberaccount,
    case when (a.productcount is null 
              and a.brm_orderdate is not null 
              and a.status = 'Active' 
              and a.type = 'Cash') then 'terminated' else a.status end agreement_status,
    c.dnextpostpaidaccountid,
    c.dnextprepaidaccountid,
    a.startdatetime,
    a.address,
    a.citycode,
    a.contactmediumid,
    case when a.cyclespecificationid is null then 'BC-01' else a.cyclespecificationid end cyclespecificationid,
    a.cyclespecificationname,
    case when a.dateshift is null then '01' else a.dateshift end dateshift,
    a.createdbysiebelid,
    a.createdbyname,
    a.lastmodifiedbyname,
    a.lastmodifiedbysiebelid,
    a.source
        from staging.customer c 
        inner join staging.agreement a on c.customerid = a.customerid 
        where c.mig_flag = '1'
        and (
            (a.productcount is not null) or 
            (a.productcount is null and a.brm_orderdate is not null) or 
            (a.type = 'Commitment' and a.status = 'Active')
            )
        and {}
    """.format(condition))
    for i in cursor.fetchall():
        pk = i[5]
        contactmid = i[13]
        if i[2] == 'Postpaid':
            cyclespecid = 'BC-01'
            billingdateshift = '01'
        else:
            cyclespecid = i[16]
            billingdateshift = i[18]
        if i[1] == None:
            lastm = "1800-01-01T00:00:00Z"
        else:
            lastm = i[1]
        if i[3] == None:
            status = ""
        else:
            status = i[3]
        
        if (i[2] == "Postpaid") and (i[8] != None):
            rating = i[8]
        elif (i[2] == "Prepaid") and (i[9] != None):
            rating = i[9]
        else:
            rating = "bug"

        if i[10] == None:
            startdate = '1800-01-19T13:08:13.689Z'
        else:
            startdate = str(i[10]).replace(' ','T')+'Z'
        if i[11] == None:
            street1 = ""
        else:
            street1 = i[11]
        if i[12] == None:
            city =""
        else:
            city = i[12]
        
        if i[7] == 'Active':
            state = 'Active'
        elif i[7] == 'terminated':
            state = 'Terminated'
        elif i[7] == 'in process':
            state = 'created'
            
        billing = {
            "id" : pk,
            'accountType':'BillingProfile',
            "description":"Migrated Account",
            "ratingType":i[2],
            "state":state,
            'name':i[0],
            'relatedParty':[{
                'id': "F" + i[4],
                'name':i[0],
                'href' : '/api/customerManagement/v4/customer/'+ "F" +i[4],
                '@referredType':'Customer',
                'role':'Customer'
            }],
            "accountRelationship":[{
                "account":{
                    "id" : rating,
                    "href":"/api/accountManagement/v4/billingAccount/"+rating,
                    "name":i[0],
                    "description": "Migration Account",
                    "@referredType":"Account"
                },
                "relationshipType":"InvoiceAccount",
                "validFor":{
                    "startDateTime":startdate
                }
            }],
            "characteristic": [
            {
                "name": "BRM_telco_ID",
                "value": str(uuid.uuid4())
            },
            {
                "name": "BRM_telephony_ID",
                "value": str(uuid.uuid4())
            }
            ],
            'externalReference':[{
                'externalReferenceType' : 'migration',
                'id':pk,
                'name':'legacyNumber'
            }],
            'billStructure':{
                    "cycleSpecification": {
                        "id": cyclespecid,
                        "href": "/api/accountManagement/v4/billingCycleSpecification/" + cyclespecid,
                        "isRef": True,
                        "name": cyclespecid                            
                    },
                    "presentationMedia" : [{
                        "href":"/api/accountManagement/v4/billPresentationMedia/8080",
                        "id":"8080",
                        "isRef":False,
                        "name":"PAPER"
                    }]
            },
            "contact":[{
                "contactName":"SERVICE_ADDRESS",
                "contactType":"SERVICE_ADDRESS",
                "validFor":{
                    "startDateTime":'1800-01-01T00:00:00Z',
                    'endDateTime': '2101-01-01T00:00:00Z'
                },
                'contactMedium':[]
            }]
        }
        if (i[21] == "PORTA_VirtualAgreement") or (i[21] == "VirtualAgreement") or (i[21] == "STB_VirtualAgreement"):
            customerid = i[4]
            try:
                cursor.execute(f"""select mediumtype,preferred,city,contacttype,stateorprovince,street1,emailaddress,phonenumber,country,street2,dnextaddressid 
                                    from staging.contactmedium where mediumtype = 'POSTAL_ADDRESS' and contacttype = 'DEFAULT_ADDRESS' and customerid = '{customerid}' """)
                for i in cursor.fetchall():
                    if i[1] == 'false':
                        pref = False
                    else:
                        pref = True
                        
                    billing['contact'][0]['contactMedium'].append({
                        'mediumType':"POSTAL_ADDRESS",
                        'preferred': pref,
                        'characteristic':{
                            'contactType':"SERVICE_ADDRESS",
                            'place':[{
                                'id': i[10],
                                'href':"/api/geographicAddressManagement/v4/geographicAddress/"+i[10],
                                'role':"SERVICE_ADDRESS",
                                'name': "SERVICE_ADDRESS"
                            }]
                        }
                    })
                    
            except:
                pass
        
        try:
            cursor.execute("""select mediumtype,preferred,city,contacttype,stateorprovince,street1,emailaddress,phonenumber,country,street2,dnextaddressid 
                            from staging.contactmedium where contactmediumid = '{}'""".format(contactmid))
            
            for i in cursor.fetchall():
                if i[5] == None:
                    street1 = ""
                else:
                    street1 = i[5]
                if i[9] == None:
                    street2 = ""
                else:
                    street2 = i[9]
                if i[2] == None:
                    city = ""
                else:
                    city = i[2]
                if i[1] == 'false':
                    pref = False
                else:
                    pref = True
                if i[4] == None:
                    stateorp = ""
                else:
                    stateorp = i[4]
                if i[3] == ('SERVICE_ADDRESS') and (street1 != "") and (i[10] == None):
                    billing['contact'][0]['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred': pref,
                        'characteristic':{
                            'city':city,
                            'contactType':"SERVICE_ADDRESS",
                            'stateOrProvince':stateorp,
                            'street1':street1,
                            'street2':street2,
                            'country':i[8]
                        }
                    })
                elif i[3] == ('SERVICE_ADDRESS') and (street1 != "") and (i[10] != None):
                    billing['contact'][0]['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred': pref,
                        'characteristic':{
                            'contactType':"SERVICE_ADDRESS",
                            'place':[{
                                'id': i[10],
                                'href':"/api/geographicAddressManagement/v4/geographicAddress/"+i[10],
                                'role':"SERVICE_ADDRESS",
                                'name': "SERVICE_ADDRESS"
                            }]
                        }
                    })
                    
                elif i[3] in ('PERSONAL_EMAIL','WORK_EMAIL','EMAIL'):
                    billing['contact'][0]['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            'emailAddress': i[6],
                            'contactType':i[3]
                        }
                    })
                    
                elif (i[3] == 'OTHER_NUMBER') or (i[3] == 'MOBILE_NUMBER') :
                    billing['contact'][0]['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            'phoneNumber': i[7],
                            'contactType':i[3]
                        }
                    })
                
        except:
            pass
        
        
        '''
        if len(billing['contact'][0]['contactMedium']) == 0:
            billing['contact'].pop()
        '''
        
        r = requests.post(conf.url['account'], data=json.dumps(billing,indent=3,default=str), headers=headers)
        
        if r.status_code != 201:
            print('Billing Account Payload Text : ' + pk)
            print(billing)
            print('Billing Account Status Code : ', r.status_code)
            cursor.execute("update staging.agreement set billingprofile_statuscode = '{}' where agreementid = '{}'".format(r.status_code,pk))
            con.commit()
            print("Billing Account Error : " + pk)
            print(r.text)
        else:
            successedrecordlist.append(pk)
            
    
    
    print("BillingAccount Success Records")
    print(successedrecordlist)        
    print("BillingAccount Success Records Count")
    print(len(successedrecordlist))
    print(args['condition'])


billingprofile(args['condition'])

con.commit()
cursor.close() 
con.close()