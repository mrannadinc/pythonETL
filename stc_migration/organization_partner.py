import pgdb
import datetime
import json
import requests
import time
import conf

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def organization():
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    orgdict = {}

    cursor.execute("""
    select 
    status,
    name,
    legalstatus,
    industry,
    parentorganization,
    role,
    tradingname,
    partyid
    from staging.organization 
    where role is not null 
    """)

    for i in cursor.fetchall():
        pk = i[7]
        if i[0] != None:
            stat = i[0]
        else:
            stat = ""
        if i[1][0] == '[':
            name = i[1][1:-1]
        else:
            name = i[1]
        if i[3] != None:
            ind = i[3]
        else:
            ind = ""
        if i[4] == None:
            parentorganization = ""
        else:
            parentorganization = i[4]
        if i[5] != None:
            role = i[5]
        else:
            role = ''
        
        try:
            
            orgdict['id'] = pk
            orgdict['status'] = stat
            orgdict['name'] = name
            orgdict['tradingName']=i[6]
            orgdict['attachment'] = []
    
            orgdict['externalReference'] = [{
                'externalReferenceType':'migration_partner',
                'id': pk,
                'name': 'legacyNumber'
            }]
            orgdict['partyCharacteristic'] = [{
                'name':"IsCustomerInBlacklist",
                'value':'NO',
                'valueType':'string'
            }]
            if role != 'shop':
                orgdict['partyCharacteristic'].append({
                    'name':'BILLMEDIA',
                    'value':'PAPER',
                    'valueType':'string'      
                })
            if (i[2] != None) and (i[2]!=""):
                orgdict['partyCharacteristic'].append({
                    'name':'juridicalInfo',
                    'value':i[2],
                    'valueType':'string'                  
                })
            if (ind != None) and (ind!=""):
                orgdict['partyCharacteristic'].append({
                    'name':'INDUSTRY',
                    'value':ind,
                    'valueType':'string'                  
                }) 
                
            cursor.execute("select identificationtype,attachmentname,issuingauthority,documentnumber,dnextattachmentid from staging.organizationidentification where partyid = '{}'".format(pk))
            for i in cursor.fetchall():
                identype = i[0]
                if i[4] == None:
                    dnextid = ""
                else:
                    dnextid = i[4]
                if i[1] == None:
                    attname = ""
                else:
                    attname = i[1]
                if i[3] == None:
                    idenid = ""
                else:
                    idenid = i[3]
                orgdict['organizationIdentification'] = [{
                    "identificationId":idenid,
                    "identificationType":i[0]
                }]
                
                if dnextid != "":
                    orgdict['attachment'].append({
                            "description":i[0]+" - "+idenid,
                            "href":"/api/documentManagement/v4/attachment/"+dnextid,
                            "id":dnextid,
                            "name":attname
                        })
            print("Organization(Partner) text : " + pk)
            print(orgdict)
            
            r = requests.post(conf.url['organization'],data = json.dumps(orgdict,indent=3,default=str),headers=headers)
            
            print('Organization Status Code : ',r.status_code)
            
            if r.status_code != 201:
                cursor.execute("update staging.organization set statuscode = '{}' where partyid = '{}'".format(r.status_code,pk))
                con.commit()    
                print("Organization(Partner) Error : " + pk)
                print(r.text)        
            
            if role == "":
                try:
                    cursor.execute("update staging.customer set engagedpartyid = '{}' where customerid = '{}'".format(r.json()['id'],pk))
                except:
                    print("Organization Post Error : {}\nPK = {}".format(r.status_code,pk))
                    print(r.text)
            else:
                try:
                    cursor.execute("update staging.partner set engagedpartyid = '{}' where partnerid = '{}'".format(r.json()['id'],pk))
                except:
                    print("Organization Post Error : {}\nPK = {}".format(r.status_code,pk))
                    print(r.text)
        except Exception as e:
            print("Exception WhenCreateOrganizationPartnerPayload for: " + pk)
            print(e)
            cursor.execute("update staging.organization set statuscode = '425' where partyid='{}'".format(pk))
            con.commit()
            pass
    



organization()
con.commit()
cursor.close() 
con.close()