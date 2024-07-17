import pgdb
import datetime
import json
import requests
import time
import conf
from awsglue.utils import getResolvedOptions
import sys
from datetime import datetime, timedelta

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()


def partyrole(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
    
    roletypeid = conf.accountManager 
	
    cursor.execute("""SELECT 
    prole.status,
	prole.fullname,
	prole.engagedpartyid,
	prole.siebelid,
	prole.partyroleid,
	null,
	null,
	prole.user_role,
	prole.login,
	prole.parent_id,
	prole.userlocation
    FROM staging.partyrole prole 
    WHERE siebelid in(SELECT siebelid FROM staging.account_manager)
    and {}
    
    """.format(condition))
    
    for i in cursor.fetchall():
        pk = i[3]
        partyroleid = i[4]
        siebelid = i[3]
        
        if(i[3] == None):
            engagedpartyid = ""
        else:
            engagedpartyid = i[3]
            
        if(i[10] == None) or (i[10] == ""):
            userLocation = "-"
        else:
            userLocation = i[10]
            
        if i[8] == None or i[8] == "":
            userLogin = "-"
        else:
            userLogin = i[8] 
            
        #CLOSED kullanıcıların tipi belli olmadığı için böyle bir mantık uyguladık
        
        try:
            
            prole={
            "id":'Acc_'+ pk,
            "status":"Approved",
            "name":i[1],
            "characteristic":[{
                "name":"legacyUserId",
                "value":i[4],
                "valueType":"string"                
            }],
            'engagedParty':{
                "@referredType": "Individual",
                'id':engagedpartyid,
                'name':i[1],
                'href':"/api/partyManagement/v4/individual/"+engagedpartyid,
                '@baseType':'PartyRef',
                '@type' : 'PartyRef'
            },
            "contactMedium":[],
            "externalReference": [{
                "id" : pk,
                "name":"legacyUser",
                "externalReferenceType" : "migration_account_manager"
            }]
            
            }
        
            prole["roleType"] ={
                "id":roletypeid,
                "href":"/api/partyRoleManagement/v4/roletype/"+roletypeid,
                "name": "Account Manager"
            }
            
            if i[3] != None:
                prole['characteristic'].append({
                    "name":"siebelId",
                    "value":siebelid,
                    "valueType":"string"
                })
            if i[8] != None:
                prole['characteristic'].append({
                    "name":"userLogin",
                    "value":userLogin,
                    "valueType":"string"
                })
            if i[10] != None:
                prole['characteristic'].append({
                    "name":"userLocation",
                    "value":userLocation,
                    "valueType":"string"
                })
                
            
            cursor.execute(f"""
                select
                   	cm.contacttype,
                	cm.mediumtype,
                	cm.preferred,
                	cm.city,
                	cm.country,
                	cm.postcode,
                	cm.stateorprovince,
                	cm.street1,
                	cm.street2,
                	cm.emailaddress,
                	cm.phonenumber
                from staging.contactmedium cm
                where cm.contactmediumid = '{siebelid}'
            """)        
            for i in cursor.fetchall():
                
                if (i[8] == None) or (i[8] == '') :
                    street2 = "-"
                else:
                    street2 = i[8]
                    
                    
                if (i[5] == None) or  (i[5] == '') :
                    postcode = "0000"
                else:
                    postcode = i[5]
                    
                if i[2] == 'true':
                    pref = True
                else:
                    pref = False
                    
                if i[0] == "DEFAULT_ADDRESS":
                    prole['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":"DEFAULT_SERVICE_ADDRESS", # Sorulacak
                            "city": i[3],
                            "country": i[4],
                            "postCode":postcode,
                            "stateOrProvince":nullCheck(i[6]),
                            "street1":i[7],
                            "street2":street2
                        }
                    })
                elif (i[0] in ('PERSONAL_EMAIL','WORK_EMAIL')) and (i[9] != '') and (i[9] != None):
                    prole['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":i[0],            # Sorulacak
                            "emailAddress":i[9]
                        }
                    })
                elif (i[0] == 'OTHER_NUMBER') or (i[0] == 'MOBILE_NUMBER'):
                    prole['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "country":"+",      # Sorulacak
                            "contactType":i[0],            # Sorulacak
                            "phoneNumber":i[10]
                        }
                    })
            
            
            print("Partyrole text: " + pk)
            print(prole)
            
            r = requests.post(conf.url['partyrole'], data=json.dumps(prole,indent=3,default=str), headers=headers)
            print("PartyRole Status Code: ",r.status_code)
            
            if r.status_code != 201:
                print("Partyrole error")
                print(r.text)
                
                cursor.execute("update staging.account_manager  set  statuscode = '{}' where siebelid = '{}'".format(r.status_code, pk))
                con.commit()
            
        except Exception as e:
            print("Exception WhenCreatePartyrolePayload for: " + pk)
            print(e)
            cursor.execute("update staging.account_manager set statuscode = '425' where siebelid='{}'".format(pk))
            con.commit()
            pass


def nullCheck(val):
    if val == None:
        return ""
    else:
        return val

    
    

partyrole(args['condition'])



con.commit()
cursor.close() 
con.close()


