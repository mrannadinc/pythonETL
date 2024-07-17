import pgdb
import datetime
import json
import requests
import time
import conf
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['condition'])

con = con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def admin(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute("""
        select 
        org.companyrepresentative_identnumber, 
        i.fullname,
        org.partyid,
        ind.partyid,
        i.givenname,
        i.middlename,
        i.familyname 
        from staging.organization org
        inner join staging.individualidentification ind on org.companyrepresentative_identnumber = ind.documentnumber
        inner join staging.individual i on i.partyid = ind.partyid 
        where i.representativeflag = '1'
        and {}
        """.format(condition))
        
        
    for i in cursor.fetchall():
        pk = i[2]
        individual_id = i[3]
        try:
            admin={
                "id" : "A_" + i[2],
                "status":"Approved",
                "name": i[1] ,
                "engagedParty":{
                    "@referredType": "Individual",
                    'id':i[3],
                    'name':i[1],
                    'href':"/api/partyManagement/v4/individual/"+i[3],
                    '@baseType':'PartyRef',
                    '@type' : 'PartyRef'
                },
                "roleType":{
                    "id":conf.admin,
                    "href":"/api/partyRoleManagement/v4/roletype/"+conf.admin,
                    "name": "Admin"   
                },
                "externalReference": [{
                "id" : pk,
                "name":"legacyUser",
                "externalReferenceType" : "migration_admin"}],
                
                "characteristic":[{
                "name":"legacyUserId",
                "value":pk,
                "valueType":"string"                
                }],
                
                "contactMedium" : []
                }
            
            cursor.execute(f"""
                select
                   	cm.contacttype,
                	cm.mediumtype,
                	cm.preferred,
                	cm.emailaddress,
                	cm.phonenumber
                from staging.contactmedium cm
                where cm.partyid = '{individual_id}' and cm.contactmediumid not like '%agr.%'
            """)        
            for i in cursor.fetchall():
                if i[2] == 'true':
                    pref = True
                else:
                    pref = False
                
                if (i[0] in ('PERSONAL_EMAIL','WORK_EMAIL')) and (i[3] != '') and (i[3] != None):
                    admin['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":i[0],       
                            "emailAddress":i[3]
                        }
                    })
                elif (i[0] == 'OTHER_NUMBER') or (i[0] == 'MOBILE_NUMBER'):
                    admin['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "country": "+",      
                            "contactType":i[0],            
                            "phoneNumber":i[4]
                        }
                    })
            
            r = requests.post(conf.url['partyrole'], data=json.dumps(admin,indent=3,default=str), headers=headers)
        
        
            if r.status_code != 201:
                print('Admin Status Code : ',r.status_code)
                print(admin)
                print(r.text)
                try:
                    cursor.execute("update staging.organization set representativestatus = '{}' where partyid = '{}'".format(r.status_code,pk))
                    con.commit()
                except:
                    pass
                
            con.commit()
        except Exception as e:
            print("Exception WhenCreateAdminPayload for: " + pk)
            print(e)
            cursor.execute("update staging.organization set representativestatus = '434' where partyid = '{}'".format(pk))
            con.commit()
            pass
    print(args['condition'])

admin(args['condition'])


con.commit()
cursor.close() 
con.close()