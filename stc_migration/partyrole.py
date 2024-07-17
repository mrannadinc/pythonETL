import pgdb
import datetime
import json
import requests
import time
import conf


con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def partyrole():
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    partyrolesuccessedrecordlist = []
    salesagentsuccessedrecordlist = []
    
    salesAgentid = conf.salesAgent
  
    cursor.execute("""select 
    prole.status,
	prole.fullname,
	prole.engagedpartyid,
	prole.siebelid,
	prole.partyroleid,
	prt.dnextpartnerid,
	prt.name,
	prole.user_role,
	prole.login,
	prole.parent_id,
	prole.userlocation,
	prole.fiscalization_user_code,
	prole.dnext_role
    from staging.partyrole prole
	left join staging.partner prt on prole.parent_id = prt.legacypartnerid
    """)
    
    for i in cursor.fetchall():
        pk = i[3]
        partyroleid = i[4]
        
        if i[8] == None or i[8] == "":
            userLogin = ""
        elif i[12] == "Technician/Installator":
            userLogin = i[8]
        else:    
            if conf.url == 'prod':
                userLogin = i[8] + "@vodafone.com"
            else:
                userLogin = i[8]
        
        if(i[5] == None):
            dnextpartnerid = ""
        else:
            dnextpartnerid = i[5]
            
        if(i[3] == None):
            engagedpartyid = ""
        else:
            engagedpartyid = i[3]
            
        if(i[11] == None) or (i[11] == ""):
            fiscalization_user_code = "-"
        else:
            fiscalization_user_code = i[11]
            
        if(i[10] == None) or (i[10] == ""):
            userlocation = "-"
        else:
            userlocation = i[10]
        
        if (i[7] == 'Vodafone Employee'):
            roletypeid = conf.vodafoneEmployee
        elif (i[7] == 'Partner Employee'):
            roletypeid = conf.partnerEmployee
            
        #CLOSED kullanıcıların tipi belli olmadığı için böyle bir mantık uyguladık
        
        elif i[7] == "Closed User":
            if i[9] == None or i[9] == "":
                roletypeid = conf.vodafoneEmployee
            else:
                roletypeid = conf.partnerEmployee
        else:
            roletypeid = conf.vodafoneEmployee
            #roletypeid = ""
        
        try:
            prole={
            "id":'E'+ pk,
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
                "externalReferenceType" : "migration"
            }]
            
            }
            if dnextpartnerid != "":
                prole['relatedParty'] = [{
                    'id':dnextpartnerid,
                    "href": "/api/partnershipManagement/v4/partnership/"+dnextpartnerid,
                    'name': i[6],
                    'role': 'Sales Partner',
                    '@referredType':'Partner'                  
                }]
            if roletypeid != "":
                prole["roleType"] ={
                    "id":roletypeid,
                    "href":"/api/partyRoleManagement/v4/roletype/"+roletypeid,
                    "name" : 'Vodafone Employee'
                    #"name": i[7]
                }
            if i[3] != None:
                prole['characteristic'].append({
                    "name":"siebelId",
                    "value":i[3],
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
                    "value":userlocation,
                    "valueType":"string"
                })
            if i[11] != None:
                prole['characteristic'].append({
                    "name":"OperatorCode",
                    "value":fiscalization_user_code,
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
                where cm.customerid = '{partyroleid}'
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
                elif(i[0] in ('PERSONAL_EMAIL','WORK_EMAIL')):
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
            print(prole)
            r = requests.post(conf.url['partyrole'], data=json.dumps(prole,indent=3,default=str), headers=headers)
            
            if r.status_code != 201:
                print("Partyrole error")
                print(r.text)
                
                cursor.execute("update staging.partyrole  set  statuscode = '{}' where partyroleid = '{}'".format(r.status_code, pk))
                con.commit()
            else:
                list_pk = 'E'+pk
                partyrolesuccessedrecordlist.append(list_pk)
                
            prole['id'] = pk
            prole["roleType"] ={
                    "id":salesAgentid,
                    "href":"/api/partyRoleManagement/v4/roletype/"+salesAgentid,
                    "name": 'Sales Agent'
                    }
                    
            r = requests.post(conf.url['partyrole'], data=json.dumps(prole,indent=3,default=str), headers=headers)
            
            if r.status_code != 201:
                print("SalesAgent error")
                print(r.text)
                
                cursor.execute("update staging.partyrole  set  statuscode = '{}' where partyroleid = '{}'".format(r.status_code, pk))
                con.commit()
            
            else:
                salesagentsuccessedrecordlist.append(pk)
                
        except Exception as e:
            print("Exception WhenCreatePartyrolePayload for: " + pk)
            print(e)
            cursor.execute("update staging.partyrole set statuscode = '425' where partyroleid ='{}'".format(pk))
            con.commit()
            pass
    
    print("PartyRole Success Records")
    print(partyrolesuccessedrecordlist)
    print("PartyRole Success Records Count")
    print(len(partyrolesuccessedrecordlist))
    
    print("SalesAgent Success Records")
    print(salesagentsuccessedrecordlist)
    print("SalesAgent Success Records Count")
    print(len(salesagentsuccessedrecordlist))

def nullCheck(val):
    if val == None:
        return ""
    else:
        return val

partyrole()

con.commit()
cursor.close() 
con.close()

