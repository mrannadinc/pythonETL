import pgdb
import datetime
import json
import requests
import time
import conf

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()
con2 = pgdb.connect(host=conf.DB['hostname'], user=conf.DB['username'], password=conf.DB['password'], dbname=conf.DB['database']['partner'], port=5432)
cursor2 = con2.cursor()


def partnerPost(pk):
    
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    cursor.execute("""
    select 
        p.name,
    	p.status,
    	p.dnextpartnerid,
    	p.partnerid,
    	p.partnertype,
    	p.sitecode,
    	p.legacypartnerid,
    	p.partnerid,
    	p.organization_type,
    	p.partner_channel,
    	p.shoplocation,
    	p.businessunitcode,
    	p.parentid,
        p.updatedate
    from staging.partner p
	inner join staging.organization o on o.partyid = p.partnerid
	where partnerid = '{}'
	""".format(pk))
	
    for i in cursor.fetchall():
        pk = i[7]
        legacypartnerid = i[6]
        lastUpdate_dateSource = i[13]
        
        if(i[11] == None) or (i[11] == ""):
            BusinUnitCode = "-"
        else:
            BusinUnitCode = i[11]
            
        if(i[5] == None) or (i[5] == ""):
            siteCode = "-"
        else:
            siteCode = i[5]
            
        if(i[9] == None) or (i[9] == ""):
            partnerChannel = "-"
        else:
            partnerChannel = i[9]
            
        if(i[7] == None) or (i[7] == ""):
            siebelId = "-"
        else:
            siebelId = i[7]
            
        if(i[6] == None) or (i[6] == ""):
            legacyPartnerId = "-"
        else:
            legacyPartnerId = i[6]
            
        if(i[8] == None) or (i[8] == ""):
            partnerType = "-"
        else:
            partnerType = i[8]
            
        if(i[10] == None) or (i[10] == ""):
            shopLocation = "-"
        else:
            shopLocation = i[10]
        
        try:
            
            #Organization
            org_test={
                "id":i[3],   #p.partnerid
                "status":"validated",
                "name":i[0],  #p.name
                "tradingName":i[0],
                "attachment":[],
                "externalReference": [{
                        "externalReferenceType" : "migration_partner",
                        "id" : i[3],
                        "name":"legacyNumber"
                        }],
                "partyCharacteristic":[
                        {
                            'name':"IsCustomerInBlacklist",
                            'value':'NO',
                            'valueType':'string'
                        },
                        {
                            'name':'INDUSTRY',
                            'value':'TELECOMS',
                            'valueType':'string'
                        }
                    ]
                }
            
            
            test={
                "id":i[3],
                "description":"VF-Albania-Partnership",
                "name":"VF-Albania-Partnership",
                "specification":{
                    "id":'1',
                    "href":"api/partnershipManagement/v4/partnershipSpecification/1",
                    "name":"Partner"
                },
                "partner":[
                    {
                        "id":i[3],
                        "name":i[0],
                        "status":i[1],
                        "engagedParty":{
                            "@referredType":"Organization",
                            "id":pk,
                            "href":"api/partyManagement/v4/organization/"+pk,
                            "name":i[0]
                        },
                        'characteristic':[],
                        "contactMedium":[]
                }],
                
                "externalReference": [{
                        "id" : pk,
                        "name":"legacyUser",
                        "externalReferenceType" : "migration"
                        }]
            }
            
            
            
            test['partner'][0]['characteristic'].append({
                "name":"partnerChannel",
                "value":partnerChannel,
                "valueType":"string"
                })
            
            
            test['partner'][0]['characteristic'].append({
                "name":"siteCode",
                "value":siteCode,
                "valueType":"string"
                })
            
            
            test['partner'][0]['characteristic'].append({
                "name":"siebelId",
                "value":siebelId,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"legacyPartnerId",
                "value":legacyPartnerId,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"partnerType",
                "value":partnerType,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"shopWarehouseLocation",
                "value":shopLocation,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"dealerCustomerID",
                "value":shopLocation,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"BusinUnitCode",
                "value":BusinUnitCode,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"SourceUpdateDate",
                "value":lastUpdate_dateSource,
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
                where cm.customerid = '{legacypartnerid}'
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
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":"DEFAULT_SERVICE_ADDRESS",
                            "city": i[3],
                            "country": i[4],
                            "postCode": postcode,
                            "stateOrProvince":nullCheck(i[6]),
                            "street1":i[7],
                            "street2":street2
                        }
                    })
                elif(i[0] in ('PERSONAL_EMAIL','WORK_EMAIL')):
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":i[0],          
                            "emailAddress":i[9]
                        }
                    })
                elif (i[0] == 'OTHER_NUMBER') or (i[0] == 'MOBILE_NUMBER'):
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "country":nullCheck(i[4]),     
                            "contactType":i[0],            
                            "phoneNumber":i[10]
                        }
                    })
            #
            ###################### Ekleme alanı - RelatedParty
    
            cursor.execute("""
                select 
                    p.parentid,
                    p.dnextpartnerid
                from staging.partner p
                inner join staging.organization o on o.partyid = p.partnerid
                where p.partnerid ='{}'
                """.format(pk))
            
            for i in cursor.fetchall():
                val = i[0]
                dnextid = i[1]
            
                try:
                    if val != None:
                        cursor.execute("select dnextpartnerid,name from staging.partner where partnerid = '{}'".format(val))
                        for i in cursor.fetchall():
                            test["partner"][0]["relatedParty"] = [{
                                    "id" : i[0],
                                    'href':'/api/partnershipManagement/v4/partnership/'+i[0],
                                    'name' : i[1],
                                    '@referredType':'Partner',
                                    "role": "Dealer"
                                    }]
                                    
                    try:
                        r_org = requests.post(conf.url['organization'],data = json.dumps(org_test,indent=3,default=str),headers=headers)
                        print(test)
                        r = requests.post(conf.url['partner'], data=json.dumps(test,indent=3,default=str), headers=headers)
                        
                        #####################################
                        
                        if r.status_code != 201:
                            print("Post StatusCode hatasi : " + pk)
                            print(r.text)
                            control = 0
                        else:
                            control = 1
                            
                        if r_org.status_code != 201:
                            temp = 0
                        else:
                            temp = 1
                        
                            
                    except Exception as e:
                        print("POST yaparken hata" + pk)
                        print(e)
                        control = 0
                        temp = 0
                        pass
                                    
                except Exception as e:
                    print("POST relatedParty alani eklenirken hata" + pk)
                    print(e)
                    control = 0
                    temp = 0
                    #con.commit()
                    pass
            
   
        except Exception as e:
            print("Exception WhenCreatePartnerPayload for: " + pk)
            print(e)
            control = 0
            temp = 0
            #cursor.execute("update staging.partner set statuscode = '425' where partnerid='{}'".format(pk))
            #con.commit()
            pass
                    
 
    return [control,temp]


def partnerPatch(pk):
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJfODRLWF81ck13T2E3bjZNanJzSFI2cFBJeUlKX2JXc3o1Wk8waGtBdFJZIn0.eyJleHAiOjE2ODQyNDA5ODUsImlhdCI6MTY4NDIzOTE4NSwianRpIjoiNWY0NmEwOGEtMTM2OC00ZTdlLTgwNDQtMzZiZDA0YzhmMWJkIiwiaXNzIjoiaHR0cHM6Ly9kaWFtLnVhdC5kbmV4dC5hbC52b2RhZm9uZS5jb20vYXV0aC9yZWFsbXMvZG5leHQiLCJhdWQiOlsicmVhbG0tbWFuYWdlbWVudCIsImRuZXh0LWJlLWNsaWVudCIsImJyb2tlciIsImFjY291bnQiXSwic3ViIjoiZGY3NGQ1ZTctMGMyYi00ZDJiLTk3ODktY2NlNmI2YmE1Y2I1IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiZG5leHQtY2xpZW50Iiwic2Vzc2lvbl9zdGF0ZSI6ImI0OTUxNzU0LTg5NjYtNGFjYi04N2M3LWMxMWI5ZDI1MWVkMCIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiKi8qIiwiKiJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiYmFja29mZmljZV9yb2xlIiwiY3JlYXRlX2N1c3RvbWVyIiwiZGVhbGVyc19vcmRlcnNfdHJhbnNhY3Rpb25zX3ZpZXciLCJvd25faW5zdGFsbGF0aW9uX2RldGFpbCIsImpvYi1zY2hlZHVsZXIiLCJkdW5uaW5nLWFkbWluIiwidmlld19vd25fY3VzdG9tZXIiLCJ2aWV3X29yZGVyc19yb2xlIiwidXBkYXRlX3Ryb3VibGVfdGlja2V0IiwiY2FsbF9jZW50ZXJfcm9sZSIsImFncmVlbWVudF9kZXRhaWxfdmlldyIsImR1bm5pbmdfYWRtaW4iLCJhbGxfaW5zdGFsYXRpb25fZGV0YWlsIiwicHJpY2Vfb3ZlcndyaXRlIiwic21zX3ZhbGlkYXRpb25fYnlwYXNzIiwic2NfYWRtaW5pc3RyYXRpb24iLCJjcmVhdGVfc3VibWl0X29yZGVyIiwiZWRpdF9jdXN0X2RldGFpbHMiLCJvZmZsaW5lX2FjY2VzcyIsInVzZXJzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImNyZWF0ZV9zaG9wcGluZ19jYXJ0IiwidW1hX2F1dGhvcml6YXRpb24iLCJ3YWl2aW5nX3BlbmFsdGllcyIsInNrZWRhX3JvbGUiLCJjcmVhdGVfYWdyZWVtZW50IiwiY3VzdG9tZXJfaW50ZXJhY3Rpb24iLCJkdW5uaW5nX3ZpZXdlciIsInNob3BzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwic2VlX2NyZWF0ZWRfc2hvcHBpbmdfY2FydHMiLCJkdW5uaW5nLXZpZXdlciIsIm1hbnVhbF9vcmRlcl9yb2xlIiwiZGVmYXVsdC1yb2xlcy1kbmV4dCIsImF0dGFjaG1lbnRfdmlld19yb2xlIiwic3VwZXJfcm9sZSIsImNyZWF0ZS1yZWFsbSIsImRjYXNlX2luc3RhbGxhdG9yX3JvbGUiLCJzZXJ2aWNlLWNhdGFsb2ciLCJjcmVhdGVfdHJvdWJsZV90aWNrZXRzIiwiYWxsX3RpY2tldHNfdmlldyIsImNvcHNfc2VjX2xvZyIsInZpZXdfYWxsX2N1c3RvbWVycyIsInZpZXdfaW5zdF9zdGF0dXNfZGV0YWlscyIsInNlZV9wb3N0cGFpZF9iaWxscyIsInJjX2FkbWluaXN0cmF0aW9uIiwiZGVhY3RpdmF0aW9uX3JvbGUiXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZWFsbS1tYW5hZ2VtZW50Ijp7InJvbGVzIjpbInZpZXctcmVhbG0iLCJ2aWV3LWlkZW50aXR5LXByb3ZpZGVycyIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwicmVhbG0tYWRtaW4iLCJjcmVhdGUtY2xpZW50IiwibWFuYWdlLXVzZXJzIiwicXVlcnktcmVhbG1zIiwidmlldy1hdXRob3JpemF0aW9uIiwicXVlcnktY2xpZW50cyIsInF1ZXJ5LXVzZXJzIiwibWFuYWdlLWV2ZW50cyIsIm1hbmFnZS1yZWFsbSIsInZpZXctZXZlbnRzIiwidmlldy11c2VycyIsInZpZXctY2xpZW50cyIsIm1hbmFnZS1hdXRob3JpemF0aW9uIiwibWFuYWdlLWNsaWVudHMiLCJxdWVyeS1ncm91cHMiXX0sImRuZXh0LWJlLWNsaWVudCI6eyJyb2xlcyI6WyJyZXNvdXJjZS1jYXRhbG9nIiwic2VydmljZS1vcmRlci1mdWxmaWxsbWVudCIsImpvYi1zY2hlZHVsZXIiLCJwYXJ0bmVyc2hpcC1tYW5hZ2VtZW50IiwicGFydHlSb2xlLW1hbmFnZW1lbnQiLCJkdW5uaW5nLWFkbWluIiwic2VydmljZS1vcmRlciIsImRuZXh0LWNsaWVudC1yb2xlIiwic2hvcHBpbmctY2FydCIsImN1c3RvbWVyLW1hbmFnZW1lbnQiLCJjdXN0b21lci1qb3VybmV5IiwicmVzb3VyY2UtaW52ZW50b3J5IiwicmVzb3VyY2Utb3JkZXJpbmciLCJwcm9kdWN0LW9yZGVyIiwicHJvZHVjdC1jYXRhbG9nIiwicmVzb3VyY2Utb3JkZXItZnVsZmlsbG1lbnQiLCJwYXltZW50LW1ldGhvZCIsImR1bm5pbmctdmlld2VyIiwib3JkZXItZ2VuZXJhdGlvbiIsInVtYV9wcm90ZWN0aW9uIiwicmVmZXJlbmNlLW1hbmFnZW1lbnQiLCJzdG9yYWdlLXNlcnZpY2UiLCJzZXJ2aWNlLWludmVudG9yeSIsImdlb2dyYXBoaWMtYWRkcmVzcy1tYW5hZ2VtZW50IiwicHJvZHVjdC1pbnZlbnRvcnktbWFuYWdlbWVudCIsImhyZWYtbWFwLW1hbmFnZW1lbnQiLCJiYWNrb2ZmaWNldGFzay1tYW5hZ2VtZW50IiwiYWdyZWVtZW50LW1hbmFnZW1lbnQiLCJwcm9kdWN0LW9yZGVyLWZ1bGZpbGxtZW50IiwicGFydHktbWFuYWdlbWVudCIsImFjY291bnQtbWFuYWdlbWVudCIsImRvY3VtZW50LW1hbmFnZW1lbnQiLCJwYXJ0eS1pbnRlcmFjdGlvbi1tYW5hZ2VtZW50IiwiY2FzZS1tYW5hZ2VtZW50Il19LCJkbmV4dC1jbGllbnQiOnsicm9sZXMiOlsicmVzb3VyY2UtY2F0YWxvZyIsImpvYi1zY2hlZHVsZXIiLCJwYXJ0eVJvbGUtbWFuYWdlbWVudCIsInNlcnZpY2Utb3JkZXItZnVsZmlsbG1lbnQiLCJwYXJ0bmVyc2hpcC1tYW5hZ2VtZW50IiwiZG5leHQtZGVmYXVsdC1jbGllbnQtcm9sZSIsImR1bm5pbmctYWRtaW4iLCJzZXJ2aWNlLW9yZGVyIiwiZG5leHQtY2xpZW50LXJvbGUiLCJzY19hZG1pbmlzdHJhdGlvbiIsInNob3BwaW5nLWNhcnQiLCJjdXN0b21lci1tYW5hZ2VtZW50IiwicGNfYWRtaW5pc3RyYXRpb24iLCJjdXN0b21lci1qb3VybmV5IiwicmVzb3VyY2UtaW52ZW50b3J5IiwicmVzb3VyY2Utb3JkZXJpbmciLCJza2VkYV9yb2xlIiwicHJvZHVjdC1vcmRlciIsInByb2R1Y3QtY2F0YWxvZyIsInJlc291cmNlLW9yZGVyLWZ1bGZpbGxtZW50IiwiZHVubmluZy12aWV3ZXIiLCJwYXltZW50LW1ldGhvZCIsInVtYV9wcm90ZWN0aW9uIiwib3JkZXItZ2VuZXJhdGlvbiIsInJlZmVyZW5jZS1tYW5hZ2VtZW50Iiwic2VydmljZS1pbnZlbnRvcnkiLCJzdG9yYWdlLXNlcnZpY2UiLCJnZW9ncmFwaGljLWFkZHJlc3MtbWFuYWdlbWVudCIsInByb2R1Y3QtaW52ZW50b3J5LW1hbmFnZW1lbnQiLCJocmVmLW1hcC1tYW5hZ2VtZW50IiwiYmFja29mZmljZXRhc2stbWFuYWdlbWVudCIsImFncmVlbWVudC1tYW5hZ2VtZW50IiwicHJvZHVjdC1vcmRlci1mdWxmaWxsbWVudCIsInBhcnR5LW1hbmFnZW1lbnQiLCJhY2NvdW50LW1hbmFnZW1lbnQiLCJzZXJ2aWNlLWNhdGFsb2ciLCJkb2N1bWVudC1tYW5hZ2VtZW50IiwicGFydHktaW50ZXJhY3Rpb24tbWFuYWdlbWVudCIsInJjX2FkbWluaXN0cmF0aW9uIiwiY2FzZS1tYW5hZ2VtZW50Il19LCJicm9rZXIiOnsicm9sZXMiOlsicmVhZC10b2tlbiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsInZpZXctYXBwbGljYXRpb25zIiwidmlldy1jb25zZW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJtYW5hZ2UtY29uc2VudCIsImRlbGV0ZS1hY2NvdW50Iiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwic2lkIjoiYjQ5NTE3NTQtODk2Ni00YWNiLTg3YzctYzExYjlkMjUxZWQwIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJuYW1lIjoiT3JiaSBUYW50IiwiZ3JvdXBzIjpbIkRlYWxlciBBZG1pbmlzdHJhdG9yIiwiYmFja29mZmljZV9yb2xlIiwiY3JlYXRlX2N1c3RvbWVyIiwiZGVhbGVyc19vcmRlcnNfdHJhbnNhY3Rpb25zX3ZpZXciLCJvd25faW5zdGFsbGF0aW9uX2RldGFpbCIsImpvYi1zY2hlZHVsZXIiLCJkdW5uaW5nLWFkbWluIiwidmlld19vd25fY3VzdG9tZXIiLCJ2aWV3X29yZGVyc19yb2xlIiwidXBkYXRlX3Ryb3VibGVfdGlja2V0IiwiY2FsbF9jZW50ZXJfcm9sZSIsImFncmVlbWVudF9kZXRhaWxfdmlldyIsImR1bm5pbmdfYWRtaW4iLCJhbGxfaW5zdGFsYXRpb25fZGV0YWlsIiwicHJpY2Vfb3ZlcndyaXRlIiwic21zX3ZhbGlkYXRpb25fYnlwYXNzIiwic2NfYWRtaW5pc3RyYXRpb24iLCJjcmVhdGVfc3VibWl0X29yZGVyIiwiZWRpdF9jdXN0X2RldGFpbHMiLCJvZmZsaW5lX2FjY2VzcyIsInVzZXJzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImNyZWF0ZV9zaG9wcGluZ19jYXJ0IiwidW1hX2F1dGhvcml6YXRpb24iLCJ3YWl2aW5nX3BlbmFsdGllcyIsInNrZWRhX3JvbGUiLCJjcmVhdGVfYWdyZWVtZW50IiwiY3VzdG9tZXJfaW50ZXJhY3Rpb24iLCJkdW5uaW5nX3ZpZXdlciIsInNob3BzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwic2VlX2NyZWF0ZWRfc2hvcHBpbmdfY2FydHMiLCJkdW5uaW5nLXZpZXdlciIsIm1hbnVhbF9vcmRlcl9yb2xlIiwiZGVmYXVsdC1yb2xlcy1kbmV4dCIsImF0dGFjaG1lbnRfdmlld19yb2xlIiwic3VwZXJfcm9sZSIsImNyZWF0ZS1yZWFsbSIsImRjYXNlX2luc3RhbGxhdG9yX3JvbGUiLCJzZXJ2aWNlLWNhdGFsb2ciLCJjcmVhdGVfdHJvdWJsZV90aWNrZXRzIiwiYWxsX3RpY2tldHNfdmlldyIsImNvcHNfc2VjX2xvZyIsInZpZXdfYWxsX2N1c3RvbWVycyIsInZpZXdfaW5zdF9zdGF0dXNfZGV0YWlscyIsInNlZV9wb3N0cGFpZF9iaWxscyIsInJjX2FkbWluaXN0cmF0aW9uIiwiZGVhY3RpdmF0aW9uX3JvbGUiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoib3JiaXRhbnQiLCJnaXZlbl9uYW1lIjoiT3JiaSIsImZhbWlseV9uYW1lIjoiVGFudCIsImVtYWlsIjoib3JiaXRhbnRAZ28uY29tIn0.ELVYVgN-w4Zdevt1vRUfP25KCoNTHcqOJigTe_J4if3_uTHzwr7riLdzkS_EkWR9C1OzXM637ZVmOA-RuR4988zaADmBAmNXTIKADEqzqTBmhYJZW28v0iYaGMVff-6Qtnq5-HMw8u__75hI4vJBX3M3Jh97V2lgg67i1rytnJoZFOIIjC1ilmPXwChXknh7YvqPI2xXrfZ_O91f2N5fu2j-vuFHa-jxRM5AaxH6cHqfPdhCIQVW5xSlEgdS6GAS0V8Dma2hGzYaO9thm7a_iHw8Sce7-z-o6466-vxkmREchs7cAb1I5r6uDftN6dixcgu-rAm64HwsBEJxxno0Zg"
    headers ={'Accept':"application/json",'Content-type':'application/merge-patch+json','Authorization': "Bearer"+ " " +token}
    
    cursor.execute("""
    select 
        p.name,
    	p.status,
    	p.dnextpartnerid,
    	p.partnerid,
    	p.partnertype,
    	p.sitecode,
    	p.legacypartnerid,
    	p.partnerid,
    	p.organization_type,
    	p.partner_channel,
    	p.shoplocation,
    	p.businessunitcode,
    	p.parentid,
        p.updatedate
    from staging.partner p
	inner join staging.organization o on o.partyid = p.partnerid
	where partnerid = '{}'
	""".format(pk))
	
    for i in cursor.fetchall():
        pk = i[7]
        legacypartnerid = i[6]
        lastUpdate_dateSource = i[13]
        
        if(i[11] == None) or (i[11] == ""):
            BusinUnitCode = "-"
        else:
            BusinUnitCode = i[11]
            
        if(i[5] == None) or (i[5] == ""):
            siteCode = "-"
        else:
            siteCode = i[5]
            
        if(i[9] == None) or (i[9] == ""):
            partnerChannel = "-"
        else:
            partnerChannel = i[9]
            
        if(i[7] == None) or (i[7] == ""):
            siebelId = "-"
        else:
            siebelId = i[7]
            
        if(i[6] == None) or (i[6] == ""):
            legacyPartnerId = "-"
        else:
            legacyPartnerId = i[6]
            
        if(i[8] == None) or (i[8] == ""):
            partnerType = "-"
        else:
            partnerType = i[8]
            
        if(i[10] == None) or (i[10] == ""):
            shopLocation = "-"
        else:
            shopLocation = i[10]
        
        try:
            
            #Organization
            org_test={
                "status":"validated",
                "name":i[0],
                "tradingName":i[0],
                "attachment":[],
                "externalReference": [{
                        "externalReferenceType" : "migration",
                        "id" : i[3],
                        "name":"legacyUser"
                        }],
                "partyCharacteristic":[
                        {
                            'name':"IsCustomerInBlacklist",
                            'value':'NO',
                            'valueType':'string'
                        },
                        {
                            'name':'INDUSTRY',
                            'value':'TELECOMS',
                            'valueType':'string'
                        }
                    ]
                }
            
            
            
            test={
                "description":"VF-Albania-Partnership",
                "name":"VF-Albania-Partnership",
                "specification":{
                    "id":'1',
                    "href":"api/partnershipManagement/v4/partnershipSpecification/1",
                    "name":"Partner"
                },
                "partner":[
                    {
                        "id":i[3],
                        "name":i[0],
                        "status":i[1],
                        "engagedParty":{
                            "@referredType":"Organization",
                            "id":pk,
                            "href":"api/partyManagement/v4/organization/"+pk,
                            "name":i[0]
                        },
                        'characteristic':[],
                        "contactMedium":[]
                }],
                
                "externalReference": [{
                        "id" : pk,
                        "name":"legacyUser",
                        "externalReferenceType" : "migration"
                        }]
            }
            
            
            
            test['partner'][0]['characteristic'].append({
                "name":"partnerChannel",
                "value":partnerChannel,
                "valueType":"string"
                })
            
            
            test['partner'][0]['characteristic'].append({
                "name":"siteCode",
                "value":siteCode,
                "valueType":"string"
                })
            
            
            test['partner'][0]['characteristic'].append({
                "name":"siebelId",
                "value":siebelId,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"legacyPartnerId",
                "value":legacyPartnerId,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"partnerType",
                "value":partnerType,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"shopWarehouseLocation",
                "value":shopLocation,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"dealerCustomerID",
                "value":shopLocation,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"BusinUnitCode",
                "value":BusinUnitCode,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"SourceUpdateDate",
                "value":lastUpdate_dateSource,
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
                where cm.customerid = '{legacypartnerid}'
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
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":"DEFAULT_SERVICE_ADDRESS", 
                            "city": i[3],
                            "country": i[4],
                            "postCode": postcode,
                            "stateOrProvince":nullCheck(i[6]),
                            "street1":i[7],
                            "street2":street2
                        }
                    })
                elif(i[0] in ('PERSONAL_EMAIL','WORK_EMAIL')):
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":i[0],          
                            "emailAddress":i[9]
                        }
                    })
                elif (i[0] == 'OTHER_NUMBER') or (i[0] == 'MOBILE_NUMBER'):
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "country":nullCheck(i[4]),      
                            "contactType":i[0],            
                            "phoneNumber":i[10]
                        }
                    })
            #
            ###################### Ekleme alanı - RelatedParty
    
            cursor.execute("""
                select 
                    p.parentid,
                    p.dnextpartnerid
                from staging.partner p
                inner join staging.organization o on o.partyid = p.partnerid
                where p.partnerid ='{}'
                """.format(pk))
            
            for i in cursor.fetchall():
                val = i[0]
                dnextid = i[1]
            
                try:
                    if val != None:
                        cursor.execute("select dnextpartnerid,name from staging.partner where partnerid = '{}'".format(val))
                        for i in cursor.fetchall():
                            test["partner"][0]["relatedParty"] = [{
                                    "id" : i[0],
                                    'href':'/api/partnershipManagement/v4/partnership/'+i[0],
                                    'name' : i[1],
                                    '@referredType':'Partner',
                                    "role": "Dealer"
                                    }]
                        
                    try:
                        print(test)
                        r = requests.patch(conf.url['partner']+'/'+pk, data=json.dumps(test,indent=3,default=str), headers=headers)
                        r_org = requests.patch(conf.url['organization']+'/'+pk, data = json.dumps(org_test,indent=3,default=str),headers=headers)
                        
                        #####################################
                    
                        if r.status_code != 200:
                            print("Patch StatusCode hatasi : " + pk)
                            print(r.text)
                            control = 0
                        else:
                            control = 1
                            
                        if r_org.status_code != 200:
                            temp = 0
                        else:
                            temp = 1
                            
                    except Exception as e:
                        print("PATCH yaparken hata" + pk)
                        print(e)
                        control = 0
                        temp = 0
                        pass
                                    
                except Exception as e:
                    print("PATCH relatedParty alani eklenirken hata" + pk)
                    print(e)
                    control = 0
                    temp = 0
                    #con.commit()
                    pass

            
        except Exception as e:
            print("Exception WhenCreatePartnerPayload for: " + pk)
            print(e)
            control = 0
            temp = 0
            #cursor.execute("update staging.partner set statuscode = '425' where partnerid='{}'".format(pk))
            #con.commit()
            pass
                       
    
    return [control,temp]


def nullCheck(val):
    if val == None:
        return ""
    else:
        return val


def dateCompare():
    partnerList = list()
    idList = list() #Post edilecek liste
    
    successedPartnerpatchlist = []
    unsuccessedPartnerpatchlist = []
    
    successedPartnerpostlist = []
    unsuccessedPartnerpostlist = []
    
    successedOrgpatchlist = []
    unsuccessedOrgpatchlist = []
    
    successedOrgpostlist = []
    unsuccessedOrgpostlist = []
    
    # where partnerid = 'VF-806QK3C' or partnerid = 'VF-99THBZL'    
    cursor.execute("select dnextpartnerid, updatedate, partnerid from staging.partner")
    
    for i in cursor.fetchall():
        partnerList.append(i)
        idList.append(i[2])
        
    partnershipList = list()
    #cursor2.execute("select id, updated_date from public.partnership") partnership tablosundaki updated_date alanı sisteme kayıt atıldığı tarihi verdiği için bir sonraki gün sisteme eklenen csv dosyasındaki last_update alanı ile aynı olma durumunda değişiklik yokmuş gibi algılıyor bundan dolayı characteristic tablosundaki SourceUpdateDate 'i kullanıyoruz
    cursor2.execute("""
                    select partnership.id, characteristic.value ,partnership.updated_date  
                    from public.partnership  partnership
                    left join public.partner  partner
                    on partner.partnership_id = partnership.id
                    left join public.characteristic   characteristic
                    on characteristic.partner_id = partner.entity_id
                    and  characteristic.name = 'SourceUpdateDate'
    """)
    for i in cursor2.fetchall():
        partnershipList.append(i)
    
    for i in partnerList:
        for j in partnershipList:
            if i[0] == j[0]:
                idList.remove(i[2])     #patch için eşleşmiş olan kayıtlar silinerek yalnızca postta kullanılacak kayıtlar listede tutuluyor.
                
                patch_source_update_control = False
                
                if j[1] == None or j[1] =="":
                   patch_source_update_control = True
                elif time.mktime(datetime.datetime.strptime(i[1],"%d-%b-%y").timetuple()) > time.mktime(datetime.datetime.strptime(j[1],"%d-%b-%y").timetuple()): 
                   patch_source_update_control = True
                
                if patch_source_update_control ==True:  
                    time.sleep(1)
                    control_patch = partnerPatch(i[2])  #partnerid gönderiliyor.
                    if control_patch[0] == 1:
                        successedPartnerpatchlist.append(i[2])
                    else:
                        unsuccessedPartnerpatchlist.append(i[2])
                        
                    if control_patch[0] == 1:
                        successedOrgpatchlist.append(i[2])
                    else:
                        unsuccessedOrgpatchlist.append(i[2])
    
    
    for i in idList:
        control_post = partnerPost(i)  #partnerid gönderiliyor.
        if control_post[0] == 1:
            successedPartnerpostlist.append(i)
        else:
            unsuccessedPartnerpostlist.append(i)
        
        if control_post[1] == 1:
            successedOrgpostlist.append(i)
        else:
            unsuccessedOrgpostlist.append(i)
    
            
    print("Basarili partner patch id = {}, ".format(successedPartnerpatchlist))
    print("Basarili organization patch id = {}, ".format(successedOrgpatchlist))
    
    print("Basarili partner post id = {}, ".format(successedPartnerpostlist))
    print("Basarili organization post id = {}, ".format(successedOrgpostlist))
    print("*********************************************************************")        
    
    print("Basarisiz partner patch id = {}, ".format(unsuccessedPartnerpatchlist))
    print("Basarisiz organization patch id = {}, ".format(unsuccessedOrgpatchlist))
    
    print("Basarisiz partner post id = {}, ".format(unsuccessedPartnerpostlist))
    print("Basarisiz organization post id = {}, ".format(unsuccessedOrgpostlist))
    print("*********************************************************************")
    
    
    ##############################
           
    print("Basarili partner patch sayisi = {}, ".format(len(successedPartnerpatchlist)))
    print("Basarili organization patch sayisi = {}, ".format(len(successedOrgpatchlist)))
    print("Basarili partner post sayisi = {}, ".format(len(successedPartnerpostlist)))
    print("Basarili organization post sayisi = {}, ".format(len(successedOrgpostlist)))
    
    print("*********************************************************************")
    
    print("Basarisiz partner patch sayisi = {}, ".format(len(unsuccessedPartnerpatchlist)))
    print("Basarisiz organization patch sayisi = {}, ".format(len(unsuccessedOrgpatchlist)))
    print("Basarisiz partner post sayisi = {}, ".format(len(unsuccessedPartnerpostlist)))
    print("Basarisiz organization post sayisi = {}, ".format(len(unsuccessedOrgpostlist)))
    
    
    con.commit()
    cursor.close()  
    con.close()
    
    con2.commit()
    cursor2.close()  
    con2.close()
    
dateCompare()