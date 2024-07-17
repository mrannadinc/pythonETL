import pgdb
import datetime
import json
import requests
import time
import conf

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()
con2 = pgdb.connect(host=conf.DB['hostname'], user=conf.DB['username'], password=conf.DB['password'], dbname=conf.DB['database']['partyrole'], port=5432)
cursor2 = con2.cursor()

def partyrolePost(proleid):
    
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
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
	prole.dnext_role,
    prole.updatedate
    from staging.partyrole prole
	left join staging.partner prt on prole.parent_id = prt.legacypartnerid
	where partyroleid = '{}'
    """.format(proleid))
    
    for i in cursor.fetchall():
        pk = i[3]
        partyroleid = i[4]
        l_siebelid = i[3]
        lastUpdate_dateSource = i[13]
        
        if i[8] == None or i[8] == "":
            userLogin = ""
        elif i[12] == "Technician/Installator":
            userLogin = i[8]
        else:    
            userLogin = i[8] + "@vodafone.com" 
        
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
                    "name":"userWarehouseLocation",
                    "value":userlocation,
                    "valueType":"string"
                })
            if i[11] != None:
                prole['characteristic'].append({
                    "name":"OperatorCode",
                    "value":fiscalization_user_code,
                    "valueType":"string"
                })
            if i[13] != None:
                prole['characteristic'].append({
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
                    
            
            
            try:
                r = requests.post(conf.url['partyrole'], data=json.dumps(prole,indent=3,default=str), headers=headers)
                
                if r.status_code != 201:
                    control = 0
                    #cursor.execute("update staging.partyrole  set  statuscode = '{}' where partyroleid = '{}'".format(r.status_code, pk))
                    #con.commit()
                else:
                    control = 1
                    
            except Exception as e:
                print("Partyrole POST yaparken hata" + pk)
                control = 0
                pass
                
            prole['id'] = pk
            prole["roleType"] ={
                    "id":salesAgentid,
                    "href":"/api/partyRoleManagement/v4/roletype/"+salesAgentid,
                    "name": 'Sales Agent'
                    }
            
            try:
                r_e = requests.post(conf.url['partyrole'], data=json.dumps(prole,indent=3,default=str), headers=headers)
                
                if r_e.status_code != 201:
                    control1 = 0
                    #cursor.execute("update staging.partyrole  set  statuscode = '{}' where partyroleid = '{}'".format(r.status_code, pk))
                    #con.commit()
                else:
                    control1 = 1
            
            except Exception as e:
                control1 = 0
                pass
            
        except Exception as e:
            print("Exception WhenCreatePartyrolePayload1 for: " + pk)
            control0 = 0
            control1 = 0
            #cursor.execute("update staging.partyrole set statuscode = '425' where partyroleid ='{}'".format(pk))
            #con.commit()
            pass
    
    ######################### Individual ################################# Individual 
    
    l = dict()
    cursor.execute("""
    select 
        i.countryofbirth,
        i.fullname,
        i.gender,
        i.givenname,
        i.middlename,
        i.nationality,
        i.placeofbirth,
        i.status,
        i.birthdate,
        i.familyname,
        i.legalstatus,
        i.role,
        i.lat,
        i.lng,
        i.representativeflag,
        i.partyid
    from staging.individual i 
    where (role is not null) 
    and (role != 'representative')
    and i.partyid = '{}'
    """.format(l_siebelid))
    
    
    for i in cursor.fetchall():
        pk = i[15]
        fullName = i[1]
        givenName = i[3]
        status = i[7]
        if (i[5] == None) or (i[5] == '') :
            nationality = 'ALBANIAN'
        else:    
            nationality = i[5]
            
        if (i[0] == None) or (i[0] == ''):
            countryOfBirth ="ALBANIA"
        else:
            countryOfBirth = i[0]
            
        if (i[6] == None) or (i[6] == '') :
            placeOfBirth = 'OTHER'
        else:
            placeOfBirth = i[6]
            
        if (i[2] == None) or (i[2] == ''):
            gender = "-"
        else:
            gender = i[2]
            
        if i[4] == None:
            middleName = ''
        else:    
            middleName = i[4]
            
        if i[8] == None:
            birthDate = "1800-01-01T00:00:00Z"
        else:
            birthDate = str(i[8]).replace(' ','T')+'Z'
            
        if (i[9] == None):
            familyName = ""
        else:
            familyName = i[9]
            
        if (i[10] == None):
            legalstatus = ""
        else:
            legalstatus = i[10]
            
        if (i[11] == None):
            role = ""
        else:
            role = i[11]
            
        if (i[12] == None):
            lat = ""
        else:
            lat = i[12]
            
        if i[13] == None:
            lont = ""
        else:
            lont = i[13]
        
        try:
            
            l = {
                'id':pk,
                'countryOfBirth' : countryOfBirth,
                'fullName' : fullName,
                'gender' : gender,
                'givenName':givenName,
                'middleName' : middleName,
                'familyName':familyName,
                'nationality':nationality,
                'placeOfBirth':placeOfBirth,
                'status' : status,
                'birthDate': birthDate
                }
        
            l['externalReference'] = [{
                'externalReferenceType' : 'migration_partyrole',
                'id':pk,
                'name':'legacyNumber'
            }]
            
            
            try:
                cursor.execute("select identificationtype,attachmentname,issuingauthority,documentnumber,dnextattachmentid,mime_type from staging.individualidentification where identificationid = '{}'".format(pk))
                for i in cursor.fetchall():
                    identype = i[0]
                    if i[1] == None:
                        attname = ""
                    else:
                        attname = i[1]
                    if i[3] == None:
                        idenid = ""
                    else:
                        idenid = i[3]
                    if i[4] == None:
                        dnextid = ""
                    else:
                        dnextid = i[4]
                    if i[5] == None:
                        mime = "DOCUMENT"
                    else:
                        mime = i[5]
                    l['individualIdentification'] = [{
                        "identificationId":idenid,
                        "identificationType":i[0]
                        }]
                    
            except:
                pass
            
            
            try:
                r_i = requests.post(conf.url['individual'],data = json.dumps(l,indent=3,default=str),headers=headers)
                #print('Individual Status Code : ',r.status_code)
                
                if r_i.status_code != 201:
                    control2 = 0
                else:
                    control2 = 1
                
            except:
                control2 = 0
                
            '''
            try:
                cursor.execute("update staging.individual set statuscode = '{}' where partyid = '{}'".format(r.status_code,pk))
                con.commit()
            except:
                pass
                
                
            
            if role == 'Vodafone Employee':
                try:
                    cursor.execute("update staging.partyrole set engagedpartyid = '{}' where siebelid = '{}'".format(r.json()['id'],pk))
                    con.commit()
                except:
                    print("Individual Post Error(Partyrole)")
                    print(r.text)
            '''
            
            
        except Exception as e:
            print("Exception WhenCreateIndividualPartyrolePayload for: " + pk)
            print(e)
            control2 = 0
            #cursor.execute("update staging.individual set statuscode = '434' where partyid='{}'".format(pk))
            #con.commit()
            pass
        
    return [control,control1,control2]


##############################################################################################################################################   PATCH PART

def partyrolePatch(proleid):
    
    token = "eyJhbGciOiJSUzI1NiIsInR5cCIgOiAiSldUIiwia2lkIiA6ICJfODRLWF81ck13T2E3bjZNanJzSFI2cFBJeUlKX2JXc3o1Wk8waGtBdFJZIn0.eyJleHAiOjE2ODQyNDIwNDksImlhdCI6MTY4NDI0MDI0OSwianRpIjoiMDI2OTE2MjAtNDM4Zi00YmE1LTkxMDktMjE2Y2I0MGRiMjJmIiwiaXNzIjoiaHR0cHM6Ly9kaWFtLnVhdC5kbmV4dC5hbC52b2RhZm9uZS5jb20vYXV0aC9yZWFsbXMvZG5leHQiLCJhdWQiOlsicmVhbG0tbWFuYWdlbWVudCIsImRuZXh0LWJlLWNsaWVudCIsImJyb2tlciIsImFjY291bnQiXSwic3ViIjoiZGY3NGQ1ZTctMGMyYi00ZDJiLTk3ODktY2NlNmI2YmE1Y2I1IiwidHlwIjoiQmVhcmVyIiwiYXpwIjoiZG5leHQtY2xpZW50Iiwic2Vzc2lvbl9zdGF0ZSI6IjQ3MGFjNjI3LTU1ZmQtNDBmZC04YjI1LTM5YmJmN2JkNzdiYSIsImFjciI6IjEiLCJhbGxvd2VkLW9yaWdpbnMiOlsiKi8qIiwiKiJdLCJyZWFsbV9hY2Nlc3MiOnsicm9sZXMiOlsiYmFja29mZmljZV9yb2xlIiwiY3JlYXRlX2N1c3RvbWVyIiwiZGVhbGVyc19vcmRlcnNfdHJhbnNhY3Rpb25zX3ZpZXciLCJvd25faW5zdGFsbGF0aW9uX2RldGFpbCIsImpvYi1zY2hlZHVsZXIiLCJkdW5uaW5nLWFkbWluIiwidmlld19vd25fY3VzdG9tZXIiLCJ2aWV3X29yZGVyc19yb2xlIiwidXBkYXRlX3Ryb3VibGVfdGlja2V0IiwiY2FsbF9jZW50ZXJfcm9sZSIsImFncmVlbWVudF9kZXRhaWxfdmlldyIsImR1bm5pbmdfYWRtaW4iLCJhbGxfaW5zdGFsYXRpb25fZGV0YWlsIiwicHJpY2Vfb3ZlcndyaXRlIiwic21zX3ZhbGlkYXRpb25fYnlwYXNzIiwic2NfYWRtaW5pc3RyYXRpb24iLCJjcmVhdGVfc3VibWl0X29yZGVyIiwiZWRpdF9jdXN0X2RldGFpbHMiLCJvZmZsaW5lX2FjY2VzcyIsInVzZXJzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImNyZWF0ZV9zaG9wcGluZ19jYXJ0IiwidW1hX2F1dGhvcml6YXRpb24iLCJ3YWl2aW5nX3BlbmFsdGllcyIsInNrZWRhX3JvbGUiLCJjcmVhdGVfYWdyZWVtZW50IiwiY3VzdG9tZXJfaW50ZXJhY3Rpb24iLCJkdW5uaW5nX3ZpZXdlciIsInNob3BzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwic2VlX2NyZWF0ZWRfc2hvcHBpbmdfY2FydHMiLCJkdW5uaW5nLXZpZXdlciIsIm1hbnVhbF9vcmRlcl9yb2xlIiwiZGVmYXVsdC1yb2xlcy1kbmV4dCIsImF0dGFjaG1lbnRfdmlld19yb2xlIiwic3VwZXJfcm9sZSIsImNyZWF0ZS1yZWFsbSIsImRjYXNlX2luc3RhbGxhdG9yX3JvbGUiLCJzZXJ2aWNlLWNhdGFsb2ciLCJjcmVhdGVfdHJvdWJsZV90aWNrZXRzIiwiYWxsX3RpY2tldHNfdmlldyIsImNvcHNfc2VjX2xvZyIsInZpZXdfYWxsX2N1c3RvbWVycyIsInZpZXdfaW5zdF9zdGF0dXNfZGV0YWlscyIsInNlZV9wb3N0cGFpZF9iaWxscyIsInJjX2FkbWluaXN0cmF0aW9uIiwiZGVhY3RpdmF0aW9uX3JvbGUiXX0sInJlc291cmNlX2FjY2VzcyI6eyJyZWFsbS1tYW5hZ2VtZW50Ijp7InJvbGVzIjpbInZpZXctcmVhbG0iLCJ2aWV3LWlkZW50aXR5LXByb3ZpZGVycyIsIm1hbmFnZS1pZGVudGl0eS1wcm92aWRlcnMiLCJpbXBlcnNvbmF0aW9uIiwicmVhbG0tYWRtaW4iLCJjcmVhdGUtY2xpZW50IiwibWFuYWdlLXVzZXJzIiwicXVlcnktcmVhbG1zIiwidmlldy1hdXRob3JpemF0aW9uIiwicXVlcnktY2xpZW50cyIsInF1ZXJ5LXVzZXJzIiwibWFuYWdlLWV2ZW50cyIsIm1hbmFnZS1yZWFsbSIsInZpZXctZXZlbnRzIiwidmlldy11c2VycyIsInZpZXctY2xpZW50cyIsIm1hbmFnZS1hdXRob3JpemF0aW9uIiwibWFuYWdlLWNsaWVudHMiLCJxdWVyeS1ncm91cHMiXX0sImRuZXh0LWJlLWNsaWVudCI6eyJyb2xlcyI6WyJyZXNvdXJjZS1jYXRhbG9nIiwic2VydmljZS1vcmRlci1mdWxmaWxsbWVudCIsImpvYi1zY2hlZHVsZXIiLCJwYXJ0bmVyc2hpcC1tYW5hZ2VtZW50IiwicGFydHlSb2xlLW1hbmFnZW1lbnQiLCJkdW5uaW5nLWFkbWluIiwic2VydmljZS1vcmRlciIsImRuZXh0LWNsaWVudC1yb2xlIiwic2hvcHBpbmctY2FydCIsImN1c3RvbWVyLW1hbmFnZW1lbnQiLCJjdXN0b21lci1qb3VybmV5IiwicmVzb3VyY2UtaW52ZW50b3J5IiwicmVzb3VyY2Utb3JkZXJpbmciLCJwcm9kdWN0LW9yZGVyIiwicHJvZHVjdC1jYXRhbG9nIiwicmVzb3VyY2Utb3JkZXItZnVsZmlsbG1lbnQiLCJwYXltZW50LW1ldGhvZCIsImR1bm5pbmctdmlld2VyIiwib3JkZXItZ2VuZXJhdGlvbiIsInVtYV9wcm90ZWN0aW9uIiwicmVmZXJlbmNlLW1hbmFnZW1lbnQiLCJzdG9yYWdlLXNlcnZpY2UiLCJzZXJ2aWNlLWludmVudG9yeSIsImdlb2dyYXBoaWMtYWRkcmVzcy1tYW5hZ2VtZW50IiwicHJvZHVjdC1pbnZlbnRvcnktbWFuYWdlbWVudCIsImhyZWYtbWFwLW1hbmFnZW1lbnQiLCJiYWNrb2ZmaWNldGFzay1tYW5hZ2VtZW50IiwiYWdyZWVtZW50LW1hbmFnZW1lbnQiLCJwcm9kdWN0LW9yZGVyLWZ1bGZpbGxtZW50IiwicGFydHktbWFuYWdlbWVudCIsImFjY291bnQtbWFuYWdlbWVudCIsImRvY3VtZW50LW1hbmFnZW1lbnQiLCJwYXJ0eS1pbnRlcmFjdGlvbi1tYW5hZ2VtZW50IiwiY2FzZS1tYW5hZ2VtZW50Il19LCJkbmV4dC1jbGllbnQiOnsicm9sZXMiOlsicmVzb3VyY2UtY2F0YWxvZyIsImpvYi1zY2hlZHVsZXIiLCJwYXJ0eVJvbGUtbWFuYWdlbWVudCIsInNlcnZpY2Utb3JkZXItZnVsZmlsbG1lbnQiLCJwYXJ0bmVyc2hpcC1tYW5hZ2VtZW50IiwiZG5leHQtZGVmYXVsdC1jbGllbnQtcm9sZSIsImR1bm5pbmctYWRtaW4iLCJzZXJ2aWNlLW9yZGVyIiwiZG5leHQtY2xpZW50LXJvbGUiLCJzY19hZG1pbmlzdHJhdGlvbiIsInNob3BwaW5nLWNhcnQiLCJjdXN0b21lci1tYW5hZ2VtZW50IiwicGNfYWRtaW5pc3RyYXRpb24iLCJjdXN0b21lci1qb3VybmV5IiwicmVzb3VyY2UtaW52ZW50b3J5IiwicmVzb3VyY2Utb3JkZXJpbmciLCJza2VkYV9yb2xlIiwicHJvZHVjdC1vcmRlciIsInByb2R1Y3QtY2F0YWxvZyIsInJlc291cmNlLW9yZGVyLWZ1bGZpbGxtZW50IiwiZHVubmluZy12aWV3ZXIiLCJwYXltZW50LW1ldGhvZCIsInVtYV9wcm90ZWN0aW9uIiwib3JkZXItZ2VuZXJhdGlvbiIsInJlZmVyZW5jZS1tYW5hZ2VtZW50Iiwic2VydmljZS1pbnZlbnRvcnkiLCJzdG9yYWdlLXNlcnZpY2UiLCJnZW9ncmFwaGljLWFkZHJlc3MtbWFuYWdlbWVudCIsInByb2R1Y3QtaW52ZW50b3J5LW1hbmFnZW1lbnQiLCJocmVmLW1hcC1tYW5hZ2VtZW50IiwiYmFja29mZmljZXRhc2stbWFuYWdlbWVudCIsImFncmVlbWVudC1tYW5hZ2VtZW50IiwicHJvZHVjdC1vcmRlci1mdWxmaWxsbWVudCIsInBhcnR5LW1hbmFnZW1lbnQiLCJhY2NvdW50LW1hbmFnZW1lbnQiLCJzZXJ2aWNlLWNhdGFsb2ciLCJkb2N1bWVudC1tYW5hZ2VtZW50IiwicGFydHktaW50ZXJhY3Rpb24tbWFuYWdlbWVudCIsInJjX2FkbWluaXN0cmF0aW9uIiwiY2FzZS1tYW5hZ2VtZW50Il19LCJicm9rZXIiOnsicm9sZXMiOlsicmVhZC10b2tlbiJdfSwiYWNjb3VudCI6eyJyb2xlcyI6WyJtYW5hZ2UtYWNjb3VudCIsInZpZXctYXBwbGljYXRpb25zIiwidmlldy1jb25zZW50IiwibWFuYWdlLWFjY291bnQtbGlua3MiLCJtYW5hZ2UtY29uc2VudCIsImRlbGV0ZS1hY2NvdW50Iiwidmlldy1wcm9maWxlIl19fSwic2NvcGUiOiJwcm9maWxlIGVtYWlsIiwic2lkIjoiNDcwYWM2MjctNTVmZC00MGZkLThiMjUtMzliYmY3YmQ3N2JhIiwiZW1haWxfdmVyaWZpZWQiOmZhbHNlLCJuYW1lIjoiT3JiaSBUYW50IiwiZ3JvdXBzIjpbIkRlYWxlciBBZG1pbmlzdHJhdG9yIiwiYmFja29mZmljZV9yb2xlIiwiY3JlYXRlX2N1c3RvbWVyIiwiZGVhbGVyc19vcmRlcnNfdHJhbnNhY3Rpb25zX3ZpZXciLCJvd25faW5zdGFsbGF0aW9uX2RldGFpbCIsImpvYi1zY2hlZHVsZXIiLCJkdW5uaW5nLWFkbWluIiwidmlld19vd25fY3VzdG9tZXIiLCJ2aWV3X29yZGVyc19yb2xlIiwidXBkYXRlX3Ryb3VibGVfdGlja2V0IiwiY2FsbF9jZW50ZXJfcm9sZSIsImFncmVlbWVudF9kZXRhaWxfdmlldyIsImR1bm5pbmdfYWRtaW4iLCJhbGxfaW5zdGFsYXRpb25fZGV0YWlsIiwicHJpY2Vfb3ZlcndyaXRlIiwic21zX3ZhbGlkYXRpb25fYnlwYXNzIiwic2NfYWRtaW5pc3RyYXRpb24iLCJjcmVhdGVfc3VibWl0X29yZGVyIiwiZWRpdF9jdXN0X2RldGFpbHMiLCJvZmZsaW5lX2FjY2VzcyIsInVzZXJzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImNyZWF0ZV9zaG9wcGluZ19jYXJ0IiwidW1hX2F1dGhvcml6YXRpb24iLCJ3YWl2aW5nX3BlbmFsdGllcyIsInNrZWRhX3JvbGUiLCJjcmVhdGVfYWdyZWVtZW50IiwiY3VzdG9tZXJfaW50ZXJhY3Rpb24iLCJkdW5uaW5nX3ZpZXdlciIsInNob3BzX29yZGVyc190cmFuc2FjdGlvbnNfdmlldyIsImRlZmF1bHQtcm9sZXMtbWFzdGVyIiwic2VlX2NyZWF0ZWRfc2hvcHBpbmdfY2FydHMiLCJkdW5uaW5nLXZpZXdlciIsIm1hbnVhbF9vcmRlcl9yb2xlIiwiZGVmYXVsdC1yb2xlcy1kbmV4dCIsImF0dGFjaG1lbnRfdmlld19yb2xlIiwic3VwZXJfcm9sZSIsImNyZWF0ZS1yZWFsbSIsImRjYXNlX2luc3RhbGxhdG9yX3JvbGUiLCJzZXJ2aWNlLWNhdGFsb2ciLCJjcmVhdGVfdHJvdWJsZV90aWNrZXRzIiwiYWxsX3RpY2tldHNfdmlldyIsImNvcHNfc2VjX2xvZyIsInZpZXdfYWxsX2N1c3RvbWVycyIsInZpZXdfaW5zdF9zdGF0dXNfZGV0YWlscyIsInNlZV9wb3N0cGFpZF9iaWxscyIsInJjX2FkbWluaXN0cmF0aW9uIiwiZGVhY3RpdmF0aW9uX3JvbGUiXSwicHJlZmVycmVkX3VzZXJuYW1lIjoib3JiaXRhbnQiLCJnaXZlbl9uYW1lIjoiT3JiaSIsImZhbWlseV9uYW1lIjoiVGFudCIsImVtYWlsIjoib3JiaXRhbnRAZ28uY29tIn0.hCaRwfUzINktN7veLspFO9NETCY-wiYl2SD67tVnKAeQDExWvAC-Rk74fme9r8jZ7Cf59Urzmx-6BDc9GJBRRuxe34ZEucZa91XrtCKGd_2rvZFSEoEJ_9dow2KBHchi340JVk2mbJCykxQazPvOayCJ7EYblQYrjgms2Z78IJhoMbZ0-RLztyA8E-zp4RBOYYn8b84y9UsNHy9jehfNaKOksB3psyUXNzjS8w529cBO2SEUUbLjmx3XgRWPPO84NFcoDcmEO8XkWVyiovuDhYSxTSz4MWF3cUQKc-evRrOj8A6pL7xf2Z31Cn8y8LXw6fNPDgASNPDeehU-2Gg_2g"
    headers ={'Accept':"application/json",'Content-type':'application/merge-patch+json','Authorization': "Bearer"+ " " +token}
    
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
	prole.dnext_role,
    prole.updatedate
    from staging.partyrole prole
	left join staging.partner prt on prole.parent_id = prt.legacypartnerid
	where partyroleid = '{}'
    """.format(proleid))
    
    for i in cursor.fetchall():
        pk = i[3]
        pk_partyrole = 'E'+ pk
        partyroleid = i[4]
        l_siebelid = i[3]
        lastUpdate_dateSource = i[13]
        
        if i[8] == None or i[8] == "":
            userLogin = ""
        elif i[12] == "Technician/Installator":
            userLogin = i[8]
        else:    
            userLogin = i[8] + "@vodafone.com" 
        
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
                    "name":"userWarehouseLocation",
                    "value":userlocation,
                    "valueType":"string"
                })
            if i[11] != None:
                prole['characteristic'].append({
                    "name":"OperatorCode",
                    "value":fiscalization_user_code,
                    "valueType":"string"
                })
            if i[13] != None:
                prole['characteristic'].append({
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
                    
            
            
            try:
                r = requests.patch(conf.url['partyrole'] + '/' + pk_partyrole, data=json.dumps(prole,indent=3,default=str), headers=headers)
                
                if r.status_code != 200:
                    control = 0
                    #cursor.execute("update staging.partyrole  set  statuscode = '{}' where partyroleid = '{}'".format(r.status_code, pk))
                    #con.commit()
                else:
                    control = 1
                    
            except Exception as e:
                print("Partyrole PATCH yaparken hata" + pk)
                control = 0
                pass
                
                
            prole["roleType"] ={
                    "id":salesAgentid,
                    "href":"/api/partyRoleManagement/v4/roletype/"+salesAgentid,
                    "name": 'Sales Agent'
                    }
            
            try:
                r_e = requests.patch(conf.url['partyrole'] + '/' + pk, data=json.dumps(prole,indent=3,default=str), headers=headers)
                print(r_e.text)
                
                if r_e.status_code != 200:
                    print("SalesAgent PATCH error id:{}".format(pk))
                    print('SalesAgent Status Code : ',r_e.status_code)
                    print(r_e.text)
                    control1 = 0
                    #cursor.execute("update staging.partyrole  set  statuscode = '{}' where partyroleid = '{}'".format(r.status_code, pk))
                    #con.commit()
                else:
                    control1 = 1
            
            except Exception as e:
                print("SalesAgent PATCH yaparken hata" + pk)
                control1 = 0
                pass
            
        except Exception as e:
            print("Exception WhenCreatePartyrolePayload2 for: " + pk)
            control0 = 0
            control1 = 0
            #cursor.execute("update staging.partyrole set statuscode = '425' where partyroleid ='{}'".format(pk))
            #con.commit()
            pass
    
    ######################### Individual ################################# Individual 
    
    l = dict()
    cursor.execute("""
    select 
        i.countryofbirth,
        i.fullname,
        i.gender,
        i.givenname,
        i.middlename,
        i.nationality,
        i.placeofbirth,
        i.status,
        i.birthdate,
        i.familyname,
        i.legalstatus,
        i.role,
        i.lat,
        i.lng,
        i.representativeflag,
        i.partyid
    from staging.individual i 
    where (role is not null) 
    and (role != 'representative')
    and i.partyid = '{}'
    """.format(l_siebelid))
    
    
    for i in cursor.fetchall():
        pk = i[15]
        fullName = i[1]
        givenName = i[3]
        status = i[7]
        if (i[5] == None) or (i[5] == '') :
            nationality = 'ALBANIAN'
        else:    
            nationality = i[5]
            
        if (i[0] == None) or (i[0] == ''):
            countryOfBirth ="ALBANIA"
        else:
            countryOfBirth = i[0]
            
        if (i[6] == None) or (i[6] == '') :
            placeOfBirth = 'OTHER'
        else:
            placeOfBirth = i[6]
            
        if (i[2] == None) or (i[2] == ''):
            gender = "-"
        else:
            gender = i[2]
            
        if i[4] == None:
            middleName = ''
        else:    
            middleName = i[4]
            
        if i[8] == None:
            birthDate = "1800-01-01T00:00:00Z"
        else:
            birthDate = str(i[8]).replace(' ','T')+'Z'
            
        if (i[9] == None):
            familyName = ""
        else:
            familyName = i[9]
            
        if (i[10] == None):
            legalstatus = ""
        else:
            legalstatus = i[10]
            
        if (i[11] == None):
            role = ""
        else:
            role = i[11]
            
        if (i[12] == None):
            lat = ""
        else:
            lat = i[12]
            
        if i[13] == None:
            lont = ""
        else:
            lont = i[13]
        
        try:
            
            l = {
                'countryOfBirth' : countryOfBirth,
                'fullName' : fullName,
                'gender' : gender,
                'givenName':givenName,
                'middleName' : middleName,
                'familyName':familyName,
                'nationality':nationality,
                'placeOfBirth':placeOfBirth,
                'status' : status,
                'birthDate': birthDate
                }
        
            l['externalReference'] = [{
                'externalReferenceType' : 'migration_partyrole',
                'id':pk,
                'name':'legacyNumber'
            }]
            
            
            try:
                cursor.execute("select identificationtype,attachmentname,issuingauthority,documentnumber,dnextattachmentid,mime_type from staging.individualidentification where identificationid = '{}'".format(pk))
                for i in cursor.fetchall():
                    identype = i[0]
                    if i[1] == None:
                        attname = ""
                    else:
                        attname = i[1]
                    if i[3] == None:
                        idenid = ""
                    else:
                        idenid = i[3]
                    if i[4] == None:
                        dnextid = ""
                    else:
                        dnextid = i[4]
                    if i[5] == None:
                        mime = "DOCUMENT"
                    else:
                        mime = i[5]
                    l['individualIdentification'] = [{
                        "identificationId":idenid,
                        "identificationType":i[0]
                        }]
                    
            except:
                pass
            
            
            try:
                r_i = requests.patch(conf.url['individual'] + '/' + pk, data = json.dumps(l,indent=3,default=str),headers=headers)
                print('Individual Status Code : ',r_i.status_code)
                
         
                
                if r_i.status_code != 200:
                    print("PATCH error individual partyid {},".format(pk))
                    print(r_i.text)
                    control2 = 0
                   
                else:
                    control2 = 1
                
            except:
                control2 = 0
                
                
            '''
            try:
                cursor.execute("update staging.individual set statuscode = '{}' where partyid = '{}'".format(r.status_code,pk))
                con.commit()
            except:
                pass
                
                
            
            if role == 'Vodafone Employee':
                try:
                    cursor.execute("update staging.partyrole set engagedpartyid = '{}' where siebelid = '{}'".format(r.json()['id'],pk))
                    con.commit()
                except:
                    print("Individual Post Error(Partyrole)")
                    print(r.text)
            '''
            
            
        except Exception as e:
            print("Exception WhenCreateIndividualPartyrolePayload for: " + pk)
            print(e)
            control2 = 0
           
            #cursor.execute("update staging.individual set statuscode = '434' where partyid='{}'".format(pk))
            #con.commit()
            pass
        
    return [control,control1,control2]



def nullCheck(val):
    if val == None:
        return ""
    else:
        return val


def dateCompare():
    partyList = list()
    idList = list() #Post edilecek liste
    
    EsuccessedPartypatchlist = []
    EunsuccessedPartypatchlist = []
    
    EsuccessedPartypostlist = []
    EunsuccessedPartypostlist = []
    ##################
    
    successedPartypatchlist = []
    unsuccessedPartypatchlist = []
    
    successedPartypostlist = []
    unsuccessedPartypostlist = []
    
    #################
    successedIndpatchlist = []
    unsuccessedIndpatchlist = []
    
    successedIndpostlist = []
    unsuccessedIndpostlist = []                                                                                  # VF-9873FH5               # VF-ET7PAP3
                                                                                                                  #patch                     #post
    #where (partyroleid = '103062' or partyroleid = '105659')      
    # 103991
    cursor.execute("select dnextpartyroleid, updatedate, partyroleid from staging.partyrole")
    
    for i in cursor.fetchall():
        partyList.append(i)
        idList.append(i[2])  #partyroleid
        
        
    partyshipList = list()
    #where (id = 'VF-9873FH5' or id = 'VF-ET7PAP3')
    
    # where id = 'VF-9GTMT3X'
    #cursor2.execute("select id, updated_date from public.party_role") partyrole tablosundaki updated_date alanı sisteme kayıt atıldığı tarihi verdiği için bir sonraki gün sisteme eklenen csv dosyasındaki last_update alanı ile aynı olma durumunda değişiklik yokmuş gibi algılıyor bundan dolayı characteristic tablosundaki SourceUpdateDate 'i kullanıyoruz
    cursor2.execute("""select  party_role.id, characteristic.value, party_role.updated_date 
                     FROM public.party_role  party_role 
                     left join public.characteristic  characteristic 
                     on party_role.id = characteristic.party_role_id 
                     and characteristic.name = 'SourceUpdateDate'
                     """)
    for i in cursor2.fetchall():
        partyshipList.append(i)
        
    for i in partyList:
        for j in partyshipList:
            if i[0] == j[0]:
                print("idList: ")
                #print(idList)
                idList.remove(i[2])     #patch için eşleşmiş olan kayıtlar silinerek yalnızca postta kullanılacak kayıtlar listede tutuluyor.
                print("idList: ")
                #print(idList)
                patch_source_update_control = False 
                
                if j[1] == None or j[1] =="":
                   patch_source_update_control = True
                elif time.mktime(datetime.datetime.strptime(i[1],"%d-%b-%y").timetuple()) > time.mktime(datetime.datetime.strptime(j[1],"%d-%b-%y").timetuple()):
                   patch_source_update_control = True
                                             

                if  patch_source_update_control ==True: 
                    
                    #time.sleep(1)
                    control_patch = partyrolePatch(i[2])  #partyroleid gönderiliyor.
                    if control_patch[0] == 1:
                        EsuccessedPartypatchlist.append(i[2])
                    else:
                        EunsuccessedPartypatchlist.append(i[2])
                    
                    if control_patch[1] == 1:
                        successedPartypatchlist.append(i[2])
                    else:
                        unsuccessedPartypatchlist.append(i[2])
                    
                    if control_patch[2] == 1:
                        successedIndpatchlist.append(i[2])
                    else:
                        unsuccessedIndpatchlist.append(i[2])
                    
    
    for i in idList:
        control_post = partyrolePost(i)  #partyroleid gönderiliyor.
        if control_post[0] == 1:
            EsuccessedPartypostlist.append(i)
        else:
            EunsuccessedPartypostlist.append(i)
        
        if control_post[1] == 1:
            successedPartypostlist.append(i)
        else:
            unsuccessedPartypostlist.append(i)
        
        if control_post[2] == 1:
            successedIndpostlist.append(i)
        else:
            unsuccessedIndpostlist.append(i)
    
    print("***********************************************************************************************************************")       
    print("Basarili party patch id = E-{} and {}, ".format(EsuccessedPartypatchlist,successedPartypatchlist))
    print("Basarili individual patch id = {}, ".format(successedIndpatchlist))
    
    print("Basarili party post id = E-{} and {}, ".format(EsuccessedPartypostlist,successedPartypostlist))
    print("Basarili individual post id = {}, ".format(successedIndpostlist))
    print("***********************************************************************************************************************")
    
    print("Basarisiz party patch id = E-{} and {}, ".format(EunsuccessedPartypatchlist,unsuccessedPartypatchlist))
    print("Basarisiz individual patch id = {}, ".format(unsuccessedIndpatchlist))
    
    print("Basarisiz party post id = E-{} and {}, ".format(EunsuccessedPartypostlist,unsuccessedPartypostlist))
    print("Basarisiz individual post id = {}, ".format(unsuccessedIndpostlist))
    print("***********************************************************************************************************************")
    
    
    ##############################
           
    print("Basarili party patch sayisi = E-{} + {}, ".format(len(EsuccessedPartypatchlist),len(successedPartypatchlist)))
    print("Basarili individual patch sayisi = {}, ".format(len(successedIndpatchlist)))
    print("Basarili party post sayisi = E-{} + {}, ".format(len(EsuccessedPartypostlist),len(successedPartypostlist)))
    print("Basarili individual post sayisi = {}, ".format(len(successedIndpostlist)))
    
    print("***********************************************************************************************************************")
    
    print("Basarisiz party patch sayisi = E-{} + {}, ".format(len(EunsuccessedPartypatchlist),len(unsuccessedPartypatchlist)))
    print("Basarisiz individual patch sayisi = {}, ".format(len(unsuccessedIndpatchlist)))
    print("Basarisiz party post sayisi = E-{} + {}, ".format(len(EunsuccessedPartypostlist),len(unsuccessedPartypostlist)))
    print("Basarisiz individual post sayisi = {}, ".format(len(unsuccessedIndpostlist)))

    
dateCompare()