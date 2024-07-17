import pgdb
import datetime
import json
import requests
import time
import conf

con = con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def individual():
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
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
    """)
    
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
                'birthDate': birthDate,
                'partyCharacteristic' : [],
                "attachment":[]
                }
        
            l['externalReference'] = [{
                'externalReferenceType' : 'migration_partyrole',
                'id':pk,
                'name':'legacyNumber'
            }]
            if (legalstatus != "") and (legalstatus != None):
            
                l['partyCharacteristic'] = [{
                    'name' : 'juridicalInfo',
                    'value' : legalstatus, 
                    'valueType':'string'
                }]
            if (lat != ""):
                if (legalstatus == "") or (legalstatus == None):
                    l['partyCharacteristic'] = [{
                        'name':'longitude',
                        'value':lont,
                        'valueType':'float'
                    },
                    {
                        'name':'latitude',
                        'value':lat,
                        'valueType':'float'
                    }]
                    
                else:
                    l['partyCharacteristic'].append({
                        'name':'longitude',
                        'value':lont,
                        'valueType':'float'
                    })
                    l['partyCharacteristic'].append({
                        'name':'latitude',
                        'value':lat,
                        'valueType':'float'
                    })
            if i[14] == '1':
                    l['partyCharacteristic'].append({
                        'name':'communicationMethod',
                        'value':'phone',
                        'valueType':'string'
                    })
            
            
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
                    if dnextid != "":
                        l["attachment"].append(
                            {
                                "description":i[0]+" - "+idenid,
                                "mimeType":mime,
                                "name":attname,
                                "href":"/api/documentManagement/v4/attachment/"+dnextid,
                                "id":dnextid,
                            }
                        )
            except:
                pass
            
            print("Individual text : " + pk)
            print(l)
            r = requests.post(conf.url['individual'],data = json.dumps(l,indent=3,default=str),headers=headers)
            print('Individual Status Code : ',r.status_code)
            
            if r.status_code != 201:
                print(r.text)
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
        
        except Exception as e:
            print("Exception WhenCreateIndividualPartyrolePayload for: " + pk)
            print(e)
            cursor.execute("update staging.individual set statuscode = '434' where partyid='{}'".format(pk))
            con.commit()
            pass
    
    
individual()
    
con.commit()
cursor.close() 
con.close()