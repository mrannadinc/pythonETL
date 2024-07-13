import pgdb
import datetime
import json
import requests
import time
import conf
import sys 
from awsglue.utils import getResolvedOptions

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()
def individual(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
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
    c.segment,
    c.segmentid,
    c.customertype,
    c.typeid,
    i.disabletype,
    i.disableorganization,
    i.disablecontactpref,
    i.disablelargeprint,
    c.subsegment,
    i.partyid
    from staging.individual i 
    inner join staging.customer c on i.partyid = c.customerunificationid 
    where 1=1
    and c.mig_flag = '1' 
    and c.masterrecordsflag = '1' 
    and i.role is null
    and {}
    """.format(condition))
    
    for i in cursor.fetchall():
        pk = i[24]
        fullName = i[1]
        givenName = i[3]
        if i[4] == None:
            middleName = ''
        else:    
            middleName = i[4]
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
            
        status = i[7]
        
        if i[8] == None:
            birthDate = "1800-01-01T00:00:00Z"
        else:
            birthDate = str(i[8]).replace(' ','T')+'Z'
        familyName = i[9]
        legalstatus = i[10]
        
        segment = i[15]
        segmentid = i[16]
        
        customertype = i[17]
        customertypeid = i[18]
        
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
                'gender' : gender ,
                'givenName':givenName,
                'middleName' : middleName,
                'familyName':familyName,
                'nationality':nationality,
                'placeOfBirth':placeOfBirth,
                'status' : status,
                'birthDate': birthDate,
                'attachment':[],
                'marketSegment': []
                
                }
        
            l['externalReference'] = [{
                'externalReferenceType' : 'migration',
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
            
            l['partyCharacteristic'].append({
                "name": "isCustomerInBlackList", 
                "value":"NO",
                "valueType":"string"
            })
            
            
            # MarketSegment - CustomerSegment
            if (segmentid != None) or (segmentid != ""):
                l['marketSegment'].append({
                        'id' : segmentid,
                        'href' : "/api/referenceManagement/v4/marketSegment/" + segmentid,
                        'name': segment,
                        '@referredType': 'MarketSegment'
                    })
            if (customertypeid != None) or (customertypeid != ""):	
                l['marketSegment'].append({
                        'id' : customertypeid,
                        'href' : "/api/referenceManagement/v4/marketSegment/" + customertypeid,
                        'name':customertype,
                        '@referredType': 'CustomerSegment'
                    })
                    
            if i[14] == '1':
                l['partyCharacteristic'].append({
                    'name':'communicationMethod',
                    'value':'phone',
                    'valueType':'string'
                })
            
            
            cursor.execute("""select 
            identificationtype,attachmentname,issuingauthority,substr(CAST(documentnumber AS varchar) ,1, 15),dnextattachmentid,mime_type 
            from staging.individualidentification where partyid = '{}'
            """.format(pk))
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
            try:
                cursor.execute("""select 
                doc_name,file_type,file_name_original,doc_type,file_name_save,dnextattachmentid 
                from staging.docs where customerid = '{}' and doc_type in ('Karte Identiteti/Pasaporte', 'NIPT') and dnextattachmentid is not null
                """.format(pk))                                                                                                             #fiziksel dokümanların testi
                for i in cursor.fetchall():
                    l['attachment'].append({
                        'description' : i[0],
                        'mimeType': i[1],
                        'name': i[2],
                        'attachmentType' : i[3],
                        'url' : i[4],
                        "href":"api/documentManagement/v4/attachment/"+i[5],
                        "id":i[5]
                    })
            except:
                pass
            
            
            print("Individual text : " + pk)
            print(l)
            r = requests.post(conf.url['individual'],data = json.dumps(l,indent=3,default=str),headers=headers)
            print('Individual Status Code : ',r.status_code)
        
            if r.status_code != 201:    
                cursor.execute("update staging.individual set statuscode = '{}' where partyid = '{}'".format(r.status_code,pk))
                con.commit()
                
                print("Individual Error : ", r.status_code)
                print(r.text)
        
        except Exception as e:
            print("Exception WhenCreateIndividualPayload for: " + pk)
            print(e)
            cursor.execute("update staging.individual set statuscode = '434' where partyid = '{}'".format(pk))
            con.commit()
            pass
    print(args['condition'])


individual(args['condition'])

con.commit()
cursor.close() 
con.close()