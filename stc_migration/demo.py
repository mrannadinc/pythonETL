import psycopg
import json
import requests
import sys

hostname = "localhost"
database = "postgres"
username = "postgres"
pwd = "512154"
port_id = 5432
conn = None
cur = None

def individual():
    try:
        with psycopg.connect(
                    host = hostname, 
                    dbname = database,
                    user = username,
                    password = pwd,
                    port = port_id) as conn:

            with conn.cursor() as cur:

                drop_table = 'DROP TABLE individual'

                cur.execute(drop_table)

                create_table_script = ''' CREATE TABLE IF NOT EXISTS individual (
                                                id varchar(255) PRIMARY KEY,
                                                gender  varchar(255) NOT NULL)'''
                
                cur.execute(create_table_script)

                insert_table_script = 'INSERT INTO individual (id, gender) VALUES (%s, %s)'
                insert_value = ('eea547a3-06fd-4d13-acf7-ee3cdc3c4120','MALE')
                cur.execute(insert_table_script, insert_value)

                select_table = 'SELECT * FROM individual'
                cur.execute(select_table)
                for i in cur.fetchall():
                    pk = 'None' #i[24]
                    fullName = 'None' #i[1]
                    givenName = 'None' #i[3]
                    #if i[4] == None: 
                    middleName = 'None' #i[4]
                    #else:    
                    #    middleName = i[4]
                    #if (i[5] == None) or (i[5] == '') :
                    #    nationality = 'ALBANIAN'
                    #else:    
                    #    nationality = i[5]
                    nationality = 'None' #i[5]
                        
                    #if (i[0] == None) or (i[0] == ''):
                    #    countryOfBirth ="ALBANIA"
                    #else:
                    #    countryOfBirth = i[0]
                    countryOfBirth = 'None' #i[0]
                        
                    #if (i[6] == None) or (i[6] == '') :
                    #    placeOfBirth = 'OTHER'
                    #else:
                    #    placeOfBirth = i[6]
                    placeOfBirth = 'None' #i[6]
                        
                    #if (i[2] == None) or (i[2] == ''):
                    #    gender = "-"
                    #else:
                    #    gender = i[2]
                    gender = i[1] #i[2]

                    status = 'None' #i[7]
                    
                    #if i[8] == None:
                    #    birthDate = "1800-01-01T00:00:00Z"
                    #else:
                    #    birthDate = str(i[8]).replace(' ','T')+'Z'
                    birthDate = 'None' #i[8]

                    familyName = 'None' #i[9]
                    legalstatus = 'None' #i[10]
                    
                    segment = 'None' #i[15]
                    segmentid = 'None' #i[16]
                    
                    customertype = 'None' #i[17]
                    customertypeid = 'None' #i[18]
                    
                    #if (i[11] == None):
                    #    role = ""
                    #else:
                    #    role = i[11]
                    role = 'None' #i[11]
                    #if (i[12] == None):
                    #    lat = ""
                    #else:
                    #    lat = i[12]
                    lat = 'None' #i[12]
                    #if i[13] == None:
                    #    lont = ""
                    #else:
                    #    lont = i[13]
                    lont = 'None' #i[13]    
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
                                
                        if '1' == '1': #i[14]
                            l['partyCharacteristic'].append({
                                'name':'communicationMethod',
                                'value':'phone',
                                'valueType':'string'
                            })
                        
                        
                        ## cur.execute("""select 
                        ## identificationtype,attachmentname,issuingauthority,substr(CAST(documentnumber AS varchar) ,1, 15),dnextattachmentid,mime_type 
                        ## from staging.individualidentification where partyid = '{}'
                        ## """.format(pk))
                        #for i in cur.fetchall():
                        #    identype = i[0]
                        #    if i[1] == None:
                        #        attname = ""
                        #    else:
                        #        attname = i[1]
                        #    if i[3] == None:
                        #        idenid = ""
                        #    else:
                        #        idenid = i[3]
                        idenid = 'None' #i[3]
                        #    if i[4] == None:
                        #        dnextid = ""
                        #    else:
                        #        dnextid = i[4]
                        #    if i[5] == None:
                        #        mime = "DOCUMENT"
                        #    else:
                        #        mime = i[5]
                        l['individualIdentification'] = [{
                            "identificationId":idenid,
                            "identificationType":'None' #i[0]
                            }]
                        try:
                            ## cur.execute("""select 
                            ## doc_name,file_type,file_name_original,doc_type,file_name_save,dnextattachmentid 
                            ## from staging.docs where customerid = '{}' and doc_type in ('Karte Identiteti/Pasaporte', 'NIPT') and dnextattachmentid is not null
                            ## """.format(pk))                                                                                                             #fiziksel dokümanların testi
                            #for i in cur.fetchall():
                            l['attachment'].append({
                                'description' : 'None', #i[0],
                                'mimeType': 'None', #i[1],
                                'name': 'None', #i[2],
                                'attachmentType' : 'None', #i[3],
                                'url' : 'None', #i[4],
                                "href":"api/documentManagement/v4/attachment/"+'None', #i[5],
                                "id":'None' #i[5]
                            })
                        except:
                            pass
                        
                        
                        print("Individual text : " + pk)
                        print(l)
                        ## r = requests.post(conf.url['individual'],data = json.dumps(l,indent=3,default=str),headers=headers)
                        ## print('Individual Status Code : ',r.status_code)
                    
                        ## if r.status_code != 201:    
                        ##     cur.execute("update staging.individual set statuscode = '{}' where partyid = '{}'".format(r.status_code,pk))
                        ##     conn.commit()
                            
                        ##     print("Individual Error : ", r.status_code)
                        ##     print(r.text)
                    
                    except Exception as e:
                        print("Exception WhenCreateIndividualPayload for: " + pk)
                        print(e)
                        #cur.execute("update staging.individual set statuscode = '434' where partyid = '{}'".format(pk))
                        conn.commit()
                        pass

                conn.commit()

    except Exception as error:
        print(error)
    finally:
        if cur is not None:
            cur.close()
        if conn is not None:
            conn.close()

individual()
