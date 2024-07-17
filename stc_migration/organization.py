import pgdb
import datetime
import json
import requests
import conf
import time
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['condition'])

roleid = list()
rolename = list()

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

cursor.execute("select dnextpartyroleid,fullname from staging.partyrole where siebelid in ('VF-ISPP','VF-5KMG6I9','VF-C7GHP32')")
for i in cursor.fetchall():
    roleid.append(i[0])
    rolename.append(i[1])


def organization(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    orgdict = {}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute("""
    select 
    o.status,
    o.name,
    o.legalstatus,
    o.industry,
    o.parentorganization,
    o.role,
    o.tradingname,
    o.taxexempt,
    c.segment,
    c.segmentid,
    c.customertype,
    c.typeid,
    o.partyid
    from staging.organization o 
    left join staging.customer c on o.partyid = c.customerunificationid 
    where  1=1
    and (c.mig_flag = '1')  
    and (c.masterrecordsflag = '1') 
    and (o.role is null)
    and {}
    """.format(condition))
    for i in cursor.fetchall():
        pk = i[12]
        if i[0] != None:
            stat = i[0]
        else:
            stat = ""
        if i[1][0] == '[':
            name = i[1][1:-1]
        else:
            name = i[1]
        if (i[3] == None) or (i[3] == "") :
            ind = 'EMPTY_SOURCE'
        else:
            ind = i[3]
        if len(i[4]) == 0:
            parentorganization = ""
        else:
            parentorganization = i[4]
        if i[5] != None:
            role = i[5]
        else:
            role = ''
        if i[7] != None:
            taxexempt = i[7]
        else:
            taxexempt = 'NO'
        
        try:
            orgdict['id'] = pk
            orgdict['status'] = stat
            orgdict['name'] = name
            orgdict['tradingName']=i[6]
            orgdict['industry']=ind
            orgdict['attachment'] = []
            orgdict['marketSegment'] = []
            
            orgdict['externalReference'] = [{
                'externalReferenceType':'migration',
                'id': pk,
                'name': 'legacyNumber'
            }]
            orgdict['partyCharacteristic'] = [{
                'name':"isCustomerInBlackList",
                'value':'NO',
                'valueType':'string'
            },
            {
                "name":"taxExempt",
                "value":taxexempt,
                "valueType":"string"
            }]
            
            if (i[2] != None) and (i[2]!=""):
                orgdict['partyCharacteristic'].append({
                    'name':'juridicalInfo',
                    'value':i[2],
                    'valueType':'string'                  
                })
            
            # MarketSegment - CustomerSegment
            if i[9] != None and i[8] != None:
                orgdict['marketSegment'].append({
        			'id' : i[9],
        			'href' : "/api/referenceManagement/v4/marketSegment/" + i[9],
                    'name': i[8],
        			'@referredType': 'MarketSegment'
                })
            
            if i[11] != None and i[10] != None:
                orgdict['marketSegment'].append({
        			'id' : i[11],
        			'href' : "/api/referenceManagement/v4/marketSegment/" + i[11],
                    'name':i[10],
        			'@referredType': 'CustomerSegment'
                })
                
            cursor.execute("select identificationtype,substr(CAST(documentnumber AS varchar) ,1, 15),issuingauthority,substr(CAST(documentnumber AS varchar) ,1, 15) from staging.organizationidentification where (partyid not like '%EUR%' or partyid not like '%USD%') and partyid = '{}'".format(pk))
            for i in cursor.fetchall():
                if i[1] == None:
                    idenid = ""
                else:
                    idenid = i[1]
                orgdict['organizationIdentification'] = [{
                    "identificationId":idenid,
                    "identificationType":i[0]
                }]
            try:
                cursor.execute("""select 
                doc_name,
                file_type,
                file_name_original,
                doc_type,
                file_name_save,
                dnextattachmentid 
                from staging.docs where customerid = '{}' and doc_type in ('Karte Identiteti/Pasaporte', 'NIPT') and dnextattachmentid is not null
                """.format(pk))                                                                                     #fiziksel dokümanların testi
                for i in cursor.fetchall():
                    orgdict['attachment'].append({
                        'description' : i[0],
                        'mimeType': i[1],
                        'name': i[2],
                        'attachmentType' : i[3],
                        'url' : i[4],
                        "href":"api/documentManagement/v4/attachment"+i[5],
                        "id":i[5]
                    })
                    
                    orgdict['partyCharacteristic'].append({
                        'name':'searchNipt',
                        'value':i[0],
                        'valueType':'string'                  
                    })
            except:
                pass
            
        
            print("Organization text : " + pk)
            print(orgdict)
            
            r = requests.post(conf.url['organization'],data = json.dumps(orgdict,indent=3,default=str),headers=headers)
            print('Organization Status Code : ',r.status_code)
            
            if r.status_code != 201:
            
                cursor.execute("update staging.organization set statuscode = '{}' where partyid = '{}'".format(r.status_code,pk))
                con.commit()
                    
                print("Organization error" + pk)
                print(r.text)
                
            if role != "":
        
                try:
                    cursor.execute("update staging.customer set engagedpartyid = '{}' where customerid = '{}'".format(r.json()['id'],pk))
                    con.commit()
                except:
                    print("Organization Post Error : {}\nPK = {}".format(r.status_code,pk))
                    print(r.text)
        
        
                cursor.execute("update staging.partner set engagedpartyid = '{}' where partnerid = '{}'".format(r.json()['id'],pk))
                con.commit()
            
                print("Organization Post Error : {}\nPK = {}".format(r.status_code,pk))
                print(r.text)
        
        except Exception as e:
            print("Exception WhenCreateOrganizationPayload for: " + pk)
            print(e)
            cursor.execute("update staging.organization set statuscode = '425' where partyid = '{}'".format(pk))
            con.commit()
            pass
    print(args['condition'])
    
organization(args['condition'])

con.commit()
cursor.close() 
con.close()