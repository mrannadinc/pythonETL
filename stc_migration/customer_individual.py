import pgdb
import datetime
import json
import requests
import time
import conf
from awsglue.utils import getResolvedOptions
import sys
                                        

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def customer(condition,main):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    post = dict()
    successedrecordlist = []
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute("""select 
    c.name,
    c.status,
    c.customertype,
    c.segment,
    c.recommendedby,
    c.engagedpartyid,
    c.createdate,
    prt.dnextpartnerid,
    prt.name,
    p.dnextpartyroleid,
    p.fullname,
    c.subsegment,
    i.fullname,
    i.dnextindividualid,
    c.customerverification,
    c.recommendedfreetext,
    c.customerunificationid,
    c.masterrecordsflag, 
    c.createdby, 
    c.lastmodifiedby,
    c.customerid,
    c.dnextpostpaidaccountid,
    c.dnextprepaidaccountid,
    nonmaster.fullname
    from staging.customer c 
    left join staging.partyrole p on c.siebelid = p.siebelid 
    left join staging.partner prt on p.parent_id = prt.legacypartnerid
    left join staging.individual i on i.partyid = c.customerid
    left join staging.individual nonmaster on nonmaster.partyid = c.customerunificationid
    where 1=1
        and c.mig_flag = '1' 
        and individual_flag = 'individual' 
        and i.role is null
        and c.statusreason != 'not_migrate'
        and {}
    """.format(condition))
    for i in cursor.fetchall():
        pk = i[20]
        
        try:
            post = {
                "id": "F" + pk,
                "name": i[0],
                "status" : i[1],
                "contactMedium" : [],
                "account":[{
                    "id":i[21],
                    "href":"/api/accountManagement/v4/billingAccount/"+ i[21],
                    "name":"BillingAccount - Postpaid",
                    "@referredType": "BillingAccount"
                    },
                    {
                    "id":i[22],
                    "href":"/api/accountManagement/v4/billingAccount/"+ i[22],
                    "name":"BillingAccount - Prepaid",
                    "@referredType": "BillingAccount"
                }],
                "relatedParty":[]
            }
            
            post['characteristic'] = list()
                    
             
            if (i[11] != ""):
                post['characteristic'] = [{
                    'name':'subSegment',
                    'value':i[11],
                    'valueType':'string'
                }]
                
                
                
            if i[4] == None:
                recommendedby =""
            else:
                recommendedby = i[4]
            cdate = str(i[6]).replace(' ','T')+'Z'
                                               
                                      
                 
                                                                    
            
            customerunificationid = i[16]
            
            if i[14] == '1' :
                customerVerified = 'TRUE'
            else:
                customerVerified = 'FALSE'
                
            if i[19] == None:
                updatedbyinlegacy = ""
            else:
                updatedbyinlegacy = i[19]
                
            if i[18] == None:
                createdbyinlegacy = ""
            else:
                createdbyinlegacy = i[18]

            if i[23] != None and i[23] != '':
                partyname = i[23]
            else:
                partyname = i[0]
            
                
            if i[9] != None:
                post['relatedParty'].append(
                    {
                        "id": i[9],
                        "href": "/api/partyRoleManagement/v4/partyRole/"+i[9],
                        "name": i[10],
                        "role": "Sales Agent",
                        '@referredType':'PartyRole'
                    }
                )
            if i[7] != None:
                post['relatedParty'].append({
                        "id": i[7],
                        "href": "/api/partnershipManagement/v4/partner/"+i[7],
                        "name": i[8],
                        "role": "Sales Partner",
                        '@referredType':'Partner'        
                })
                
            post['externalReference'] = [{
                'externalReferenceType' :'migration',
                'id':pk,
                'name':'legacyNumber'
            }]
            
            post['characteristic'].append( {
                    'name' : 'customerNo',
                    'value':"F" + pk,
                    'valueType':'string'
                })
           
            post['characteristic'].append( {
                    'name' : 'customerVerified',
                    'value': customerVerified,
                    'valueType':'string'
                })
                
            if (cdate != "") and (cdate != None):
                post['validFor'] = {
                    'startDateTime' : cdate
                }
                
            post['contactMedium'].append({
                        'mediumType':'communicationMethod',
                        'characteristic':{
                            "contactType": "EMAIL_SMS"
                        }
                    })
                
                
            if (i[15] != None) and (i[15] != ""): # RecommendedText
                post['characteristic'].append( {
                    'name' : 'recommendedBy',
                    'value':i[15],
                    'valueType':'string'
                })
                
                
            if ( updatedbyinlegacy != ""):
                post['characteristic'].append({
                    'name':'updatedByInLegacy',
                    'value':updatedbyinlegacy,
                    'valueType':'string'
                })
                
            if ( createdbyinlegacy != ""):
                post['characteristic'].append({
                    'name':'createdByInLegancy',
                    'value':createdbyinlegacy,
                    'valueType':'string'
                })
                
            if ('EUR' in pk) or ('USD' in pk):
                
                post['characteristic'].append({
                    'name':"IsSplitCustomer",
                    'value':"True",
                    'valueType':'string'
                })
            
            
            post['engagedParty'] = {
                '@referredType':main.capitalize(),
                'id' : customerunificationid,
                'name': partyname,
                'href':'/api/partyManagement/v4/{}/'.format(main)+customerunificationid,
                '@baseType':'Individual',
                '@type' : 'Individual'
            }
            post['attachment'] = list()
            cursor.execute("""select 
            doc_name,file_type,file_name_original,doc_type,file_name_save,dnextattachmentid 
            from staging.docs where customerid = '{}' and doc_type not in ('Karte Identiteti/Pasaporte', 'NIPT') and dnextattachmentid is not null
            """.format(pk))                                                                                             #fiziksel dokümanların testi
            for i in cursor.fetchall():
                post['attachment'].append({
                    'description' : i[0],
                    'mimeType': i[1],
                    'name': i[2],
                    'attachmentType' : 'Document',
                    'documentType' : i[3],
                    'url' : i[4],
                    "href":"api/documentManagement/v4/attachment"+i[5],
                    "id":i[5]
                })
        
            cursor.execute("select mediumtype,preferred,city,contacttype,stateorprovince,street1,emailaddress,phonenumber,country,street2,dnextaddressid from staging.contactmedium where customerid = '{}'".format(pk))
            for i in cursor.fetchall():
                if i[5] == None:
                    street1 = ""
                else:
                    street1 = i[5]
                if i[9] == None:
                    street2 = "-"
                else:
                    street2 = i[9]
                if i[2] == None:
                    city = "-"
                else:
                    city = i[2]
                if i[1] == 'true':
                    pref = True
                else:
                    pref = False
        
                if i[3] == ('DEFAULT_ADDRESS') and (street1 != "") and (i[10] != None):
                    post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred': pref,
                        'characteristic':{
                            'contactType':"DEFAULT_SERVICE_ADDRESS",
                            'place':[{
                                'id': i[10],
                                'href':"/api/geographicAddressManagement/v4/geographicAddress/"+i[10],
                                'role':"DEFAULT_SERVICE_ADDRESS",
                                'name': "DEFAULT_SERVICE_ADDRESS"
                            }]
                        }
                    })
                    
                elif (i[3] in ('PERSONAL_EMAIL','WORK_EMAIL')) and (i[6] != '') and (i[6] != None):
                    post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            'emailAddress': i[6],
                            'contactType':i[3]
                        }
                    })
                
                elif (i[3] == 'OTHER_NUMBER') :
                    post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            "country": "+",
                            'phoneNumber': i[7],
                            'contactType': "FIX_NUMBER"
                        }
                    })
                
                elif  (i[3] == 'MOBILE_NUMBER'):
                    post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            "country": "+",
                            'phoneNumber': i[7],
                            'contactType':i[3]
                        }
                    })
                
            r = requests.post(conf.url['customer'],data = json.dumps(post,indent=3,default=str),headers=headers)
            print(post)
            if r.status_code != 201:
                cursor.execute("update staging.customer set statuscode = '{}' where customerid = '{}'".format(r.status_code,pk))
                con.commit()
                
                print("Customer Error : " + pk)
                print(r.text)
                print(post)
            
            else:
                successedrecordlist.append(pk)    
                
        except Exception as e:
            print("Exception WhenCreateIndividualCustomerPayload for : " + pk)
            print(e)
            cursor.execute("update staging.customer set statuscode = '434' where customerid = '{}'".format(pk))
            con.commit()
            pass
            
    print("Customer Success Records")
    print(successedrecordlist)
    print("Customer Success Records Count")
    print(len(successedrecordlist))
    print(args['condition'])
            
customer(args['condition'],'individual')    

con.commit()
cursor.close() 
con.close()