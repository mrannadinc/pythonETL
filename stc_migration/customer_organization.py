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
    trim(o.companyrepresentative),
    o.partyid,
    c.customerverification,
    c.recommendedfreetext,
    c.customerunificationid,
    c.masterrecordsflag,
    c.createdby, 
    c.lastmodifiedby,
    c.customerid,
    nonmaster.name,
    o.companyrepresentative_identnumber,
    acc.siebelid,
    acc.fullname
    from staging.customer c left join staging.partyrole p on c.siebelid = p.siebelid 
    left join staging.partner prt on p.parent_id = prt.legacypartnerid
    left join staging.organization o on o.partyid = c.customerid
    left join staging.organization nonmaster on nonmaster.partyid = c.customerunificationid
    left join staging.account_manager acc on acc.customerid = c.customerid
    where (c.individual_flag = 'organization')  
    and (c.mig_flag = '1')  
    and (o.role is null)
    and c.statusreason != 'not_migrate'
    and {}
    """.format(condition))
    
    for i in cursor.fetchall():
        pk = i[20]    
        try:
            post['id'] = "F" + pk
            post['relatedParty'] = []
            post['name'] = i[0]
            post['status'] = i[1]
            post['contactMedium'] = []
            post['account'] =[{
                "id":"Post_" + pk,
                "href":"/api/accountManagement/v4/billingAccount/"+"Post_" +pk,
                "name":"BillingAccount - Postpaid",
                "@referredType": "BillingAccount"
                },
                {
                "id":"Pre_" + pk,
                "href":"/api/accountManagement/v4/billingAccount/"+"Pre_" +pk,
                "name":"BillingAccount - Prepaid",
                "@referredType": "BillingAccount"
            }]
    
            if i[4] == None:
                recommendedby =""
            else:
                recommendedby = i[4]
                
            if i[9] == None:
                salesagentid = ""
            else:
                salesagentid = i[9]
            if i[7] == None:
                posid = ""
            else:
                posid = i[7]
            
            cdate = str(i[6]).replace(' ','T')+'Z'
                                      
                 
                                                                    
            
            customerunificationid = i[16]
            
            if i[21] != None and i[21] != '' :
                partyname = i[21]
            else:
                partyname = i[0]
                
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
                
            '''    
            try:
                post['relatedParty'] = [
                    {
                        "id":roleid[0],
                        "href":"/api/partyRoleManagement/v4/partyRole/"+roleid[0],
                        "name":rolename[0],
                        "role":"Credit_Team",
                        "@referredType":"PartyRole"               
                    },
                    {
                        "id":roleid[1],
                        "href":"/api/partyRoleManagement/v4/partyRole/"+roleid[1],
                        "name":rolename[1],
                        "role":"FirstClass_Responsible",
                        "@referredType":"PartyRole"
                    },
                    {
                        "id":roleid[2],
                        "href":"/api/partyRoleManagement/v4/partyRole/"+roleid[2],
                        "name":rolename[2],
                        "role":"Account_Manager",
                        "@referredType":"PartyRole"            
                    }
                    ]
            except:
                post['relatedParty'] = []
            '''
            
            post['engagedParty'] = {
                '@referredType':main.capitalize(),
                'id' : customerunificationid,
                'name': partyname,
                'href':'/api/partyManagement/v4/{}/'.format(main)+customerunificationid,
                '@type' : main.capitalize()
            }
            
            if (i[13] != None) and (i[12] != None) and (i[12] != "") and (i[22] != None) and (i[22] != "") and (i[22] != "d41d8cd98f00b204e9800998ecf8427e"):
                post['relatedParty'].append({
                    "id":"A_" + i[13],
                    "href":"/api/partyRoleManagement/v4/partyRole/A_"+i[13],
                    "name":i[12],
                    "role":"Admin",
                    "@referredType":"PartyRole"                
                })
            if salesagentid != "":
                post['relatedParty'].append(
                    {
                        "id": salesagentid,
                        "href": "/api/partyRoleManagement/v4/partyRole/"+salesagentid,
                        "name": i[10],
                        "role": "Sales Agent",
                        '@referredType':'PartyRole'
                    }
                )
                
            if (i[23] != "") and (i[23] != None) and (i[24] != "") and (i[24] != None):
                post['relatedParty'].append(
                    {
                        "id": "Acc_" + i[23],
                        "href": "/api/partyRoleManagement/v4/partyRole/Acc_"+i[23],
                        "name": i[24],
                        "role": "Account Manager",
                        '@referredType':'PartyRole'
                    }
                )    
            if posid != "":
                post['relatedParty'].append({
                        "id": i[7],
                        "href": "/api/partnershipManagement/v4/partner/"+i[7],
                        "name": i[8],
                        "role": "Sales Partner",
                        '@referredType':'Partner'        
                })
                
            post['contactMedium'].append({
                        'mediumType':'communicationMethod',
                        'characteristic':{
                            "contactType": "EMAIL_SMS"
                        }
                    })
                    
            post['externalReference'] = [{
                'externalReferenceType' :'migration',
                'id':pk,
                'name':'legacyNumber'
            }]
            post['characteristic'] = list()
            
            post['characteristic'].append( {
                    'name' : 'customerNo',
                    'value':"F" + pk,
                    'valueType':'string'
                })
        
            post['characteristic'].append( {
                    'name' : 'customerVerified',
                    'value':customerVerified,
                    'valueType':'string'
                })
            if (i[15] != None) and (i[15] != ""): # RecommendedText
                post['characteristic'].append( {
                    'name' : 'recommendedBy',
                    'value':i[15],
                    'valueType':'string'
                })
                
            if (i[11] != None) and (i[11] != ""):
                post['characteristic'].append( {
                        'name' : 'subSegment',
                        'value':i[11],
                        'valueType':'string'
                    })
                    
            if (cdate != "") and (cdate != None):
                post['validFor'] = {
                    'startDateTime' : cdate
                }
                
            if ( updatedbyinlegacy != ""):
                post['characteristic'].append({
                        'name':'updatedByInLegacy',
                        'value':updatedbyinlegacy,
                        'valueType':'string'
                    })
                
            if ( createdbyinlegacy != ""):
                post['characteristic'].append({
                        'name':'createdByInLegacy',
                        'value':createdbyinlegacy,
                        'valueType':'string'
                    })
                
            if ('EUR' in pk) or ('USD' in pk):
                post['characteristic'].append({
                    'name':"IsSplitCustomer",
                    'value':"True",
                    'valueType':'string'
                })   
                
            cursor.execute("select mediumtype,preferred,city,contacttype,country,stateorprovince,street1,emailaddress,phonenumber,street2,dnextaddressid from staging.contactmedium where (contacttype != 'SERVICE_ADDRESS') and customerid = '{}'".format(pk))
            for i in cursor.fetchall():
                if i[6] == None:
                    street1 = ""
                else:
                    street1 = i[6]
                if i[9] == None:
                    street2 = ""
                else:
                    street2 = i[9]
                if i[2] == None:
                    city = ""
                else:
                    city = i[2]
                if i[1] == 'true':
                    pref = True
                else:
                    pref = False
                if i[4] == None:
                    country = ""
                else:
                    country = i[4]
        
        
                if (i[3] == 'DEFAULT_ADDRESS') and (street1 != "") and (i[10] != None): 
                  post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred': pref,
                        'characteristic':{
                            'contactType':"DEFAULT_SERVICE_ADDRESS",
                            'place':[{
                                "id":i[10],
                                "href":"/api/geographicAddressManagement/v4/geographicAddress/"+i[10],
                                "name":"DEFAULT_SERVICE_ADDRESS",
                                "role":"DEFAULT_SERVICE_ADDRESS"
                            }]
                        }
                    })
                
                elif (((i[7] != '') and (i[7] != None)) and ((i[3] == 'WORK_EMAIL') or (i[3] == 'PERSONAL_EMAIL'))):
                    post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            'emailAddress': i[7],
                            'contactType':i[3]
                        }
                    })
                    
                elif (i[3] == 'OTHER_NUMBER') :
                    post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            "country": "+",
                            'phoneNumber': i[8],
                            'contactType': "FIX_NUMBER"
                        }
                    })
                    
                elif  (i[3] == 'MOBILE_NUMBER'):
                    post['contactMedium'].append({
                        'mediumType':i[0],
                        'preferred':pref,
                        'characteristic':{
                            "country": "+",
                            'phoneNumber': i[8],
                            'contactType':i[3]
                        }
                })
        
            post['attachment'] = list()
            
            try:
                cursor.execute("""
                select 
                doc_name,
                file_type,
                file_name_original,
                doc_type,
                file_name_save,
                dnextattachmentid 
                from staging.docs 
                where dnextattachmentid is not null
                and doc_type not in ('Karte Identiteti/Pasaporte', 'NIPT') 
                and customerid = '{}' 
                """.format(pk))  
                for i in cursor.fetchall():
                    post['attachment'].append({
                        'id':i[5],
                        'href':"api/documentManagement/v4/attachment/"+i[5],
                        'description' : i[0],
                        'mimeType': i[1],
                        'name': i[2],
                        'attachmentType' : "Document",
                        'documentType' : i[3],
                        'url' : i[4]
                    })
            except Exception as e:
                print("except Customer Attachment")
                print(e)
                pass
            
            #print(post)
            r = requests.post(conf.url['customer'],data = json.dumps(post,indent=3,default=str),headers=headers)
            
            
            if r.status_code != 201:
                cursor.execute("update staging.customer set statuscode = '{}' where customerid = '{}'".format(r.status_code,pk))
                con.commit()
                print("Customer Organization error : " + pk)
                print(r.text)
                print(post)
            
            else:
                successedrecordlist.append(pk)
            
        except Exception as e:
            print("Exception WhenCreateOrganizationCustomerPayload for: " + pk)
            print(e)
            cursor.execute("update staging.customer set statuscode = '434' where customerid = '{}'".format(pk))
            con.commit()
            pass
        
    print("Customer Success Records")
    print(successedrecordlist)
    print("Customer Success Records Count")
    print(len(successedrecordlist))
    print(args['condition'])
            
customer(args['condition'],'organization')

con.commit()
cursor.close() 
con.close()        