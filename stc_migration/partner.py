import pgdb
import datetime
import json
import requests
import time
import conf

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()
cursor2 = con.cursor()

def partner():
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    successedrecordlist = []
    
    cursor.execute(f"""
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
    	pp.name
    from staging.partner p
	inner join staging.organization o on o.partyid = p.partnerid
	left join staging.partner pp on p.parentid = pp.partnerid
	""")
    for i in cursor.fetchall():
        pk = i[7]
        legacypartnerid = i[6]
        
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
            
            
            if i[12] != 'None' and i[12] != '':
                test['partner'][0]['relatedParty'] = [
                    {
                            "id" : i[12],
                            'href':'/api/partnershipManagement/v4/partnership/'+i[12],
                            'name' : i[13],
                            '@referredType':'Partner',
                            "role": "Dealer"
                    }]
                
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
                "name":"shopLocation",
                "value":shopLocation,
                "valueType":"string"
                })
                
            test['partner'][0]['characteristic'].append({
                "name":"BusinUnitCode",
                "value":BusinUnitCode,
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
                            "contactType":"DEFAULT_SERVICE_ADDRESS", # Sorulacak
                            "city": i[3],
                            "country": i[4],
                            "postCode": postcode,
                            "stateOrProvince":nullCheck(i[6]),
                            "street1":i[7],
                            "street2":street2
                        }
                    })
                elif(i[0] in ('PERSONAL_EMAIL','WORK_EMAIL')) and i[9] != None and i[9] != "":
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "contactType":i[0],            # Sorulacak
                            "emailAddress":i[9]
                        }
                    })
                elif (i[0] == 'OTHER_NUMBER') or (i[0] == 'MOBILE_NUMBER'):
                    test['partner'][0]['contactMedium'].append({
                        "mediumType":i[1],
                        "preferred" : pref,
                        "characteristic":{
                            "country":nullCheck(i[4]),      # Sorulacak
                            "contactType":i[0],            # Sorulacak
                            "phoneNumber":i[10]
                        }
                    })
            
            r = requests.post(conf.url['partner'], data=json.dumps(test,indent=3,default=str), headers=headers)
            
            if r.status_code != 201:
                cursor.execute("update staging.partner set statuscode = '{}' where partnerid = '{}'".format(r.status_code,pk))
                print("Partner Error : " + pk)
                print(r.text)
                con.commit()
            
            else:
                successedrecordlist.append(pk)
                
        except Exception as e:
            print("Exception WhenCreatePartnerPayload for: " + pk)
            print(e)
            cursor.execute("update staging.partner set statuscode = '425' where partnerid='{}'".format(pk))
            con.commit()
            pass
        
    print("Partner Success Records")
    print(successedrecordlist)
    print("Partner Success Records Count")
    print(len(successedrecordlist))    


def nullCheck(val):
    if val == None:
        return ""
    else:
        return val

partner()


con.commit()
cursor.close() 
cursor2.close() 
con.close()