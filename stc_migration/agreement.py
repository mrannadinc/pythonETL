import pgdb
import datetime
import json
import requests
import time
import conf
from awsglue.utils import getResolvedOptions
import sys
from datetime import datetime, timedelta
from dateutil import relativedelta

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def agreement(condition):
    successedrecordlist = []
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute("""
        select 
        a.type,
        case when (a.productcount is null 
                  and a.brm_orderdate is not null 
                  and a.status = 'Active' 
                  and a.type = 'Cash') then 'terminated' else a.status end dnext_status,
        c.customerid,
        a.startdatetime,
        a.enddatetime,
        c.name,
        a.agreementtype,
        case when a.duration is null then '1' else a.duration end duration,
        a.autorenewalflag,
        a.agreementperiod,
        a.createdbyname,
        a.createdbysiebelid,
        a.agreementid, --12
        a.ratingtype,
        a.address,
        a.createdby,
        a.lastmodifiedby,
        case when (a.productcount is null 
                  and a.brm_orderdate is not null 
                  and a.status = 'Active' 
                  and a.type = 'Cash') then '1' else '0' end terminated_before_end_date_flag,
        pstartdate.productterminationdate,
        a.source,
        cm.street1 || cm.street2 manualOrderAdreess
        from staging.agreement a
        left join staging.customer c on a.customerid = c.customerid
        left join (
		select agreementid,
		    max(terminationdate) productterminationdate
		    from staging.product where prod_mig_flag = '1'
		    group by agreementid) pstartdate on pstartdate.agreementid = a.agreementid
        left join staging.contactmedium cm on c.customerid = cm.customerid
        where c.mig_flag ='1' 
        and (
            (a.productcount is not null) or 
            (a.productcount is null and a.brm_orderdate is not null) or 
            (a.type = 'Commitment' and a.status = 'Active')
            )
        and cm.contacttype = 'DEFAULT_ADDRESS'
        and cm.role not in ('shop','Vodafone Employee')
        and {}
        """.format(condition))
    for i in cursor.fetchall():
        pk = i[12]
        agreementname = i[7] + "Month" + i[0]
        if i[8] == "1":
            flag = True
        else:
            flag = False
            
        try:
            if (i[18] != None) and (i[18] != "") :
                productterminationdate = datetime.strptime(i[18], '%Y-%m-%d %H:%M:%S')
                
            if (i[4] != None) and (i[4] != "") :
                endDateTime = datetime.strptime(i[4], '%Y-%m-%d %H:%M:%S')
                
            if (i[18] == None) or (i[18] == "") or (i[4] == None) or (i[4] == ""):
                remainingPaymentCycle = 0
            else:
                delta = relativedelta.relativedelta(endDateTime, productterminationdate)
                remainingPaymentCycle = delta.months + (delta.years * 12)
                if remainingPaymentCycle > 0 :
                    remainingPaymentCycle = remainingPaymentCycle - 1
                
            a = {
                "id":pk,
                "agreementType":i[0],
                "status":i[1],
                'name':agreementname,
                'initialDate':stringToTimestamp(i[3]),
                "agreementPeriod":{
                    "startDateTime":stringToTimestamp(i[3]),
                    "endDateTime":stringToTimestamp(i[4])
                },
                "agreementItem":[{
                    "product": [],
                    "productOffering": [],
                    "termOrCondition": [],
                    "externalReference": [],
                    "@baseType": "Agreement",
                    "@type": "Commercial"
                }],
                "agreementAuthorization":[{
                    "date":stringToTimestamp(i[3]),
                    "signatureRepresentation":"physical",
                    "state":"approved"
                }],
                'engagedParty':[{
                    'id': "F" +i[2], 
                    'href':'/api/customerManagement/v4/customer/'+ "F" +i[2],
                    'name': i[5], 
                    'role':'Customer',
                    '@referredType':'Customer'
                }],
                'externalReference':[{
                    'id':pk,
                    'name':'legacyNumber',
                    'externalReferenceType':'migration'
                }],
                "characteristic":[
                {
                    "name":"autoRenewal",
                    "value":flag,
                    "valueType":"Boolean"
                },
                {
                    "name":"commitmentUnit",
                    "value":"Month",
                    "valueType":"string"
                },
                {
                    "name":"ratingType",
                    "value":i[13],
                    "valueType":"string"
                },
                {
                    "name":"remainingPaymentCycle",
                    "value":remainingPaymentCycle,
                    "valueType":"float"
                }
                ]
            }
            
            if i[1] == 'terminated':
                
                if i[19] == 'STB_VirtualAgreement':
                    now = datetime.now()
                    agreementterminationdate = now.strftime("%Y-%m-%d %H:%M:%S")
                    agreementterminationdate = str(agreementterminationdate).replace(' ','T')+'Z'
                    
                    a['characteristic'].append({
                        "name":"terminationDate",
                        "value":agreementterminationdate,
                        "valueType":"string"
                    })
                
                elif i[17] == '1':
                    now = datetime.now()
                    agreementterminationdate = now.strftime("%Y-%m-%d %H:%M:%S")
                    agreementterminationdate = str(agreementterminationdate).replace(' ','T')+'Z'
                    
                    a['characteristic'].append({
                        "name":"terminationDate",
                        "value":agreementterminationdate,
                        "valueType":"string"
                    })
                    
                elif i[17] == '0':
                    a['characteristic'].append({
                        "name":"terminationDate",
                        "value":stringToTimestamp(i[4]),
                        "valueType":"string"
                    })
            
            if i[7] != None and i[0] == "Commitment":
                a['characteristic'].append({
                    "name":"commitmentAmount",
                    "value":i[7],
                    "valueType":"string"
                })
                
            if  i[7] != None and i[0] == "Cash":
                a['characteristic'].append({
                    "name":"commitmentAmount",
                    "value":"-1",
                    "valueType":"string"
                })
                
            if i[14] != None:
                a['characteristic'].append({
                    "name":"address",
                    "value":i[14],
                    "valueType":"string"
                })
            
            elif i[20] != None:
                a['characteristic'].append({
                    "name":"address",
                    "value":i[20],
                    "valueType":"string"
                })
            
            
            if i[15] != None:
                a['characteristic'].append({
                        "name":"createdByInLegancy",
                        "value":i[15],
                        "valueType":"string"
                    })
            
            if i[16] != None:    
                a['characteristic'].append({
                        "name":"updatedByInLegacy",
                        "value":i[16],
                        "valueType":"string"
                    })
            
            if i[12].find('Pr') != -1 :
                a['characteristic'].append({
                    "name":"isManualorder",
                    "value" : "true",
                    "valueType":"string"
                })
                
            # Henüz API kabul etmiyo. Duruma göre bakılacak
            """
            if i[11] != None:
                a['relatedParty'] = [{
                    "id": i[11],
                    'href':"/api/partyRoleManagement/v4/partyRole/"+i[11],
                    "name":i[10],
                    "role":"salesAgent",
                    "@referredType":"PartyRole"      
                }]
            """
            
            r = requests.post(conf.url['agreement'],data = json.dumps(a,indent=3,default=str),headers=headers)
            
            if r.status_code != 201:
                print("Agreement Payload text : " + pk)
                print(a)        
                print("Agreement Status Code : ", r.status_code)
                
                cursor.execute("update staging.agreement set statuscode = '{}' where agreementid = '{}'".format(r.status_code,pk))
                con.commit()
                
                print("Agreement Error : " + pk)
                print(r.text)
            else:
                successedrecordlist.append(pk)
        
        except Exception as e:
            print("Exception WhenCreateAgreementPayload for: " + pk)
            print(e)
            cursor.execute("update staging.agreement set statuscode = '434' where agreementid='{}'".format(pk))
            con.commit()
            pass

    print("Agreement Success Records")
    print(successedrecordlist)
    print("Agreement Success Records Count")
    print(len(successedrecordlist))
    print(args['condition'])
    
def stringToTimestamp(val):
    if((val == None) or (val == "")):
        return "1800-01-01T00:00:00.000Z"
    else:
        val = str(val).replace(' ','T')+'.000Z'
        return val    

agreement(args['condition'])

con.commit()
cursor.close() 
con.close()
