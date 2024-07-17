from numpy import product
import pgdb
import datetime
import json
import requests
import time
import conf
from awsglue.utils import getResolvedOptions
import sys
from datetime import datetime,timedelta

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()
resourcecursor = con.cursor()

def productOrder(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute(f"""select distinct 
    p.agreementid,
    min(p.startdate),
    min(a.duration),
    min(a.type),
    min(c.customerid),
    min(c.name),
    min(p.createdbysiebelid),
    min(p.createdbyname),
	min(prt.dnextpartnerid),
	min(prt.name),
	min(p.productid)
    from staging.product p
    	left join staging.customer c on c.customerid = p.customerid
    	inner join staging.agreement a on p.agreementid = a.agreementid
        left join staging.partyrole prole on prole.siebelid = c.siebelid
    	left join staging.partner prt on prt.legacypartnerid = prole.parent_id
    	inner join staging.resource r on r.subscriberaccountkey = p.subscriberaccountkey
        where 1 = 1
        and r.migflag = '1' 
        and c.customerid || a.agreementid || p.legacyproductofferingid in (
            select 
            	distinct c.customerid || a.agreementid || p.legacyproductofferingid
            from staging.agreement a
            	inner join staging.customer c on a.customerid = c.customerid
            	left join staging.product p on a.agreementid = p.agreementid
            where 1 = 1
                and c.mig_flag = '1'
                and {condition}
            	and p.prod_mig_flag = '1'
            	and p.status = 'pendingActive'
            except
            select 
            	distinct c.customerid || a.agreementid || p.legacyproductofferingid
            from staging.agreement a
            	inner join staging.customer c on a.customerid = c.customerid
            	left join staging.product p on a.agreementid = p.agreementid
            where 1 = 1
                and c.mig_flag = '1'
                and {condition}
                and p.prod_mig_flag = '1'
            	and p.status != 'pendingActive'
            )
        and p.productid != '20269110'
        group by p.agreementid,p.legacyproductofferingid
    """)
    #pendingActive product except active product(type2 except type1)
    
    for a in cursor.fetchall():
        record_pk = a[10]
        aggid = a[0]
        
        try:
            agreementname = a[2] + "Month" + a[3]
            
            prod = {
                "id":record_pk,
                "description":"migration-record",
                "requestedStartDate":stringToTimestamp(a[1]),
                "agreement":[{
                    "id" : aggid,
                    "href" : "/api/agreementManagement/v4/agreement/"+aggid,
                    "name" : agreementname,
                    "@referredType" : "Agreement"
                }],
                "billingAccount":{
                    "id" : aggid,
                    "href" : "/api/accountManagement/v4/billingAccount/"+aggid,
                    "name" : "migration"
                },
                "relatedParty":[{
                    "id" : "F" + a[4],
                    "href" : "/api/customerManagement/v4/customer/F"+a[4],
                    "name" : a[5],
                    "role" : "Customer",
                    "@referredType" : "Customer"
                }],
                "productOrderItem":[]        
            }    
            
    
            if a[6] != None:
                prod['relatedParty'].append({
                    "id": a[6],
                    'href':"/api/partyRoleManagement/v4/partyRole/"+a[6],
                    "name":a[7],
                    "role":"Sales Agent",
                    "@referredType":"PartyRole"
                })
            if a[8] != None:
                prod['relatedParty'].append({
                    "id": a[8],
                    'href':"/api/partnershipManagement/v4/partner/"+a[8],
                    "name":a[9],
                    "role":"Sales Partner",
                    "@referredType":"Partnership"
                })
            
            cursor.execute(f"""select 
                    p.isbundle,
                    ps.dnextproductofferingname,	
                    p.updatedate,
                    p.status,
                    p.agreementid,
                    c.customerid,
                    c.name,
                    ps.dnextproductofferingid,
                    p.subscriberaccountkey,
                    p.legacyproductofferingid,
                    p.startdate,                            --10
                    p.terminationdate,
                    c.dnextbillingid,
                    COALESCE (CAST(p.sumprice as real),0 ) sumprice,
                    p.createdate,
                    case when (ctl.oss_code is null or trim(ctl.oss_code) = '')  then ' ' else ctl.oss_code end oss_code,
                    p.createdbyname,
                    p.createdbysiebelid,
                    p.lastmodifiedbyname,
                    p.lastmodifiedbysiebelid,
                    COALESCE (CAST(p.vatprice as real),0 ) vatprice,  --20
                    p.dnextbundleid,
                    p.dnextbundlename,
                    prt.dnextpartnerid,
                    prt.name,
                    ctl.article_number,
                    r.resourceid,                        --26
                    r.dnextserviceid,
                    r.service,
                    r.agent,
                    p.staticip,  --30
                    case when (ps.erpglcode is null or trim(ps.erpglcode) = '') then ' ' else ps.erpglcode end erpglcode,
                    case when (p.macaddress is null or trim(p.macaddress) = '') then ' ' else p.macaddress end macaddress,
                    case when (r.serviceaccount is null or trim(r.serviceaccount) = '') then ' ' else r.serviceaccount end serviceaccount,
                    p.staticip,
                    case when (ps.downloadspeed is null or trim(ps.downloadspeed) = '') then ' ' else ps.downloadspeed end downloadspeed,
                    case when (r.legacyresourcename is null or trim(r.legacyresourcename) = '') then ' ' else r.legacyresourcename end legacyresourcename,
                    case when (ps.uploadspeed is null or trim(ps.uploadspeed) = '') then ' ' else ps.uploadspeed end uploadspeed,
                    case when (ps.brandname is null or trim(ps.brandname) = '') then ' ' else ps.brandname end brandname,
                    case when (r.returnvalue is null or trim(r.returnvalue) = '') then ' ' else r.returnvalue end returnvalue,
                    case when (p.internetinfrastructure is null or trim(p.internetinfrastructure) = '') then ' ' else p.internetinfrastructure end internetinfrastructure,  --40
                    ps.specificationid,			
                    ps.specificationname,
                    case when (r.osssystem is null or trim(r.osssystem) = '') then ' ' else r.osssystem end osssystem,
                    case when (r.grtype is null or trim(r.grtype) = '') then ' ' else r.grtype end grtype,
                    case when (p.serviceaccount is null or trim(p.serviceaccount) = '') then ' ' else p.serviceaccount end productserviceaccount,
                    p.invoiceid,
                    case when (r.internetpolicy is null or trim(r.internetpolicy) = '') then ' ' else r.internetpolicy end internetpolicy,
                    p.legacyproductofferingid,
                    prole.siebelid,
                    prole.firstname || ' ' || prole.middlename || ' ' || prole.lastname as partyrole_name,          --50
                    a.type,
                    a.duration, --52
                    p.bundletype,
                    p.masterproductoffering,
                    r.accountlink,
                    r.password,
                    r.subscriberaccount,
                    a.dnextbillingid,
                    a.contractcurrency,
                    p.productid,  --60
                    cm.contacttype,
                    cm.dnextaddressid,
                    p.dnextpaymentid,
                    case when p.legacyproductofferingid like 'DISC%' then 'Product'
                         when (ps.spectype is null or trim(ps.spectype) = '') then ' ' else ps.spectype end spectype,  --64
                    case when p.legacyproductofferingid like 'DISC%' then 'Discount'
                         when (ps.specsubtype is null or trim(ps.specsubtype) = '') then ' ' else ps.specsubtype end specsubtype,  --65
                    case when (ps.postpaidtype is null or trim(ps.postpaidtype) = '') then ' ' else ps.postpaidtype end postpaidtype,
                    a.ratingtype,
                    case
                        when p.legacyproductofferingid like 'DISC%' then '{conf.discountbrmproductid_all}'
                        when (ps.brmproductid_all is null or trim(ps.brmproductid_all) = '') then ' ' else ps.brmproductid_all 
                    end brmproductid_all,   --68
                    case when (ps.brmproductid_eur is null or trim(ps.brmproductid_eur) = '') then ' ' else ps.brmproductid_eur end brmproductid_eur,  --69
                    case when (ps.brmproductid_usd is null or trim(ps.brmproductid_usd) = '') then ' ' else ps.brmproductid_usd end brmproductid_usd,  --70
                    case when (ps.brmservicetype is null or trim(ps.brmservicetype) = '') then ' ' else ps.brmservicetype end brmservicetype,
                    case when (r.sla is null or trim(r.sla) = '') then ' ' else r.sla end sla,
                    ps.fup,
                    case when (ps.cryptoguardsubscriptionpackageid is null or trim(ps.cryptoguardsubscriptionpackageid) = '') then ' ' else ps.cryptoguardsubscriptionpackageid end cryptoguardsubscriptionpackageid,
                    case when (ps.abelproductidentifier is null or trim(ps.abelproductidentifier) = '') then ' ' else ps.abelproductidentifier end abelproductidentifier,
                    case when (ps.glcode is null or trim(ps.glcode) = '')  then ' ' else ps.glcode end glcode,
                    r.fupflag,
                    case when (r.guaranteecredit is null or trim(r.guaranteecredit) = '') then ' ' else r.guaranteecredit end guaranteecredit,
                    case when (ps.devicetype is null or trim(ps.devicetype) = '') then ' ' else ps.devicetype end devicetype,
                    case when (ps.casid is null or trim(ps.casid) = '') then ' ' else ps.casid end casid, --80
                    case when (ctl.kaltura_subscription_id is null or trim(ctl.kaltura_subscription_id) = '') then ' ' else ctl.kaltura_subscription_id end kaltura_subscription_id,
                    case when (ps.portabillingplanid is null or trim(ps.portabillingplanid) = '') then ' ' else ps.portabillingplanid end portabillingplanid,
                    case when (ps.zonetype is null or trim(ps.zonetype) = '') then ' ' else ps.zonetype end zonetype,
                    case when (ps.creditlimit is null or trim(ps.creditlimit) = '') then ' ' else ps.creditlimit end creditlimit,
                    case when (ps.crmservicetype is null or trim(ps.crmservicetype) = '') then ' ' else ps.crmservicetype end crmservicetype,
                    case when (ps.fixnumber is null or trim(ps.fixnumber) = '') then ' ' else ps.fixnumber end fixnumber,
                    case when (ps.brmproductidchild_all is null or trim(ps.brmproductidchild_all) = '') then ' ' else ps.brmproductidchild_all end brmproductidchild_all,
                    case when (ps.brmproductidchild_eur is null or trim(ps.brmproductidchild_eur) = '') then ' ' else ps.brmproductidchild_eur end brmproductidchild_eur,
                    case when (ps.brmproductidchild_usd is null or trim(ps.brmproductidchild_usd) = '') then ' ' else ps.brmproductidchild_usd end brmproductidchild_usd,
                    case when (ps.brmservicetypechild is null or trim(ps.brmservicetypechild) = '') then ' ' else ps.brmservicetypechild end brmservicetypechild, --90
                    case when (ps.grinfo is null or trim(ps.grinfo) = '') then ' ' else ps.grinfo end grinfo,
                    case when (cry.encrypted is null or trim(cry.encrypted) = '') then ' ' else cry.encrypted end encrypted2,
                    r.dnextproductofferingid,
                    r.dnextproductofferingname,
                    r.dnextproductspecificationid,
                    r.dnextproductspecificationname,
                    case when resourceProduct.resourceid is not null then '1' else '0' end already_created_resourceProduct --97
                    from staging.product p
                        left join staging.customer c on c.customerid = p.customerid
                        inner join staging.agreement a on p.agreementid = a.agreementid
                        left join staging.partyrole prole on prole.siebelid = c.siebelid
                        left join staging.partner prt on prt.legacypartnerid = prole.parent_id
                        left join staging.contactmedium cm on cm.contactmediumid = a.contactmediumid
                        inner join staging.resource r on r.subscriberaccountkey = p.subscriberaccountkey
                        left join staging.crypto cry on cry.id = r.resourceid
                        left join staging.catalogue ctl on upper(p.updated_productofferingid) = upper(ctl.code)
                        left join staging.productoffering po on updated_productofferingid || a.producttype = po.legacyofferingid_joinkey
                        left join staging.product_specs ps on po.dnextproductofferingid = ps.dnextproductofferingid
                        left join (select distinct r.resourceid from staging.resource r
                            inner join staging.customer c on c.customerid = r.customerid
                            left join staging.product p on r.subscriberaccountkey = p.subscriberaccountkey
                            inner join staging.agreement a on p.agreementid = a.agreementid
                            left join staging.catalogue ctl on upper(p.updated_productofferingid) = upper(ctl.code)
                            left join staging.product_specs ps on r.dnextproductofferingid = ps.dnextproductofferingid
                            left join staging.crypto cry on cry.id = r.resourceid
                        where 1 = 1
                            and c.mig_flag = '1'
                            and p.prod_mig_flag = '1'
                            and r.migflag = '1'
                            and ((p.status = 'terminated' and a.type = 'Commitment' and a.status = 'Active') or (p.status = 'active' and a.productcount is not null and p.resourceproduct_migflag = '1'))
                            and p.legacyproductofferingid != 'SETUP'
                            and ps.dnextproductofferingid is not null
                        ) resourceProduct on r.resourceid = resourceProduct.resourceid
                        where 1 = 1
                        and r.migflag = '1' 
                        and c.customerid || a.agreementid || p.legacyproductofferingid in (
                            select 
                                distinct c.customerid || a.agreementid || p.legacyproductofferingid
                            from staging.agreement a
                                inner join staging.customer c on a.customerid = c.customerid
                                left join staging.product p on a.agreementid = p.agreementid
                            where 1 = 1
                                and c.mig_flag = '1'
                                and {condition}
                                and p.prod_mig_flag = '1'
                                and p.status = 'pendingActive'
                            except
                            select 
                                distinct c.customerid || a.agreementid || p.legacyproductofferingid
                            from staging.agreement a
                                inner join staging.customer c on a.customerid = c.customerid
                                left join staging.product p on a.agreementid = p.agreementid
                            where 1 = 1
                                and c.mig_flag = '1'
                                and {condition}
                                and p.prod_mig_flag = '1'
                                and p.status != 'pendingActive'
                            )
                        and a.agreementid = '{aggid}'
                        --and p.productid = '20410929'
                        """)
            
            
            for i in cursor.fetchall():
                counter = len(prod["productOrderItem"])
                pk = i[60]
                
                ctlcode = i[48]
                aggid = i[4]
                
                legacyprodof = i[9]
                subscriberac = i[8]
                legacyofferingid = i[9]
                p_resourceid = i[26]
                    
                if (i[7] == None) or (i[7] == ""):
                    dnextofferid = "tbd-dnextproductofferingid"
                    dnextoffername = "tbd-dnextproductofferingname"
                else:
                    dnextofferid = i[7]
                    dnextoffername = i[1]
            
                if (i[41] == None) or (i[41] == ''):
                    productspecid = "tbd-productspecid"
                    productspecname = "tbd-productspecname"
                else:
                    productspecid = i[41]
                    productspecname = i[42]
                
                if i[13] == i[20]:
                    taxrate = 0
                else:
                    taxrate = 20
                
                if (i[73] == None) :
                    FUP = " "
                else:
                    FUP = i[73]
                    
                productorderdate = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                productorderdate = str(productorderdate).replace(' ','T')+'Z'
                
                prod['productOrderItem'].append({
                    "@type" : "ProductOrderItem",
                    "id" : pk +"c",
                    "quantity" : 1,                
                    "action" : "add",
                    "itemPrice" : [{
                        "name" : i[1],
                        "priceType" : "Recurring charge", 
                        "recurringChargePeriod" : "monthly",
                        "price" : {
                            "taxRate" : taxrate,
                            "dutyFreeAmount" : {
                                "unit" : i[59],
                                "value" : float(i[20])
                            },
                            "taxIncludedAmount" : {
                                "unit" : i[59],
                                "value" : float(i[13])
                            }
                        }
                    }],
                    "itemTotalPrice" : [{
                        "priceType" : "Recurring charge",
                        "recurringChargePeriod" : "monthly",
                        "price" : {
                            "taxRate" : taxrate,
                            "dutyFreeAmount" : {
                                "unit" : i[59],
                                "value" : float(i[20])
                            },
                            "taxIncludedAmount" : {
                                "unit" : i[59],
                                "value" : float(i[13])
                            }                        
                        }
                    }],
                    "payment":[],
                    "product":{
                        "isBundle" : False,
                        "name" : dnextoffername,
                        "agreement":[{
                            "id" : aggid,
                            "href" : "/api/agreementManagement/v4/agreement/"+aggid,
                            "name" : agreementname,
                            "@referredType" : "Agreement"
                        }],
                        "billingAccount":{
                            "id" : i[58],
                            "href" : "/api/accountManagement/v4/billingAccount/"+i[58],
                            "name" : "migration"
                        },
                        "relatedParty":[{
                            "id": "F" + i[5],
                            'href':"/api/customerManagement/v4/customer/F"+i[5],
                            "name":i[6],
                            "role":"Customer",
                            "@referredType":"Customer"
                        }],
                        "place" : [],
                        "productCharacteristic":[],
                        "productTerm":[],
                        "productPrice":[{
                            "priceType":"Recurring charge",               
                            "recurringChargePeriod":"monthly",
                            "price":{
                                "taxRate" : taxrate,
                                "dutyFreeAmount":{
                                    "unit":i[59],
                                    "value":round(float(i[20]))
                                },
                                "taxIncludedAmount":{
                                    "unit":i[59],
                                    "value":float(i[13])
                                },
                                "@type":"price"
                            }
                        }],
                        "productSpecification": {
                            "id": productspecid,
                            "href": "/api/productCatalogManagement/v4/productSpecification/" + productspecid,
                            "name": productspecname
                        }
                    },
                    "productOrderItemRelationship": [
                        {
                            "id": i[26],
                            "relationshipType": "hasAddon",
                            "@type": "productOfferingRelationship"
                        },
                        {
                            "id": i[26],
                            "relationshipType": "uses",
                            "@type": "fulfillmentRelationship"
                        },
                        {
                            "id": i[26],
                            "relationshipType": "reliesOn",
                            "@type": "fulfillmentRelationship"
                        }
                    ],
                    "productOffering":{
                        "id": dnextofferid,
                        "href":"/api/productCatalogManagement/v4/productOffering/"+dnextofferid,
                        "name":dnextoffername
                    },
                    "channel": [{
                    "id": "16470582-9989-4c3a-874c-5d185ab77444",
                    "href": "/api/referenceManagement/v4/marketSegment/16470582-9989-4c3a-874c-5d185ab77444",
                    "name": "Retail"
                    }]
                    })
                
                resourceproduct_list = []
                for t in prod["productOrderItem"]:
                    resourceproduct_list.append(t["id"])
                
                if i[26] not in resourceproduct_list:
                    prod['productOrderItem'].append({
                        "@type" : "ProductOrderItem",
                        "id" : i[26],
                        "quantity" : 1,                
                        "action" : "add",
                        "itemPrice" : [{
                            "name" : i[94],
                            "priceType" : "Recurring charge", 
                            "recurringChargePeriod" : "monthly",
                            "price" : {
                                "taxRate" : taxrate,
                                "dutyFreeAmount" : {
                                    "unit" : i[59],
                                    "value" : 0
                                },
                                "taxIncludedAmount" : {
                                    "unit" : i[59],
                                    "value" : 0
                                }
                            }
                        }],
                        "itemTotalPrice" : [{
                            "priceType" : "Recurring charge",
                            "recurringChargePeriod" : "monthly",
                            "price" : {
                                "taxRate" : taxrate,
                                "dutyFreeAmount" : {
                                    "unit" : i[59],
                                    "value" : 0
                                },
                                "taxIncludedAmount" : {
                                    "unit" : i[59],
                                    "value" : 0
                                }                        
                            }
                        }],
                        "product":{
                            "isBundle" : False,
                            "name" : i[94],
                            "agreement":[{
                                "id" : aggid,
                                "href" : "/api/agreementManagement/v4/agreement/"+aggid,
                                "name" : agreementname,
                                "@referredType" : "Agreement"
                            }],
                            "billingAccount":{
                                "id" : i[58],
                                "href" : "/api/accountManagement/v4/billingAccount/"+i[58],
                                "name" : "migration"
                            },
                            "relatedParty":[{
                                "id": "F" + i[5],
                                'href':"/api/customerManagement/v4/customer/F"+i[5],
                                "name":i[6],
                                "role":"Customer",
                                "@referredType":"Customer"
                            }],
                            "place" : [],
                            "productCharacteristic":[],
                            "productTerm":[],
                            "productPrice":[{
                                "priceType":"Recurring charge",               
                                "recurringChargePeriod":"monthly",
                                "price":{
                                    "taxRate" : taxrate,
                                    "dutyFreeAmount":{
                                        "unit":i[59],
                                        "value":0
                                    },
                                    "taxIncludedAmount":{
                                        "unit":i[59],
                                        "value":0
                                    },
                                    "@type":"price"
                                }
                            }],
                            "productSpecification": {
                                "id": i[95],
                                "href": "/api/productCatalogManagement/v4/productSpecification/" + i[95],
                                "name": i[96]
                            }
                        },
                        "productOffering":{
                            "id": i[93],
                            "href":"/api/productCatalogManagement/v4/productOffering/" + i[93],
                            "name": i[94]
                        },
                        "channel": [{
                            "id": "16470582-9989-4c3a-874c-5d185ab77444",
                            "href": "/api/referenceManagement/v4/marketSegment/16470582-9989-4c3a-874c-5d185ab77444",
                            "name": "Retail"
                            }]
                        })
                
                #get index for resourceProduct    
                resourceproduct_getindex = next((index for (index, d) in enumerate(prod["productOrderItem"]) if d["id"] == i[26]), None)
                
                if len(i[46]) >=9 or i[51]=="Cash":
                    prod['productOrderItem'][counter]['product']['productPrice'][0]['priceType'] = "One time charge"
                    prod['productOrderItem'][counter]['itemPrice'][0]['priceType'] = "One time charge"
                    prod['productOrderItem'][counter]['itemTotalPrice'][0]['priceType'] = "One time charge"
                    
                    prod['productOrderItem'][counter]['product']['productPrice'][0].pop('recurringChargePeriod',None)
                    prod['productOrderItem'][counter]['itemPrice'][0].pop('recurringChargePeriod',None)
                    prod['productOrderItem'][counter]['itemTotalPrice'][0].pop('recurringChargePeriod',None)
                    
                
                    
                    prod['productOrderItem'][resourceproduct_getindex]['product']['productPrice'][0]['priceType'] = "One time charge"
                    prod['productOrderItem'][resourceproduct_getindex]['itemPrice'][0]['priceType'] = "One time charge"
                    prod['productOrderItem'][resourceproduct_getindex]['itemPrice'][0].pop('recurringChargePeriod')
                    prod['productOrderItem'][resourceproduct_getindex]['product']['productPrice'][0].pop('recurringChargePeriod')
                    
                
                if i[63] != None:
                    prod["productOrderItem"][counter]['payment'].append({
                      "id": i[63],
                      "href": "/api/paymentManagement/v1/payment/" + i[63],
                      "name": "migration-payment"
                    })
                
                if i[51] == "Commitment":
                    producttermname = i[52] + "Month" + i[51]    
                    prod["productOrderItem"][counter]["product"]["productTerm"].append({
                        "name":producttermname,
                        "duration":{
                            "amount":int(i[52]),
                            "units":"Month"
                        },
                        "@type":"commitmentTerm"
                    })
                    
                    prod["productOrderItem"][counter]["product"]["productTerm"].append({
                        "name":"1MonthUsageTerm",
                        "duration":{
                            "amount":1,
                            "units":"Month"
                        },
                        "@type":"usageTerm"
                    })
                    
                    prod["productOrderItem"][resourceproduct_getindex]["product"]["productTerm"].append({
                        "name":producttermname,
                        "duration":{
                            "amount":int(i[52]),
                            "units":"Month"
                        },
                        "@type":"commitmentTerm"
                    })
                    
                    prod["productOrderItem"][resourceproduct_getindex]["product"]["productTerm"].append({
                        "name":"1MonthUsageTerm",
                        "duration":{
                            "amount":1,
                            "units":"Month"
                        },
                        "@type":"usageTerm"
                    })
                    
                    
                elif i[51] == "Cash":
                    producttermname = i[52] + "Month" + i[51]
                    prod["productOrderItem"][counter]["product"]["productTerm"].append({
                        "name":producttermname,
                        "duration":{
                            "amount":int(i[52]),
                            "units":"Month"
                         },
                        "@type":"usageTerm"
                    })
                    
                    prod["productOrderItem"][counter]["product"]["productTerm"].append({
                        "name":"Cash",
                        "duration":{
                            "amount":0,
                            "units":"Month"
                         },
                        "@type":"commitmentTerm"
                    })
                    
                    
                    prod["productOrderItem"][1]["product"]["productTerm"].append({
                        "name":producttermname,
                        "duration":{
                            "amount":int(i[52]),
                            "units":"Month"
                         },
                        "@type":"usageTerm"
                    })
                    
                    prod["productOrderItem"][resourceproduct_getindex]["product"]["productTerm"].append({
                        "name":"Cash",
                        "duration":{
                            "amount":0,
                            "units":"Month"
                         },
                        "@type":"commitmentTerm"
                    })
                    
                    
                
                if(i[61] == "SERVICE_ADDRESS"):
                    prod['productOrderItem'][counter]['product']['place'].append({
                        "id":i[62],
                        "href": f"/api/geographicAddressManagement/v4/geographicAddress/{i[62]}",
                        "name":"CART_ITEM_SERVICE_ADDRESS",
                        "role":"CART_ITEM_SERVICE_ADDRESS",
                        "@type": "FieldedAddress",						  
                        "@referredType":"FieldedAddress",
                        "@baseType": "GeographicAddress"
                    })
                    
                    
                    prod['productOrderItem'][resourceproduct_getindex]['product']['place'].append({
                        "id":i[62],
                        "href": f"/api/geographicAddressManagement/v4/geographicAddress/{i[62]}",
                        "name":"CART_ITEM_SERVICE_ADDRESS",
                        "role":"CART_ITEM_SERVICE_ADDRESS",
                        "@type": "FieldedAddress",						  
                        "@referredType":"FieldedAddress",
                        "@baseType": "GeographicAddress"
                    })
                    
                    
                ### productCharacteristicler ###
                #ortak productCharacteristicler
                prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                                {
                                    "name":"SpecType",
                                    "value":i[64]
                                }
                            )
                            
                prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                                {
                                    "name":"SpecSubType",
                                    "value":i[65]
                                }
                            )
                
                prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                                {
                                    "name":"PostPaidType",
                                    "value":i[66]
                                }
                            )
                
                prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                                {
                                    "name":"RatingType",
                                    "value":i[67]
                                }
                            )
                            
                if ((i[42] != "Penalty_PS")  and (i[42] != "PhoneNumber_PS")):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BRMProductId_USD",
                            "value":i[70]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BRMProductId_ALL",
                            "value":i[68]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BRMProductId_EUR",
                            "value":i[69]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"BRMServiceType",
                                "value":i[71]
                            }
                        )
                        
                
                if ((i[42] != "GenericResource_PS") and (i[42] != "Penalty_PS") and (i[42] != "Discount_PS")):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"SLA",
                            "value":i[72]
                        }
                    )
                    
                    
                # TV 
                
                if(i[42] == "CableTV_PS"):
                    
                    if (i[43] == "Abel"):
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"AbelProductIdentifier",
                                "value":i[75]
                            }
                        ) 
                    
                    if (i[43] == "Cryptoguard"):
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"CryptoGuardSubscriptionPackageId",
                                "value":i[74]
                            }
                        )
                        
                        
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                        
                        
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TVInfrastructure",
                            "value":i[43]
                        }
                    )
                        
                        
                # Internet
                
                if(i[42] == 'Internet_PS'):
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"OssCode",
                            "value":i[15]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"InternetPolicy",
                            "value":i[47]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"Infrastructure",
                            "value":i[40]
                        }
                    )
                    
                    if (i[77] != "-1") :
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"FUP",
                                "value":FUP
                            }
                        )
                        
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"UploadSpeed",
                            "value":i[37]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"DownloadSpeed",
                            "value":i[35]
                        }
                    )
                    
                # Modem_PS
                
                if(i[42] == 'Modem_PS'):
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                    
                    if (i[40] == 'FIBER') and (i[55] == 'fiber000'):
                        prod["productOrderItem"][0]["product"]["productCharacteristic"].append(
                        {
                            "name":"PPPoEUser",
                            "value":i[33]
                        }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"PPPoEPassword",
                                "value":i[93]
                            }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"Infrastructure",
                                "value":i[40]
                            }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"ResourceModel",
                                "value":i[36]
                            }
                        )
                        
                    elif (i[40] == 'FIBER') and (i[55] != 'fiber000'):
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"PPPoEUser",
                            "value":i[33]
                        }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"PPPoEPassword",
                                "value":i[93]
                            }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"ONTSerialNumber",
                                "value":i[33]
                            }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"Infrastructure",
                                "value":i[40]
                            }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"ResourceModel",
                                "value":i[36]
                            }
                        )
                    
                    if (i[40] == 'COAXIAL'):
                    
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"MACAddress",
                                "value":i[32]
                            }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"Infrastructure",
                                "value":i[40]
                            }
                        )
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"ResourceModel",
                                "value":i[36]
                            }
                        )
                        
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ERPGLCode",
                            "value":i[31]
                        }
                    )
                        
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BrandName",
                            "value":i[38]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TotalCost",
                            "value":i[39]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GuaranteeAmount",
                            "value":i[78]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"DeviceType",
                            "value":i[79]
                        }
                    )
                
                
                # STB_PS    
                if(i[42] == 'STB_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BrandName",
                            "value":i[38]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TotalCost",
                            "value":i[39]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GuaranteeAmount",
                            "value":i[78]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ResourceModel",
                            "value":i[36]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ERPGLCode",
                            "value":i[31]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TVInfrastructure",
                            "value":i[43]
                        }
                    )
                    
                # STB - VTV    
                if(i[42] == 'STB - VTV'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BrandName",
                            "value":i[38]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TotalCost",
                            "value":i[39]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GuaranteeAmount",
                            "value":i[78]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ResourceModel",
                            "value":i[36]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ERPGLCode",
                            "value":i[31]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TVInfrastructure",
                            "value":i[43]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"CAS ID",
                            "value":i[80]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"KalturaSubscriptionId",
                            "value":i[81]
                        }
                    )
                    
                    
                # SmartCard_PS    
                if(i[42] == 'SmartCard_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"SmartCardSerialNumber",
                            "value":i[33]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BrandName",
                            "value":i[38]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TotalCost",
                            "value":i[39]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GuaranteeAmount",
                            "value":i[78]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"DeviceType",
                            "value":i[79]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ResourceModel",
                            "value":i[36]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ERPGLCode",
                            "value":i[31]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TVInfrastructure",
                            "value":i[43]
                        }
                    )
                    
                # VOIP_PS
                
                if(i[42] == 'VOIP_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"OssCode",
                            "value":i[15]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"PhoneNumber", 
                            "value":i[45]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"Infrastructure",  #internetinfrastructure
                            "value":i[40]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"PortaBillingPlanID",
                            "value":i[82]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"SipPassword",
                            "value":i[93]
                        }
                    )
                    
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BRMProductIdChild_ALL",
                            "value":i[87]
                        }
                    )
                    
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BRMProductIdChild_EUR",
                            "value":i[88]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BRMProductIdChild_USD",
                            "value":i[89]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"BRMServiceTypeChild",
                            "value":i[90]
                        }
                    )
                    
                # SETUP_PS
                
                if(i[42] == 'SETUP_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                # LeasedLine_PS
                
                if(i[42] == 'LeasedLine_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"Infrastructure",  #internetinfrastructure
                            "value":i[40]
                        }
                    )
                    
                    
                # PublicIP_PS
                
                if(i[42] == 'PublicIP_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"IPAddress",
                            "value":i[45]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"OssCode",
                            "value":i[15]
                        }
                    )
                    
                    
                # Datalink_PS
                
                if(i[42] == 'Datalink_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"OssCode",
                            "value":i[15]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"UploadSpeed",
                            "value":i[37]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"DownloadSpeed",
                            "value":i[35]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ServiceAccount",
                            "value":i[45]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"Infrastructure",
                            "value":i[40]
                        }
                    )
                        
                # DeviseLeasing_PS
                
                if (i[42] == 'DeviseLeasing_PS') :
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"ResourceModel",
                            "value":i[36]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                        
                # PhoneNumber_PS
                
                if(i[42] == 'PhoneNumber_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"PhoneNumber",
                            "value":i[45]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"SipPassword",
                            "value":i[93]
                        }
                    )
                    
                # GenericResource_PS
                
                if(i[42] == 'GenericResource_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GRType",
                            "value":i[44]
                        })
                    
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GRInfo",
                            "value":i[45]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"PPPoEUser",
                            "value":i[33]
                        }
                    )
                        
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"PPPoEPassword",
                            "value":i[93]
                        }
                    )
                
                
                # Penalty_PS
                
                if(i[42] == 'Penalty_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                        
                # Discount_PS
                
                if(i[42] == 'Discount_PS'):
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        }
                    )
                
                
                # CableTVAddOnSpec
                if(i[42] == 'CableTVAddOnSpec'):
                    if (i[43] == "Abel"):
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"AbelProductIdentifier",
                                "value":i[75]
                            })
                    
                    if (i[43] == "Cryptoguard"):
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"CryptoGuardSubscriptionPackageId",
                                "value":i[74]
                            })
                    
                    if (i[43] == "Vtv"):
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                            {
                                "name":"KalturaSubscriptionId",
                                "value":i[81]
                            })
                    
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"GLCode",
                            "value":i[76]
                        })
                        
                    prod["productOrderItem"][counter]["product"]["productCharacteristic"].append(
                        {
                            "name":"TVInfrastructure",
                            "value":i[43]
                        }
                    )
                
                resourcecursor.execute(f"""select
                    r.resourceid,
                    r.dnextproductspecificationname,
                    case when (ps.spectype is null or trim(ps.spectype) = '') then ' ' else ps.spectype end spectype,
                    case when (ps.specsubtype is null or trim(ps.specsubtype) = '') then ' ' else ps.specsubtype end specsubtype,
                    case when (ps.postpaidtype is null or trim(ps.postpaidtype) = '') then ' ' else ps.postpaidtype end postpaidtype,
                    case when (a.ratingtype is null or trim(a.ratingtype) = '') then ' ' else a.ratingtype end ratingtype,
                    case when (ps.brmproductid_all is null or trim(ps.brmproductid_all) = '') then ' ' else ps.brmproductid_all end brmproductid_all,  
                    case when (ps.brmproductid_eur is null or trim(ps.brmproductid_eur) = '') then ' ' else ps.brmproductid_eur end brmproductid_eur,
                    case when (ps.brmproductid_usd is null or trim(ps.brmproductid_usd) = '') then ' ' else ps.brmproductid_usd end brmproductid_usd,
                    case when (ps.brmservicetype is null or trim(ps.brmservicetype) = '') then ' ' else ps.brmservicetype end brmservicetype,
                    case when (r.sla is null or trim(r.sla) = '') then ' ' else r.sla end sla,                 --10
                    case when (p.internetinfrastructure is null or trim(p.internetinfrastructure) = '') then ' ' else p.internetinfrastructure end internetinfrastructure,
                    r.accountlink,
                    case when (ps.glcode is null or trim(ps.glcode) = '') then ' ' else ps.glcode end glcode,
                    case when (r.serviceaccount is null or trim(r.serviceaccount) = '') then ' ' else r.serviceaccount end serviceaccount,
                    case when (cry.encrypted is null or trim(cry.encrypted) = '') then ' ' else cry.encrypted end encrypted2,
                    case when (r.legacyresourcename is null or trim(r.legacyresourcename) = '') then ' ' else r.legacyresourcename end legacyresourcename,
                    case when (p.macaddress is null or trim(p.macaddress) = '') then ' ' else p.macaddress end macaddress,
                    case when (ps.erpglcode is null or trim(ps.erpglcode) = '') then ' ' else ps.erpglcode end erpglcode,
                    case when (ps.brandname is null or trim(ps.brandname) = '') then ' ' else ps.brandname end brandname,
                    case when (r.returnvalue is null or trim(r.returnvalue) = '') then ' ' else r.returnvalue end returnvalue,  --20
                    case when (r.guaranteecredit is null or trim(r.guaranteecredit) = '') then ' ' else r.guaranteecredit end guaranteecredit,
                    case when (ps.devicetype is null or trim(ps.devicetype) = '') then ' ' else ps.devicetype end devicetype,
                    case when (r.osssystem is null or trim(r.osssystem) = '') then ' ' else r.osssystem end osssystem,
                    case when (ctl.kaltura_subscription_id is null or trim(ctl.kaltura_subscription_id) = '') then ' ' else ctl.kaltura_subscription_id end kaltura_subscription_id,
                    case when (ps.casid is null or trim(ps.casid) = '') then ' ' else ps.casid end casid,
                    case when (ctl.oss_code is null or trim(ctl.oss_code) = '')  then ' ' else ctl.oss_code end oss_code,
                    case when (p.serviceaccount is null or trim(p.serviceaccount) = '') then ' ' else p.serviceaccount end productserviceaccount,
                    case when (ps.portabillingplanid is null or trim(ps.portabillingplanid) = '') then ' ' else ps.portabillingplanid end portabillingplanid,
                    case when (ps.zonetype is null or trim(ps.zonetype) = '') then ' ' else ps.zonetype end zonetype,
                    case when (ps.creditlimit is null or trim(ps.creditlimit) = '') then ' ' else ps.creditlimit end creditlimit,  --30
                    case when (ps.fixnumber is null or trim(ps.fixnumber) = '') then ' ' else ps.fixnumber end fixnumber,
                    case when (ps.brmproductidchild_all is null or trim(ps.brmproductidchild_all) = '') then ' ' else ps.brmproductidchild_all end brmproductidchild_all,
                    case when (ps.brmproductidchild_eur is null or trim(ps.brmproductidchild_eur) = '') then ' ' else ps.brmproductidchild_eur end brmproductidchild_eur,
                    case when (ps.brmproductidchild_usd is null or trim(ps.brmproductidchild_usd) = '') then ' ' else ps.brmproductidchild_usd end brmproductidchild_usd,
                    case when (ps.brmservicetypechild is null or trim(ps.brmservicetypechild) = '') then ' ' else ps.brmservicetypechild end brmservicetypechild,
                    case when (ps.crmservicetype is null or trim(ps.crmservicetype) = '') then ' ' else ps.crmservicetype end crmservicetype,
                    case when (ps.uploadspeed is null or trim(ps.uploadspeed) = '') then ' ' else ps.uploadspeed end uploadspeed,
                    case when (ps.downloadspeed is null or trim(ps.downloadspeed) = '') then ' ' else ps.downloadspeed end downloadspeed,
                    case when (r.grtype is null or trim(r.grtype) = '') then ' ' else r.grtype end grtype,
                    case when (ps.grinfo is null or trim(ps.grinfo) = '') then ' ' else ps.grinfo end grinfo --40
                    from staging.resource r
                    inner join staging.customer c on c.customerid = r.customerid
                    left join staging.partyrole prole on prole.siebelid = r.createdbysiebelid
                    left join staging.partyrole partyprole on partyprole.siebelid = r.lastmodifiedbysiebelid
                    left join staging.partner prt on prt.legacypartnerid = prole.parent_id
                    left join staging.product p on r.subscriberaccountkey = p.subscriberaccountkey
                    inner join staging.agreement a on p.agreementid = a.agreementid
                    left join staging.contactmedium cm on cm.contactmediumid = a.contactmediumid
                    left join staging.catalogue ctl on upper(p.updated_productofferingid) = upper(ctl.code)
                    left join staging.product_specs ps on r.dnextproductofferingid = ps.dnextproductofferingid
                    left join staging.crypto cry on cry.id = r.resourceid
                    where 1 = 1
                    and c.mig_flag = '1'
                    and p.prod_mig_flag = '1'
                    and r.migflag = '1'
                    and p.status = 'pendingActive'
                    and p.legacyproductofferingid != 'SETUP'
                    and ps.dnextproductofferingid is not null
                    and r.resourceid = '{p_resourceid}' """)
                
                for temp in resourcecursor.fetchall():
                    ### productCharacteristicler ###
                    #ortak productCharacteristicler
                    prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                        "name":"SpecType",
                        "value":temp[2]
                    })
                    
                    prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                        "name":"SpecSubType",
                        "value":temp[3]
                    })
                    
                    prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                        "name":"PostPaidType",
                        "value":temp[4]
                    })
                    
                    prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                        "name":"RatingType",
                        "value":temp[5]
                    })
                    
                    if ((temp[1] != "Penalty_PS")  and (temp[1] != "PhoneNumber_PS")):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMProductId_USD",
                            "value":temp[8]
                        })
                    
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMProductId_ALL",
                            "value":temp[6]
                        })
                    
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMProductId_EUR",
                            "value":temp[7]
                        })
                    
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMServiceType",
                            "value":temp[9]
                        })
                        
                
                    if ((temp[1] != "GenericResource_PS") and (temp[1] != "Penalty_PS") and (temp[1] != "Discount_PS")):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"SLA",
                            "value":temp[10]
                        })
                        
                    
                    # Modem_PS
                    if (temp[1] == 'Modem_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                        
                        
                        if (temp[11] == 'FIBER') and (temp[12] == 'fiber000'):
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"PPPoEUser",
                                "value":temp[14]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"PPPoEPassword",
                                "value":temp[15]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"Infrastructure",
                                "value":temp[11]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"ResourceModel",
                                "value":temp[16]
                            })
                            
                        elif (temp[11] == 'FIBER') and (temp[12] != 'fiber000'):
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"PPPoEUser",
                                "value":temp[14]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"PPPoEPassword",
                                "value":temp[15]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"ONTSerialNumber",
                                "value":temp[14]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"Infrastructure",
                                "value":temp[11]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"ResourceModel",
                                "value":temp[16]
                            })
                        
                        if (temp[11] == 'COAXIAL'):
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"MACAddress",
                                "value":temp[17]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"Infrastructure",
                                "value":temp[11]
                            })
                            
                            prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                                "name":"ResourceModel",
                                "value":temp[16]
                            })
                            
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ERPGLCode",
                            "value":temp[18]
                        })
                            
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BrandName",
                            "value":temp[19]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TotalCost",
                            "value":temp[20]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GuaranteeAmount",
                            "value":temp[21]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"DeviceType",
                            "value":temp[22]
                        })
                    
                
                    # STB_PS    
                    if(temp[1] == 'STB_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BrandName",
                            "value":temp[19]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TotalCost",
                            "value":temp[20]
                        })
                        
                        prod["productOrderItem"][counter]["product"]["productCharacteristic"].append({
                            "name":"GuaranteeAmount",
                            "value":temp[21]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ResourceModel",
                            "value":temp[16]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ERPGLCode",
                            "value":temp[18]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TVInfrastructure",
                            "value":temp[23]
                        })
                        
                    # STB - VTV    
                    if(temp[1] == 'STB - VTV'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BrandName",
                            "value":temp[19]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TotalCost",
                            "value":temp[20]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GuaranteeAmount",
                            "value":temp[21]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ResourceModel",
                            "value":temp[16]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ERPGLCode",
                            "value":temp[18]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TVInfrastructure",
                            "value":temp[23]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"CAS ID",
                            "value":temp[25]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"KalturaSubscriptionId",
                            "value":temp[24]
                        })
                        
                        
                    # SmartCard_PS    
                    if(temp[1] == 'SmartCard_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"SmartCardSerialNumber",
                            "value":temp[14]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BrandName",
                            "value":temp[19]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TotalCost",
                            "value":temp[20]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GuaranteeAmount",
                            "value":temp[21]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"DeviceType",
                            "value":temp[22]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ResourceModel",
                            "value":temp[16]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ERPGLCode",
                            "value":temp[18]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TVInfrastructure",
                            "value":temp[23]
                        })
                        
                    # VOIP_PS
                    
                    if(temp[1] == 'VOIP_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"OssCode",
                            "value":temp[26]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"PhoneNumber", 
                            "value":temp[27]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"Infrastructure",  #internetinfrastructure
                            "value":temp[11]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"PortaBillingPlanID",
                            "value":temp[28]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"SipPassword",
                            "value":temp[15]
                        })
                        
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMProductIdChild_ALL",
                            "value":temp[32]
                        })
                        
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMProductIdChild_EUR",
                            "value":temp[33]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMProductIdChild_USD",
                            "value":temp[34]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BRMServiceTypeChild",
                            "value":temp[35]
                        })
                        
                    # LeasedLine_PS
                    
                    if(temp[1] == 'LeasedLine_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"Infrastructure",  
                            "value":temp[11]
                        })
                        
                    # Radio_PS    
                    if(temp[1] == 'Radio_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"BrandName",
                            "value":temp[19]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"TotalCost",
                            "value":temp[20]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GuaranteeAmount",
                            "value":temp[21]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ResourceModel",
                            "value":temp[16]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ERPGLCode",
                            "value":temp[18]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"PPPoEUser",
                            "value":temp[14]
                        })
                            
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"PPPoEPassword",
                            "value":temp[15]
                        })
                            
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"Infrastructure",
                            "value":temp[11]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"DeviceType",
                            "value":temp[22]
                        })
                        
                    # PublicIP_PS
                    
                    if(temp[1] == 'PublicIP_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"IPAddress",
                            "value":temp[27]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"OssCode",
                            "value":temp[26]
                        })
                        
                        
                    # Datalink_PS
                    
                    if(temp[1] == 'Datalink_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"OssCode",
                            "value":temp[26]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"UploadSpeed",
                            "value":temp[37]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"DownloadSpeed",
                            "value":temp[38]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ServiceAccount",
                            "value":temp[27]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"Infrastructure",
                            "value":temp[11]
                        })
                            
                    # DeviseLeasing_PS
                    
                    if (temp[1] == 'DeviseLeasing_PS') :
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"ResourceModel",
                            "value":temp[16]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                            
                    # PhoneNumber_PS
                    
                    if(temp[1] == 'PhoneNumber_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"PhoneNumber",
                            "value":temp[14]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"SipPassword",
                            "value":temp[15]
                        })
                        
                    # GenericResource_PS
                    
                    if(temp[1] == 'GenericResource_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GRType",
                            "value":temp[39]
                        })
                        
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GRInfo",
                            "value":temp[14]
                        })
                    
                    # Penalty_PS
                    
                    if(temp[1] == 'Penalty_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                        
                    # Discount_PS
                    
                    if(temp[1] == 'Discount_PS'):
                        prod["productOrderItem"][resourceproduct_getindex]["product"]["productCharacteristic"].append({
                            "name":"GLCode",
                            "value":temp[13]
                        })
                
                if i[97] == '1':
                    prod['productOrderItem'][counter].pop('productOrderItemRelationship',None)
                    deleteproductindex = counter + 1
                    del prod["productOrderItem"][resourceproduct_getindex]
                    
                    prod['productOrderItem'][counter]["product"]["productRelationship"] = [
                        {
                            "relationshipType": "uses",
                            "product": {
                                "id": i[26],
                                "href": "/api/productInventoryManagement/v4/product/" + i[26]
                            }
                        },
                        
                        {
                            "relationshipType": "hasAddon",
                            "product": {
                                "id": i[26],
                                "href": "/api/productInventoryManagement/v4/product/" + i[26]
                            }
                        },
                        
                        {
                            "relationshipType": "reliesOn",
                            "product": {
                                "id": i[26],
                                "href": "/api/productInventoryManagement/v4/product/" + i[26]
                            }
                        }
                    ]
            
            def get_id(orderItem):
                return orderItem.get('id')
            
            prod["productOrderItem"].sort(key=get_id)
        
            new_list = []
            for yy in prod["productOrderItem"]:
                new_listproductname = []
                if len(new_list) > 0:
                    for xx in new_list:
                        new_listproductname.append(xx["product"]["name"])
                if yy["product"]["name"] not in new_listproductname:
                    new_list.append(yy)
                else:
                    mainproduct_getindex = next((index for (index, d) in enumerate(new_list) if d["product"]["name"] == yy["product"]["name"]), None)
                    print("debug 2020",int(yy['itemPrice'][0]['price']["dutyFreeAmount"]["value"]))
                    print("debug 2021", int(new_list[mainproduct_getindex]['itemPrice'][0]['price']["dutyFreeAmount"]["value"]))
                    new_list[mainproduct_getindex]['itemPrice'][0]['price']["dutyFreeAmount"]["value"] = float(yy['itemPrice'][0]['price']["dutyFreeAmount"]["value"]) + float(new_list[mainproduct_getindex]['itemPrice'][0]['price']["dutyFreeAmount"]["value"])
                    new_list[mainproduct_getindex]['itemPrice'][0]['price']["taxIncludedAmount"]["value"] = float(yy['itemPrice'][0]['price']["taxIncludedAmount"]["value"]) + float(new_list[mainproduct_getindex]['itemPrice'][0]['price']["taxIncludedAmount"]["value"])
                    new_list[mainproduct_getindex]['itemTotalPrice'][0]['price']["dutyFreeAmount"]["value"] = float(yy['itemTotalPrice'][0]['price']["dutyFreeAmount"]["value"]) + float(new_list[mainproduct_getindex]['itemTotalPrice'][0]['price']["dutyFreeAmount"]["value"])
                    new_list[mainproduct_getindex]['itemTotalPrice'][0]['price']["taxIncludedAmount"]["value"] = float(yy['itemTotalPrice'][0]['price']["taxIncludedAmount"]["value"]) + float(new_list[mainproduct_getindex]['itemTotalPrice'][0]['price']["taxIncludedAmount"]["value"])
    
                    new_list[mainproduct_getindex]["product"]['productPrice'][0]['price']["dutyFreeAmount"]["value"] = float(yy['product']["productPrice"][0]['price']["dutyFreeAmount"]["value"]) + float(new_list[mainproduct_getindex]["product"]['productPrice'][0]['price']["dutyFreeAmount"]["value"])        
                    new_list[mainproduct_getindex]["product"]['productPrice'][0]['price']["taxIncludedAmount"]["value"] = float(yy['product']["productPrice"][0]['price']["taxIncludedAmount"]["value"]) + float(new_list[mainproduct_getindex]["product"]['productPrice'][0]['price']["taxIncludedAmount"]["value"])        
            
            
            
            prod["productOrderItem"] = new_list
            
            #print("Product Order Text : " + record_pk)
            #print(prod)
            
            r = requests.post(conf.url['productorder'],data=json.dumps(prod),headers=headers)
            #print("Product Order Status Code : ",r.status_code)
            
            cursor.execute("update staging.product set productorder_statuscode = '{}' where productid = '{}'".format(r.status_code,record_pk))
            con.commit()
            
            if r.status_code != 201:
                print("Product Order Error: " + record_pk)
                print(r.text)
            
        except Exception as e:
            print("Exception WhenCreateProductOrderPayload for: " + record_pk)
            print(e)
            cursor.execute("update staging.product set productorder_statuscode = '425' where productid='{}'".format(record_pk))
            con.commit()
            pass
        
    
    
def stringToTimestamp(val):
    if((val == None) or (val == "")):
        return "1800-01-01T00:00:00Z"
    else:
        val = str(val).replace(' ','T')+'Z'
        return val


productOrder(args['condition'])

con.commit()
cursor.close() 
con.close()
