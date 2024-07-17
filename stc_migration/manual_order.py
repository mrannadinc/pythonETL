import sys
import pgdb
import datetime
import requests
import time
import pymongo
import conf
import dns
from pymongo import MongoClient
import json
import pandas as pd
from bson.json_util import dumps
import boto3
from datetime import datetime, timedelta
import io
from awsglue.utils import getResolvedOptions
import uuid
import datetime
from bson import objectid

print("debug 21")


args = getResolvedOptions(sys.argv, ['condition'])

print("debug 23")

#uri = "mongodb+srv://{}:{}@{}/{}?retryWrites=true&w=majority".format(conf.DB_Mongo['username'], conf.DB_Mongo['password'], conf.DB_Mongo['hostname'],  conf.DB_Mongo['database']['product'])

uri1="mongodb+srv://manualorder_db:vEBr4ViqifRu@mongo-dnext-prod.c3sg4.mongodb.net/test"

client = MongoClient(uri1)
db = client["manualorder_db"]
collection = db.get_collection("manuelOrder")

con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

print("debug 34")

def manuelorder(condition):
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
    
    print("debug 42")
    
    cursor.execute(f""" 
    
     WITH tt_max AS (select pp.customerid,MAX(pp.productid)  as productid 
   from staging.product pp left join staging.resource rs on rs.subscriberaccountkey = pp.subscriberaccountkey 
        where rs.service ='TV_CABLE'  
        and rs.migflag = '1'
        and pp.status = 'active' 
        and pp.prod_mig_flag  ='1' 
group by pp.customerid )
    

    select
     p.productid
    ,p.createdate
    ,p.createdby
    ,c.customerid
    ,p.startdate
    ,p.terminationdate
    ,ps.cryptoguardsubscriptionpackageid
    ,ps.spectype
    ,ps.specsubtype
    ,ps.glcode
    ,ps.tvinfrastructure
    ,ps.brmproductid_all
    ,ps.brmproductid_usd
    ,ps.brmproductid_eur
    ,ps.ratingtype
    ,ps.sla
    ,ps.brmservicetype
    ,ps.postpaidtype
    ,ps.abelproductidentifier
    ,ps.kalturasubscriptionid
    ,ps.osscode
    ,ps.infrastructure
    ,ps.uploadspeed
    ,ps.downloadspeed
    ,ps.fup
    ,ps.internetpolicy
    ,ps.type
    ,ps.macaddress
    ,ps.ontserialnumber
    ,ps.brandname
    ,ps.totalcost
    ,ps.guaranteeamount
    ,ps.devicetype
    ,ps.pppoepassword
    ,ps.pppoeuser
    ,ps.resourcemodel
    ,ps.erpglcode
    ,ps.ontserialnumber
    ,c.name
    ,case
        when p.legacyproductofferingid like 'DISC%' then '{conf.discountproductspecid}'
        when ps.dnextproductofferingid is not null then ps.specificationid
    end specificationid
    ,case
        when p.legacyproductofferingid like 'DISC%' then '{conf.discountproductspecname}'
        when ps.dnextproductofferingid is not null then ps.specificationname
    end specificationname
    ,r.fupflag
    ,hh.productid as externalreferenceId
    ,ps.dnextproductofferingname
    ,ps.dnextproductofferingid
    ,ps.grtype
    ,ps.grinfo
    ,ppk.subscriberaccount as   account_id
    from staging.product p
        inner join staging.customer c on c.customerid = p.customerid
        left join staging.agreement a on p.agreementid = a.agreementid
        left join staging.resource r on r.subscriberaccountkey = p.subscriberaccountkey
        inner join staging.product_specs ps on p.legacyproductofferingid = ps.productcode --ps.dnextproductofferingname='TV - Kanale pa sinjal'
        inner join tt_max           hh  on hh.customerid = p.customerid 
        inner join staging.product  ppk on hh.productid = ppk.productid and ppk.customerid=hh.customerid
        where 1 = 1
        --and length(p.invoiceid) > 9
        and c.mig_flag = '1'
        and r.migflag = '1'
        and p.status = 'active'
        --and p.customerid ='4443822'
        and p."legacyproductofferingid"  = 'CTVPASIN'
    """)
    
    print("debug 117")
    
    for i in cursor.fetchall():
        JUUID=uuid.uuid1()
        id = i[0]
        createdDate = str(i[1]).replace(' ','T')+'Z'
        createdBy=i[2]
        customerNo=i[3]
        serviceStartDate = str(i[4]).replace(' ','T')+'Z'
        serviceEndDate = str(i[5]).replace(' ','T')+'Z'
        CryptoGuardSubscriptionPackageId=i[6]
        SpecType=i[7]
        SpecSubType=i[8]
        GLCode=i[9]
        TVInfrastructure=i[10]
        BRMProductId_ALL=i[11]
        BRMProductId_USD=i[12]
        BRMProductId_EUR=i[13]
        RatingType=i[14]
        SLA=i[15]
        BRMServiceType=i[16]
        PostPaidType=i[17]
        AbelProductIdentifier=i[18]
        KalturaSubscriptionId=i[19]
        OssCode=i[20]
        Infrastructure=i[21]
        UploadSpeed=i[22]
        DownloadSpeed=i[23]
        FUP=i[24]
        InternetPolicy=i[25]
        Type=i[26]
        MACAddress=i[27]
        ONTSerialNumber=i[28]
        BrandName=i[29]
        TotalCost=i[30]
        GuaranteeAmount=i[31]
        DeviceType=i[32]
        PPPoEPassword=i[33]
        PPPoEUser=i[34]
        ResourceModel=i[35]
        ERPGLCode=i[36]
        OLTSerial=i[37]
        customerName=i[38]
        specid=i[39]
        specname=i[40]
        fupflag=i[41]
        externalreferenceId=i[42]
        #dnextproductofferingname=i[43]
        #dnextproductofferingid=i[44]
        if (i[44] == None) or (i[44] == ""):
            dnextproductofferingid = "tbd-dnextproductofferingid"
            dnextproductofferingname = "tbd-dnextproductofferingname"
        else:
            dnextproductofferingid = i[44]
            dnextproductofferingname = i[43]
        GRType=i[45]
        GRInfo=i[46]
        accountid=i[47]

        manuelorder = { 
        
        #"_id":"hkmJUUID("+"'"+str(JUUID)+"'"+")hkm",
	    "createDate":createdDate,
	    "status": "inprogress",
	    "customerNo":"F"+customerNo,
	    "serviceStartDate":str(serviceStartDate),
	    "serviceEndDate":str(serviceEndDate),
	    "note": "MIGRATED-TEMP", 
	    "createUserName": createdBy, 
	    "action": "activate", 
	    "manualOrderProductInventories": [
		    {
			"billingAccount": {
				"_id": accountid,
				"href": "/api/accountManagement/v4/billingAccount/"+accountid,
				"name": customerName
			},
			"channel": [],#default boş dendş
			"description": dnextproductofferingname, #productOfferingden gelecek  #dnextproductofferingname
			"externalReference": [
				{
					"_id": externalreferenceId,#--geçici olarak şimdiki product inventory ID (product talbosundaki product ID)     dnextteki productInventoryID
					"name": "sourceInventoryId" #default böyle
				}
			],
			"isBundle": "false", #productOfferingden gelecek default false 
			"name": dnextproductofferingname,#productOfferingden gelecek dnextproductofferingname
			"productCharacteristic": [],
		    "productOffering": { 
				"_id": dnextproductofferingid, #dnextproductofferingid
				"schemaLocation": "/api/productCatalogManagement/v4/schema/productoffering.json",
				"href": "/api/productCatalogManagement/v4/productOffering/"+dnextproductofferingid,
				"name": dnextproductofferingname #dnextproductofferingname
			},
			"relatedParty": [#customer no ile customer tablosundan 
				{
					"_id": "F"+customerNo,
					"referredType": "Customer",
					"href": "/api/customerManagement/v4/customer/F"+customerNo,
					"name": customerName,
					"role": "Customer"
				}
			],
			"status": "activated",
			"statusReason": "Migrated"
		    }
	    ],
	    "manualOrderType": "Type1",
	    "baseInventoryStatus": "active",
	    "_class": "com.pia.dnext.vfal.manualorder.model.ManuelOrder" 
	    
	   
	    }
	   
        #ortak productCharacteristicler
        
        if((SpecType!=None) and (SpecType!="")):
            manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"SpecType",
                        "value":SpecType,
                         
                    }
                 )

        if((SpecSubType!=None) and (SpecSubType!="")):            
            manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"SpecSubType",
                        "value":SpecSubType,
                        
                    }
                 )

        if((PostPaidType!=None) and (PostPaidType!="")):            
            manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"PostPaidType",
                        "value":PostPaidType,
                         
                    }
                 )

        if((RatingType!=None) and (RatingType!="")):            
            manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"RatingType",
                        "value":RatingType,
                       
                    }
                )
     
        if ((i[39] != "GenericResource_PS") and (i[39] != "Penalty_PS")  and (i[39] != "PhoneNumber_PS")):
         
            if((BRMProductId_USD!=None) and (BRMProductId_USD!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"BRMProductId_USD",
                        "value":BRMProductId_USD,
                        
                    }
                )

            if((BRMProductId_ALL!=None) and (BRMProductId_ALL!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"BRMProductId_ALL",
                        "value":BRMProductId_ALL,
                        
                    }
                )

            if((BRMProductId_EUR!=None) and (BRMProductId_EUR!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"BRMProductId_EUR",
                        "value":BRMProductId_EUR,
                       
                    }
                )

            if((BRMServiceType!=None) and (BRMServiceType!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"BRMServiceType",
                        "value":BRMServiceType,
                        
                    }
                )
        if ((i[39] != "GenericResource_PS") and (i[39] != "Penalty_PS") and (i[39] != "Discount_PS")):
 
 
            if((SLA!=None) and (SLA!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"SLA",
                        "value":SLA,
                        
                    }
                )
     
     # TV için 
         
        if(i[39] == "CableTV_PS"):
            if (i[40]== "ABEL"):
             
                if((AbelProductIdentifier!=None) and (AbelProductIdentifier!="")):            
                    manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"AbelProductIdentifier",
                        "value":AbelProductIdentifier,
                        
                    }
                )
                 
            if (i[40] == "CRYPTOGUARD"):          
                if((CryptoGuardSubscriptionPackageId!=None) and (CryptoGuardSubscriptionPackageId!="")):            
                    manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"CryptoGuardSubscriptionPackageId",
                        "value":CryptoGuardSubscriptionPackageId,
                         
                    }
                )
                 
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                {
                        "name":"GLCode",
                        "value":GLCode,
                         
                }
                )                    
                 
            if((TVInfrastructure!=None) and (TVInfrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TVInfrastructure",
                        "value":TVInfrastructure,
                         
                    }
                )                   
                 
                 
                 

         # Internet


        if(i[39] == 'Internet_PS'):

            if((OssCode!=None) and (OssCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"OssCode",
                        "value":OssCode,
                         
                    }
                )    

            if((InternetPolicy!=None) and (InternetPolicy!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"InternetPolicy",
                        "value":InternetPolicy,
                         
                    }
                )

            if((Infrastructure!=None) and (Infrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"Infrastructure",
                        "value":Infrastructure,
                         
                    }
                )


            if (fupflag != "-1") :
             
                if((FUP!=None) and (FUP!="")):            
                    manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"FUP",
                        "value":FUP,
                       
                    }
                )
         
             
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GLCode",
                        "value":GLCode,
                        
                    }
                )     
          
                 
            if((UploadSpeed!=None) and (UploadSpeed!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"UploadSpeed",
                        "value":UploadSpeed,
                        
                    }
                )

            if((DownloadSpeed!=None) and (DownloadSpeed!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"DownloadSpeed",
                        "value":DownloadSpeed,
                        
                    }
                )

     # Modem_PS
         
        if(i[39] == 'Modem_PS'):
         
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GLCode",
                        "value":GLCode,
                        
                    }
            )     
      
            if (Infrastructure != 'Coaxial'):
             
                if((ONTSerialNumber!=None) and (ONTSerialNumber!="")):            
                    manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ONTSerialNumber",
                        "value":ONTSerialNumber,
                        
                    }
                )
                     
            if((PPPoEUser!=None) and (PPPoEUser!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"PPPoEUser",
                        "value":PPPoEUser,
                         
                    }
                )                        
                     
            if((PPPoEPassword!=None) and (PPPoEPassword!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"PPPoEPassword",
                        "value":PPPoEPassword,
                         
                    }
                )                       
                     
            if((MACAddress!=None) and (MACAddress!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"MACAddress",
                        "value":MACAddress,
                         
                    }
                )
                     
                     
            if((BrandName!=None) and (BrandName!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"BrandName",
                        "value":BrandName,
                         
                    }
                )
                     
            if((TotalCost!=None) and (TotalCost!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TotalCost",
                        "value":TotalCost,
                         
                    }
                )                        
                     
            if((GuaranteeAmount!=None) and (GuaranteeAmount!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GuaranteeAmount",
                        "value":GuaranteeAmount,
                       
                    }
                )
                        
            if((DeviceType!=None) and (DeviceType!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"DeviceType",
                        "value":DeviceType,
                        
                    }
                )
                        
                        
            if((ResourceModel!=None) and (ResourceModel!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ResourceModel",
                        "value":ResourceModel,
                        
                    }
                )                        

            if((ERPGLCode!=None) and (ERPGLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ERPGLCode",
                        "value":ERPGLCode,
                        
                    }
                )                          
                        
            if((Infrastructure!=None) and (Infrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"Infrastructure",
                        "value":Infrastructure,
                        
                    }
                )
     # STB_PS    
        if(i[39] == 'STB_PS'):     
         
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                            "name":"GLCode",
                            "value":GLCode,
                            
                    }
                )    
             
                     
            if((TotalCost!=None) and (TotalCost!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TotalCost",
                        "value":TotalCost,
                        
                    }
                )             
            
            if((GuaranteeAmount!=None) and (GuaranteeAmount!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GuaranteeAmount",
                        "value":GuaranteeAmount,
                       
                    }
                )            
            
            if((ResourceModel!=None) and (ResourceModel!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ResourceModel",
                        "value":ResourceModel,
                        
                    }
                )                    
         
            if((ERPGLCode!=None) and (ERPGLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ERPGLCode",
                        "value":ERPGLCode,
                        
                    }
                )  
                 
                 
            if((TVInfrastructure!=None) and (TVInfrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TVInfrastructure",
                        "value":TVInfrastructure,
                        
                    }
                )       
                 
     # STB - VTV    
        if(i[39] == 'STB - VTV'):     
         
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GLCode",
                        "value":GLCode,
                            
                    }
                )              
         
            if((BrandName!=None) and (BrandName!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"BrandName",
                         "value":BrandName,
                            
                    }
                )            
         
            if((TotalCost!=None) and (TotalCost!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TotalCost",
                        "value":TotalCost,
                        
                    }
                )               

            if((GuaranteeAmount!=None) and (GuaranteeAmount!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GuaranteeAmount",
                        "value":GuaranteeAmount,
                       
                    }
                )   

            if((ResourceModel!=None) and (ResourceModel!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ResourceModel",
                        "value":ResourceModel,
                        
                    }
                )      
                 
                 
                 
            if((ERPGLCode!=None) and (ERPGLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ERPGLCode",
                        "value":ERPGLCode,
                        
                    }
                )                      
                 
                 
            if((TVInfrastructure!=None) and (TVInfrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TVInfrastructure",
                        "value":TVInfrastructure,
                        
                    }
                )                      
                 
                 
            if((KalturaSubscriptionId!=None) and (KalturaSubscriptionId!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"KalturaSubscriptionId",
                        "value":KalturaSubscriptionId,
                        
                    }
                ) 
                 
             # SmartCard_PS    
        if(i[39] == 'SmartCard_PS'):
         
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                {
                        "name":"GLCode",
                        "value":GLCode,
                        
                }
            )
             
            if((BrandName!=None) and (BrandName!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                {
                        "name":"BrandName",
                        "value":BrandName,
                        
                }
            )                 
         
            if((TotalCost!=None) and (TotalCost!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TotalCost",
                        "value":TotalCost,
                        
                    }
                )               

            if((GuaranteeAmount!=None) and (GuaranteeAmount!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GuaranteeAmount",
                        "value":GuaranteeAmount,
                       
                    }
                )   


            if((DeviceType!=None) and (DeviceType!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"DeviceType",
                        "value":DeviceType,
                        
                    }
                )

            if((ResourceModel!=None) and (ResourceModel!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ResourceModel",
                        "value":ResourceModel,
                        
                    }
                )                        

            if((ERPGLCode!=None) and (ERPGLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ERPGLCode",
                        "value":ERPGLCode,
                        
                    }
                )     

            if((TVInfrastructure!=None) and (TVInfrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TVInfrastructure",
                        "value":TVInfrastructure,
                        
                    }
                ) 

        if(i[39] == 'VOIP_PS'):

            if((OssCode!=None) and (OssCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"OssCode",
                        "value":OssCode,
                        
                    }
                )   

            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GLCode",
                        "value":GLCode,
                            
                    }
                )   

            if((Infrastructure!=None) and (Infrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"Infrastructure",
                        "value":Infrastructure,
                        
                    }
                )

     # LeasedLine_PS
         
        if(i[39] == 'LeasedLine_PS'):
         
            if((Infrastructure!=None) and (Infrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"Infrastructure",
                        "value":Infrastructure,
                        
                    }
                )            
      
     # PublicIP_PS
     
        if(i[39] == 'PublicIP_PS'):      
         
          
            if((OssCode!=None) and (OssCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"OssCode",
                        "value":OssCode,
                         
                    }
                )                 
          
        if(i[39] == 'Datalink_PS'):             
          
            if((OssCode!=None) and (OssCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"OssCode",
                        "value":OssCode,
                        
                    }
                )  
                 
            if((UploadSpeed!=None) and (UploadSpeed!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"UploadSpeed",
                        "value":UploadSpeed,
                        
                    }
                )

            if((DownloadSpeed!=None) and (DownloadSpeed!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"DownloadSpeed",
                        "value":DownloadSpeed,
                        
                    }
                ) 
                 
                 
            if((Infrastructure!=None) and (Infrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"Infrastructure",
                        "value":Infrastructure,
                        
                    }
                )
                 
                 
     # DeviceLeasing_PS
         
        if (i[24] == 'DeviceLeasing_PS') :    
         
         
            if((ResourceModel!=None) and (ResourceModel!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"ResourceModel",
                        "value":ResourceModel,
                        
                    }
                )
                 
     
     # PhoneNumber_PS            
         
     #if(i[39] == 'PhoneNumber_PS'):   
         
         
     #GenericResource_PS    
        if(i[39] == 'GenericResource_PS'):    
            
            if((GRType!=None) and (GRType!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                {
                        "name":"GRType",
                        "value":GRType,
                        
                }
            )    
            if((GRInfo!=None) and (GRInfo!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                {
                        "name":"GRInfo",
                        "value":GRInfo,
                        
                }
            )             
     
     
        # Penalty_PS            
         
        if(i[39] == 'Penalty_PS'):
         
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                {
                        "name":"GLCode",
                        "value":GLCode,
                        
                }
            ) 

        # Discount_PS            
         
        if(i[39] == 'Discount_PS'):
         
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                {
                        "name":"GLCode",
                        "value":GLCode,
                        
                }
            )
             
     # CableTVAddOnSpec 
         
        if(i[39] == "CableTVAddOnSpec"):
            if (i[40]== "ABEL"):
             
                if((AbelProductIdentifier!=None) and (AbelProductIdentifier!="")):            
                    manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"AbelProductIdentifier",
                        "value":AbelProductIdentifier,
                        
                    }
                )
                 
            if (i[40] == "CRYPTOGUARD"):          
                if((CryptoGuardSubscriptionPackageId!=None) and (CryptoGuardSubscriptionPackageId!="")):            
                    manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"CryptoGuardSubscriptionPackageId",
                        "value":CryptoGuardSubscriptionPackageId,
                        
                    }
                )
                 
            if((KalturaSubscriptionId!=None) and (KalturaSubscriptionId!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"KalturaSubscriptionId",
                        "value":KalturaSubscriptionId,
                        
                    }
                )
                 
            if((GLCode!=None) and (GLCode!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"GLCode",
                        "value":GLCode,
                            
                    }
                )
             
            if((TVInfrastructure!=None) and (TVInfrastructure!="")):            
                manuelorder['manualOrderProductInventories'][0]['productCharacteristic'].append(
                    {
                        "name":"TVInfrastructure",
                        "value":TVInfrastructure,
                        
                    }
                )                 
             
         
        try:
            print(manuelorder)
            #print(customerNo)
            #print("??????????????????????")

            x=collection.insert_one(manuelorder)   
            
            print("insert altı")

        except Exception as e: 
            print(e)
            print("ExceptionA1")
            pass

manuelorder(args['condition'])

print("debug 996")

con.commit()
cursor.close()
con.close()