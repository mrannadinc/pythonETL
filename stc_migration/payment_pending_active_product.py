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

def payment(condition):
    successedrecordlist = []
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"
        
    cursor.execute(f"""
     select  
        min(p.productid),
    	min(p.customerid),
    	min(p.startdate),
    	min(c.name),
    	min(ps.dnextproductofferingid),
    	min(ps.dnextproductofferingname),
    	min('PA_' || p.legacyproductofferingid || '_' || p.agreementid) paymentmethodid,
    	min(a.contractcurrency),
    	sum(cast(p.sumprice as double precision)) sumprice,
    	min(case when a.ratingtype = 'Postpaid' then 'Post' else 'Pre' end || a.dnextbillingid),
    	min(a.type),
        min(p.agreementid),
        min(prole.fullname), 
        min(prt.name)
    from staging.product p
    	left outer join (
    		select p1."agreementid",p1."legacyproductofferingid"
    		from staging.product p1
    		where p1.status = 'active'
    			and p1."prod_mig_flag" = '1'
    		group by p1."agreementid",p1."legacyproductofferingid"
    	) active_agreements on active_agreements."agreementid" = p.agreementid
    	                    and active_agreements.legacyproductofferingid = p.legacyproductofferingid
    	inner join staging.customer c on c.customerid = p.customerid
    	inner join staging.agreement a on p.agreementid = a.agreementid
    	left join staging.catalogue ctl on upper(p.updated_productofferingid) = upper(ctl.code)
    	left join staging.productoffering po on updated_productofferingid || a.producttype = po.legacyofferingid_joinkey
    	left join staging.product_specs ps on po.dnextproductofferingid = ps.dnextproductofferingid
    	left join staging.partyrole prole on prole.siebelid = p.createdbysiebelid
    	left join staging.partner prt on prt.legacypartnerid = prole.parent_id
    where p.status = 'pendingActive'
    	and c.mig_flag = '1'
    	and p."prod_mig_flag" = '1'
    	and active_agreements.agreementid is null
    	and ps.dnextproductofferingid is not null
    	group by p.agreementid,p.legacyproductofferingid
											  
        """)
    for i in cursor.fetchall():
        pk = i[0]
        
        taxamount = float(i[8]) / 1.2 * 0.2
        try:
            payment = {
                "@baseType": "Payment",
                "@referredType": "Payment",
                "@type": "Payment",
                "description": "Migration-Payment",
            	"status": "done",
                "paymentDate": stringToTimestamp(i[2]),
                "statusDate": stringToTimestamp(i[2]),
                "amount": {
                    "unit": i[7],
                    "value": i[8]
                },
                "taxAmount": {
                    "unit": i[7],
                    "value": taxamount
                },
                "totalAmount": {
                    "unit": i[7],
                    "value": i[8]
                },
                "characteristic": [
                {
                    "name": "agreement.id",
                    "value":  i[11],
                    "valueType": "STRING"
                },
                {
                    "name": "salesAgent.name",
                    "value": i[12],
                    "valueType": "STRING"
                },
                {
                    "name": "salesPartner.name",
                    "value": i[13],
                    "valueType": "STRING"
                }
                ],
                "payer": {
                    "id": "F" + i[1],
                    "href": "api/customerManagement/v4/customer/F" + i[1],
                    "@baseType": "RelatedParty",
                    "@referredType": "Customer",
                    "@type": "RelatedParty",
                    "name": i[3],
                    "role": "Customer"
                },
 
                "paymentMethod": {
                    "id": "63587117-7800-48e1-94ed-404cf55daba6",
                    "href": "/api/paymentMethod/v4/paymentMethod/63587117-7800-48e1-94ed-404cf55daba6",
                    "@baseType": "PaymentMethodRefOrValue",
                    "@referredType": "PaymentMethodRefOrValue",
                    "@type": "Cash",
                    "name": "BANKNOTE"
                },

                "paymentItem": [{
                            "@baseType": "PaymentItem",
                            "@referredType": "PaymentItem",
                            "@type": "Product",  
                            "name": i[5],
                            "paymentTransactionType": "ProductOrderItem",
                            "actionType": "add",
                            "amount": {
                                "unit": i[7],
                                "value": i[8]
                            },
                            "taxAmount": {
                                "unit": i[7],
                                "value": taxamount
                            },
                            "totalAmount": {
                                "unit": i[7],
                                "value": i[8]
                            } 
                        }]
                
                }
            
            if i[9] != None:
                payment['account'] = {
                    "id": i[9],
                    "href": "/api/accountManagement/v4/billingAccount/" + i[9],
                    "@baseType": "AccountRef",
                    "@referredType": "BillingAccount",
                    "@type": "AccountRef",
                    "name": i[10]
                }
            
            print(payment)
            r = requests.post(conf.url['payment'],data=json.dumps(payment),headers=headers)
            
            cursor.execute("update staging.product set payment_statuscode = '{}' where productid = '{}' ".format(r.status_code,pk))
            con.commit()
            
            if r.status_code == 200:
                cursor.execute("update staging.product set dnextpaymentid = '{}' where productid = '{}'".format(r.json()['id'],pk))
                con.commit()
                
                successedrecordlist.append(r.json()['id'])
            else:
                print("Error Payment Payload ProductId: " + pk)
                print("Status Code: ",r.status_code)
                print(r.text)

        except Exception as e:
            print("Exception WhenCreatePaymentPayload productId: " + pk)
            print(e)
            cursor.execute("update staging.product set payment_statuscode = '434' where productid = '{}' ".format(pk))
            con.commit()
            pass
    
    print("Payment(pendingActive) Success Records")
    print(successedrecordlist)
    print("Payment(pendingActive) Success Records Count")
    print(len(successedrecordlist))
    print(args['condition'])
                
       
def stringToTimestamp(val):
    if((val == None) or (val == "")):
        return "1800-01-01T00:00:00Z"
    else:
        val = str(val).replace(' ','T')+'Z'
        return val    
        
payment(args['condition'])

con.commit()
cursor.close() 
con.close()
