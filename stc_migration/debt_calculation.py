import pgdb
import datetime
import json
import requests
import time
import conf
from datetime import datetime
from dateutil import parser
import pandas as pd
import boto3
from datetime import datetime, timedelta
import io
from awsglue.utils import getResolvedOptions
import sys

args = getResolvedOptions(sys.argv, ['condition'])

con = pgdb.connect(host=conf.DB['hostname'], user=conf.DB['username'], password=conf.DB['password'],
                   dbname=conf.DB['database']['staging'], port=5432)
cursor = con.cursor()

def debt_calculation(condition):
    headers = {"Accept": "application/json", 'Content-type': 'application/json'}
    
    if condition == 'template': #you can run job as manual
        condition = "1 = 1"

    cursor.execute("""
        select p.agreementid,
        	max(p.terminationdate) terminationdate,  current_date curDate
        from staging.product p
        	inner join staging.customer c on c.customerid = p.customerid
        	inner join staging.agreement a on p.agreementid = a.agreementid
        	inner join staging.resource r on r.subscriberaccountkey = p.subscriberaccountkey
        	left join staging.productoffering po on updated_productofferingid || a.producttype = po.legacyofferingid_joinkey
        	left join staging.product_specs ps on po.dnextproductofferingid = ps.dnextproductofferingid
        where 1 = 1
        	and c.mig_flag = '1'
        	and p.prod_mig_flag = '1'
        	and r.migflag = '1'
        	and a.ratingType = 'Prepaid'
        	and (
        		(
        			p.status = 'terminated'
        			and a.type = 'Commitment'
        			and a.status = 'Active'
        		)
        	)
        	and (ps.dnextproductofferingid is not null)
        	and {}
        group by p.agreementid;
    """.format(condition))

    account_id_1 = []
    account_id_2 = []
    account_id_3 = []
    print("start")

    query_results = cursor.fetchall()
    for i in query_results:
        datetime_obj = datetime.strptime(i[1], '%Y-%m-%d %H:%M:%S')

        my_time = datetime.min.time()
        now = datetime.combine(i[2], my_time)
        control = (now - datetime_obj).days

        print(control)

        pk = i[0]
        if control > 0 and control < 31:
            account_id_1.append(i[0])
            debt = {
                "action": "charge",
                "billingAccountId": i[0]
            }

            r = requests.post(conf.url['shoppingCart'],data=json.dumps(debt, indent=3, default=str), headers=headers)
            print(r)
            print(r.text)

            cursor.execute("""insert into staging.debt_log (agreementId, type, statusCode,message, requestDate)  VALUES ( '{}','{}','{}','{}','{}')""".format(
                    pk, '1 month debt', r.status_code, r.text, i[1]))
            con.commit()
            print("1 month debt")

        elif control >= 31:
            account_id_2.append(i[0])
            debt = {
                "action": "charge",
                "billingAccountId": i[0]
            }

            r = requests.post(conf.url['shoppingCart'],data=json.dumps(debt, indent=3, default=str), headers=headers)

            cursor.execute("""insert into staging.debt_log (agreementId, type, statusCode,message, requestDate)  VALUES ( '{}','{}','{}','{}','{}')""".format(
                    pk, '2 month debt', r.status_code, r.text, i[1]))
            con.commit()
            print(r.status_code)
            

            print("Second Post")
            r = requests.post(conf.url['shoppingCart'],data=json.dumps(debt, indent=3, default=str), headers=headers)
            cursor.execute("""insert into staging.debt_log (agreementId, type, statusCode,message, requestDate)  VALUES ( '{}','{}','{}','{}','{}')""".format(
                    pk, '2 month debt 2nd', r.status_code, r.text, i[1]))
            con.commit()
            print("2 month debt")
            print(r.text)

        else:
            account_id_3.append(i[0])
            print("no debt")


    print(account_id_1)
    print(account_id_2)
    print(account_id_3)
    print("End")
    print(args['condition'])
    

debt_calculation(args['condition'])

con.commit()
cursor.close()
con.close()