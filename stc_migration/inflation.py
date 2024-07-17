#product price inflation 
import Inflation
import pgdb
import datetime
import json
import requests
import time
import conf
import sys
import subprocess

con_staging = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor_staging = con_staging.cursor()

def ProductInflation():
    
    successedpatchlist = []
    unsuccessfulpatchlist = []
    #token = ''  ,'Authorization': "Bearer"+ " " +token
    headers ={'Accept':"application/json",'Content-type':'application/json-patch+json'}
																						   
    inflation_rate=float(Inflation.inflation_rate)
    jira_id = Inflation.ticket_id
    char_add = Inflation.characteristic_add
    tax_rate_value = Inflation.tax_rate
    product_status_list = Inflation.product_status_list_generation
    print(product_status_list)
    try:
        cursor_staging.execute("""
        select
        product_id,duty_free_amount, tax_included_amount, tax_rate  
        from staging.vf_brm_inflation_update
        """)
        for i in cursor_staging.fetchall():
            id_list = i[0]
            duty_free_amount = float(i[1].replace(',', '.'))
            tax_included_amount = float(i[2].replace(',', '.'))
            tax_rate = i[3]
            
            get_product_url = conf.url['product'] + '/' + id_list
            get_product_json = requests.get(get_product_url, headers=headers).json()
            productcharacteristic = get_product_json["productCharacteristic"]
            product_Price = get_product_json["productPrice"]
            productstatus = get_product_json["status"]
            try:
                externalReference = get_product_json["externalReference"]
            except:
                externalReference = []
            try:
                pricetype = product_Price[0]
                duty_free_amount_value = pricetype["price"]["dutyFreeAmount"]["value"]
                tax_included_amount_value = pricetype["price"]["taxIncludedAmount"]["value"]
                tax_rate_value = pricetype["price"]["taxRate"]
                if productstatus in product_status_list:
                    if duty_free_amount_value > 0:
                        product_characteristic = []
                        found_characteristic = False
                        flag_found = False
                        found_characteristic2 = False
                        found_characteristic3 = False
                        if duty_free_amount_value != duty_free_amount:
                            pricetype["price"]["dutyFreeAmount"]["value"] = duty_free_amount
                            pricetype["price"]["taxIncludedAmount"]["value"] = tax_included_amount
                            pricetype["price"]["taxRate"] = tax_rate
                            flag_found = True
                            if char_add == "yes":
                                for p in productcharacteristic:
                                    if p['name'] == 'CP2024_Inflation_IncrementPercent':
                                        found_characteristic = True
                                        infvalue = p['value']
                                        if infvalue != inflation_rate:
                                            p['value'] = inflation_rate
                                            flag_found = True
                                if not found_characteristic:
                                    productcharacteristic.append({
                                                "name": "CP2024_Inflation_IncrementPercent",
                                                "valueType": "string",
                                                "value": inflation_rate })
                                    flag_found = True
                                for f in productcharacteristic:
                                    if f['name'] == 'CP2024_dutyFreeAmount_old':
                                        found_characteristic3 = True
                                        dutyvalue = f['value']
                                        if dutyvalue == duty_free_amount_value:
                                            f['value'] = duty_free_amount_value
                                            flag_found = True
                                if not found_characteristic3:
                                    productcharacteristic.append({
                                        "name": "CP2024_dutyFreeAmount_old",
                                        "valueType": "string",
                                        "value": duty_free_amount_value })
                                    flag_found = True
                                for t in productcharacteristic:
                                    if t['name'] == 'CP2024_taxIncludedAmount_old':
                                        found_characteristic2 = True
                                        taxvalue = t['value']
                                        if taxvalue != tax_included_amount_value:
                                            t['value'] = tax_included_amount_value
                                            flag_found = True
                                if not found_characteristic2:  
                                    productcharacteristic.append({
                                        "name": "CP2024_taxIncludedAmount_old",
                                        "valueType": "string",
                                        "value": tax_included_amount_value })
                                    flag_found = True
                            else:
                                print("no characteristic added")
                                pass
                        if flag_found == True:
                            externalReference.append({"id": jira_id,
                                    "externalReferenceType": "migration",
                                    "name": "migration_patch"})
                       
                        patch = [{"op":"add","path":"/productPrice","value":pricetype},{"op":"add","path":"/productCharacteristic","value":productcharacteristic},{"op": "add", "path": "/externalReference", "value":externalReference}]
                        try:
                            r = requests.patch(conf.url['product']+'/'+id_list, data=json.dumps(patch,indent=3,default=str), headers=headers)
                            if r.status_code != 200:
                                print('product Patch Error.id_list :{}'.format(id_list))
                                print(r.text)
                                unsuccessfulpatchlist.append(id_list)
                            else:
                                successedpatchlist.append(id_list)
                        except Exception as e:
                            print('productPatch Error.id_list :{}'.format(id_list))
                            unsuccessfulpatchlist.append(id_list)
                            print(e)
                            pass
                       
                    else:
                        try:
                            product_id = id_list
                            agreement_id = get_product_json["agreement"][0]["id"]
                            customer_id = get_product_json["relatedParty"][0]["id"]
                            tax_rate = tax_rate_value
                            duty_free_amount = duty_free_amount_value
                            tax_included_amount = tax_included_amount_value
                            product_status = productstatus
                            
                            insert_query = """
                            INSERT INTO staging.inflation_error_list(
                            product_id,
                        	agreement_id,
                        	customer_id,
                        	tax_rate,
                        	duty_free_amount,
                        	tax_included_amount,
                        	product_status
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s)
                            """
                            cursor_staging.execute(insert_query, (product_id, agreement_id, customer_id, tax_rate, duty_free_amount, tax_included_amount, product_status))
                            con_staging.commit()
                        except:
                            print("Price information is empty!")
                    
                else:
                    try:
                        product_id = id_list
                        agreement_id = get_product_json["agreement"][0]["id"]
                        customer_id = get_product_json["relatedParty"][0]["id"]
                        tax_rate = tax_rate_value
                        duty_free_amount = duty_free_amount_value
                        tax_included_amount = tax_included_amount_value
                        product_status = productstatus
                        
                        insert_query = """
                        INSERT INTO staging.inflation_error_list(
                        product_id,
                    	agreement_id,
                    	customer_id,
                    	tax_rate,
                    	duty_free_amount,
                    	tax_included_amount,
                    	product_status
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        """
                        cursor_staging.execute(insert_query, (product_id, agreement_id, customer_id, tax_rate, duty_free_amount, tax_included_amount, product_status))
                        con_staging.commit()
                    except Exception as e:
                        print("Error:")
                        print(e)
                    print("status not appropriate")
                    print(id_list)
                    pass
            except Exception as e:
                print("Price_Null GET ERROR FOR:", id_list)
                print(e)
                pass

    except Exception as e:
        print("PRODUCT GET ERROR FOR:", id_list)
        print(e)
        pass

    print("Basarili id list->{}, ".format(successedpatchlist))
    print("Basarisiz id list->{}, ".format(unsuccessfulpatchlist))
    print("Basarili patch sayisi->{}, ".format(len(successedpatchlist)))
    print("Basarisiz patch sayisi->{}, ".format(len(unsuccessfulpatchlist)))
    cursor_staging.close()
    con_staging.close()
    
ProductInflation()
