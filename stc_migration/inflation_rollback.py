#product price inflation ROLLBACK
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
    idlist = Inflation.inflation_rollback_list
    jira_id = Inflation.ticket_id
    try:
        cursor_staging.execute("""select product_id from staging.inflation_rollback """)
        for i in cursor_staging.fetchall():
            id_list = i[0]
            
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
                try:
                    if duty_free_amount_value > 0:
                        flag_found = False
                        for f in productcharacteristic:
                            if f['name'] == 'CP2024_dutyFreeAmount_old':
                                dutyvalue = f['value']
                        for t in productcharacteristic:
                            if t['name'] == 'CP2024_taxIncludedAmount_old':
                                taxvalue = t['value']
                        pricetype["price"]["dutyFreeAmount"]["value"] = dutyvalue
                        pricetype["price"]["taxIncludedAmount"]["value"] = taxvalue
                        found_characteristic = False
                        product_characteristic = []
                        for p in productcharacteristic:
                            if p['name'] == 'CP2024_Inflation_percentReverse':
                                found_characteristic = True
                                flag_found = True
                            elif p['name'] not in ['CP2024_dutyFreeAmount_old', 'CP2024_taxIncludedAmount_old', 'CP2024_Inflation_IncrementPercent']:
                                product_characteristic.append(p)
                                flag_found = True
                        if not found_characteristic:
                            product_characteristic.append({
                                        "name": "CP2024_Inflation_percentReverse"})
                            flag_found = True
                            
                        if flag_found:
                            externalReference.append({"id": jira_id,
                                    "externalReferenceType": "migration",
                                    "name": "migration_patch_rollback"})
                        patch = [{"op":"add","path":"/productPrice","value":pricetype},{"op": "add", "path": "/externalReference", "value":externalReference},{"op": "replace", "path": "/productCharacteristic", "value": product_characteristic}]
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
                        pass
                except:
                    print("Price information is empty!")
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
