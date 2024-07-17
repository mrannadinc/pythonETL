import pgdb
import datetime
import json
import requests
import time
import conf
import sys
import subprocess


headers ={'Accept':"application/json",'Content-type':'application/json-patch+json'}
con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()


con_2 = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['account'],port=5432)
cursor_2 = con_2.cursor()

successedpatchlist = [] 
unsuccessedpatchlist = [] 
 
def PatchAccount():
    
    cursor.execute("""
       SELECT
        distinct
        customer_id,
        rating_type,
        credit_class
        FROM staging.credit_class_update""")
    
    for i in cursor.fetchall():
        customer_id = i[0] 
        rating_type = i[1]
        creditClass = i[2]
        
        try:
            cursor_2.execute("""
                select t3.id
                from public.billing_account t3 
                inner join public.related_party t2 on t3.id = t2.billing_account_id
                and t3.account_type  = 'InvoiceAccount'
                and t3.rating_type = '{}'
                and t2.id= '{}'
                """.format(rating_type, customer_id))
                
            for k in cursor_2.fetchall():
                accountId = k[0]
    
                try:
                    get_account_url = conf.url['account']+'/'+accountId
                    get_account_json = requests.get(get_account_url,headers=headers).json()
                    newCharacteristic = []
                    
                    try:
                        characteristic = get_account_json["characteristic"] #payload'da characteristic alanı yoksa except'e düşer.
                        for a in characteristic: #payload'da characteristic alanı varsa döngüye sokar.
                            if a["name"] == "CreditClassId": #CreditClassId krakteristiği varsa pas geçer.
                                pass
                            else:
                                newCharacteristic.append(a) #Geriye kalan karakteristiklerin hepsini ekler.
                        
                        newCharacteristic.append({
                                "name": "CreditClassId",
                                "value": creditClass,
                                "@baseType": "Characteristic",
                                "@schemaLocation": "/api/accountManagement/v4/schema/characteristic.json",
                                "@type": "Characteristic"
                            }) #son olarak CreditClassId karakteristiğini yeni değeriyle ekler.
                            
                    except: #characteristic alanı yoksa excepte düşer ve direkt o alanı ekler.
                        newCharacteristic.append({
                                        "name": "CreditClassId",
                                        "value": creditClass,
                                        "@baseType": "Characteristic",
                                        "@schemaLocation": "/api/accountManagement/v4/schema/characteristic.json",
                                        "@type": "Characteristic"
                                    })
                                    
                    try:
                        externalReference = get_account_json["externalReference"] #externalReference alanı varsa flag ekler. name yapılan Jira no'ya göre değişir.
                        
                        for external in externalReference:
                            if external['id'] == "creditClass_update":
                                externalReference.remove(external)
                        
                        externalReference.append({"id": "creditClass_update",
                                                "externalReferenceType": "migration",
                                                "name": "migration_patch",
                                                "@baseType": "ExternalReference",
                                                "@schemaLocation": "/api/accountManagement/v4/schema/externalreference.json",
                                                "@type": "ExternalReference"})
                                                
                    except:
                        externalReference = [] #externalReference alanı yoksa o alanı oluşturur ve flag ekler. name yapılan Jira no'ya göre değişir.
                        externalReference.append({"id": "creditClass_update",
                                                "externalReferenceType": "migration",
                                                "name": "migration_patch",
                                                "@baseType": "ExternalReference",
                                                "@schemaLocation": "/api/accountManagement/v4/schema/externalreference.json",
                                                "@type": "ExternalReference"})
                                                
                    patch = [{"op":"add","path":"/characteristic","value":newCharacteristic}, 
                            {"op":"add","path":"/externalReference","value":externalReference}]
                     
                    try:
                        r_account = requests.patch(conf.url['account']+'/'+accountId, data=json.dumps(patch,indent=3,default=str), headers=headers)
                        if r_account.status_code != 200:
                            print('Account Patch Error')
                            print(r_account.text)
                            unsuccessedpatchlist.append(accountId)
                        else:
                            successedpatchlist.append(accountId)
                        
                    except Exception as e:
                        print('Account Patch Error')
                        unsuccessedpatchlist.append(accountId)
                        pass
                    
                    
                except Exception as e:
                    print("Account Error Found: " + accountId)
                    print(e)
                    pass
                    
        except Exception as e:
            print("Exception WhenCreatePayload for: " + accountId)
            print(e)
    
    con.commit()
    con.close()
    cursor.close()
    con_2.close()
    cursor_2.close()
    
    print("successed list->{}, ".format(successedpatchlist))
    print("unsuccessed list->{}, ".format(unsuccessedpatchlist))
    print("successed count->{}, ".format(len(successedpatchlist)))
    print("unsuccessed count->{}, ".format(len(unsuccessedpatchlist)))
    

PatchAccount()