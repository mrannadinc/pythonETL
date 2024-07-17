import pgdb
import datetime
import json
import requests
import time
import conf


con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

def resource():
    headers = {"Accept": "application/json",'Content-type':'application/json'}
    
    cursor.execute("""select 
        id,
    	serviceaccount,
    	service,
    	type,
    	vlan,
    	ip_range,
    	note,
    	autoprov,
    	dateadd,
    	crm_used,
    	statuscode,
    	errordesc,
    	dnextresourceid
        from staging.networkresource
        limit 10
        
    	 """)
    	 
    	 
    for i in cursor.fetchall():
                
        id = i[0] 
        
     
        resource = { 
            "name":"service",
            "id" :i[0],
            "externalReference" :[],
            "resourceCharacteristic" :[],
            "resourceStatus":"available",
            "@type":"LogicalResource"
        }
        
        ##externalReference
        if (i[0] != None) and (i[0] != ""):
            resource['externalReference'].append({
                'externalReferenceType' : 'migration',
                'id':i[0],
                'name':'legacyNumber'
            })
        if (i[1] != None) and (i[1] != ""):
            resource['externalReference'].append({
                'externalReferenceType' : 'migration',
                'id':i[1],
                'name':'serviceAccount'
            })  
            
            
        ##resourceCharacteristic
        if (i[2] != None) and (i[2] != ""):
            resource['resourceCharacteristic'].append({
                "name":"service",
                "value":i[2],
                "valueType" :"string"
            })
        if (i[3] != None) and (i[3] != ""):
            resource['resourceCharacteristic'].append({
                "name":"resourceType",
                "value":i[3],
                "valueType" :"string"
            })     
        if (i[4] != None) and (i[4] != ""):
            resource['resourceCharacteristic'].append({
                "name":"vlan",
                "value":i[4],
                "valueType" :"string"
            })    
        if (i[5] != None) and (i[5] != ""):
            resource['resourceCharacteristic'].append({
                "name":"ipRange",
                "value":i[5],
                "valueType" :"string"
            })    
        if (i[6] != None) and (i[6] != ""):
            resource['resourceCharacteristic'].append({
                "name":"note",
                "value":i[6],
                "valueType" :"string"
            })       
        if (i[7] != None) and (i[7] != ""):
            resource['resourceCharacteristic'].append({
                "name":"autoProvision",  
                "value":i[7], 
                "valueType" :"string"
            })          
        if (i[8] != None) and (i[8] != ""):
            resource['resourceCharacteristic'].append({
                "name":"dateadd",   
                "value":i[8],
                "valueType" :"DateTime"
            })             
        if (i[9] != None) and (i[9] != ""):
            resource['resourceCharacteristic'].append({
                "name":"crmUsed", 
                "value":i[9],        
                "valueType" :"string"
            })               
            
        try:
        
          ##request   
            print(resource)
            r = requests.post(conf.url['resource'],data = json.dumps(resource,indent=3,default=str),headers=headers)
       
       
            cursor.execute("update staging.networkresource set statuscode = '{}' where id = '{}'".format(r.status_code,id))
            print(r.status_code)
            if r.status_code != 201:
                print(r.text) 
            else:
                cursor.execute("update staging.networkresource set dnextresourceid = '{}' where id = '{}'".format(r.json()['id'],i[0]))
            con.commit()        
        except Exception as e: 
            print(e)
            print("ExceptionA1")
            pass
            
       
       
        
resource()

con.commit()
cursor.close() 
con.close()