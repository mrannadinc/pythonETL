import pgdb
import datetime
import json
import requests
import time
import conf


con = pgdb.connect(host=conf.DB['hostname'],user=conf.DB['username'],password=conf.DB['password'],dbname=conf.DB['database']['staging'],port=5432)
cursor = con.cursor()

headers = {"Accept": "application/json",'Content-type':'application/json'}

configCity = list()

configCity.append({
    "id": "4b169b42-947d-4c62-84fc-143d6b83c344",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/4b169b42-947d-4c62-84fc-143d6b83c344",
    "country": "ALBANIA",
    "name": "BAJRAM CURRI",
	"city": "BAJRAM CURRI",
    "@type": "city"}
    )
configCity.append({
    "id": "4a11473f-6fe3-440e-9289-5ade7ed4b31d",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/4a11473f-6fe3-440e-9289-5ade7ed4b31d",
    "country": "ALBANIA",
    "name": "BAJZE",
	"city": "BAJZE",	
    "@type": "city"}
    )
    
configCity.append({
    "id": "24543a00-0cb6-4325-a46e-7f8104071f49",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/24543a00-0cb6-4325-a46e-7f8104071f49",
    "country": "ALBANIA",
    "name": "BALLSH",
	"city": "BALLSH",
    "@type": "city"}
    )     

configCity.append({
    "id": "78496d4e-6f4e-4efc-9032-e955871763cf",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/78496d4e-6f4e-4efc-9032-e955871763cf",
    "country": "ALBANIA",
    "name": "BERAT",
	"city": "BERAT",
    "@type": "city"}
    )

configCity.append({
    "id": "70346ea7-1a98-486b-9ef5-62cd05360719",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/70346ea7-1a98-486b-9ef5-62cd05360719",
    "country": "ALBANIA",
    "name": "BILISHT",
	"city": "BILISHT",
    "@type": "city"}
    )

configCity.append({
    "id": "ec3eb314-6e5d-4031-b273-e3bd05ac3815",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/ec3eb314-6e5d-4031-b273-e3bd05ac3815",
    "country": "ALBANIA",
    "name": "BULQIZE",
	"city": "BULQIZE",
    "@type": "city"}
    )
    
configCity.append({
    "id": "8d2e6180-3427-4185-a2c7-d337c5d32501",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/8d2e6180-3427-4185-a2c7-d337c5d32501",
    "country": "ALBANIA",
    "name": "BURREL",
	"city": "BURREL",
    "@type": "city"}
    )

configCity.append({
    "id": "ff204285-9de2-4ad8-9659-43050269a630",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/ff204285-9de2-4ad8-9659-43050269a630",
    "country": "ALBANIA",
    "name": "CERRIK",
	"city": "CERRIK",
    "@type": "city"}
    )

configCity.append({
    "id": "4422cbaf-e9d5-4f55-b772-284c9bea6f7f",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/4422cbaf-e9d5-4f55-b772-284c9bea6f7f",
    "country": "ALBANIA",
    "name": "COROVODE",
	"city": "COROVODE",
    "@type": "city"}
    )

configCity.append({
    "id": "9fffeb63-ad79-4120-bc7b-be84ea1e64e6",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/9fffeb63-ad79-4120-bc7b-be84ea1e64e6",
    "country": "ALBANIA",
    "name": "DELVINE",
	"city": "DELVINE",
    "@type": "city"}
    )

configCity.append({
    "id": "21f146d4-b485-4774-a573-72160a232bb3",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/21f146d4-b485-4774-a573-72160a232bb3",
    "country": "ALBANIA",
    "name": "DIBER",
	"city": "DIBER",
    "@type": "city"}
    )

configCity.append({
    "id": "1d184dbe-dd52-42e0-937a-ad07f25e88d9",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/1d184dbe-dd52-42e0-937a-ad07f25e88d9",
    "country": "ALBANIA",
    "name": "DIVJAKE",
	"city": "DIVJAKE",
    "@type": "city"}
    )

configCity.append({
    "id": "d2e24177-70e3-4328-b2db-3ad5b43b8612",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/d2e24177-70e3-4328-b2db-3ad5b43b8612",
    "country": "ALBANIA",
    "name": "DURRES",
	"city": "DURRES",
    "@type": "city"}
    )

configCity.append({
    "id": "37dc9921-44b8-4bca-b4a6-5fe20512f76d",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/37dc9921-44b8-4bca-b4a6-5fe20512f76d",
    "country": "ALBANIA",
    "name": "ELBASAN",
	"city": "ELBASAN",
    "@type": "city"}
    )

configCity.append({
    "id": "b483608a-0042-4711-905e-8409e2d297c4",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/b483608a-0042-4711-905e-8409e2d297c4",
    "country": "ALBANIA",
    "name": "ERSEKE",
	"city": "ERSEKE",
    "@type": "city"}
    )

configCity.append({
    "id": "4cc17180-708f-459e-8863-4e002264625f",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/4cc17180-708f-459e-8863-4e002264625f",
    "country": "ALBANIA",
    "name": "FIER",
	"city": "FIER",
    "@type": "city"}
    )

configCity.append({
    "id": "37caac67-5395-4230-9373-959b5a16dc89",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/37caac67-5395-4230-9373-959b5a16dc89",
    "country": "ALBANIA",
    "name": "FIERZE",
	"city": "FIERZE",
    "@type": "city"}
    )

configCity.append({
    "id": "d7feec78-0fe2-4a5a-b0b7-e2438ca014fd",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/d7feec78-0fe2-4a5a-b0b7-e2438ca014fd",
    "country": "ALBANIA",
    "name": "FINIQ",
	"city": "FINIQ",
    "@type": "city"}
    )
    
configCity.append({
    "id": "c9b23dff-eaa7-4f53-b59d-c3811694c00b",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/c9b23dff-eaa7-4f53-b59d-c3811694c00b",
    "country": "ALBANIA",
    "name": "FUSHE ARREZ",
	"city": "FUSHE ARREZ",
    "@type": "city"}
    )

configCity.append({
    "id": "60c857d9-f7c3-46bb-9af3-43b181907949",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/60c857d9-f7c3-46bb-9af3-43b181907949",
    "country": "ALBANIA",
    "name": "FUSHE KRUJE",
	"city": "FUSHE KRUJE",
    "@type": "city"}
    )
    
configCity.append({
    "id": "757d9e7d-9687-4e51-bea4-96ffde017181",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/757d9e7d-9687-4e51-bea4-96ffde017181",
    "country": "ALBANIA",
    "name": "GJIROKASTER",
	"city": "GJIROKASTER",
    "@type": "city"}
    )
    
configCity.append({
    "id": "7fff078c-c9ca-4c96-8436-cf09c2113fd5",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/7fff078c-c9ca-4c96-8436-cf09c2113fd5",
    "country": "ALBANIA",
    "name": "GOLEM",
	"city": "GOLEM",
    "@type": "city"}
    )
    
configCity.append({    
    "id": "96be3f0b-eb58-431f-9129-001d554e1a3f",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/96be3f0b-eb58-431f-9129-001d554e1a3f",
    "country": "ALBANIA",
    "name": "GRAMSH",
	"city": "GRAMSH",
    "@type": "city"}
    )

configCity.append({ 
    "id": "5809d46d-b2a3-45b3-8fc0-97ea493231e2",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/5809d46d-b2a3-45b3-8fc0-97ea493231e2",
    "country": "ALBANIA",
    "name": "HIMARE",
	"city": "HIMARE",
    "@type": "city"}
    )
 
configCity.append({
    "id": "2a828dc5-53bf-4504-a577-7c517c94768a",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/2a828dc5-53bf-4504-a577-7c517c94768a",
    "country": "ALBANIA",
    "name": "KAMEZ",
	"city": "KAMEZ",
    "@type": "city"}
    )
  
configCity.append({
    "id": "e1478b3b-6065-409b-8588-6d78034b48c7",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/e1478b3b-6065-409b-8588-6d78034b48c7",
    "country": "ALBANIA",
    "name": "KAVAJE",
	"city": "KAVAJE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "3b108a3a-a015-4d39-b5a1-b68076eaca72",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/3b108a3a-a015-4d39-b5a1-b68076eaca72",
    "country": "ALBANIA",
    "name": "KELCYRE",
	"city": "KELCYRE",
    "@type": "city"}
    )
 
configCity.append({
    "id": "8c1d32dd-bb48-4505-a384-bdc4ef4718ab",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/8c1d32dd-bb48-4505-a384-bdc4ef4718ab",
    "country": "ALBANIA",
    "name": "KLOS",
	"city": "KLOS",
    "@type": "city"}
    )

configCity.append({
    "id": "23362400-b887-427b-b768-b8586bc43954",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/23362400-b887-427b-b768-b8586bc43954",
    "country": "ALBANIA",
    "name": "KONISPOL",
	"city": "KONISPOL",
    "@type": "city"}
    )
  
configCity.append({
    "id": "1e6e7327-dee3-456c-8f99-e3d64a3304eb",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/1e6e7327-dee3-456c-8f99-e3d64a3304eb",
    "country": "ALBANIA",
    "name": "KOPLIK",
	"city": "KOPLIK",
    "@type": "city"}
    )

configCity.append({
    "id": "7aa3befa-a375-48f5-bec9-9f2dada68a22",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/7aa3befa-a375-48f5-bec9-9f2dada68a22",
    "country": "ALBANIA",
    "name": "KORCE",
	"city": "KORCE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "7cbeb0d2-1ac5-40b6-a3d8-4d2229dcee8a",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/7cbeb0d2-1ac5-40b6-a3d8-4d2229dcee8a",
    "country": "ALBANIA",
    "name": "KRASTE",
	"city": "KRASTE",
    "@type": "city"}
    )
 
configCity.append({
    "id": "7de693bb-ae1d-4d80-b7f2-00932e340a83",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/7de693bb-ae1d-4d80-b7f2-00932e340a83",
    "country": "ALBANIA",
    "name": "KRRABE",
	"city": "KRRABE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "4247c9a4-4ac9-420e-a032-e0a321b89cc8",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/4247c9a4-4ac9-420e-a032-e0a321b89cc8",
    "country": "ALBANIA",
    "name": "KRUJE",
	"city": "KRUJE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "b5e71325-868e-4150-a6bc-396803990a62",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/b5e71325-868e-4150-a6bc-396803990a62",
    "country": "ALBANIA",
    "name": "KRUME",
	"city": "KRUME",
    "@type": "city"}
    )
  
configCity.append({
    "id": "9ea1df1c-8f6d-447d-946c-b75de0678a2a",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/9ea1df1c-8f6d-447d-946c-b75de0678a2a",
    "country": "ALBANIA",
    "name": "KUCOVE",
	"city": "KUCOVE",
    "@type": "city"}
    )
 
configCity.append({
    "id": "be154f92-7789-49c9-8d77-84823a1a9421",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/be154f92-7789-49c9-8d77-84823a1a9421",
    "country": "ALBANIA",
    "name": "KUKES",
	"city": "KUKES",
    "@type": "city"}
    )
  
configCity.append({
    "id": "52772dd5-aa80-4683-b6c0-cae93b15516a",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/52772dd5-aa80-4683-b6c0-cae93b15516a",
    "country": "ALBANIA",
    "name": "KURBNESH",
	"city": "KURBNESH",
    "@type": "city"}
    )
  
configCity.append({
    "id": "ee507c1b-e97c-4d64-986d-2cdac2c68d6d",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/ee507c1b-e97c-4d64-986d-2cdac2c68d6d",
    "country": "ALBANIA",
    "name": "LAC",
	"city": "LAC",
    "@type": "city"}
    )
 
configCity.append({
    "id": "11563342-b47c-4a76-af90-6f3f82cd7cbb",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/11563342-b47c-4a76-af90-6f3f82cd7cbb",
    "country": "ALBANIA",
    "name": "LESKOVIK",
	"city": "LESKOVIK",
    "@type": "city"}
    )
  
configCity.append({
    "id": "9661f454-2554-48d2-990e-a534b0a59750",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/9661f454-2554-48d2-990e-a534b0a59750",
    "country": "ALBANIA",
    "name": "LEZHE",
	"city": "LEZHE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "25402358-e365-42e3-a7da-cfb7f8cc105b",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/25402358-e365-42e3-a7da-cfb7f8cc105b",
    "country": "ALBANIA",
    "name": "LIBOHOVE",
	"city": "LIBOHOVE",
    "@type": "city"}
    )
 
configCity.append({
    "id": "dc2fc69f-dc2b-4e66-8a89-42c06c86e314",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/dc2fc69f-dc2b-4e66-8a89-42c06c86e314",
    "country": "ALBANIA",
    "name": "LIBRAZHD",
	"city": "LIBRAZHD",
    "@type": "city"}
    )
  
configCity.append({
    "id": "1e65ddea-9ef0-459f-99fd-d5d10d56231d",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/1e65ddea-9ef0-459f-99fd-d5d10d56231d",
    "country": "ALBANIA",
    "name": "LUSHNJE",
	"city": "LUSHNJE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "71535f44-f205-4750-9732-fbe29afe74b6",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/71535f44-f205-4750-9732-fbe29afe74b6",
    "country": "ALBANIA",
    "name": "LUZ",
	"city": "LUZ",
    "@type": "city"}
    )
  
configCity.append({
    "id": "4c1f14ae-333c-4f4b-852f-c9a414ca579b",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/4c1f14ae-333c-4f4b-852f-c9a414ca579b",
    "country": "ALBANIA",
    "name": "LUZ.S",
	"city": "LUZ.S",
    "@type": "city"}
    )
  
configCity.append({
    "id": "5f29c3b8-a54f-472d-a784-4a96b92707d4",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/5f29c3b8-a54f-472d-a784-4a96b92707d4",
    "country": "ALBANIA",
    "name": "MALIQ",
	"city": "MALIQ",
    "@type": "city"}
    )
  
configCity.append({
    "id": "aed49cec-4653-4747-97ce-3b2277c652b6",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/aed49cec-4653-4747-97ce-3b2277c652b6",
    "country": "ALBANIA",
    "name": "MAMURRAS",
	"city": "MAMURRAS",
    "@type": "city"}
    )
  
configCity.append({
    "id": "fa6c4579-4570-4528-8ded-0a95903842ff",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/fa6c4579-4570-4528-8ded-0a95903842ff",
    "country": "ALBANIA",
    "name": "MANEZ",
	"city": "MANEZ",
    "@type": "city"}
    )
  
configCity.append({
    "id": "b11f5125-409f-48a2-82b3-165bc229fa41",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/b11f5125-409f-48a2-82b3-165bc229fa41",
    "country": "ALBANIA",
    "name": "MEMALIAJ",
	"city": "MEMALIAJ",
    "@type": "city"}
    )
  
configCity.append({
    "id": "30b642c4-90d8-4a82-ab63-4fa3579d8c52",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/30b642c4-90d8-4a82-ab63-4fa3579d8c52",
    "country": "ALBANIA",
    "name": "MILOT",
	"city": "MILOT",
    "@type": "city"}
    )
 
configCity.append({
    "id": "75044e39-8332-4c41-80f9-5c4e9bc5f520",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/75044e39-8332-4c41-80f9-5c4e9bc5f520",
    "country": "ALBANIA",
    "name": "ORIKUM",
	"city": "ORIKUM",
    "@type": "city"}
    )
 
configCity.append({
    "id": "e34d6671-4919-4533-9b71-f81102877909",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/e34d6671-4919-4533-9b71-f81102877909",
    "country": "ALBANIA",
    "name": "PATOS",
	"city": "PATOS",
    "@type": "city"}
    )
 
configCity.append({
    "id": "a57c17ed-2de4-4b1b-9d87-5a066f84089b",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/a57c17ed-2de4-4b1b-9d87-5a066f84089b",
    "country": "ALBANIA",
    "name": "PEQIN",
	"city": "PEQIN",
    "@type": "city"}
    )
  
configCity.append({
    "id": "e7074c9b-4cb2-4618-b239-f09e1ad5c94c",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/e7074c9b-4cb2-4618-b239-f09e1ad5c94c",
    "country": "ALBANIA",
    "name": "PERMET",
	"city": "PERMET",
    "@type": "city"}
    )
 
configCity.append({
    "id": "4943ac72-1f7a-40c3-981f-99441c29a538",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/4943ac72-1f7a-40c3-981f-99441c29a538",
    "country": "ALBANIA",
    "name": "PESHKOPI",
	"city": "PESHKOPI",
    "@type": "city"}
    )
 
configCity.append({
    "id": "faf21973-9a57-47d0-959d-fa250a980a42",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/faf21973-9a57-47d0-959d-fa250a980a42",
    "country": "ALBANIA",
    "name": "POGRADEC",
	"city": "POGRADEC",
    "@type": "city"}
    )
  
configCity.append({
    "id": "7453936d-bcec-46ce-85c3-745ba458886e",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/7453936d-bcec-46ce-85c3-745ba458886e",
    "country": "ALBANIA",
    "name": "POLICAN",
	"city": "POLICAN",
    "@type": "city"}
    )
  
configCity.append({
    "id": "ac0af44d-eac3-4e4c-b91f-f1593cbf3e5d",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/ac0af44d-eac3-4e4c-b91f-f1593cbf3e5d",
    "country": "ALBANIA",
    "name": "PRRENJAS",
	"city": "PRRENJAS",
    "@type": "city"}
    )
  
configCity.append({
    "id": "ec12b943-4096-4aca-af4d-8dfcf3e0a58e",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/ec12b943-4096-4aca-af4d-8dfcf3e0a58e",
    "country": "ALBANIA",
    "name": "PUKE",
	"city": "PUKE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "6473f43a-b219-4d81-8a9c-21d1bef2885a",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/6473f43a-b219-4d81-8a9c-21d1bef2885a",
    "country": "ALBANIA",
    "name": "REPS",
	"city": "REPS",
    "@type": "city"}
    )
  
configCity.append({
    "id": "a3c2e746-89f3-48e1-92b8-f509f1cf069c",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/a3c2e746-89f3-48e1-92b8-f509f1cf069c",
    "country": "ALBANIA",
    "name": "ROSKOVEC",
	"city": "ROSKOVEC",
    "@type": "city"}
    )
  
configCity.append({
    "id": "68c307f8-27ee-4e8b-9ea4-c0b865a66843",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/68c307f8-27ee-4e8b-9ea4-c0b865a66843",
    "country": "ALBANIA",
    "name": "RRESHEN",
	"city": "RRESHEN",
    "@type": "city"}
    )
  
configCity.append({
    "id": "f816319c-9d0d-4a1d-b591-e0a7c2fa7ae3",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/f816319c-9d0d-4a1d-b591-e0a7c2fa7ae3",
    "country": "ALBANIA",
    "name": "RROGOZHINE",
	"city": "RROGOZHINE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "648490d6-aeb0-421b-be2b-ca260bb9a842",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/648490d6-aeb0-421b-be2b-ca260bb9a842",
    "country": "ALBANIA",
    "name": "RUBIK",
	"city": "RUBIK",
    "@type": "city"}
    )
  
configCity.append({
    "id": "a85d34cd-1c56-4942-9dc9-71a9c6504770",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/a85d34cd-1c56-4942-9dc9-71a9c6504770",
    "country": "ALBANIA",
    "name": "SARANDE",
	"city": "SARANDE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "e77e37d9-e8d9-4c7f-b925-52eaa039ccd1",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/e77e37d9-e8d9-4c7f-b925-52eaa039ccd1",
    "country": "ALBANIA",
    "name": "SELENICE",
	"city": "SELENICE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "5a82fc56-850c-4abe-b699-a5548a805d1f",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/5a82fc56-850c-4abe-b699-a5548a805d1f",
    "country": "ALBANIA",
    "name": "SHENGJIN",
	"city": "SHENGJIN",
    "@type": "city"}
    )
  
configCity.append({
    "id": "9a10602e-23eb-4483-8a92-907519a8bac8",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/9a10602e-23eb-4483-8a92-907519a8bac8",
    "country": "ALBANIA",
    "name": "SHIJAK",
	"city": "SHIJAK",
    "@type": "city"}
    )
  
configCity.append({
    "id": "3a20b722-d4cd-489a-9210-be7cebc684ca",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/3a20b722-d4cd-489a-9210-be7cebc684ca",
    "country": "ALBANIA",
    "name": "SHKODER",
	"city": "SHKODER",
    "@type": "city"}
    )
 
configCity.append({
    "id": "74463af7-ce49-4d0b-aa2f-d9ae1efceb14",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/74463af7-ce49-4d0b-aa2f-d9ae1efceb14",
    "country": "ALBANIA",
    "name": "SUKTH",
	"city": "SUKTH",
    "@type": "city"}
    )
  
configCity.append({
    "id": "67cdc109-6859-4ebb-9a89-f2fd422affdd",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/67cdc109-6859-4ebb-9a89-f2fd422affdd",
    "country": "ALBANIA",
    "name": "TEPELENE",
	"city": "TEPELENE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "45c7d6e0-c04e-4957-b581-c553997a48e1",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/45c7d6e0-c04e-4957-b581-c553997a48e1",
    "country": "ALBANIA",
    "name": "TIRANE",
	"city": "TIRANE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "16bed23b-5a97-4b45-b74f-ab5715ed83df",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/16bed23b-5a97-4b45-b74f-ab5715ed83df",
    "country": "ALBANIA",
    "name": "ULEZ",
	"city": "ULEZ",
    "@type": "city"}
    )
  
configCity.append({
    "id": "af4cd326-4126-4e15-b6d4-dd5752edb74c",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/af4cd326-4126-4e15-b6d4-dd5752edb74c",
    "country": "ALBANIA",
    "name": "URA VAJGURORE",
	"city": "URA VAJGURORE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "7f06892f-1f7d-4c1c-8350-542093f93a2d",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/7f06892f-1f7d-4c1c-8350-542093f93a2d",
    "country": "ALBANIA",
    "name": "VAU I DEJES",
	"city": "VAU I DEJES",
    "@type": "city"}
    )
 
configCity.append({
    "id": "e72dbd5c-174d-4406-bb2d-7f4df06693dc",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/e72dbd5c-174d-4406-bb2d-7f4df06693dc",
    "country": "ALBANIA",
    "name": "VELIPOJE",
	"city": "VELIPOJE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "b698ba38-9e7b-4354-a184-58ccaf79e9b6",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/b698ba38-9e7b-4354-a184-58ccaf79e9b6",
    "country": "ALBANIA",
    "name": "VLORE",
	"city": "VLORE",
    "@type": "city"}
    )
  
configCity.append({
    "id": "03014355-2fcb-446c-a2df-c59207ddfa43",
    "href": "/api/geographicAddressManagement/v4/geographicAddress/03014355-2fcb-446c-a2df-c59207ddfa43",
    "country": "ALBANIA",
    "name": "VORE",
	"city": "VORE",
    "@type": "city"}
    )
c=0
for i in configCity:
    c = c+1
    print(c)
    print(i)
    r = requests.post(conf.url['contactmedium'], data=json.dumps(i,indent=3,default=str), headers=headers)
    