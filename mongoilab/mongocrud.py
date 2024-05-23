from pymongo import MongoClient

# MongoDB'ye bağlanma
client = MongoClient("mongodb://localhost:27017/") #mongoclint buraya yazılır
db = client["mydb"] 

# Create (Oluşturma)
def create_document():
    # Yeni bir belge (document) oluşturmak için insert_one kullanabilirsiniz.
    new_document = {
        "isim": "Halil İbrahim Annadınç",
        "yas": 28,
        "email": "h.ibrahim2321@gmail.com",
        "addres": "Hürel mahallesi 22/1 sokak 9/10 mamak ankara"
    }
    db.users.insert_one(new_document)
    print("Belge oluşturuldu...")

# Read (Okuma)
def read_documents():
    # Belirli bir koşula göre belgeleri bulmak için find kullanabilirsiniz.
    # Örneğin, tüm kullanıcıları bulmak için:
    all_users = db.users.find()
    for user in all_users:
        print(user)

# Update (Güncelleme)
def update_document():
    # Belirli bir koşula göre belgeyi güncellemek için update_one kullanabilirsiniz.
    filter_query = {"isim": "Halil İbrahim Annadınç"}
    update_query = {"$set": {"addres": "Hürel mahallesi 11 sokak 4/9 mamak ankara"}}
    db.users.update_one(filter_query, update_query)
    print("Belge güncellendi.")

# Delete (Silme)
def delete_document():
    # Belirli bir koşula göre belgeyi silmek için delete_one kullanabilirsiniz.
    filter_query = {"isim": "Halil İbrahim Annadınç"}
    db.users.delete_one(filter_query)
    print("Belge silindi.")

# Örneklerin çağrılması
create_document()
read_documents()
update_document()
read_documents()  # Güncellemeden sonra belgelerin tekrar okunması
delete_document()
read_documents()  # Silmeden sonra belgelerin tekrar okunması
