# ogrenciler = {
#     '120' : {
#         'ad' : 'Ali',
#         'soyad' : 'Yılmaz',
#         'telefon' : '532 000 00 01'
#     },
#     '125' : {
#         'ad' : 'Can',
#         'soyad' : 'Korkmaz',
#         'telefon' : '532 000 00 02'
#     },
#     '128' : {
#         'ad' : 'Volkan',
#         'soyad' : 'Yükselen',
#         'telefon' : '532 000 00 03'
#     }
# }

# 1-) Bilgileri verilen öğrencileri kullanıcadan aldığınız bilgilerle distionary içerisinde saklayınız
# 2-) Öğrenci numarasını kullanıcıdan alıp ilgili öğrenci bilgisini gösterin


ogrenci = {}

# 1-)
studentCount = input('Kaç öğrenci eklemek istiyorsunuz?: ')
i = 1

while i <= int(studentCount):
    print(f'{i}. Öğrenci')
    number = input('Öğrenci No: ')
    name = input('Öğrenci Adı: ')
    surname = input('Öğrenci Soyad: ')
    phone = input('Öğrenci Telefon: ')

    # ogrenci[number] = {
    #     'Ad' : name,
    #     'Soyad' : surname,
    #     'Telefon' : phone
    # }
    ogrenci.update({
        number : {
            'ad' : name,
            'soyad' : surname,
            'Telefon' : phone
        }
    }) #birdem fazla eklemek için update metodu kullanılabilir
    i = i + 1

print(ogrenci)

# 2-)
print('*'*50)
print('Öğrenci bilgisini öğrenmek için')
ogrNo = input('Öğrenci No: ')
ogrenciWithNo = ogrenci[ogrNo]

print(f"Aradığıınız {ogrNo} nolu öğrencinin adı: {ogrenciWithNo['ad']}, soyadı: {ogrenciWithNo['soyad']}, telefon: {ogrenciWithNo['Telefon']} ")