# key - value

# 06 => Ankara 71 => Kırıkkale

# sehir = ['Ankara', 'Kırıkkale']
# plaka = [6, 71]

# dic = plaka[sehir.index('Ankara')]
# print(dic)

# plaka = {'Ankara' : 6,
#         'Kırıkkale' : 71}

# print(plaka['Ankara'])
# print(plaka['Kırıkkale'])

# plaka['İstanbul'] = 34
# plaka['Ankara'] = 'New Value'


# print(plaka)

users = {
    'HalilAnnadinc' : {
        'age' : 26,
        'roles' : ['user'],
        'email' : 'h.ibrahim2321@gmail.com',
        'address' : 'Ankara',
        'phone' : '1234567879'
    },
    'AhmetAnnadinc' : {
        'age' : 29,
        'roles' : ['admin', 'user'],
        'email' : 'ahmet@gmail.com',
        'address' : 'Ankara',
        'phone' : '987654321'
    }
}

print(users['AhmetAnnadinc']['age'])
print(users['AhmetAnnadinc']['roles'][0])
print(users['AhmetAnnadinc']['email'])
print(users['AhmetAnnadinc']['address'])
print(users['AhmetAnnadinc']['phone'])