# value types => string, number(int, floar) (İKİ DEĞİŞKENİN DEĞERLERİ EŞİTLENİR)

x = 5
y = 25

x = y
y = 10

# print(x ,y)

# referance types => list, class (BURADA DEĞİŞKENE KARŞILIK GELEN ADRESTİR. BU YÜZDEN BİR EŞİTLEME YAPILDIĞINDA ARTIK ADRESLER DE AYNI OLUR VE İKİ LİSTE AYNI ANDA DEĞİŞİR.)

a = ['apple','banana']
b = ['apple','banana']

a = b

b[0] = 'orange'

print(a, b)