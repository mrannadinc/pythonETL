maasAli = 5000
maasAhmet = 4000
vergi = 0.27

print(maasAli - (maasAli * vergi))
print(maasAhmet - (maasAhmet * vergi))


# Değişken Tanımlama Kuralları

# Sayısal ifade ile başlayamaz

number1 = 10
print(number1)
number1 = 20
print(number1)
number1 += 30
print(number1)

#büyük küçük harf duyarlılığı vardır

age = 20
AGE = 30

print(age)
print(AGE)

# Türkçe karakter kullanmayalım

yas = 20
_age = 20

x = 1
y = 2.3
name = "Halil"
isStudent = True

a = '10'
b = '20'
print(a+b) # 30 => 1020

firtName = "Halil"
lastName = "Annadinç"

print(firtName + " " + lastName) # Halil Annadinç