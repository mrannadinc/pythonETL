from unicodedata import name


my_list = ['Bmw', 'Mercedes', 'Opel', 'Mazda']
# print(len(my_list))

ilk_son_eleman = [my_list[0], my_list[-1]]
# print(ilk_son_eleman)

my_list[-1] = 'Toyata'
# print(my_list)

# isInside = 'Mercedes'
# if isInside in my_list:
#     print('exists')
# else:
#     print('not exists')

# print(my_list[-2])

# print(my_list[0:3])

my_list[-2] = 'Toyata'
my_list[-1] = 'Renault'
# print(my_list)

my_list.append('Audi')
my_list.append('Nissan')
# print(my_list)

my_list.pop()
# print(my_list)

# print(my_list[::-1])

numbers = [1, 10, 5, 16, 4, 9, 10]
letters = ['a', 'g', 's', 'b', 'y', 'a', 's']

val = min(numbers)
val = max(numbers)
val = max(letters)

numbers.append(49)
numbers.append(59)
numbers.insert(3, 78)
numbers.insert(-1, 52)

# numbers.pop()
numbers.pop(0)
numbers.remove(49)

numbers.sort()
numbers.reverse()

# print(numbers.count(10))
# print(numbers)

names = ['Ali', 'Yağmur', 'Hakan', 'Deniz']
years = [1998, 2000, 1998, 1987]

names.append('Cenk')
print(names)

names.insert(0, 'Sena')
print(names)

# names.remove('Deniz')
# print(names)

indeks = names.index('Deniz')
print(indeks)

print('Ali' in names)

names.reverse()
print(names)
names.sort()
print(names)

years.sort()
print(years)

str = "Chevrolet,Dacia"
li = list(str.split(','))
print(li)

print(min(years))
print(max(years))

print(years.count(1998))

years.clear()
print(years)

i = 1
marka = []
while i <= 3:
    str = input('Marka bilgisi giriniz: ')
    marka.append(str)
    i = i + 1


print(marka)






