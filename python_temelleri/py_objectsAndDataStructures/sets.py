fruits = {'orange', 'apple', 'banana'}

# print(fruits[0]) indekslenemez

# for x in fruits:
#     print(x)

fruits.add('cherry')
fruits.update(['mango', 'grape', 'apple'])

fruits.remove('mango')
fruits.discard('apple')

fruits.pop()

for x in fruits:
    print(x)


# myList = [1,2,5,4,4,2,1]
# print(set(myList))