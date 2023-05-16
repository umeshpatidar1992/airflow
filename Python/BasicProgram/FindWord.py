word = 'hello'
str = 'My name is Umesh hello lets see how this works'
for i in range(len(str)):
    if str[i] != word:
        print(str.index(str[i]))
        print("not found")
    else:
        print("found")
        break
