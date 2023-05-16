def reverse(s):
    str = ""
    for i in s:
        str = i + str
    return str
print(reverse("name"))

ls = ["test", "hello", "Umesh"]
for i in range(len(ls)-1,-1,-1):
    print(ls[i])


for i in range(6,0,-1):
    print(i)