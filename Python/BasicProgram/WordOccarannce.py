sen_data = 'Hello this is Umesh and Hello working as a Data Engineer'

d = dict()

for i in sen_data.split(" "):
    print(i)
    if i in d:
        print(d[i]) #1
        d[i] = d[i] + 1 #2
    else:
        d[i] = 1
print(d)
for key in d.keys():
    print(key, ":", d[key])