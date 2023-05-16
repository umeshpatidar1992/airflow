data = "test wonderful wonderful"
d = {}
for i in data.split(" "):
	if i in d:
	    i = i + 1
	else:
		i = 1

for key in d.keys():
	print(key ,":" , d[key])