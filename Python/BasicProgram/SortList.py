ls = [1,0,0,0,1,0,1,0,0]
for i in range(len(ls)):
    for j in range(i + 1, len(ls)):
        print(i,j)
        if ls[i] > ls[j]:
            ls[i],ls[j] = ls[j], ls[i]
        print(ls)
print(ls)