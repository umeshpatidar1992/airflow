'''
def extractMaximum(ss):
    num, res = 0, 0

    # start traversing the given string
    for i in range(len(ss)):

        print(i, ss[i])
        if ss[i] >= "0" and ss[i] <= "9":
            num = num * 10 + int(int(ss[i]) - 0)
        else:
            res = max(res, num)
            num = 0

    return max(res, num)


# Driver Code
ss = "klhabcabcbg"

print(extractMaximum(ss))




def extractMax(ss):
    num, res = 0, 0
    for i in range(len(ss)):
        if ss[i] >= '0' and ss[i] <= '9':
            num  = num * 10 + int(int(ss[i]-0))
        else:
            res = max(res,num)
            num = 0

    return max(num,res)


x = 10
y = 5

def modify():
    global x
    x = 2
    y = 3

modify()
print(x)
print(y)
'''

def cal():
    try:
        return 1
    finally:
        return -1

x = cal()
print(x)