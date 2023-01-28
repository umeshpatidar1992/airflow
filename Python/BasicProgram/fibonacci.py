'''
Example: 10
1 = 1
1+1 =2
1+2 =3
2+3 =5
3+5 = 8
'''

if __name__ == '__main__':
    num = int(input())
    t1 = 0
    t2 = 1
    fb = 0
    while fb < num:
        print(t1)
        fb = t1+t2
        t1 = t2
        t2 = fb
        fb += 1
