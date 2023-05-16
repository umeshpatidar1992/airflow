
def list_comprehensive():
    dataList = ['hello', 1,2,3,4,5]
    for i in dataList:
        print(i)
    output_list = [i for i in dataList ]
    print(output_list)

    alphabet = ['a', 'b', 'c']
    integers = [1, 2, 3]
    print(list(zip(alphabet, integers)))

if __name__ == '__main__':
    list_comprehensive()


