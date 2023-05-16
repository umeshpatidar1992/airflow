def remove_char(word, n):
    removed_word = word[n:]
    print(removed_word)

def cal_curr_previous_num():
    previous_num = 0
    for i in range(10):
        x_sum = i + previous_num
        print("Current Number", i, "Previous Number ", previous_num, " Sum: ", x_sum)
        previous_num = i


def cal_product(num1, num2):
    product = num1 * num2
    if product < 1000:
        print(product)
    else:
        print(num1 + num2)

def even_index():
    text = str(input("Enter string : "))
    for i in range(0, len(text), 2):
        print(text[i])

def first_last_same(l):
    print("List : ", l)
    x_l1 = l[0]
    x_l2 = l[len(l)-1]
    if x_l1 == x_l2:
        print("Same Number: ", True)
    else:
        print('Not a same number: ', False)

def count_substring():
    str_x = "Emma is good developer. Emma is a writer"
    count = 0
    for i in len(str_x):
        if str_x[i:i+4] == "Emma":
            count+1
    print("number of time Emma present: ",count)

if __name__ == '__main__':
    #num1 = int(source("Enter 1st Number: "))
    #num2 = int(source("Enter 2nd Number: "))
    #cal_product(num1, num2)
    #remove_char('Hello', 6)
    #cal_curr_previous_num()
    #even_index()
    #first_last_same([10,20,30,10,50])
    count_substring()