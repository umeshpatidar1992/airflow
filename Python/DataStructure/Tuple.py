def alpha():
    data = input()
    final_val = ''
    len_data = len(data)
    temp_val = ''
    for i in range(0,len_data):
        temp_val = data[i]
        if i == 0:
            final_val = temp_val.capitalize()
        elif i > 0:
            val = temp_val.capitalize()
            for k in range(0,i):
                final_val = val + '|' + temp_val

    return final_val


print(alpha())