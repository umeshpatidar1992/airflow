def dict_comrehensive():
    data_dict = {1: 1, 3: 27, 5: 125, 7: 343}
    for k in data_dict.keys():
        print(k)
    for v in data_dict.values():
        print(v)

    key_list=[]
    value_list = []
    for k,v in data_dict.items():
        print("key is ",k,"and value is ",  v)
        key_list.append(k)
        value_list.append(v)

    dict_com = {value:key for (key,value) in zip(key_list,value_list) }
    print("reversed dictionary: ", dict_com)

if __name__ =='__main__':
    dict_comrehensive()