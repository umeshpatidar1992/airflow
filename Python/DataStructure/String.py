text = "Hello This is user"

def cap(name):
    for i in name:
        first = i[0]
        mid = i[1:2]
    return first.upper() + mid


if __name__=='__main__':
    print(cap(text))