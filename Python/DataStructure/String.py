text = "hello this is user"

def cap(name):
    for i in name.split():
        print(i[0].capitalize()+(i[1:len(i)]))

if __name__=='__main__':
    cap(text)