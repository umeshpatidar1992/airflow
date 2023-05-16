text = "hello this is user"
dict = {}
def count_word():
    for i in text.split(" "):
       dict[i] = text.count(i)
       print(dict)

def cap(name):
    for i in name.split(" "):
        print(i[0].capitalize()+(i[1:len(i)]))

if __name__=='__main__':
    cap(text)
    count_word()