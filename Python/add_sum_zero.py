

def find_out_sum(l):
    l1 = set(l)
    if len(l) > 2:
      for i in l:
        if -i in l1:
            return True
    else:
        return False
l = [1,2]
print(find_out_sum(l))
