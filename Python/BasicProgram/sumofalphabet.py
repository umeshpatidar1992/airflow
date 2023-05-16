
def firstBadVersion(self, n: int) -> int:
    for i in range(1,n):

        if i == n:
            print(i)
            print(n)
            return i
        elif i == n -1:
            print(i)
            print(n)
            return i


def findTheSum(alpha):

    # Stores the sum of order of values
    score = 0

    for i in range(0, len(alpha)):
        print(ord(alpha[i]))
        # Find the score
        if (ord(alpha[i]) >= ord('A') and ord(alpha[i]) <= ord('Z')):
            score += ord(alpha[i]) - ord('A') + 1
        else:
            score += ord(alpha[i]) - ord('a') + 1

    # Return the resultant sum
    return score
if __name__ == "__main__":
    print(findTheSum("AZaz"))
    print(firstBadVersion(5,4))