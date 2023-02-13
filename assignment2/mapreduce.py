import re
def wordCount(input:str):
    wordFreq = {}
    splitInput = re.sub(r'[^a-zA-Z0-9\'\’]', ' ', input)
    splitInput = re.split(' ', splitInput)
    for x in splitInput:
        x = x.lower()
        if x != '':
            if x in wordFreq.keys():
                wordFreq[x] += 1
            else:
                wordFreq[x] = 1
    return wordFreq

def wordCountReduce(input):
    wordFreq = {}
    for x in input.keys():
        for y in input[x].keys():
            if y in wordFreq.keys():
                wordFreq[y] += input[x][y]
            else:
                wordFreq[y] = input[x][y]
    return wordFreq
    