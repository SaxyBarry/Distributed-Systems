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
    
def mapIndex(input):
    wordIndex = {}
    for file in input.keys():
        y = input[file]
        splitInput = re.sub(r'[^a-zA-Z0-9\'\’]', ' ', y)
        splitInput = re.split(' ', splitInput)
        for word in splitInput:
            word = word.lower()
            if word != '':
                if not(word in wordIndex.keys()):
                    wordIndex[word] = [file]
                else:
                    if not(file in wordIndex[word]):
                        wordIndex[word].append(file)
    return wordIndex

def reduceIndex(input):
    words = {}
    for x in input.keys():
        for y in input[x].keys():
            if y in words.keys():
                for z in input[x][y]:
                    words[y].append(z)
            else:
                words[y] = input[x][y]
    return words

# docs = {"doc1": "The quick brown fox jumps over the lazy dog.", "doc2": "The quick brown fox is not lazy."}
# docs2 = {"doc3": "The quick brown fox jumps over the lazy dog.", "doc4": "The quick brown fox is not lazy."}
# mapOut = {0:mapIndex(docs), 1:mapIndex(docs2)}
# print(reduceIndex(mapOut))
                