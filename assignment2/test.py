import mapreduce,os,sys,hashlib
input =  "distribution mature ac Mature  ewded shs "
input = input*50
result = mapreduce.wordCount(input)
print(result)
result = {0:{'distribution': 50, 'mature': 100, 'ac': 50, 'ewded': 50, 'shs': 50}, 1 : {'distribution': 50, 'mature': 100, 'ac': 50, 'ewded': 50, 'shs': 50}}
result = mapreduce.wordCountReduce(result)
print(result)