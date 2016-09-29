import math

from guineapig import *
import sys
import re

# supporting routines can go here
def tokens(line):
    docid = line.split('\t')[0]
    for tok in line.split('\t')[2].split():
        tok = re.sub("\W","",tok)
        if len(tok) > 0:
            yield (tok,docid)

def totalVocabNumKey(line):
    for tok in line.split('\t')[2].split():
        tok = re.sub("\W","",tok)
        if len(tok) > 0:
            yield tok



def getLabel(line):
    for tok in line.split('\t')[1].split(','):
        yield tok

def score(c1,c2):
    return c2


class WordCount(Planner):
    D = GPig.getArgvParams()
    # testNumCount = ReadLines(D["corpus"]) | Flatten(by=totalVocabNumKey) | Group(by= lambda x:x,reducingTo=ReduceToCount())
    # vocabsize = Group(testNumCount,by=lambda (word,totalCount):1,retaining=lambda (word,totalCount):word,reducingTo=ReduceToCount())
    testWc = ReadLines(D["corpus"]) | Flatten(by=tokens) | Group(by= lambda (toc,docid):(toc,docid),reducingTo=ReduceToCount())
    wc = ReadLines(D['wc']) | Flatten(by=lambda x:x) | Group(by=lambda  x:x,reducingTo= lambda x:0)

    Jl = Jin(testWc,lambda ((word,docid),count):word)
    Jr = Jin(wc,lambda ((word,label),count):word)

    # cmp = Join(Jl,Jr) | ReplaceEach(by= lambda (((word1,docid),count1),((word2,label),count2)) : (word1,docid,label,score(count1,count2)))


# always end like this
if __name__ == "__main__":
    WordCount().main(sys.argv)
