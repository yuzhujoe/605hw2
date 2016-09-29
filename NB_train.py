import math

from guineapig import *
import sys
import re

# supporting routines can go here
def tokens(line):
    labels = getLabel(line)
    for l in labels:
        for tok in line.split('\t')[2].split():
            tok = re.sub("\W","",tok)
            if len(tok) > 0:
                yield (tok,l)

def tokensTest(line):
    docid = line.split('\t')[0]
    for tok in line.split('\t')[2].split():
        tok = re.sub("\W","",tok)
        if len(tok) > 0:
            yield (tok,docid)

def score(c1,c2):
    return c2

def totalLineNumKey(line):
    labels = getLabel(line)
    for l in labels:
        yield 1

def totalLabelNumKey(line):
    labels = getLabel(line)
    for l in labels:
        yield l

def totalTokenNumPair(line):
    labels = getLabel(line)
    count = 0
    for tok in line.split('\t')[2].split():
        tok = re.sub("\W","",tok)
        if len(tok) > 0:
            count += 1

    for l in labels:
        yield (l,count)

def getLabel(line):
    for tok in line.split('\t')[1].split(','):
        yield tok

def getDocId(line):
    yield line.split('\t')[0]

def getScore(line):
    word = line[0]
    docid = line[1]
    i = 1
    for score in line[2]:
        yield docid,i,word,score
        i+=1

def myscore(x):
    labelList = x[1][1]
    labelset = set(x[1][1])

    wordCountMp = {}
    wordTokenMp = {}

    countList = x[0][2]
    sl = []
    for tuple in countList:
        if tuple[0][0] == '$' and tuple[-2] in labelset:
            wordTokenMp[tuple[-2]] = tuple[-1]
        elif tuple[0][0] != '#' and tuple[0][0] != '$' and tuple[-2] in labelset:
            wordCountMp[tuple[-2]] = tuple[-1]

    for l in labelList:
        if l in wordTokenMp:
            # smooth factor here
            sl.append(math.log(wordCountMp[l]+1) - math.log(wordTokenMp[l]+100))
        else:
            sl.append(-1 * math.log(100))

    return sl


class WordCount(Planner):
    D = GPig.getArgvParams()
    wc = ReadLines(D["corpus"]) | Flatten(by=tokens) | Group(by= lambda x:x,reducingTo=ReduceToCount())
    lnum = ReadLines(D["corpus"]) | Flatten(by=totalLineNumKey) | Group(by= lambda x:x,reducingTo=ReduceToCount())
    labelNum = ReadLines(D["corpus"]) | Flatten(by=totalLabelNumKey) | Group(by= lambda x:x,reducingTo=ReduceToCount())
    tokenNum = ReadLines(D["corpus"]) | Flatten(by=totalTokenNumPair) | Group(by= lambda (key,value):key,retaining=lambda (key,value):value,reducingTo=ReduceToSum())

    # produce reorganized k v pair

    distWord = Group(wc,by= lambda ((word,label),count):word, retaining= lambda x:1,reducingTo=ReduceTo(int,by=lambda accum,val:1))
    wdWithTotal = Augment(distWord,sideview=lnum,loadedBy=lambda v:GPig.onlyRowOf(v))

    wdWithTokenTotal = Join(Jin(wc,by=lambda ((word,label),count):label),Jin(tokenNum,by=lambda x:x[0])) | ReplaceEach(by = lambda (x,y):(x[0][0],"$TotalTokenNum",y[0],y[1]))

    wdWithLabelTotal = Join(Jin(wc,by=lambda ((word,label),count):label),Jin(labelNum,by=lambda x:x[0])) | ReplaceEach(by = lambda (x,y):(x[0][0],"#TotalLabelNum",y[0],y[1]))

    disWTotal = ReplaceEach(wdWithTotal,by=lambda x:(x[0][0],"*TotalLineNum",x[1][1]))

    wc = ReplaceEach(wc,by=lambda x:(x[0][0],x[0][1],x[1]))

    all = Union(wc,disWTotal,wdWithLabelTotal,wdWithTokenTotal)

    ga = Group(all,by= lambda x:x[0],retaining=lambda x:x[1:])

    # produce flattened test file

    testWc = ReadLines(D["test"]) | Flatten(by=tokensTest) | Group(by= lambda (toc,docid):(toc,docid),reducingTo=ReduceToCount())
    flattenedTestWc = ReplaceEach(testWc,by=lambda x:(x[0][0],x[0][1]))

    allLabel = Group(labelNum,by=lambda x:1,retaining=lambda x:x[0])

    # Join test with reorganized kv pair by word
    Jl = Jin(ga,by= lambda x:x[0])
    Jr = Jin(flattenedTestWc,lambda x:x[0])

    cmp = Join(Jr,Jl) | ReplaceEach(by= lambda (x,y) : (x[0],x[1], y[1]))
    augcmp = Augment(cmp,sideview=allLabel,loadedBy=lambda v:GPig.onlyRowOf(v))

    scoreCmp = ReplaceEach(augcmp,by=lambda x:(x[0][0],x[0][1],myscore(x)))


    docScore = Flatten(scoreCmp,by=getScore)
    docSFinal = Group(docScore,by = lambda x:(x[0],x[1]),retaining= lambda x:x[3],reducingTo=ReduceToSum())
    docIdx = Group(docSFinal,by=lambda x:x[0][0],retaining=lambda x:x[1],reducingTo=ReduceTo(float,by=lambda accum,val:val if val > accum else accum))
    # Group(scoreCmp,by=lambda x:x[1],reducingTo=MyReduce([],lambda accum,x:x[2]))

    # cmp = Join(Jl,Jr) | ReplaceEach(by= lambda (((word1,docid),count1),((word2,label),count2)) : (word1,docid,label,score(count1,count2)))



# always end like this
if __name__ == "__main__":
    WordCount().main(sys.argv)
