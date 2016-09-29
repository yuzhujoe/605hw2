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
        yield ("%",l)
        yield ("%","%")

def tokensTest(line):
    docid = line.split('\t')[0]
    for tok in line.split('\t')[2].split():
        tok = re.sub("\W","",tok)
        if len(tok) > 0:
            yield (tok,docid)

    yield ("%",docid)


def tokensTestVocab(x):
    if x[0] != '%':
        yield x[0]


def totalLineNumKey(line):
    labels = getLabel(line)
    for l in labels:
        yield 1

def totalLabelNumKey(x):
    for tuple in x[1]:
        yield tuple[0]

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
    labelList = line[3][1]
    i = 0
    for score in line[2]:
        yield docid,labelList[i],word,score
        i+=1

def myscore(x):
    word = x[0][0]
    labelList = x[1][1]
    labelset = set(x[1][1])
    testVocabSize = x[1][2]
    labelSize = len(labelset)

    wordCountMp = {}
    wordTokenMp = {}
    wordLabelMp = {}

    countList = x[0][2]
    tokeCountList = x[0][3]
    sl = []
    totalCount = 10
    if word == '%':
        for tuple in countList:
            if tuple[0] == '%':
                totalCount = tuple[-1]
            elif tuple[-2] in labelset:
                wordLabelMp[tuple[-2]] = tuple[-1]
        for l in labelList:
            if l in wordLabelMp:
                # TODO maybe wrong because did not multiply wordOfCount
                sl.append(math.log(wordLabelMp[l]+1)-math.log(totalCount + labelSize))
            else:
                sl.append(-1 * math.log(totalCount + labelSize))
    else:
        for tuple in countList:
            if tuple[-2] in labelset:
                wordCountMp[tuple[-2]] = tuple[-1]
        for tuple in tokeCountList:
                wordTokenMp[tuple[-2]] = tuple[-1]

        for l in labelList:
            if l in wordCountMp:
                sl.append((math.log(wordCountMp[l]+1) - math.log(wordTokenMp[l] + testVocabSize)))
            else:
                sl.append(- math.log(wordTokenMp[l] +testVocabSize))

    return sl

def duplicate(x):
# (('zone', 'Dhulabari'), [('zone', 'Dhulabari')])
    key = x[0]
    for val in x[1]:
        yield val

def findHighest(x):
    maxScore = float('-inf')
    maxScoreLabel = None
    docid = x[0]
    plist = x[1]
    for tuple in plist:
        if tuple[1] > maxScore:
            maxScore = tuple[1]
            maxScoreLabel = tuple[0]

    yield (docid,maxScoreLabel,maxScore)


def excludeSpecial(x):
    if x[0] != '%':
        yield (x[0],x[1],x[2])



class WordCount(Planner):
    D = GPig.getArgvParams()
    wc = ReadLines(D["trainFile"]) | Flatten(by=tokens) | Group(by= lambda x:x,reducingTo=ReduceToCount()) | ReplaceEach(by = lambda x:(x[0][0],x[0][1],x[1]))

    tokenNum = wc | Flatten(by=lambda x:excludeSpecial(x))| Group(by= lambda (word,label,count):label,retaining=lambda (word,label,count):count,reducingTo=ReduceToSum()) | Group(by=lambda x:1)
    # produce reorganized k v pair

    wc3 =  Group(wc,by= lambda x:x[0],retaining=lambda x:(x[1],x[2])) | Augment(sideview=tokenNum,loadedBy=lambda v:GPig.onlyRowOf(v))|ReplaceEach(by=lambda x:(x[0][0],x[0][1],x[1][1]))
    # produce flattened test file

   #word docid count in that doc
    flattenedTestWc = ReadLines(D["testFile"]) | Flatten(by=tokensTest) | Group(by= lambda (toc,docid):(toc,docid)) | Flatten(by = lambda x:duplicate(x))

    testVocabSize = flattenedTestWc | Flatten(by=tokensTestVocab) | Group(by= lambda toc:toc,retaining= lambda x:1,reducingTo=ReduceTo(int,lambda accum,val:1)) | Group(by= lambda x:1,reducingTo=ReduceToCount())

    # allLabelWithVocabSize = ReadLines(D["trainFile"]) | Flatten(by=totalLabelNumKey) | Group(by= lambda x:x,reducingTo=ReduceToCount()) | Group(by=lambda x:1,retaining=lambda x:x[0]) | Augment(sideview=testVocabSize,loadedBy=lambda v:GPig.onlyRowOf(v)) | ReplaceEach(by= lambda x:(x[0][0],x[0][1],x[1][1]))

    allLabelWithVocabSize = tokenNum | Flatten(by=totalLabelNumKey) | Group(by=lambda x:1)| Augment(sideview=testVocabSize,loadedBy=lambda v:GPig.onlyRowOf(v)) | ReplaceEach(by= lambda x:(x[0][0],x[0][1],x[1][1]))

    # Join test with reorganized kv pair by word
    # word docid scorelist labellist
    output = Join(Jin(flattenedTestWc,lambda x:x[0]),Jin(wc3,by= lambda x:x[0])) | ReplaceEach(by= lambda (x,y) : (x[0],x[1],y[1],y[2])) | Augment(sideview=allLabelWithVocabSize,loadedBy=lambda v:GPig.onlyRowOf(v)) | ReplaceEach(by=lambda x:(x[0][0],x[0][1],myscore(x),x[1])) | Flatten(by=getScore) | Group(by = lambda x:(x[0],x[1]),retaining= lambda x:x[3],reducingTo=ReduceToSum()) | Group(by= lambda x:x[0][0],retaining= lambda x:(x[0][1],x[1])) | Flatten(by= lambda x:findHighest(x))

# always end like this
if __name__ == "__main__":
    # start = time.time()
    WordCount().main(sys.argv)
    # end = time.time()
    # print >>sys.stderr , end - start