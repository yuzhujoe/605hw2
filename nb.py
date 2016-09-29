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
                yield ('#',l)
        yield ("%",l)
        yield ("%","%")

def tokensTest(line):
    docid = line.split('\t')[0]
    for tok in line.split('\t')[2].split():
        tok = re.sub("\W","",tok)
        if len(tok) > 0:
            yield (tok,docid)

    yield ('#',docid)
    yield ("%",docid)


def tokensTestVocab(x):
    if x[0] != '%' and x[0] != '#':
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
    docid = line[1]
    i = 0
    for score in line[2]:
        yield docid,i,score
        i+=1

def myscore2(x):
    docid = x[0][0]
    tokenlist = x[0][1][0]
    labellist = x[0][1][1]
    testVocabSize = x[1][1]
    totalCount = labellist[1][0][1]

    maxScore = float('-inf')
    maxScoreLabel = None
    allTokenCountlist = tokenlist[1]
    allLabelCountlist = labellist[1]

    wordTokenList = {}
    wordLabelList = {}
    scoreList = {}

    for tuple in allTokenCountlist:
        wordTokenList[tuple[0]] = tuple[1]

    for tuple in allLabelCountlist:
        if tuple[0] != '%':
            wordLabelList[tuple[0]] = tuple[1]

    labelSize = len(wordTokenList)

    for i in xrange(2,len(x[0][1])):
        wordCountList = x[0][1][i]
        ll = wordCountList[1]
        wset = []
        for l in ll:
            wset.append(l[0])
            if l[0] in scoreList:
                scoreList[l[0]] += math.log(l[1]+1) - math.log(wordTokenList[l[0]] + testVocabSize)
            else:
                scoreList[l[0]] = math.log(l[1]+1) - math.log(wordTokenList[l[0]] + testVocabSize)

        wset = set(wset)
        for l in wordTokenList:
            if l not in wset:
                if l in scoreList:
                    scoreList[l] -= math.log(wordTokenList[l] +testVocabSize)
                else:
                    scoreList[l] = -1 * math.log(wordTokenList[l] +testVocabSize)

    for l in wordTokenList:
        if l in wordLabelList:
            if l in scoreList:
                scoreList[l] += math.log(wordLabelList[l] + 1) - math.log(totalCount + labelSize)
            else:
                scoreList[l] =  math.log(wordLabelList[l] + 1) - math.log(totalCount + labelSize)
        else:
            if l in scoreList:
                scoreList[l] -= math.log(totalCount + labelSize)
            else:
                scoreList[l] =  -1 * math.log(totalCount + labelSize)

    for i in scoreList:
        if scoreList[i] > maxScore:
            maxScore = scoreList[i]
            maxScoreLabel = i

    return docid,maxScoreLabel,maxScore

def duplicate(x):
    for val in x[1]:
        yield val

def findHighest(x):
    maxScore = float('-inf')
    maxScoreLabel = None
    docid = x[0][0]
    plist = x[0][1]
    labelList = x[1][1]
    for tuple in plist:
        if tuple[1] > maxScore:
            maxScore = tuple[1]
            maxScoreLabel = labelList[tuple[0]][0]

    yield (docid,maxScoreLabel,maxScore)


def excludeSpecial(x):
    if x[0][0] != '%':
        if x[0][1] != '%':
            yield x[0][1]


class WordCount(Planner):

    D = GPig.getArgvParams()

    wc = ReadLines(D["trainFile"]) | Flatten(by=tokens) | Group(by= lambda x:x,reducingTo=ReduceToCount())
    wc3 =  Group(wc,by= lambda x:x[0][0],retaining=lambda x:(x[0][1],x[1]))

    # produce flattened test file

   #word docid count in that doc
    flattenedTestWc = ReadLines(D["testFile"]) | Flatten(by=tokensTest) | Group() | Flatten(by = lambda x:duplicate(x))

    # Distinct
    testVocabSize = flattenedTestWc | Flatten(by=tokensTestVocab) | Distinct() | Group(by=lambda x:1,reducingTo=ReduceToCount())

    # Join test with reorganized kv pair by word
    # word docid scorelist labellist

    output = Join(Jin(flattenedTestWc,lambda x:x[0]),Jin(wc3,by= lambda x:x[0])) | Group(by=lambda x:x[0][1],retaining=lambda x:x[1]) | Augment(sideview=testVocabSize,loadedBy=lambda v:GPig.onlyRowOf(v)) |  ReplaceEach(by=lambda x:(myscore2(x)))


# always end like this
if __name__ == "__main__":
    WordCount().main(sys.argv)
