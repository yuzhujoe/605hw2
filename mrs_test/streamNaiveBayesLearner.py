#streaming naive bayes

import sys
import math
import logging

class EventCounter(object):
    def __init__(self,maxSize=1000):
        self._ctr = {}
        self.maxSize = maxSize
        self.size = 0
    def get(self,event):
        if event in self._ctr: return self._ctr[event]
        else: return 0
    def inc(self,event,delta=1):
        if event in self._ctr: 
            self._ctr[event] += delta
        else: 
            self._ctr[event] = delta
            self.size += 1
        if self.maxSize and self.size>self.maxSize:
            self.flush()
    def flush(self):
        k = 0
        for event,count in self._ctr.items():
            print "%s\t%g" % (event,count)
            k += 1
            if not k % 10000: logging.info('flushed %d/%d items = %.3f%%' % (k,self.size, ((100.0*k)/self.size)))
        self._ctr = {}
        self.size = 0
    def load(self,file):
        self._ctr = {}
        for line in open(file):
            parts = line.strip().split("\t")
            self._ctr[parts[0]] = float(parts[1])

def parseLine(line):
    parts = line.split("\t")
    labels = parts[1].split(",")
    words = parts[2:]
    return labels,words

def trainLine(line,ec):
    labels,words = parseLine(line)
    ec.inc("lab=_")
    for lab in labels:
        ec.inc("lab=%s" % lab)
    for word in words:
        ec.inc("word=_")
        ec.inc("word=%s" % word)
        for lab in labels:
            ec.inc("word=%s,lab=%s" % (word,lab))

def testLine(line,ec,label,vocabSize):
    def logProb(k,n,nClass):
        #one smoothing method
        #return math.log(k+1.0) - math.log(n+nClass)
        #simpler smoothing, used by minorthird
        return math.log((k+0.5)/(n+1.0))
    trueLabels,words = parseLine(line)
    nAll = ec.get("lab=_")
    nPos = ec.get("lab=%s" % label)
    score = logProb(nPos,nAll,2)
    nonScore = logProb(nAll-nPos,nPos,2)
    #print 'priors',score,nonScore,'for k/n',nLab,nInstance
    for word in words:
        kw = ec.get("word=%s,lab=%s" % (word,label))
        nw = ec.get("word=%s" % word)
        score += logProb(kw,nPos,vocabSize)
        #BUG: this assumes that words aren't duplicated in examples, which isn't always true
        nonScore += logProb(nw-kw,nAll-nPos,vocabSize)
    trueLab = 'POS' if label in trueLabels else 'NEG'
    print '%f\t%s' % ((score-nonScore),trueLab)

if __name__=="__main__":
    logging.basicConfig(level=logging.INFO)
    if len(sys.argv)>1 and sys.argv[1] == '--train':
        logging.info('started training')
        ec = EventCounter(maxSize=0)
        k = 0
        for line in sys.stdin:
            trainLine(line.strip(),ec)
            k += 1
            if not k % 1000: logging.info('processed '+str(k)+' lines')
        logging.info('finished training')
        ec.flush()
        logging.info('flushed')
    elif len(sys.argv)>1 and sys.argv[1] == '--streamTrain':
        logging.info('started streamTrain')
        ec = EventCounter(maxSize=int(sys.argv[2]))
        k = 0
        for line in sys.stdin:
            trainLine(line.strip(),ec)
            k += 1
            if not k % 1000: logging.info('processed '+str(k)+' lines from '+sys.argv[1])
        logging.info('finished streamTrain buf size '+sys.argv[2])
        ec.flush()
        logging.info('flushed '+sys.argv[1])
    elif len(sys.argv)>3 and sys.argv[1] == '--test':
        file = sys.argv[2]
        ec = EventCounter(maxSize=0)
        ec.load(file)
        testLabel = sys.argv[3]
        vocabSize = int(sys.argv[4])
        for line in sys.stdin:
            testLine(line.strip(),ec,testLabel,vocabSize)
    else:
        print 'usage: --train or --streamTrain BUFFER_SIZE or --test EVENTS LABEL VOCAB_SIZE'
