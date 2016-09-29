import sys


def interval(begin, end):
    bh,bm,bs = begin.split(":")
    eh,em,es = end.split(":")
    if bm == em:
        return int(es) - int(bs)
    else:
        return 60-int(bs) + (int(em) - int(bm)) + int(es)
    pass


def caltime(filename):
    time = 0
    beginFlag = False
    begin = None
    end = None
    with open(filename) as f:
        for line in f:
            line = line.strip()
            if line .endswith("map 0% reduce 0%") and beginFlag == False:
                begin = line[10:18]
                beginFlag = True
            if line.endswith("map 100% reduce 100%") and beginFlag == True:
                end = line[10:18]
                time += interval(begin,end)
                beginFlag = False
    return time

def main():
    filename = sys.argv[1]
    totaltime = 0
    totaltime = caltime(filename)
    print totaltime