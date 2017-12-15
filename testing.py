import findspark
findspark.init()

# import pyspark
import sys
import re
import random

from pyspark import SparkConf, SparkContext

def parseVector(line):
    line.split(r'::')
    return int(line[0]),(int(line[1]),float(line[2]))

def getSampleInteractions(user, film, n):
    if len(film)> n:
        return user, random.sample(film,n)
    else:
        return user, film
    return


def findItemItem():
    return

if __name__=="__main__":
    if len(sys.argv)< 3:
        print >> sys.stderr, "Usage: MovieLens ratings movies"
        exit(-1)
    print "hello"

    ratings_file = sys.argv[1]
    movies_file = sys.argv[2]

    print "ratings {0}\nmovies {1}".format(ratings_file,movies_file)
    sc = SparkContext(appName = "MovieLens")
    # print "hello"
    lines = sc.textFile(ratings_file)
    # data = lines.map(lambda gen: re.split(r'::', gen))
    # data1 = data.map(lambda x:x[2])
    # inmem = data1.persist()
    # inmem.saveAsTextFile("rates1")


    # create an item-item matrix of films

    user_item= lines.map(parseVector).groupByKey().map(lambda line: getSampleInteractions(line[0],line[1],50).cache())


    inmem = user_item.persist()
    inmem.saveAsTextFile("rates3")
