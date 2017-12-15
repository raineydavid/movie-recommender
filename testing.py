import findspark
findspark.init()

import pyspark
import sys
import re

from pyspark import SparkConf, SparkContext

if __name__=="__main__":
    if len(sys.argv)< 3:
        print >> sys.stderr, "Usage: MovieLensBRATS ratings movies"
        exit(-1)
    print "hello"

    ratings_file = sys.argv[1]
    movies_file = sys.argv[2]
   
    print ratings_file
    print movies_file
    sc = SparkContext(sys.argv[1], appName = "MovieLens")
    print "hello"
    lines = sc.textFile(ratings_file)
    data = lines.map(lambda gen: re.split(r'::', gen))  
    data1 = data.map(lambda x:x[2])
    inmem = data1.persist()
    inmem.saveAsTextFile("rates1")

