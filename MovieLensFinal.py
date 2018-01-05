#!/usr/bin/python

import findspark
findspark.init()


import pyspark
import sys
import re
import random
#import numpy

from pyspark import SparkConf, SparkContext
sc = SparkContext(appName = "MovieLens")
from math import sqrt
#sc.addPyFile("similarity.py")
sc.addPyFile("movielensfcn.py")


from movielensfcn import parseMovies, removeDuplicates, itemItem
#from similarity import cosine_similarity, jaccard_similarity


if __name__=="__main__":
    if len(sys.argv)< 3:
        print >> sys.stderr, "Usage: MovieLens ratings movies"
	print >> "Example parameters filename ratingsfile moviesfile filmid threshold topN COSINE"
	print >> "spark-submit MovieLensFinal.py /data/movie-ratings/ratings.dat /data/movie-ratings/movies.dat  1 .95 50 100 COSINE"
        exit(-1)
    ratings_file = sys.argv[1]
    movies_file = sys.argv[2]
    if len(sys.argv)>6:
        movie_id = int(sys.argv[3])
        threshold = float(sys.argv[4])
        topN= int(sys.argv[5])
        minOccurence = int(sys.argv[6])
        algorithm = sys.argv[7].upper()


print '{0}, {1}, {2}, {3}, {4} {5} {6} {7}'.format(ratings_file, movies_file, movie_id, threshold, topN, minOccurence, algorithm)

def jaccard_similarity(ratingPairs):
 #   "The Jaccard similarity coefficient is a commonly used indicator of the similarity between two sets. For sets A and B it is defined to be the ratio of the number of elements of their intersection and the number of elements of their union If A and B are both empty, we define Jaccard_Similarity(A,B) = 1."

    numPairs = 0
    intersect_xy=setX=setY={}
    for ratingX, ratingY in ratingPairs:
        setX =set(ratingX).union(setX)
        setY =set(ratingY).union(setY)
        intersect_xy = setX.intersect(setY)
        numPairs += 1

    numerator = intersect_xy
    denominator = len(setX) + len(setY) - len(intersectXandY)

    score = 0
    if (denominator):
        score = ((float(numerator)) / (float(denominator)))

    return (score, numPairs)
    
def cosine_similarity(ratingPairs):

    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0

    for ratingX, ratingY in ratingPairs:

        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1

    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)

    score = 0
    if (denominator):
        score = ((float(numerator)) / (float(denominator)))

    return (score, numPairs)





ratings_data = sc.textFile(ratings_file)
movies_data = sc.textFile(movies_file)

# data = sc.parallelize([(2.0,5.0), (2.5,4.5), (3.0,1.0), (5.0,2.0)])
# data1 = data.map(cosine_similarity).saveAsTextFile("test1")

ratings_header = ratings_data.take(1)[0]
movies_header = movies_data.take(1)[0]

movies= movies_data.filter(lambda line: line!=movies_header).map(lambda line: re.split(r',',line)).map(lambda x: (int(x[0]),(x[1],x[2])))

ratings = ratings_data.filter(lambda line: line!=ratings_header).map(lambda line: re.split(r',',line)).map(lambda x: (int(x[0]),(int(x[1]),float(x[2])))).partitionBy(100)

user_ratings_data = ratings.join(ratings)

unique_joined_ratings = user_ratings_data.filter(removeDuplicates)



movie_pairs = unique_joined_ratings.map(itemItem).partitionBy(100)

movie_pairs_ratings= movie_pairs.groupByKey()

if algorithm == "JACCARD" :
	item_item_similarities = movie_pairs_ratings.mapValues(jaccard_similarity).persist()
elif algorithm == "COSINE" :
	item_item_similarities = movie_pairs_ratings.mapValues(cosine_similarity).persist()
else:
	item_item_similarities = movie_pairs_ratings.mapValues(cosine_similarity).persist()


item_item_sorted=item_item_similarities.sortByKey()


#item_item_sorted.saveAsTextFile("movie-similar0001")
# Sample output
#((1, 2), (0.9633070604343126, 71))
#((1, 3), (0.9269097345177958, 39))
#((1, 4), (0.932135764636765, 7))

item_item_sorted.persist()

# Filter for movies with this sim that are "good" as defined by
# our quality thresholds above
filteredResults = item_item_sorted.filter(lambda((item_pair,similarity_occurence)): \
        (item_pair[0] == movie_id or item_pair[1] == movie_id) \
        and similarity_occurence[0] > threshold and similarity_occurence[1] > minOccurence)

if (topN==0):
    topN=10

results = filteredResults.map(lambda((x,y)): (y,x)).sortByKey(ascending = False)
resultsTopN = results.take(topN)
results.saveAsTextFile("top10test9")

 #   print "Top 10 similar movies for " + nameDict[movieID]
 #   for result in resultsTopN:
 #       (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
 #       similarMovieID = pair[0]
 #       if (similarMovieID == movieID):
 #           similarMovieID = pair[1]
 #       print nameDict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1])
