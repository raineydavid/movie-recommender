import findspark
findspark.init()

# import pyspark
import sys
import re
import random
from similarity import jaccard_similarity, cosine_similarity

from pyspark import SparkConf, SparkContext

def parseVector(line):
    # line.split("::")
    return line.split(r'::')

# def parseMovies(line):
#     # line.split("::")
#     fields = parseVector(line)
#     return fields[0],(fields[1],fields[2])

def parseRatings(line):
    # line.split("::")
    fields= parseVector(line)
    return int(fields[0]),(int(fields[1]),float(fields[2]))

def parseMovies(line,movieDict):
    fields= parseVector(line)
    movieDict[int(fields[0])] = fields[1]
    return fields[0],(fields[1],fields[2])

def getSampleInteractions(user, film, n):
    if len(film) > n:
        return user, random.sample(film,n)
    else:
        return user, film
    return


#  item = movie, value = rating
def itemitem((item,values)):
    (itemA, valueA) = values[0]
    (itemB, valueB) = values[1]
    return ((itemA, itemB), (valueA, valueB))

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
    movies_data = sc.textFile(movies_file)
    movieDict={}
    movies= movies_data.map(lambda line: parseMovies(line,movieDict))

    # inmem = user_item.persist()
    # inmem.saveAsTextFile("rates8")

    ratings_data = sc.textFile(ratings_file)
    # create an item-item matrix of films
    ratings = ratings_data.map(parseRatings)
    # user_ratings = user_ratings_data.filter(filterDuplicates)
    inmem = user_item.persist()
    inmem.saveAsTextFile("user-ratings10")
    # Find all movies rated by the same user
    user_ratings_data= ratings.join(ratings)

    # Make movie pairs
    movie_ratings_pairs_data= user_ratings_data.map(itemitem)


    movie_movie_data =movie_ratings_pairs_data.groupByKey().map(lambda line: getSampleInteractions(line[0],line[1],50)
    # If a user loves to rate the movie - get rid of their duplicates

    # movie_movie_similarity = movie_movie_data.mapValues(lambda line: jaccard_similarity(line[0],line[1])).cache()
    movie_movie_similarity = movie_movie_data.map(lambda line: jaccard_similarity(line[0],line[1])).map(lambda line: (line[0], list(line[1]))).cache()

    movie_movie_similarity.sortByKey()
    movie_movie_similarity.saveAsTextFile("movie-sample5")
