def parseVector(line):
    # line.split(r'::')
    return line.split(r'::')

# def parseRatings(line):
#     # line.split("::")moviePairSimilarities.saveAsTextFile("movie-sims")
#     fields= parseVector(line)
#     return int(fields[0]),(int(fields[1]),float(fields[2]))
def loadMovieNames():
    movieNames = {}
    with open("/data/movie-ratings/movies.dat") as f:
        for line in f:
            fields = line.split("::")
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames

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

def removeDuplicates((userid, ratings)): #movies the user has rated 
    (item1, value1) = ratings[0]
    (item2, value2) = ratings[1]
    return item1 < item2

def itemitem((item,values)):
    (itemA, valueA) = values[0]
    (itemB, valueB) = values[1]
    return ((itemA, itemB), (valueA, valueB))
