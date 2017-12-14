import findspark
findspark.init()


from pyspark import SparkConf, SparkContext


sc = pyspark.SparkContext(appName = "MovieLens")



# 2. Manipulating Data
mydataRDD = sc.textFile("/data/movie-ratings/movies.dat")
# "/data/movie-ratings/movies.dat"

mydataRDD.map(lambda x: x.split('::')).map(lambda y: (y[0], y[2], y[1]))


# remove films with no ratings
mydataRDD.map(lambda x: x.split('::')).map(lambda y: (int(y[0]), float(y[2]), int(y[1])))
# We now have data that looks like (196, 3.0, 242).
# 3.Aggregating Data

# Use the reduce function with two parameters
# x  original value
# y  new value
mydataRDD.map(lambda x:(x[1])).reduce(lambda x,y:(x+y))

# 3. Aggregate the data

user_rating_sumcount = mydataRDD.map(lambda x:(x[0],(x[1],x[2])))

user_sumcount = user_rating_sumcount.aggregateByKey((0,0.0),(lambda x, y: (x[0]+y[0],x[1]+1)),(lambda rdd1, rdd2: (rdd1[0]+rdd2[0], rdd1[1]+rdd2[1])))


user_avgrating = user_sumcount.mapValues(lambda x:(x[0]/x[1]))
