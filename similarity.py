#!/usr/bin/python
from __future__ import print_function
# Implement jaccard similarity
def jaccard_similarity(ratingPair):
    "The Jaccard similarity coefficient is a commonly used indicator of the similarity between two sets. For sets A and B it is defined to be the ratio of the number of elements of their intersection and the number of elements of their union If A and B are both empty, we define Jaccard_Similarity(A,B) = 1."

    numPairs = 0
    intersect_xy=setX=setY={}
    for ratingX, ratingY in ratingPairs:
        setX =set(ratingX).union(setX)
        setY =set(ratingY).union(setY)
        intersect_xy = setX.intersect(setY)
        numPairs += 1

    numerator = intersect_xy
    denominator = float(len(setX) + len(setY) - len(intersectXandY))

    score = 0
    if (denominator):
        score = ((float(numerator)) / (float(denominator)))

    return (score, numPairs)
#     setA = set(a)
#     setB= set(b)
#     intersectAandB =setA.intersection(setB)

#     if not a and not b:
#         c =1
#     else:
#         denom =float(len(setA) + len(setB) - len(intersectAandB))
#         c = float(len(intersectAandB)/ denom)

#     print("A={0}\nB={1}".format(a,b));
#     print("AandB={0}".format(intersectAandB));
#     print("Length(A)={0} \nLength(B)={1}\nLength(AandB)={2} ".format(len(setA),len(setB), len(intersectAandB)))
#     print("Jaccard_similarity= {0}".format(c));

    # print("hello world")
    # list(setA + setB)/list(setA & setB)
#     return c

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

# Implement cosine similarity
# def cosine_similarity
#     "This does the following"
#     return


# Adventure|Animation|Children|Comedy|Fantasy
# Comedy
# Crime|Drama
# Comedy|Drama|Romance|War
# Comedy|Romance
# Adventure|Animation|Children|Drama|Musical|IMAX
# Drama|War
# Comedy|Drama|Fantasy|Romance|Thriller
# Comedy
# Drama
# Adventure|Comedy|Sci-Fi
# Comedy|Drama|Sci-Fi
# Drama|Romance
# Comedy
# Adventure|Animation|Children|Comedy|Fantasy
# Adventure|Animation|Children|Comedy|Fantasy|Romance
# Adventure|Animation|Children|Comedy|Fantasy
# Adventure|Fantasy
# Adventure|Fantasy
# Adventure|Animation|Children|Comedy
# Action|Adventure|Drama|Fantasy
# Comedy|Drama|Romance

jaccard_similarity(((2.0,3.0),(2.0,5.0)))
# jaccard_similarity("", "")
# jaccard_similarity("", "Comedy")
# jaccard_similarity("Adventure|Animation|Children|Comedy|Fantasy", "")
# jaccard_similarity("Adventure|Animation|Children|Comedy|Fantasy", "Drama")
# jaccard_similarity("Adventure|Animation|Children|Comedy|Fantasy", "Adventure|Animation|Children|Comedy|Fantasy")
# jaccard_similarity("Adventure|Animation|Children|Comedy|Fantasy", "Adventure|Animation|Drama|Fantasy")
# jaccard_similarity("Adventure|Animation|Children|Comedy|Fantasy", "Adventure|Animation|Children|Comedy|Fantasy|Romance")
