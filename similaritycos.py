import numpy 
from numpy import sqrt

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
        score = (numerator / (float(denominator)))

    return (score, numPairs)
        
