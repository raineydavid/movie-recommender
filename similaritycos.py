def cosine_similarity(ratingPairs):
     
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
        
    for ratingX, ratingY in ratingPairs:
         
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1