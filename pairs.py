def iterate(iterable):
    r = []
    for v1_iterable in iterable:
        for v2 in v1_iterable:
            r.append(v2)

    return tuple(r)

def parseVector(line):
    '''
    Parse each line of the specified data file, assuming a "," delimiter.
    Converts each rating to a float
    '''
    line = line.split(",")
    return line[0],(line[1],float(line[2]))

def FindPairs(user_id,items_with_usage):
    t = []   
    for movie1,movie2 in combinations(items_with_usage,2):
        t.append(((item1[0],item2[0]),(item1[1],item2[1])))
    return t
