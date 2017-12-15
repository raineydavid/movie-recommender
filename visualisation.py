import seaborn as sns
import pandas as pd
import numpy as np

import matplotlib.pyplot as mp


file='/homes/sc325/bigdata/ml-latest-small/ratings.csv'

r_cols= ['userId', 'movieId', 'rating', 'timestamp']

users = pd.read_csv(file, sep=',', names=r_cols)

users.head()


