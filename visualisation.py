
import pandas as pd
import numpy as np

import matplotlib.pyplot as plt


file='/homes/sc325/bigdata/ml-latest-small/ratings.csv'

r_cols= ['userId', 'movieId', 'rating', 'timestamp']

users = pd.read_csv(file, sep=',', names=r_cols, encoding='latin-1')

users=users.drop(users.index[[0]])

print users

movieId_count=users['movieId'].value_counts()[:20]


print movieId_count

bar_graph=users['movieId'].value_counts()[:20].plot(kind='barh')

plt.ylabel('MovieId')
plt.xlabel('MovieId Count')
plt.title('Bar Graph for MovieId Count')
plt.show()
