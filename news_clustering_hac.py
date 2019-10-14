"""News Clustering with Hierarchical agglomerative clustering
"""

import matplotlib.pyplot as plt
import pandas as pd
#%matplotlib inline
import json
import numpy as np
from sklearn.cluster import AgglomerativeClustering
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity


news_data_file = 'data/news.json'
collection = []
news_ids = []
sources = ['The Hindu', 'The Times of India', 'News24', 'Google News']

subset_news = 0
total_news = 0
#stop_point = 1000
with open(news_data_file) as f:
    for line in f:
        news = json.loads(line)
        total_news += 1
        if news['source'] not in sources:
            continue
        subset_news += 1
        text = ''
        text += str(news['title'])
        #text += '. ' + str(news['description'])
        if text:
            collection.append(text)
            news_ids.append(news['_id']['$oid'])
        #if stop_point and len(news_ids) > stop_point:
        #    break

print('Total news: ' + str(total_news) + '\n')
print('News Subset: ' + str(subset_news) + '\n')

tfidf_vectorizer = TfidfVectorizer()
tfidf_matrix = tfidf_vectorizer.fit_transform(collection)


"""
ni = 10
res = cosine_similarity(tfidf_matrix[ni:ni+1], tfidf_matrix)
#print(res)
pos = 0
max = 0
for val in res[0]:
    if val < 1 and val > max:
        max = val
        mpos = pos
    pos += 1
print(max)
print(mpos)
print(collection[ni])
print(collection[mpos])
"""

cluster = AgglomerativeClustering(affinity='cosine', linkage='average', n_clusters=None, distance_threshold=.9, compute_full_tree=True)
cluster.fit_predict(tfidf_matrix.toarray())

print(cluster.labels_)
news_cluster = {}
idx = 0
for label in cluster.labels_:
    if label not in news_cluster:
        news_cluster[int(label)] = [news_ids[idx]]
        news_cluster[int(label)] = [news_ids[idx]]
    else:
        news_cluster[int(label)].append(news_ids[idx])
    idx += 1

#"""
print(len(news_cluster))

with open('data/cluster_news.json', 'w+') as f:
    json.dump(news_cluster, f)

max_cluster = 0
for label in news_cluster:
    #print(label, news_cluster[label])
    if len(news_cluster[label]) > max_cluster:
        max_cluster = len(news_cluster[label])
        max_cluster_label = label
"""
for news_idx in news_cluster[max_cluster_label]:
    print(collection[news_idx])
    print('\n******************************\n')
"""