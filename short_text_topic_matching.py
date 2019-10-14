import pandas
import pickle
import operator
import numpy as np
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer
import math


stop_words = set(stopwords.words('english'))
stemmer = PorterStemmer()

from gsdmm import MovieGroupProcess


def text_preprocess(tweet_text, extra_stop_words):
    stop_words.update(extra_stop_words)
    doc = []
    tokens = word_tokenize(tweet_text)
    for word in tokens:
        if len(word) < 3:
            continue
        if word not in stop_words:
            doc.append(stemmer.stem(word))
    return doc


def gsdmm_train(training_text, extra_stop_words, K=20, alpha=0.1, beta=0.1, n_iters=30):
    stop_words.update(extra_stop_words)

    # Preprocessing
    docs = []
    for text in training_text:
        tokens = word_tokenize(text)
        docs.append([])
        for word in tokens:
            if len(word) < 3:
                continue
            if word not in stop_words:
                docs[-1].append(stemmer.stem(word))

    vocab = set(x for doc in docs for x in doc)
    n_terms = len(vocab)

    mgp = MovieGroupProcess(K=K, alpha=alpha, beta=beta, n_iters=n_iters)

    n_terms = len(vocab)
    y = mgp.fit(docs, n_terms)  # Save model
    with open("v1.model", "wb") as f:
        pickle.dump(mgp, f)
        f.close()

    doc_count = np.array(mgp.cluster_doc_count)
    #print('Number of documents per topic :', doc_count)
    #print('*'*20)

    top_index = doc_count.argsort()[-10:][::-1]
    #print('Most important clusters (by number of docs inside):', top_index)
    #print('*'*20)

    idx = 0
    for distr in mgp.cluster_word_distribution:
        sorted_x = sorted(distr.items(), key=operator.itemgetter(1))
        #print(idx, sum([y for x,y in sorted_x]), sorted_x[-20:])
        idx += 1

    return mgp


def get_kl_divergence(distr1, distr2, sum1, sum2):
    kl_divergence = 0
    flag = False
    for word1 in distr1:
        if word1 in distr2:
            flag = True
            p = distr1[word1]/sum1
            q = distr2[word1]/sum2

            kl_divergence += (p-q)*math.log(p/q)
    if flag:
        return kl_divergence
    else:
        return -1


def topic_matching(model1, model2, thresold=.008):
    topic_scores = {}
    for idx1 in range(len(model1)):
        distr1 = model1[idx1]
        sum1 = 0
        for key in distr1:
            sum1 += distr1[key]
        for idx2 in range(len(model2)):
            distr2 = model2[idx2]
            sum2 = 0
            for key in distr1:
                sum2 += distr1[key]
            score = get_kl_divergence(distr1, distr2, sum1, sum2)
            #print(score)
            if score == -1:
                continue
            if score > thresold:
                continue
            topic_scores[(idx1, idx2)] = score
    return topic_scores


if __name__ == "__main__":
    data_file = '/home/satish/smacs/evaluations/news_test_tweet_link_5aug.csv'
    extra_stop_words = ['...', 'n\'t', 'the', 'http', 'https', 'amp']

    df = pandas.read_csv(data_file)

    # Tweet Topics
    tweets = set(df['Tweet_Text'])
    model = gsdmm_train(tweets, extra_stop_words, K=20)
    tweet_set = {}
    for idx, row in df.iterrows():
        if row['tweet_id'] not in tweet_set:
            doc = text_preprocess(row['Tweet_Text'], extra_stop_words)
            x, y = model.choose_best_label(doc)
            tweet_set[row['tweet_id']] = (x, y)

    # News Topics
    news_set = set()
    news_text = []
    for idx, row in df.iterrows():
        if row['news_id'] not in news_set:
            news_text.append(row['news_title'] + ' ' + row['News_Text'])
            news_set.add(row['news_id'])

    model_news = gsdmm_train(news_text, extra_stop_words, K=10)
    news_set = {}
    for idx, row in df.iterrows():
        if row['news_id'] not in news_set:
            doc = text_preprocess(row['news_title'] + ' ' + row['News_Text'], extra_stop_words)
            x, y = model_news.choose_best_label(doc)
            news_set[row['news_id']] = (x, y)

    topic_scores = topic_matching(model.cluster_word_distribution, model_news.cluster_word_distribution)
    news_tweet_topic_scores = {}
    for tid in tweet_set:
        for nid in news_set:
            tup = (tweet_set[tid][0], news_set[nid][0])
            if tup in topic_scores:
                news_tweet_topic_scores[(tid, nid)] = topic_scores[tup]
                #print(tid, nid, topic_scores[tup])
    #print(news_tweet_topic_scores)
    print(topic_scores)
    for idx1, idx2 in topic_scores:
        print(model.cluster_word_distribution[idx1], model_news.cluster_word_distribution[idx2])