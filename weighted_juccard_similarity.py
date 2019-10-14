from collections import defaultdict
import json
import csv


def get_tweet_list_per_entity(tweet_data):
    tweet_per_ent = defaultdict(list)
    for tweet in tweet_data:
        for ent in tweet['corenlp_entities']:
            tweet_per_ent[ent].append(tweet['id_str'])
    return tweet_per_ent


def get_news_list_per_entity(news_data):
    news_per_ent = defaultdict(list)
    for news in news_data:
        for ent in news['corenlp_entities']:
            news_per_ent[ent].append(news['_id']['$oid'])
    return news_per_ent


def get_entity_count_per_news(news_data):
    entity_count_per_news = {}
    for news in news_data:
        entity_count_per_news[news['_id']['$oid']] = len(news['corenlp_entities'])
    return entity_count_per_news


def weighted_juccard_similarity(news_per_ent, tweet_per_ent, news_data, tweet_data):
    weighted_juccard_scores = [('news_id', 'tweet_id', 'weighted_juccard_similarity')]
    num_tweet = len(tweet_data)
    num_news = len(news_data)

    for tweet in tweet_data:
        tweet_ent_set = set(tweet['corenlp_entities'])
        for news in news_data:
            news_ent_set = set(news['corenlp_entities'])
            ent_union = news_ent_set.union(tweet_ent_set)
            ent_intersection = news_ent_set.intersection(tweet_ent_set)

            if not ent_intersection:
                continue

            intersection_sum = 0
            for ent in ent_intersection:
                idf_news = num_news/len(news_per_ent[ent])
                idf_tweet = num_tweet/len(tweet_per_ent[ent])
                intersection_sum += min(idf_news, idf_tweet)

            union_sum = 0
            idf_news = 0
            idf_tweet = 0
            for ent in ent_union:
                if ent in news_per_ent:
                    idf_news = num_news/len(news_per_ent[ent])
                if ent in tweet_per_ent:
                    idf_tweet = num_tweet/len(tweet_per_ent[ent])
                union_sum += max(idf_news, idf_tweet)
            weighted_juccard_scores.append((news['_id']['$oid'], tweet['id_str'], intersection_sum/union_sum))
    return weighted_juccard_scores


def main():
    news_json = '/home/satish/smacs/gold/gold_news_data.json'
    tweet_json = '/home/satish/smacs/gold/gold_tweet_data.json'
    output_csv = '/home/satish/smacs/gold/m2_Weighted_juccard_score.csv'

    with open(news_json) as f1:
        news_data = json.load(f1)
    news_per_ent = get_news_list_per_entity(news_data)
    #entity_count_per_news = get_entity_count_per_news(news_data)
    #print(news_per_ent)

    with open(tweet_json) as f2:
        tweet_data = json.load(f2)
    tweet_per_ent = get_tweet_list_per_entity(tweet_data)
    weighted_juccard_scores = weighted_juccard_similarity(news_per_ent, tweet_per_ent, news_data, tweet_data)

    with open(output_csv, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerows(weighted_juccard_scores)

main()