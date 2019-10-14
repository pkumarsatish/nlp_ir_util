"""CoreNLP NER + Wikification with continuous read and write from mongodb collection

Given a live twitter collection on mongodb server, program does the following:
    * Periodically prob the twiiter collection for new tweets
    * Process the new tweets in batches
    * For each tweet in a batch
        ** Performs NER - Stanford CRF based NER with 3 class
        ** Entity linking through Wikification - Maintain a local 
            wiki-redirect mapping to minimizeWeb based wiki redirect
    * Write the entity lists for each tweet in the batch at once

Created By:     Satish Kumar
Last Modified:  16 Jun 2019
"""

from pycorenlp import StanfordCoreNLP
from pymongo import MongoClient
import pymongo
import time
import requests

# Collection Information
client = MongoClient('mongodb://localhost:27017/')
db = client['xxxx']
collection = db.filtered_tweets
new_collection = db.filtered_tweets_corenlp

BATCH_SIZE = 1000   # 1 hour ~ 1900 Tweets
SLEEP_TIME = 900

debug_flag = False


# CoreNLP Server launched on Certain Port
corenlp = StanfordCoreNLP('http://localhost:8000/')
#regect_entity_types = ['DATE', 'TIME', 'DURATION', 'SET', 'MONEY', 'NUMBER', 'ORDINAL', 'PERCENT']
#regect_pos_tags = ['PRP', 'PRP$']


# Wiki Redirect
S = requests.Session()
WIKI_URL = "https://en.wikipedia.org/w/api.php"
PARAMS_wiki = {
    'action':"query",
    'list':"search",
    'srsearch': "",
    'format':"json"
}


def get_wiki_web_redirect(term):
    wiki_entity = ''

    PARAMS_wiki['srsearch'] = term
    while True:
        try:
            R1 = S.get(url=WIKI_URL, params=PARAMS_wiki)
            DATA = R1.json()
        except Exception as e:
            print(e)
            continue
        break

    if 'query' not in DATA:
        if debug_flag:
            print('Wiki redirect failed for term :' + term)
        return wiki_entity

    if DATA['query']['search']:
        if "disambiguation" in DATA['query']['search'][0]['title']:
            if len(DATA['query']['search'])>1:
                wiki_entity = DATA['query']['search'][1]['title']
        else:
            wiki_entity = DATA['query']['search'][0]['title']
    return wiki_entity


def get_wiki_redirect(terms):
    res = []
    new_wiki_items = []
    for term in terms:
        wiki_items = db.wiki_news.find({'word': term})
        wiki_flag = False
        for wiki_item in wiki_items:
            if 'entity' in wiki_item:
                wiki_flag = True
                res.append(wiki_item['entity'])
                #print(term + ': ' + wiki_item['entity'])
        if not wiki_flag:
            #print('Getting the new entery')
            wiki_term = get_wiki_web_redirect(term)
            if wiki_term:
                res.append(wiki_term)
                new_wiki_items.append({'word': term, 'entity': wiki_term})
            else:
                res.append(term)

    #print(len(new_wiki_items))
    if new_wiki_items:
        db.wiki_news.insert_many(new_wiki_items)
    return res


def get_output_json_corenlp(text):
    nlp_output = corenlp.annotate(text, properties={
        'outputFormat': 'json',
        'timeout': 90000,
        'annotators': 'ner',
        'ner.model': 'edu/stanford/nlp/models/ner/english.all.3class.distsim.crf.ser.gz',
        'ner.applyNumericClassifiers': False,
        'ner.applyFineGrained': False,
        #'ner.buildEntityMentions': False,
        'ner.combinationMode': 'NORMAL',
        'ner.useSUTime': False
    })
    return nlp_output


def get_corenlp_entities(nlp_output=None, text=None, verbosity=False):
    default_output = []

    if nlp_output:
        sentences = nlp_output
    elif text:
        sentences = get_output_json_corenlp(text)
    else:
        print('Niether JSON nor Text given!\n')
        return default_output

    # Entities
    ent_lst = []
    try:
        for s in sentences["sentences"]:
            tokens = s['tokens']
            #print(tokens)
            for ent in s['entitymentions']:
                #print(ent)
                if ent['ner'] in regect_entity_types:
                    print('Not Possible')
                    continue

                if tokens[ent['tokenBegin']]['pos'] in regect_pos_tags:
                    if debug_flag:
                        print('Regected Tag')
                    continue

                ent_lst.append(ent['text'])

                if verbosity:
                    print(ent['text'], ent['ner'])

    except TypeError:
        print('TypeError, Failed to get entities!\n')
        return default_output
    except:
        print('Some error, failed to entities!\n')
        return default_output

    return ent_lst


def get_corenlp_all(text, verbosity=False):
    output = {}
    text = str(text).strip()
    if text:
        sentences = get_output_json_corenlp(text)
    else:
        print('This is empty text!')
        return None

    if not sentences:
        print('No senetence for text: ' + text)
        return None

    # Entities after wiki redirects
    ent_lst = get_corenlp_entities(nlp_output=sentences, verbosity=verbosity)
    #print(ent_lst)
    ent_resolved = get_wiki_redirect(set(ent_lst))
    #print(ent_resolved)
    output['entities'] = set(ent_resolved)
    #print(output['entities'])

    return output


def get_tweet_text(tweet):
    tweet_text = ''
    got_text = False
    if 'truncated' in tweet:
        if 'extended_tweet' in tweet:
            tweet_text = tweet['extended_tweet']['full_text']
            got_text = True
    if not got_text:
        if 'full_text' in tweet:
            tweet_text = tweet['full_text']
        elif 'text' in tweet:
            tweet_text = tweet['text']
        else:
            print('Could not find the text for ' + str(tweet['id_str']))
    return tweet_text


def process_tweet_corenlp(tweet):
    id_str = tweet['id_str']
    text = get_tweet_text(tweet)
    corenlp_dict = get_corenlp_all(text)
    corenlp_dict['id_str'] = id_str
    return corenlp_dict


def main():
    # First Processing
    tweet_entities = []
    tweets = list(collection.find().sort('timestamp').limit(BATCH_SIZE))
    for tweet in tweets:
        if tweet['lang'] != 'en':
            continue
        tweet_entities.append(process_tweet_corenlp(tweet))
        print(tweet_entities[-1])
    new_collection.create_index([("id_str", pymongo.ASCENDING)], unique=True)
    new_collection.insert_many(tweet_entities)

    # Batch processing
    while True:
        cursor = collection.find({'timestamp': {'$gte': tweets[-1]['timestamp']}})
        tweets = list(cursor.limit(1000).sort('timestamp'))
        if not tweets:
            time.sleep(SLEEP_TIME)
            continue
        tweet_entities = []
        for tweet in tweets:
            if tweet['lang'] != 'en':
                continue
            tweet_entities.append(process_tweet_corenlp(tweet))
            print(tweet_entities[-1])
        new_collection.insert_many(tweet_entities)


main()