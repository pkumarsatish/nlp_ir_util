"""Linking Tweets and news Based on URL
Given start_time and end_time, Using the tweet and news collection of the mongodb,
get a file containing list of news-tweet pair linked based on URL
Essential Steps:
    * Tweet collection filtered based on timestamp (mktime) field, 'get_timestamp' to convert 'created_at' to 'mktime'
    * Pymongo Cursor to loop over tweets, Batchwise. 'TweetBlockSize' tweets query at once.
    * Ignore tweet if:
        - no url field (t['entities']['urls'])
        - url['expanded_url'] has domain from 'regect_domain'
    * If passed the above filter, request api to get redirected standard api
    * Query the news colletcion for the standard URL. If available, Store them
Inputs:     start_time, end_time, db
Parameters: TweetBlockSize, request_timeout, regect_domain, display_tweet_count
Output: 'filename' .log file (save result at run time), 'outfile' JSON file upon completion
Created By:     Satish Kumar
Last Modified:  23 May 2019
"""


from pymongo import MongoClient
import time
import datetime
import requests
import json


# Connect to the local mongo Setup
client = MongoClient('mongodb://localhost:27017/')
db = client['xxxx']

# Parameters
TweetBlockSize = 30*60
request_timeout = 15
regect_domain = ['twitter.com', 'youtu.be', 'youtube.com', 'www.youtube.com']
display_tweet_count = 1000
start_time = 'Mon May 20 00:00:00 +0000 2019'
end_time = 'Wed May 22 00:00:00 +0000 2019'
filename = 'May20_May22_news_tweets_url_links.log'
outfile = 'May20_May22_news_tweets_link' + str(datetime.datetime.now()) + '.json'


def get_timestamp(created_at):
    time_format = '%a %b %d %H:%M:%S +0000 %Y'
    d = time.strptime(created_at, time_format)
    return int(time.mktime(d))


start_ts = get_timestamp(start_time)
end_ts = get_timestamp(end_time)
n_ts = (end_ts - start_ts) // TweetBlockSize


# Log File
fp = open(filename, 'w+')
fp.write('Starting from ' + str(datetime.datetime.now()) + '\n')
fp.flush()

# Stats Related Variables
all_url_list = {
    'Success': 0,
    'TimeOut': 0,
    'ProxyErr': 0,
    'TooManyRedirects': 0,
    'SSLErr': 0,
    'OtherErr': 0
}

retweet_counts = 0
quoted_counts = 0
regected_counts = 0
total_counts = 0


news_tweet_links = []
for i_ts in range(n_ts):
    tsi = start_ts + i_ts*TweetBlockSize
    tsf = tsi + TweetBlockSize

    #fp.write('Query loop num: ' + str(i_ts) + '\n')
    print('Query loop num: ' + str(i_ts) + '\n')

    ###################################################
    ## MONGODB QUERY
    ###################################################
    tweets = db.tweets.find({'timestamp': {'$gte': tsi, '$lt': tsf}})

    for t in tweets:
        # Display periodically tweet counts processed
        total_counts += 1
        if total_counts % display_tweet_count == 0:
            #fp.write(str(total_counts) + ' Tweets processed\n')
            print(str(total_counts) + ' Tweets processed\n')

        # Filter the retweets and quoted tweets
        if 'retweeted_status' in t:
            retweet_counts += 1
            continue
        if 'quoted_status' in t:
            quoted_counts += 1
            continue

        for url in t['entities']['urls']:
            url_ext = url['expanded_url']

            # Skip regected domains
            start_idx = 8
            if url_ext[4] == ':':
                start_idx = 7
            match_flag = False
            for domain in regect_domain:
                match_flag = False
                if len(domain) <= len(url_ext) - start_idx:
                    match_flag = True
                    for idx in range(len(domain)):
                        if url_ext[start_idx + idx] != domain[idx]:
                            match_flag = False
                            break
                if match_flag:
                    break
            if match_flag:
                regected_counts += 1
                continue

            # Process the url ext to request
            try:
                req_ret = requests.get(url_ext, timeout=request_timeout)
                req_url = req_ret.url
                all_url_list['Success'] += 1
            except requests.exceptions.Timeout:
                all_url_list['TimeOut'] += 1
                continue
            except requests.exceptions.ProxyError:
                all_url_list['ProxyErr'] += 1
                continue
            except requests.exceptions.TooManyRedirects:
                all_url_list['TooManyRedirects'] += 1
                continue
            except requests.exceptions.SSLError:
                all_url_list['SSLErr'] += 1
                continue
            except:
                all_url_list['OtherErr'] += 1
                continue

            ################################################################
            # Query for url match
            ################################################################
            matched_news = db.news.find({"url": req_url.split('?')[0]})
            if matched_news.count() > 0:
                news_list = []
                for news in matched_news:
                    news_list.append(str(news['_id']))
                    print('Match Found!!!\n')
                    fp.write('\n' + str(news['_id']) + ' ' + t['id_str'] + '\n')
                    fp.flush()
                news_tweet_links.append({t['id_str']: news_list})
            matched_news.close()
    tweets.close()
    time.sleep(1)

with open(outfile, 'w') as otf:
    json.dump(news_tweet_links, otf)

fp.write('\n')
fp.write('Total Tweet Counts: ' + str(total_counts) + '\n')
fp.write('Retweet Counts:' + str(retweet_counts))
fp.write('\nQTweets Counts:' + str(quoted_counts))
fp.write('\nRegected Counts:' + str(regected_counts) + '\n')
fp.write('\n')

print(all_url_list)
for key in all_url_list:
    print(key + ': ' + str(all_url_list[key]))
    fp.write(key + ': ' + str(all_url_list[key]))

fp.close()