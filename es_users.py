from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections
es = Elasticsearch(['localhost:9201'], timeout= 160, max_retries=15, retry_on_timeout=True)

def getAllUsers():
    res = es.search(size = 1000, scroll = '1m', index="user_profiles", doc_type="Self_Reported_Profiles_40k", body={"query": {  "match_all": {}},"_source": ["screen_name"]}, request_timeout=60)
    s_id = res['_scroll_id']
    scroll_size = res['hits']['total']
    #print res
    all_users = res['hits']['hits']
    while (scroll_size > 0):
        res = es.scroll(scroll_id = s_id, scroll = '1m' , request_timeout=160)
        s_id = res['_scroll_id']
        scroll_size = len(res['hits']['hits'])
        tweets = res['hits']['hits']
        all_users = all_users + tweets
        #print "scroll_size: " + str(scroll_size)
    return all_users


def getUserProfile(screen_name):
    res = es.search(index="user_profiles", doc_type="Self_Reported_Profiles_40k", body={"query": { "match": {"_id" : screen_name}}}, request_timeout=60)
    #print res
    user = res['hits']["hits"]
    return user

def getUser(screen_name):
    res = es.search(index="user_profiles", doc_type="Self_Reported_Profiles_40k", body={"query": { "match": {"_id" : screen_name}}}, request_timeout=160)
    #print res
    profile_count = res['hits']['total']
    return profile_count

def getUserStoredInfo(screen_name):
    res = es.search(index="user_profiles", doc_type="Self_Reported_Profiles_40k", body={"query": { "match": {"_id" : screen_name}}}, request_timeout=160)
    #print res
    profile_count = res['hits']['hits']
    return profile_count

def getFollowersCount(screen_name):
    res = es.search(index="user_profiles", doc_type="Self_Reported_Profiles_40k", body={"_source" : "followers_count", "query": { "match": {"screen_name": screen_name}}}, request_timeout=160)
    followers_count = res['hits']['hits'][0]["_source"]["followers_count"]
    return followers_count

def getStoredTweetCount(screen_name):
    res = es.search(index="depressed_tweets", doc_type="tweepyTweet", body={"query": { "match": {"user.screen_name": screen_name}}}, request_timeout=160)
    #print res
    tweet_count = res['hits']['total']
    return tweet_count

def getStoredTweets(screen_name):
    res = es.search(size = 1000, scroll = '1m', index="depressed_tweets", doc_type="tweepyTweet", body={"query": { "match": {"user.screen_name": screen_name}}}, request_timeout=160)
    s_id = res['_scroll_id']
    scroll_size = res['hits']['total']
    #print res
    all_tweets = res['hits']['hits']
    while (scroll_size > 0):
        res = es.scroll(scroll_id = s_id, scroll = '1m' , request_timeout=160)
        s_id = res['_scroll_id']
        scroll_size = len(res['hits']['hits'])
        tweets = res['hits']['hits']
        all_tweets = all_tweets + tweets
        #print "scroll_size: " + str(scroll_size)
    return all_tweets

def getMax_Id(screen_name):
    res = es.search(index="depressed_tweets", doc_type="tweepyTweet", body={"query": { "match": {"user.screen_name": screen_name}}}, request_timeout=160)
    #print("%d documents found" % res['hits']['total'])
    tweet_ids = []
    for doc in res['hits']['hits']:
        #print("%s) %s" % (doc['_id'], doc['_source']))
        id_str = (doc['_id'], doc['_source']['id_str'])
        #print ((int(id_str[1])))
        tweet_ids.append((int(id_str[1])))
    #print "Max", max(tweet_ids)
    return max(tweet_ids)

def getMin_Id(screen_name):
    res = es.search(index="depressed_tweets", doc_type="tweepyTweet", body={"query": { "match": {"user.screen_name": screen_name}}}, request_timeout=160)
    #print("%d documents found" % res['hits']['total'])
    tweet_ids = []
    for doc in res['hits']['hits']:
        #print("%s) %s" % (doc['_id'], doc['_source']))
        id_str = (doc['_id'], doc['_source']['id_str'])
        #print ((int(id_str[1])))
        tweet_ids.append((int(id_str[1])))
    #print "Max", max(tweet_ids)
    return min(tweet_ids)

def getTweet(id_str):
    res = es.search(index = "depressed_tweets", doc_type = "tweepyTweet", body = {"query": { "match": {"id_str":id_str} } }, request_timeout=160)
    tweet_count = res['hits']['total']
    return tweet_count


def indexTweet(tweet_json):
    res = es.index(index = "depressed_tweets", doc_type = "tweepyTweet", body = tweet_json)
    print res

def indexUser(index_id, user_profile_json):
    res = es.index(index = "user_profiles", doc_type = "Self_Reported_Profiles_40k", body = user_profile_json, id = index_id)
    print res
