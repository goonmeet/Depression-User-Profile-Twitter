from elasticsearch import Elasticsearch
from elasticsearch_dsl import Search, Q
from elasticsearch_dsl.connections import connections
es = Elasticsearch(['localhost:9201'], timeout= 160, max_retries=15, retry_on_timeout=True)


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

#
# def get_all_tweets(screen_name):
#         #Twitter only allows access to a users most recent 3240 tweets with this method
#
#         auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
#         auth.set_access_token(access_key, access_secret)
#         #authorize twitter, initialize tweepy
#         api = tweepy.API(auth)
#
#         #initialize a list to hold all the tweepy Tweets
#         alltweets = []
#
#         #make initial request for most recent tweets (200 is the maximum allowed count)
#         new_tweets = api.user_timeline(screen_name = screen_name,count=200)
#
#         #save most recent tweets
#         alltweets.extend(new_tweets)
#  oldest = alltweets[-1].id - 1
#
#         #keep grabbing tweets until there are no tweets left to grab
#         while len(new_tweets) > 0:
#                 print ("getting tweets before" + str(oldest))
#
#                 #all subsiquent requests use the max_id param to prevent duplicates
#                 new_tweets = api.user_timeline(screen_name = screen_name,count=200,max_id=oldest)
#
#                 #save most recent tweets
#                 alltweets.extend(new_tweets)
#
#                 #update the id of the oldest tweet less one
#                 oldest = alltweets[-1].id - 1
#
#                 print ("tweets downloaded so far " +str((len(alltweets))))
#
#         # Index using elastic search
#         return alltweets
#
#
# def read_self_reported_users(fin):
#         fin= "/home/amir/code/tweet_mappings/"+ fin
#         with open(fin, buffering=200000) as f:
#                 account_lists = []
#                 try:
#                         for line in f:
#                                 account = (line.split(',')[1]).strip()
#                                 account_lists.append(str(account))
#                         return account_lists
#                 except Exception as e:
#                         print(str(e))
#
# def indexTweets(tweets):
#
#     counter = 0
#
#     for tweet in tweets:
#
#         result = es.index(index=index_name, doc_type="tweepyTweet", body=tweet._json)
#
#         counter += 1
#
#         print (counter)
#
#
# # This gets the tweets and index them using elasticsearch
# def elasticIndexer():
#         fin =  "uniqueUsers.csv"
#         twitter_profiles = read_self_reported_users(fin)
#         #twitter_profiles=twitter_profiles[:100]
#         #print (twitter_profiles)
#         #sys.exit()
#         #twitter_profiles = ["grownhellokitty", "lovelovely_14", "caramel300"]
#
#         for i in range(len(twitter_profiles)):
#                 try:
#                         tweets = get_all_tweets(twitter_profiles[i])
#                         indexTweets(tweets)
#                         print (twitter_profiles[i] + "Done!")
#                         time.sleep(3)
#                 except:
#                         pass
#
#
# if __name__ == "__main__":
#
#     # index tweets
#     elasticIndexer()
