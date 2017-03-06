import json, es_users, sys
import csv
from TwitterAPI import *
from twitter import *
from twitter import Twitter
from time import sleep, time
import pandas as pd
import numpy as np

serverNumber = sys.argv[1]
print serverNumber
# Create connection to twitter api load in keys from file
key_file = 'keys/twitter_api_keys_' + str(serverNumber) + '.json'
key_file = open(key_file)
keys_dict = json.load(key_file)
API = TwitterAPI(keys_dict["consumer_key"], keys_dict["consumer_secret"], keys_dict["access_token_key"], keys_dict["access_token_secret"])
statuses_count = 0

error_log  = open('error_log_' + str(serverNumber) + '.csv', "wb")
writer = csv.writer(error_log, quotechar='"', quoting=csv.QUOTE_ALL)

# Rate limit status
r = API.request('application/rate_limit_status')
#print r.json()
def logError(screen_name, error):
    writer.writerow([screen_name, error])

def getAndIndexTimeline(screen_name):
    since_id = 0
    count = 0
    # Get user timeline by screen_name
    response = API.request('users/show', {'screen_name': screen_name})
    while ("errors" in response.json()):
        print "Too many requests... waiting"
        sleep(180)
        response = API.request('users/show', {'screen_name': screen_name})
    user_response_obj = response.json()
    # If a response is returned parse it
    if ((user_response_obj["protected"] == False) and (len(response.json()) > 0)):
        #print user_response_obj
        if user_response_obj.has_key("errors"):
            logError(screen_name, json.dumps(user_response_obj))
            return 0
        user_response_json = json.dumps(user_response_obj)
        # If the user is not in the data base store their profile in ES
        stored_tweets_count = es_users.getStoredTweetCount(screen_name)
        if (es_users.getUser(screen_name) < 1):
            es_users.indexUser(screen_name, user_response_json)
            print "{} added to ES".format(screen_name)
        else:
            print "{} already in ES".format(screen_name)

        # Get the number of total statuses the user has posted
        statuses_count = user_response_obj["statuses_count"]
        print "{} tweets on {}'s timeline".format(statuses_count, user_response_obj["screen_name"])
        tweets = []
        request_json = {'count':'1', 'screen_name': user_response_obj["screen_name"]}
        bef_time = time()
        diff_time = 0
        if ((statuses_count != stored_tweets_count)):
            # Get all tweets in the user's current timeline
            while (count < statuses_count): # and (diff_time < 120)
                r = API.request('statuses/user_timeline', request_json)
                #print (r.json())
                if "errors" in r.json():
                	print "Too many requests... waiting"
                	sleep(180)
                	r = API.request('statuses/user_timeline', request_json)

                else:
                    for item in r.get_iterator():
                        #print json.dumps(item)
                        # If the tweet is not indexed in ES add it to ES:
                        if (es_users.getTweet(item["id_str"]) < 1):
                            tweet_json = json.dumps(item)
                            es_users.indexTweet(item)
                        else:
                            print "{} already indexed".format(item["id_str"])
                        # Update max_id and since_id
                        if 'text' in item:
                            #print str(item['text'].encode('utf-8'))
                            max_id = item['id']
                            if item['id'] > since_id:
                                since_id = item['id']
                            tweets.append(item)
                            count = count + 1
                            # Add a max_id feild to response object to avoid
                            request_json['max_id'] = (max_id - 1)
                        elif 'message' in item and item['code'] == 88:
                            print 'SUSPEND, RATE LIMIT EXCEEDED: %s\n' % item['message']
            	sleep(5)
        print "{} tweets collected from {}'s profile".format(count, user_response_obj["screen_name"])


if __name__ == "__main__":
    #getAndIndexTimeline("goonmeet")
    f_in =  "users_list/unique_users" + str(serverNumber) + ".csv"
    df = pd.read_csv(f_in, names=['screen_name'], quotechar="\"")
    for screen_name in df["screen_name"]:
        getAndIndexTimeline(screen_name)
        sleep(15)
