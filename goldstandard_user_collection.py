import json, es_users, sys, os
import csv
from TwitterAPI import *
from twitter import *
from twitter import Twitter
from time import sleep, time
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize

def get_user_tweet_objs(screen_name):
	tweet_jsons = es_users.getStoredTweets(screen_name)
	all_user_tweet_obj = []
	tweet_obj = {}
	#print screen_name
	for tweet_json in tweet_jsons:
		#print tweet_json["_source"]
		tweet_json = json.loads(json.dumps(tweet_json))
		tweet_obj["es_id"] = tweet_json["_id"]
		tweet_obj["text"] = tweet_json["_source"]["text"]
		#print tweet_obj["text"]
		tweet_obj["created_at"] = tweet_json["_source"]["created_at"]
		tweet_obj["favorite_count"] = tweet_json["_source"]["favorite_count"]
		tweet_obj["retweet_count"] = tweet_json["_source"]["retweet_count"]
		tweet_obj["twitter_id_str"] = tweet_json["_source"]["id_str"]
		tweet_obj["lang"] = tweet_json["_source"]["lang"]
		if "place" in tweet_json["_source"]:
			if ["place"] is not None:
				tweet_obj["place"] = tweet_json["_source"]["place"]
		if "coordinates" in tweet_json:
			if "coordinates" in tweet_json["coordinates"]:
				tweet_obj["coordinates"] = tweet_json["coordinates"]["coordinates"]
		#print tweet_obj
		all_user_tweet_obj.append(tweet_obj)
		tweet_obj = {}
	#print len(all_user_tweet_obj)
	#Get all unique tweets:
	unique_tweets = []
	unique_tweets_obj = []
	for x in all_user_tweet_obj:
		#print x
		if x["text"] not in unique_tweets:
			unique_tweets_obj.append(x)
			unique_tweets.append(x["text"])
			#print len(unique_tweets_obj)
	all_user_tweet_obj = unique_tweets_obj
	#print len(all_user_tweet_obj)
	#syst.exit()
	return all_user_tweet_obj


def get_user_data(list_df):
    df_rows = []
    for index, row in list_df.iterrows():
	    user_profile_json = es_users.getUserProfile(row['USER_ID'])
	    if (user_profile_json == []):
	        x = 1
	    else:
	        user_profile_json = user_profile_json[0]
	        user_profile_json = json.loads(json.dumps(user_profile_json["_source"]))
	        user_tweets_obj = get_user_tweet_objs(row['USER_ID'])
	        user_all_tweets_string, avg_fav_count, avg_retweet_count, min_fav_count, max_fav_count = user_tweets_string(user_tweets_obj)# avg_fav_count, avg_retweet_count, min_fav_count, max_fav_count = 0
	        user_profile_json["tweets"] = user_all_tweets_string
	        user_profile_json["avg_fav_count"] = avg_fav_count
	        user_profile_json["avg_retweet_count"] = avg_retweet_count
	        user_profile_json["min_fav_count"] = min_fav_count
	        user_profile_json["max_fav_count"] = max_fav_count
	        df_rows.append(user_profile_json)
    user_data = json_normalize(df_rows)
    #print list(user_data)
    #print len(list(user_data))
    #print user_data
    return user_data

def user_tweets_string(user_tweets_obj):
	user_all_tweets_string = ""
	sum_fav_count = 0
	max_fav_count = 0
	min_fav_count = 0
	sum_retweet_count = 0
	for user_tweet_obj in user_tweets_obj:
		#print user_tweet_obj["text"]
		user_all_tweets_string = user_all_tweets_string + (user_tweet_obj["text"] .replace("\n", " "))
		sum_retweet_count = user_tweet_obj["retweet_count"] + sum_retweet_count
		sum_fav_count = user_tweet_obj["favorite_count"] + sum_fav_count
		if user_tweet_obj["favorite_count"] > max_fav_count:
			max_fav_count = user_tweet_obj["favorite_count"]
		if user_tweet_obj["favorite_count"] < min_fav_count:
			min_fav_count = user_tweet_obj["favorite_count"]
	avg_fav_count = sum_fav_count / (len(user_tweets_obj)*1.0)
	avg_retweet_count = sum_retweet_count / (len(user_tweets_obj)*1.0)
	# print user_all_tweets_string
	# print "avg fav " + str(avg_fav_count)
	# print "avg retweet " + str(avg_retweet_count)
	# print "max fav " + str(max_fav_count)
	# print "min fav " + str(min_fav_count)
	return user_all_tweets_string, avg_fav_count, avg_retweet_count, min_fav_count, max_fav_count

if __name__ == "__main__":
    df = pd.read_csv("users_list/profile_goldstandard.csv", quotechar="\"", header = 0)
    foo = "DEPRESSED/NO"
    yes_df =  df.loc[df["DEPRESSED/NO"] == "yes"]
    no_df =  df.loc[df['DEPRESSED/NO'] == "no"]
    yes_user_data = get_user_data(yes_df)
    no_user_data = get_user_data(no_df)
    print (list(yes_user_data))
    #print yes_user_data.ix[0]["tweets"]
    #print no_user_data.ix[0]
    #print len(get_user_tweet_objs("rahzamdy"))


# def get_user_data(yes_df):
#     df_rows = []
#     df_cols = []
#
#     row_index = -1
#     #print list(yes_df)
#     for index, row in yes_df.iterrows():
#         user_profile_json = es_users.getUserProfile(row['USER_ID'])
#         if user_profile_json == []:
#             x = 1
#             #print row['USER_ID']
#         else:
#             row_index = row_index + 1
#             user_profile_json = user_profile_json[0]
#             #print user_profile_json
#             user_profile_json = json.loads(json.dumps(user_profile_json["_source"]))
#             #user_profile_json = json_normalize(user_profile_json)
#             user_row = []
#             #yes_user_data.append(json_normalize(user_profile_json), ignore_index = True)
#             df_rows.append(user_profile_json)
#             for key in user_profile_json:
#                 if key not in df_cols:
#                     #print key
#                     df_cols.append(key)
#                 #print user_profile_json[key]
#                 # if isinstance(user_profile_json[key], list):
#                 #     print user_profile_json[key]
#                 #     print "hi"
#             #     print key
#             #     print row_index
#             #     yes_user_data.loc[row_index,key] = user_profile_json[key]
#     #yes_user_data = pd.DataFrame(columns = df_cols)
#     yes_user_data = json_normalize(df_rows)
#     #print yes_user_data
#     #print df_rows
#     # for x in df_rows:
#     #     print x
#     #     yes_user_data.append(x, ignore_index = True)
#     print list(yes_user_data)
#     print len(list(yes_user_data))
#     print yes_user_data
