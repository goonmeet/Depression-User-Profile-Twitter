import json, es_users, sys, os
import csv
from TwitterAPI import *
from twitter import *
from twitter import Twitter
from time import sleep, time
import pandas as pd
import numpy as np

def determine_users_to_collect(user_objects, cursor, continue_writer):
	p2 = []
	for user_object in user_objects:
		print user_object["screen_name"]
		if ((user_object["protected"] == False) and (user_object["lang"] == "en")):
			#print user_object.keys()
			if (("status" in user_object.keys()) and (user_object["status"]["lang"] == "en")):
				if user_object["statuses_count"] > 2000:
					if (1 == 1):
						print "Collecting"
						print user_object['screen_name']
						getAndIndexTimeline(user_object['screen_name'], [], [])
						print "done"
						print user_object['screen_name']
					else:
						p2.append(user_object)
				else:
						p2.append(user_object)
	#print p2
	return p2

def collectFollowers(screen_name, continue_writer, done_writer):
    request_json = {'screen_name': screen_name}
    r = API.request('followers/list', request_json)
    r = r.json()
    while( "errors" in r.keys()):
    	print r
    	sleep(180)
    	r = API.request('followers/list', request_json)
    	r = r.json()
    response = []
    #print r
    response = response + r["users"]
    cursor = r['next_cursor']
    for_later = ["goonmeet"]
    p2 = determine_users_to_collect(response, cursor, continue_writer)
    for_later = p2
    #print for_later
    while (cursor != 0):
    	request_json = {'screen_name': screen_name, 'cursor' : cursor}
    	r = API.request('followers/list', request_json)
    	r = r.json()
    	if "errors" in r.keys():
    		print r
    		print "Error 88"
    		continue_writer.writerow([cursor, response])
    		sleep(180)
    	else:
    		#print r
    		response = response + r["users"]
    		cursor = r['next_cursor']
    		#break
    		#sleep(60)
    	p2 = determine_users_to_collect(r["users"], cursor, continue_writer)
    	done_writer.writerow([cursor, r["users"]])
    	for_later.extend(p2)
    	sleep(60)
    return response
		
def startFromBeginning(user_response_obj):
    request_json = {'count':'20', 'screen_name': user_response_obj["screen_name"]}
    # Get all tweets in the user's current timeline
    count = 0
    break_now = False
    while (count < user_response_obj["statuses_count"] or break_now == True or count < 1000): # and (diff_time < 120)
        exception_occured = False
        try:
           r = API.request('statuses/user_timeline', request_json)
        except:
            sleep(100)
            exception_occured = True
            r = API.request('statuses/user_timeline', request_json)

        #print r.json()
        if (len(r.json()) == 0) and (exception_occured  == False):
            break_now = True
            return 0
        if "errors" in r.json():
            print "Too many requests... waiting"
            sleep(180)
            #r = API.request('statuses/user_timeline', request_json)
        else:
            for item in r.get_iterator():
                #print json.dumps(item)
                if 'text' in item:
                    #print str(item['text'].encode('utf-8'))
                    max_id = item['id']
                    count = count + 1
                    # Add a max_id feild to response object to avoid
                    request_json['max_id'] = (max_id - 1)
                elif 'message' in item and item['code'] == 88:
                    print 'SUSPEND, RATE LIMIT EXCEEDED: %s\n' % item['message']
                # If the tweet is not indexed in ES add it to ES:
                if (es_users.getTweet(item["id_str"]) < 1):
                    tweet_json = json.dumps(item)
                    es_users.indexTweet(item)
                    count = count + 1
                else:
                    print "{} already indexed".format(item["id_str"])
        sleep(10)
    return count

def logError(screen_name, error):
    error_writer.writerow([screen_name, error])

def isPublic(screen_name, user_response_obj, private_list):
    if ((user_response_obj["protected"] == True)):
        #print user_response_obj["protected"]

        if screen_name not in private_list:
            private_list = private_list.append(screen_name)
            private_writer.writerow([screen_name])
        print "{} is private".format(screen_name)
        return False
    else:
        return True

def someTweetsStored(screen_name, user_response_obj):
    stored_tweets_count = es_users.getStoredTweetCount(screen_name)
    return stored_tweets_count
    # if stored_tweets_count > 0:
    #     since_id = es_users.getMax_Id(screen_name)
    #     return since_id
    # else:
    #     return 0

def getAndIndexTimeline(screen_name, private_list, suspended_list):
    #print screen_name
    if screen_name in suspended_list:
        return 0
    since_id = 0
    count = 0
    # Get user timeline by screen_name
    response = API.request('users/show', {'screen_name': screen_name})
    if "errors" in response.json():
        print response.json()['errors']
        for x in response.json()['errors']:
            if x['code'] == 50:
                logError(screen_name, x['message'])
                return 0
            if x['code'] == 63:
                if screen_name not in suspended_list:
                    suspended_list = suspended_list.append(screen_name)
                    suspended_writer.writerow([screen_name])
                print "{} is suspended".format(screen_name)
                return 0
    while ("errors" in response.json()):
        print "Too many requests... waiting"
        sleep(180)
        response = API.request('users/show', {'screen_name': screen_name})
    user_response_obj = response.json()
    if user_response_obj.has_key("errors"):
        logError(screen_name, json.dumps(user_response_obj))
        return 0
    # If a response is returned parse it
    crawl_user = isPublic(screen_name, user_response_obj, private_list)
    if crawl_user == False:
        return 0
    if ((crawl_user == True) and (len(response.json()) > 0)):

        #print user_response_obj
        user_response_json = json.dumps(user_response_obj)

        # If the user is not in the data base store their profile in ES:
        if (es_users.getUser(screen_name) < 1):
            es_users.indexUser(screen_name, user_response_json)
            print "{} added to ES".format(screen_name)
        else:
            print "{} already in ES".format(screen_name)

        # Get the number of total statuses the user has posted
        statuses_count = user_response_obj["statuses_count"]
        print "{} tweets on {}'s timeline".format(statuses_count, user_response_obj["screen_name"])

        #count = startFromBeginning(user_response_obj)
        # See how many tweets we have stored in ES, if 0 then start crawl from beginning, else use since_id
        stored_tweets_count = someTweetsStored(screen_name, user_response_obj)
        if (stored_tweets_count < user_response_obj["statuses_count"]):
            count = startFromBeginning(user_response_obj)

        print "{} new tweets collected from {}'s profile".format(count, user_response_obj["screen_name"])
    return 0


if __name__ == "__main__":
    serverNumber = sys.argv[1]
    print serverNumber
    # Create connection to twitter api load in keys from file
    key_file = 'keys/twitter_api_keys_' + str(serverNumber) + '.json'
    key_file = open(key_file)
    keys_dict = json.load(key_file)
    API = TwitterAPI(keys_dict["consumer_key"], keys_dict["consumer_secret"], keys_dict["access_token_key"], keys_dict["access_token_secret"])
    statuses_count = 0
    
    error_filename = 'error_log_' + str(serverNumber) + '.csv'
    if os.path.exists(error_filename):
        append_write = 'a+' # append if already exists
    else:
        append_write = 'w+' # make a new file if not
    error_log  = open(error_filename, append_write)
    error_writer = csv.writer(error_log, quotechar='"', quoting=csv.QUOTE_ALL)
    error_list = pd.read_csv(error_filename, names=['screen_name'], quotechar="\"")

    private_filename = 'private_log_' + str(serverNumber) + '.csv'
    if os.path.exists(private_filename):
        append_write = 'a+' # append if already exists
    else:
        append_write = 'w+' # make a new file if not
    private_log  = open(private_filename, append_write)
    private_writer = csv.writer(private_log, quotechar='"', quoting=csv.QUOTE_ALL)
    private_list = pd.read_csv(private_filename, names=['screen_name'], quotechar="\"")
    private_list = private_list.values.tolist()

    suspended_filename = 'suspended_log_' + str(serverNumber) + '.csv'
    if os.path.exists(suspended_filename):
        append_write = 'a+' # append if already exists
    else:
        append_write = 'w+' # make a new file if not
    suspended_log  = open(suspended_filename, append_write)
    suspended_writer = csv.writer(suspended_log, quotechar='"', quoting=csv.QUOTE_ALL)
    suspended_list = pd.read_csv(suspended_filename, names=['screen_name'], quotechar="\"")
    suspended_list = suspended_list.values.tolist()
    
    continue_filename = 'continue_log_' + str(serverNumber) + '.csv'
    if os.path.exists(continue_filename):
        append_write = 'a+' # append if already exists
    else:
        append_write = 'w+' # make a new file if not
    continue_log  = open(continue_filename, append_write)
    continue_writer = csv.writer(continue_log, quotechar='"', quoting=csv.QUOTE_ALL)
    continue_list = pd.read_csv(continue_filename, names=['screen_name'], quotechar="\"")
    continuee_list = continue_list['screen_name'].values.tolist()
    
    done_filename = 'done_log_' + str(serverNumber) + '.csv'
    if os.path.exists(done_filename):
        append_write = 'a+' # append if already exists
    else:
        append_write = 'w+' # make a new file if not
    done_log  = open(done_filename, append_write)
    done_writer = csv.writer(done_log, quotechar='"', quoting=csv.QUOTE_ALL)
    done_list = pd.read_csv(done_filename, names=['screen_name', 'curu', 'res'], quotechar="\"")
    done_list = done_list['screen_name'].values.tolist()
    temp = []

    #f_in =  "users_list/self_unique_users" + str(serverNumber) + ".csv"
    #df = pd.read_csv(f_in, names=['screen_name', "count"], quotechar="\"")
    # Rate limit status
    #r = API.request('application/rate_limit_status')
    #getAndIndexTimeline("Holly_Marie10", private_list, suspended_list)
    #print r.json()
    
#     df_ric = pd.read_csv("users_list/profile_goldstandard_ric.csv", quotechar="\"", header = 0, error_bad_lines=False, encoding='utf-8', engine='c')
#     df_des = pd.read_csv("users_list/profile_goldstandard_des.csv", quotechar="\"", header = 0, error_bad_lines=False, encoding='utf-8', engine='c')
#     foo = "DEPRESSED/NO"
#     yes_df = df_ric.loc[df_ric["DEPRESSED/NO"] == "yes"]
#     print len(yes_df)
#     yes_df = yes_df.append(df_des.loc[df_des["DEPRESSED/NO"] == "yes"])
#     print len(yes_df)
   # no_df =  df.loc[df['DEPRESSED/NO'] == "no"]
    u_o = []
#     for index, row in yes_df.iterrows():
    for name in ["nicolaeliasuk", "anxiety59", "AnxietyProbzz", "AnxietyInTeens", "The_Anxiety_Guy", "secretlycutting", "GmCutting" ,"suchascrewup", "thoughtswdildo"]:
		user_profile_json = es_users.getUserProfile(name)
		if str(name) not in [""]:
			if len(user_profile_json) == 0:
				getAndIndexTimeline(name, private_list, suspended_list)
			#print row['USER_ID']
			else:
				#followers_count = es_users.getFollowersCount(name)
				print name
				response = collectFollowers(name, continue_writer, done_writer)
					#break
					#print followers_count

# if __name__ == "__main__":
#     serverNumber = sys.argv[1]
#     print serverNumber
#     # Create connection to twitter api load in keys from file
#     key_file = 'keys/twitter_api_keys_' + str(serverNumber) + '.json'
#     key_file = open(key_file)
#     keys_dict = json.load(key_file)
#     API = TwitterAPI(keys_dict["consumer_key"], keys_dict["consumer_secret"], keys_dict["access_token_key"], keys_dict["access_token_secret"])
#     statuses_count = 0

#     error_filename = 'error_log_' + str(serverNumber) + '.csv'
#     if os.path.exists(error_filename):
#         append_write = 'a+' # append if already exists
#     else:
#         append_write = 'w+' # make a new file if not
#     error_log  = open(error_filename, append_write)
#     error_writer = csv.writer(error_log, quotechar='"', quoting=csv.QUOTE_ALL)
#     error_list = pd.read_csv(error_filename, names=['screen_name'], quotechar="\"")
# 
#     private_filename = 'private_log_' + str(serverNumber) + '.csv'
#     if os.path.exists(private_filename):
#         append_write = 'a+' # append if already exists
#     else:
#         append_write = 'w+' # make a new file if not
#     private_log  = open(private_filename, append_write)
#     private_writer = csv.writer(private_log, quotechar='"', quoting=csv.QUOTE_ALL)
#     private_list = pd.read_csv(private_filename, names=['screen_name'], quotechar="\"")
#     private_list = private_list.values.tolist()
# 
#     suspended_filename = 'suspended_log_' + str(serverNumber) + '.csv'
#     if os.path.exists(suspended_filename):
#         append_write = 'a+' # append if already exists
#     else:
#         append_write = 'w+' # make a new file if not
#     suspended_log  = open(suspended_filename, append_write)
#     suspended_writer = csv.writer(suspended_log, quotechar='"', quoting=csv.QUOTE_ALL)
#     suspended_list = pd.read_csv(suspended_filename, names=['screen_name'], quotechar="\"")
#     suspended_list = suspended_list.values.tolist()
# 
#     done_filename = 'done_log_' + str(serverNumber) + '.csv'
#     if os.path.exists(done_filename):
#         append_write = 'a+' # append if already exists
#     else:
#         append_write = 'w+' # make a new file if not
#     done_log  = open(done_filename, append_write)
#     done_writer = csv.writer(done_log, quotechar='"', quoting=csv.QUOTE_ALL)
#     done_list = pd.read_csv(done_filename, names=['screen_name'], quotechar="\"")
#     done_list = done_list['screen_name'].values.tolist()
#     temp = []
#     for x in done_list:
#         temp.append(x)
#     #print temp
#     f_in =  "users_list/self_unique_users" + str(serverNumber) + ".csv"
#     df = pd.read_csv(f_in, names=['screen_name', "count"], quotechar="\"")
# 
#     # Rate limit status
#     r = API.request('application/rate_limit_status')
#     #getAndIndexTimeline("Holly_Marie10", private_list, suspended_list)
#     #print r.json()
#     test = []
# 
#     for screen_name in df["screen_name"]:
#         if screen_name in temp:
#             print "{} is already crawlled".format(screen_name)
#         if screen_name not in temp:
#             if screen_name not in error_list:
#                 getAndIndexTimeline(screen_name, private_list, suspended_list)
#                 done_writer.writerow([screen_name])
#                 temp.append(screen_name)
#                 print "{} added to done log!".format(screen_name)
#                 sleep(15)


# def startFromSince_Id(user_response_obj, since_id):
#     print "using since_id"
#     request_json = {'count':'1', 'screen_name': user_response_obj["screen_name"]}
#     # Get all tweets in the user's current timeline
#     count = 0
#     break_now = False
#     while (count < user_response_obj["statuses_count"] or break_now == True): # and (diff_time < 120)
#         r = API.request('statuses/user_timeline', request_json)
#         #print r.json()
#         if len(r.json()) == 0:
#             break_now = True
#             return 0
#         if "errors" in r.json():
#             print "Too many requests... waiting"
#             sleep(180)
#             r = API.request('statuses/user_timeline', request_json)
#
#         else:
#             for item in r.get_iterator():
#                 #print json.dumps(item)
#                 # Update max_id and since_id
#                 if 'text' in item:
#                     #print str(item['text'].encode('utf-8'))
#                     max_id = item['id']
#                     if item['id'] > since_id:
#                         since_id = item['id']
#                     count = count + 1
#                     # Add a max_id feild to response object to avoid
#                     request_json['max_id'] = (max_id - 1)
#                     request_json['since_id'] = since_id
#                 elif 'message' in item and item['code'] == 88:
#                     print 'SUSPEND, RATE LIMIT EXCEEDED: %s\n' % item['message']
#                 # If the tweet is not indexed in ES add it to ES:
#                 if (es_users.getTweet(item["id_str"]) < 1):
#                     tweet_json = json.dumps(item)
#                     es_users.indexTweet(item)
#                 else:
#                     print "{} already indexed".format(item["id_str"])
#
#         sleep(5)
