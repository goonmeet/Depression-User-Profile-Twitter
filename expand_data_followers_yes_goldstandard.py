import json, es_users, sys, os, user_timeline_collection
import csv
from TwitterAPI import *
from twitter import *
from twitter import Twitter
from time import sleep, time
import pandas as pd
import numpy as np
from pandas.io.json import json_normalize

if __name__ == "__main__":
	df_ric = pd.read_csv("users_list/profile_goldstandard_ric.csv", quotechar="\"", header = 0, error_bad_lines=False, encoding='utf-8', engine='c')
	df_des = pd.read_csv("users_list/profile_goldstandard_des.csv", quotechar="\"", header = 0, error_bad_lines=False, encoding='utf-8', engine='c')
	foo = "DEPRESSED/NO"
	yes_df =  df_ric.loc[df_ric["DEPRESSED/NO"] == "yes"]
	print len(yes_df)
	yes_df_des = (df_des.loc[df_des["DEPRESSED/NO"] == "yes"])
	yes_df = yes_df.append(yes_df_des)
	print len(yes_df)
	#no_df =  df.loc[df['DEPRESSED/NO'] == "no"]
	u_o = []
	for index, row in yes_df.iterrows():
		user_profile_json = es_users.getUserProfile(row['USER_ID'])
		if len(user_profile_json) == 0:
			x = 1
			#print row['USER_ID']
		else:
			print row['USER_ID']
# 			followers_count = es_users.getFollowersCount(row['USER_ID'])
# 			if followers_count > 17000:
# 				print row['USER_ID']
# 				print user_timeline_collection.collectFollowers(row['USER_ID'])
# 				#print followers_count
# 				u_o.append(followers_count)
	#sorted(u_o)
	
	# protected = false
	# lang = en
	# lang = en-US
	# statuses_count > 1000
	# ThatL0nelyGuy