import numpy as np
import pandas as pd
import csv

if __name__ == "__main__":
    f_in =  "users_need.txt"
    df = pd.read_csv(f_in, names=['screen_name'])
    subNames = (np.array_split(df["screen_name"], 2))
    count = 04

    for x in subNames:
    	ofile  = open('users_list/profile_goldstandard' + str(count) + '.csv', "wb")
    	writer = csv.writer(ofile, quotechar='"', quoting=csv.QUOTE_ALL)
    	for y in x:
    		writer.writerow([y])
    	count = count + 1
