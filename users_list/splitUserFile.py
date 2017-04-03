import numpy as np
import pandas as pd
import csv

def splitDataFrameIntoSmaller(df, chunkSize):
    listOfDf = list()
    numberChunks = len(df) // chunkSize + 1
    for i in range(numberChunks):
        listOfDf.append(df[i*chunkSize:(i+1)*chunkSize])
    return listOfDf


if __name__ == "__main__":
    f_in =  "self_reported_users.csv"
    #df = pd.read_csv(f_in, names=['screen_name', 'count'])
    df = pd.read_csv(f_in)
    subNames = (np.array_split(df, 17))
    #subNames = splitDataFrameIntoSmaller(df, 1000)
    count = 00

    for x in subNames:
        x.to_csv('self_unique_users' + str(count) + '.csv')
    	# ofile  = open('self_unique_users' + str(count) + '.csv', "wb")
    	# writer = csv.writer(ofile, quotechar='"', quoting=csv.QUOTE_ALL)
    	# for y in x:
    	# 	writer.writerow([y])
    	count = count + 1
