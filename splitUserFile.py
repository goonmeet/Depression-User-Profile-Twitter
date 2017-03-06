import numpy as np
import pandas as pd
import csv

if __name__ == "__main__":
    f_in =  "unique_users_list_goonmeet.csv"
    df = pd.read_csv(f_in, names=['NA', 'screen_name'])
    subNames = (np.array_split(df["screen_name"], 20))
    count = 00
    
    for x in subNames:
    	ofile  = open('unique_users' + str(count) + '.csv', "wb")
    	writer = csv.writer(ofile, quotechar='"', quoting=csv.QUOTE_ALL)
    	for y in x:
    		writer.writerow([y])   		
    	count = count + 1
    		
    	
    	
    		