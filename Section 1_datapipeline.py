#!/usr/bin/env python
# coding: utf-8

# In[74]:


import pandas as pd
url = 'https://raw.githubusercontent.com/jaabberwocky/dataeng_test/master/dataset.csv'
df = pd.read_csv(url)


# In[76]:


# Split the name field into first_name, and last_name
def split_first_last_name(df):
    '''
    input: dataframe
    output: dataframe with new field first_name, and last_name
    methodology: 
    1) Split name strings into tokens
    2) Remove designation eg. Mr. , Mrs.
    3) Store first_name, last_name into one list each 
    4) Append to dataframe
    '''
    first_name = []
    last_name = []
    for i in df["name"]:
        string = i.split()
        to_remove = ["PhD", "MD", "Ms.", "Mrs.", "Dr.", "Jr.", "Mr.", "DDS"," "]
        string = [i for i in string if i not in to_remove]
        first_name += [string[0]]
        last_name +=  [" ".join(string[1:len(string)])]
    df["first_name"] = first_name
    df["last_name"] = last_name
    return df


# In[77]:


# Remove any zeros prepended to the price field (need to clarify)
def remove_prepended_zeros(df):
    '''
    input: dataframe
    output: dataframe price field converted from string to float thereby removing prepended 0
    '''
    df["price"] = df["price"].apply(float)
    return df


# In[129]:


# Delete any rows which do not have a name
def remove_rows_without_name(df):
    '''
    input: dataframe
    output: dataframe with empty or "" entries removed from names column
    '''
    return df[(df["name"]!="") & (df["name"].notna()) ]


# In[89]:


# Create a new field named above_100, which is true if the price is strictly greater than 100
def append_col_above_100(df):
    '''
    input: dataframe
    output: dataframe with True when price>100 and False Otherwise in the "above_100" column appended
    '''
    df["above_100"] = df["price"].apply(lambda x: True if x>100 else False)
    return df


# In[153]:


# Compile Pipeline and Set up a Scheduler
import time
import schedule

def datapipeline():
    
    # 1) Import Data from Github
    import pandas as pd
    url = 'https://raw.githubusercontent.com/jaabberwocky/dataeng_test/master/dataset.csv'
    df = pd.read_csv(url)
    
    # 2) Split the name field into first_name, and last_name
    def split_first_last_name(df):
        '''
        input: dataframe
        output: dataframe with new field first_name, and last_name
        methodology: 
        1) Split name strings into tokens
        2) Remove designation eg. Mr. , Mrs.
        3) Store first_name, last_name into one list each 
        4) Append to dataframe
        '''
        first_name = []
        last_name = []
        for i in df["name"]:
            string = i.split()
            to_remove = ["PhD", "MD", "Ms.", "Mrs.", "Dr.", "Jr.", "Mr.", "DDS"," "]
            string = [i for i in string if i not in to_remove]
            first_name += [string[0]]
            last_name +=  [" ".join(string[1:len(string)])]
        df["first_name"] = first_name
        df["last_name"] = last_name
        return df
    # 3) Remove any zeros prepended to the price field (need to clarify)
    def remove_prepended_zeros(df):
        '''
        input: dataframe
        output: dataframe price field converted from string to float thereby removing prepended 0
        '''
        df["price"] = df["price"].apply(float)
        return df
    
    # 4) Delete any rows which do not have a name
    def remove_rows_without_name(df):
        '''
        input: dataframe
        output: dataframe with empty or "" entries removed from names column
        '''
        return df[(df["name"]!="") & (df["name"].notna())]
    
    # 5) Create a new field named above_100, which is true if the price is strictly greater than 100
    def append_col_above_100(df):
        '''
        input: dataframe
        output: dataframe with True when price>100 and False Otherwise in the "above_100" column appended
        '''
        df["above_100"] = df["price"].apply(lambda x: True if x>100 else False)
        return df
    
    #Apply all the functions
    df = split_first_last_name(df)
    df = remove_prepended_zeros(df)
    df = remove_rows_without_name(df)
    df = append_col_above_100(df)
    
    #Update the file with the updated date
    datetoday = time.strftime(time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime()))[0:10].replace("-","")
    df.to_csv("dataset_updated_{}.csv".format(datetoday))

    return df

# Set Up Schedule to update everyday at 1.01am
schedule.every().day.at("01:01").do(datapipeline)

while True:
    schedule.run_pending()
    time.sleep(1)
    
# schedule.every(1).minutes.do(datapipeline)
# schedule.clear()
# datapipeline()

