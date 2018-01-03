# -*- coding: utf-8 -*-
"""
Created on Tue Oct 31 14:32:40 2017

@author: nwalker
"""
"""
effort - import as eff

The purpose of this package is to allow new students to import, clean, and
model Twitter data out of the box without needing to reinvent the wheel.

Functions included:
    file_json_pandas()
        Takes a single JSON file and returns a pandas DataFrame.
        
    folder_json_pandas()
        Takes a folder path of JSON files, imports them, and returns a
        pandas DataFrame.
        
    remove_duplicates()
        Takes imported Twitter data (pandas DF) and removes duplicate tweets,
        returning a reduced pandas DataFrame.
        *   Requires spaCy (v. 1.9.0): https://spacy.io
                **spaCy 2.0+ is radically different and will not work yet.
        *** Possibly time-intensive. A small dataset (10,000) finishes in 
            seconds. A medium-sized dataset (600,000) takes about 10 hours.
            Set logger for progress reporting.
            
    topic_model()    

"""

def file_json_pandas(json_file):
    """
    Takes in a single JSON file and returns a pandas DataFrame. Option to 
    pickle or CSV.
    
    json_file: (str)
        Full location of JSON file. No default.
            e.g., json_file = "C:\\Users\\nwalker\\data\\filename.json"
    
    pickle_out: (str)
        Desired pickled filename. Defaults to None (no output).
            e.g., pickleout="tweetdf.txt"
            
    csv_out: (str)
        Desired CSV filename. Defaults to None (no output).
            e.g., csvout="tweetdf.csv"
    
    Returns: pandas DataFrame
    """

    import pandas as pd
    import json
    import re
    
    
    with open(json_file, "r", encoding = "utf-8") as file:
        tweet_data = json.load(file)
     
    df = pd.DataFrame(
        {
        "tweet_id" : [
            tweet['id_str'] 
            for tweet in tweet_data
            ],
            #for each row in tweet_data, select the 'id_str'
        "tweet_created_at" : [
            tweet['created_at'] 
            for tweet in tweet_data
            ],
        "tweet_text" : [
            tweet['text'] 
            for tweet in tweet_data
            ],
        "tweet_favorite_count" : [
            tweet['favorite_count'] 
            for tweet in tweet_data
            ],
        "tweet_retweet_count" : [
            tweet['retweet_count'] 
            for tweet in tweet_data
            ],
        "tweet_in_reply_to_screen_name" : [ 
            tweet['in_reply_to_screen_name'] 
            for tweet in tweet_data
            ],
        "tweet_in_reply_to_status_id_str" : [
            tweet['in_reply_to_status_id_str'] 
            for tweet in tweet_data
            ],
        "tweet_in_reply_to_user_id_str" : [
            tweet['in_reply_to_user_id_str'] 
            for tweet in tweet_data
            ],
        "tweet_hashtags" : [
            [item['text'] 
            for item in item_list] 
                for item_list in [tweet.get('entities').get('hashtags') 
                    for tweet in tweet_data]
            ],
            # for each row in tweet_data, select 'entities' then 'hashtags'
                # for each item in 'hashtags', select the dictionary
                    # in each dictionary, select 'text'
                        # add it to the list
        "tweet_urls" : [
            [item['expanded_url'] 
                for item in item_list] 
                    for item_list in [tweet.get('entities').get('urls') 
                        for tweet in tweet_data]
            ],
        "tweet_mentions_id" : [
            [item['id_str'] 
                for item in item_list] 
                    for item_list in [tweet.get('entities').get('user_mentions') 
                        for tweet in tweet_data]
            ],
        "tweet_mentions_name" : [
            [item['name'] 
            for item in item_list] 
                for item_list in [tweet.get('entities').get('user_mentions') 
                    for tweet in tweet_data]
            ],
        "tweet_mentions_screen_name" : [
            [item['screen_name'] 
            for item in item_list] 
                for item_list in [tweet.get('entities').get('user_mentions') 
                    for tweet in tweet_data]
            ],
        "tweet_media_url" : [
            [item['expanded_url'] 
            for item in item_list] 
                for item_list in [tweet.get('extended_entities').get('media') 
                if tweet.get('extended_entities') is not None 
                else '' 
                    for tweet in tweet_data]
            ],
                 # for each row in tweet_data, select 'extended_entities' then 
                 #'media', but only attempt if 'extended_entities' exists
                     #for each item in 'media', select the dictionary
                         # in each dictionary, select 'expanded_url'
                             # add it to the list
        "tweet_media_type" : [
            [item['type'] 
            for item in item_list] 
                for item_list in [tweet.get('extended_entities').get('media') 
                if tweet.get('extended_entities') is not None 
                else '' 
                    for tweet in tweet_data]
            ],
        "user_created" : [
            tweet.get('user').get('created_at') 
                for tweet in tweet_data
            ],
        "user_description" : [
            tweet.get('user').get('description') 
                for tweet in tweet_data
            ],
        "user_description_url" : [
            [item['expanded_url'] 
            for item in item_list] 
                for item_list in [
                        tweet.get('user').get('entities')
                            .get('url').get('urls') 
                        if tweet.get('user').get('entities')
                            .get('url') is not None 
                        else '' 
                            for tweet in tweet_data]
            ],
                 # for each row in tweet_data, select 'user' then 'entities' then 'url', but only attemtp if 'url' exists
                     # for each item in 'url', select the dictionary
                         # in each dictionary, select 'expanded_url'
                             # add it to the list
        "user_favorites_count" : [
            tweet.get('user').get('favourites_count') 
            for tweet in tweet_data
            ],
        "user_followers_count" : [
            tweet.get('user').get('followers_count') 
            for tweet in tweet_data
            ],
        "user_friends_count" : [
            tweet.get('user').get('friends_count') 
            for tweet in tweet_data
            ],
        "user_id" : [
            tweet.get('user').get('id_str') 
            for tweet in tweet_data
            ],
        "user_name" : [
            tweet.get('user').get('name') 
            for tweet in tweet_data
            ],
        "user_screen_name" : [
            tweet.get('user').get('screen_name') 
            for tweet in tweet_data
            ],
        "user_verified" : [
            tweet.get('user').get('verified') 
            for tweet in tweet_data
            ],
        "rt_tweet_id" : [
            tweet.get('retweeted_status').get('id_str') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_tweet_text" : [
            tweet.get('retweeted_status').get('text') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_tweet_created_at" : [
            tweet.get('retweeted_status').get('created_at') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_tweet_favorite_count" : [
            tweet.get('retweeted_status').get('favorite_count') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_tweet_retweet_count" : [
            tweet.get('retweeted_status').get('retweet_count') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_tweet_in_reply_to_screen_name" : [
            tweet.get('retweeted_status').get('in_reply_to_screen_name') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_tweet_in_reply_to_status_id_str" : [
            tweet.get('retweeted_status').get('in_reply_to_status_id_str') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_tweet_in_reply_to_user_id_str" : [
            tweet.get('retweeted_status').get('in_reply_to_user_id_str') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_id" : [
            tweet.get('retweeted_status').get('user').get('id_str') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_description" : [
            tweet.get('retweeted_status').get('user').get('description') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_description_url" :  [
            [item['expanded_url'] 
                for item in item_list] 
                    for item_list in [
                            tweet.get('retweeted_status').get('user')
                                .get('entities').get('url').get('urls') 
                            if tweet.get('retweeted_status') is not None 
                            and tweet.get('retweeted_status').get('user')
                                .get('entities').get('url') is not None 
                            else '' 
                                for tweet in tweet_data]
            ],
        "rt_user_favorites_count" : [
            tweet.get('retweeted_status').get('user').get('favourites_count') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_followers_count" : [
            tweet.get('retweeted_status').get('user').get('followers_count') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_created_at" : [
            tweet.get('retweeted_status').get('user').get('created_at') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_name" : [
            tweet.get('retweeted_status').get('user').get('name') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_screen_name" : [
            tweet.get('retweeted_status').get('user').get('screen_name') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ],
        "rt_user_verified" : [
            tweet.get('retweeted_status').get('user').get('verified') 
            if tweet.get('retweeted_status') is not None 
            else '' 
                for tweet in tweet_data
            ]             
        }
    )

    # create string columns of some lists (space as separator)
    df['tweet_hashtags_str'] = [' '.join(i) for i in df.tweet_hashtags]
    df['tweet_media_type_str'] = [' '.join(i) for i in df.tweet_media_type]
    df['tweet_mentions_id_str'] = [' '.join(i) for i in df.tweet_mentions_id]

    # convert dates from string to pandas datetime
    df['tweet_created_at'] = pd.to_datetime(df['tweet_created_at'], 
                                         format = "%a %b %d %H:%M:%S +0000 %Y")
    df['user_created'] = pd.to_datetime(df['user_created'], 
                                         format = "%a %b %d %H:%M:%S +0000 %Y")   
    
    # remove new line ( "\n" ) from tweets.
    # Leaving these in will make all CSVs import incorrectly, jumping to
    # a new row every time \n is seen in text.
    
    df.loc[:, 'tweet_text'] = df.loc[:, 'tweet_text'].apply(
                                                 lambda x: re.sub('\n', '', x))
    df.loc[:, 'rt_tweet_text'] = df.loc[:, 'rt_tweet_text'].apply(
                                                 lambda x: re.sub('\n', '', x))
    df.loc[:, 'rt_user_description'] = df.loc[:, 'rt_user_description'].apply(
                                                 lambda x: re.sub('\n', '', x))    
    df.loc[:, 'user_description'] = df.loc[:, 'user_description'].apply(
                                                 lambda x: re.sub('\n', '', x))        
 
    """
    # It seems just like bloat to have pickling and csv-creation as built-in
    # options. Yes, it would be super handy to have them automatically
    # created, but only if you're 100% new to Python. Otherwise you can
    # handle your files yourself. I think I can *almost* just as easily
    # include pickling/CSV options in the example code. My fear is that I'll
    # have waaaay too many variables down the line and it will magically
    # go from useful to useless.
    
    if pickle_out != None:
        with open(pickle_out, "wb") as file:
            try:
                pickle.dump(df, file)
                print("Created pickle with filename " 
                      + pickle_out)
            except:
                print("Couldn't pickle. Was your filename complete?")
                pass
            
    if csv_out != None:
        try:
            df.to_csv(csv_out)
            print("Created CSV with filename " 
                  + csv_out)
        except:
            print("Couldn't create CSV. Was your filename complete?")
            pass            
    """
    return df


def folder_json_pandas(in_folder):
    """
    Imports all JSON files in in_folder and returns a single pandas DataFrame.
    Option to pickle or CSV.
    
    in_folder: (str)
        Full location of folder with all JSON files. No default.
            e.g., "C:\\Users\\nwalker\\data\\json_folder"
            
    pickle_out: (str)
        Name of pickled data. Default is None (no output).
            e.g., "tweet_pickles.txt"
    
    csv_out: (str)
        Name of CSV file for output. Default is None (no output).
            e.g., "all_my_tweets.csv"
    
    Returns pandas DataFrame
    """
    
    import os
    import pandas as pd 
        
    # import data
    folder = os.listdir(in_folder)
    folder = [i for i in folder if i[-4:] == 'json'] # remove non-JSON files
    
    # create list of dataframes
    all_dfs = []
    for file in folder:
        json_df = file_json_pandas(in_folder + '\\' + file)
        all_dfs.append(json_df)    
    
    df = pd.concat(all_dfs)
    
    """
    # See note above on removing this data.
    if pickle_out != None:
        with open(pickle_out, "wb") as file:
            try:
                pickle.dump(df, file)
                print("Created pickle with filename " 
                      + pickle_out)
            except:
                print("Couldn't pickle.")
                pass
            
    if csv_out != None:
        try:
            df.to_csv(csv_out)
            print("Created CSV with filename " 
                  + csv_out)
        except:
            print("Couldn't create CSV.")
            pass            
    """
    
    return df
    
def spacy_parse(data,
                tweet_text = "tweet_text",
                tweet_id = "tweet_id"):
    """
    Parses each tweet using spaCy v.2.0. Requires English model linked as 'en'
    which can be installed by typing "python -m spacy download en" at the
    command prompt (will probably require you to right-click and "run as 
    Administrator" to be able to link).
    
    data (pandas DataFrame)
        DataFrame containing at least 
    
    tweet_text (str)
    
    tweet_id (str)
    
    Returns nothing, but 'parsed' column is added inplace to data DataFrame.
    This new dataset can be pickled
    """
    
    import spacy.lang.en
    
    # load parser
    nlp = spacy.load('en')

    tweets = data.loc[:, tweet_text].tolist()
    with open('tweet_as_doc.txt', 'w', encoding = 'utf-8') as file:
        for tweet in tweets:
            file.write(tweet + '\n')
    
    tweet_list = []
    with open('tweet_as_doc.txt', 'r', encoding = 'utf-8') as file:
        parsed = nlp.pipe(file)
        for doc in parsed:
            tweet_list.append(doc)
    
    data['parsed'] = tweet_list


def remove_duplicates(data, 
                      sim_amt = .99,
                      tweet_id = "tweet_id",
                      tweet_text = "tweet_text",
                      tweet_created_at = "tweet_created_at",
                      remove_zeroes = True):
    
    """
    Remove tweets that meet a specific threshold of similarity (based on cosine
    similarity). This is, for the most part, one big function that could
    really be broken into many sub-functions for ease of understanding.
    However, this is basically a port of the way it was first made and it works
    so I'm keeping it like this.
    
    data: (pandas DataFrame)
        pandas DataFrame of tweets. Automatically set up to flow from effort
        file_json_pandas() or folder_json_pandas(). Requires at least three
        columns: tweet IDs, tweet text, and date of tweet creation (see below).
        
    sim_amt: (float)
        Number between 0 and ~1. Cosine similarity threshold of tweets. 
        Look at the duplicate tweets dataframe to see whether you need to 
        raise or lower the amount. Defaults to .99.
                
    tweet_id: (str)
        Column name in data for the ID number of tweets. Defaults to "tweet_id"
        
    tweet_text: (str)
        Column name in data for the text of tweets. Defaults to "tweet_text"
        
    tweet_created_at: (str)
        Column name in data for the date the tweet was posted. Defaults
        to "tweet_created_at"
        
    remove_zeroes: (bool)
        Controls whether or not tweets with no semantic information are left
        in the reduced dataset or discarded as duplicates (of gibberish).
        Defaults to True.
            
    Returns list of pandas DataFrames:
        [0]: duplicate tweets removed
        [1]: removed duplicate tweets
        [2]: only duplicates, grouped together
    """
    
    import numpy as np
    import time
    import pandas as pd
    import logging
    
    logger = logging.getLogger()
        
    # set index to column, set tweet_id to index
    data.loc[:, 'index_col'] = data.index
    data.set_index(tweet_id, inplace=True, drop=False)
    data.sort_index(inplace=True)    
    
    # parse tweets
    if not 'parsed' in data.columns:
        spacy_parse(data,
                    tweet_text = tweet_text,
                    tweet_id = tweet_id)
    
    '''
    Create dictionary of non-zero .vector_norm tweets:
    There are likely tweets that have a vector_norm of 0. We'll take 
    them out because we divide by vector_norm, so if it is zero, we will get 
    NaN, which we don't want. This is 12x faster than using spaCy's 
    .similarity() method. When dealing with hundreds of thousands of tweets, 
    this can save days.    
    
    We're also taking out the vectors for URLs, punctuation, spaces, "RT",
    and Twitter hanldes (@so-and-so) because people can take the same tweet
    and add people's handles or hashtags to them and we want to catch them
    all as the "same" tweet (which we won't, otherwise). URLs are their own
    problem -- in spaCy v. 1.x most of these had empty vectors. The new models
    in v. 2.0 have given them their own vectors. This is good, but makes things
    tricky here so we will take them out.
    
    The structure of this dictionary is:
        keys: tweet IDs
        0: None -- we will use this to identify its matched group
        1: (int) -- this is the length of the tweet
        2: vector -- this is the .vector for each tweet
        3: (int) -- this is the .vector_norm for each tweet
    '''
    
    id_list = list(data.index)
    tweet_lens = list()
    zeroes = list()
    tweet_vector_dict = dict()

    for tweet in id_list:
        keepers = [token.vector 
                   for token in data.loc[tweet, 'parsed']
                   if not (thing_to_remove(token)
                           or token.lemma_[0] == '@')] # remove Twitter handles
        summed = sum(keepers)
        norm = np.sqrt(np.dot(summed, summed))
        if norm == 0:
            zeroes.append(tweet)
        else:
            tweet_vector_dict[tweet] = list(['',
                                             len(keepers),
                                             summed,
                                             norm])
        
    '''
    Here, we create a list of tweets (by length) in advance so we don't have to 
    re-index and eat up computation time on the back end.
    '''
    
    len_list = [tweet_vector_dict[tw][1] for tw in tweet_vector_dict] 
    
    tweet_lens = list()
    
    for leng in set(len_list):
        tweet_lens.append(
                {t: list([tweet_vector_dict[t][0], 
                          tweet_vector_dict[t][1], 
                          tweet_vector_dict[t][2], 
                          tweet_vector_dict[t][3]]) 
                    for t in tweet_vector_dict 
                    if tweet_vector_dict[t][1] == leng})
            
            
            
            
            
    '''
    Comparison 1: same-length vs same-length
    '''
    itercounter = 0
    itertime = time.time()
    
    for leng in tweet_lens:
        comp_set = set(list(leng.keys()))
        compiter = list(comp_set)
        
        for id1 in compiter:
            comp_set.discard(id1)
            if leng[id1][0] != '': # test if it's already matched
                continue 
            '''
            This next step does our copmutation for each non-matched pair:
            Numerator: dot product(id1.vector, id2.vector)
            Denominator: id1.vector_norm * id2.vector_norm
            '''
            comp_list = list(comp_set)
            comp_scores = [[id2, (np.dot(leng[id1][2], leng[id2][2]) / 
                                 (leng[id1][3] * leng[id2][3]))] 
                            for id2 in comp_list]
        
            # list of ids for tweets with similarity over sim_amt
            ids_comp = [k 
                        for k, v in comp_scores 
                        if v > sim_amt]
            
            # for every match, change the "matched group" to id1
            for id2 in ids_comp:
                leng[id2][0] = id1
                leng[id1][0] = id1
                comp_set.difference_update(ids_comp)

        #log
        itercounter += 1
        now = time.time() - itertime
        hours = int(np.floor(now / 60 / 60))
        minutes = int(np.floor((now - (hours * 60 * 60)) / 60)) 
        seconds = int(np.floor(now - ((hours * 60 * 60) + (minutes * 60))))    
        logger.info("Comparison 1: Done with round " 
                    + repr(itercounter) + " of " 
                    + repr(len(tweet_lens)) + " at "
                    + repr(hours) + " hours, " 
                    + repr(minutes) + " minutes, and " 
                    + repr(seconds) + " seconds.")       
    
    
    '''
    Comparison 2: "matched group" by +/- 2 length window of unmatched tweets
    '''
    
    itercounter = -1
    m_1 = set()
    m_2 = set()
    itertime = time.time()

    matched_list = list()
    for leng in tweet_lens:
        matched_list.append(set([v[0] 
                                for k, v in leng.items() 
                                if v[0] != '']))
            # add to list if "matched group" != ''

    for leng in tweet_lens:
        itercounter += 1
        '''
        Set up comparison windows of +/- 2 around each length. 
        '''
        if itercounter == 1:
            m_1 = set([k 
                       for k, v in tweet_lens[0].items() 
                       if v[0] == ''])
        if itercounter > 1:
            m_2 = set([k 
                       for k, v in tweet_lens[itercounter - 2].items() 
                       if v[0] == ''])
            m_1 = set([k 
                       for k, v in tweet_lens[itercounter - 1].items() 
                       if v[0] == ''])
                
        if itercounter < (len(tweet_lens) - 2):
            p_2 = set([k 
                       for k, v in tweet_lens[itercounter + 2].items() 
                       if v[0] == ''])
            p_1 = set([k 
                       for k, v in tweet_lens[itercounter + 1].items() 
                       if v[0] == ''])
        if itercounter == (len(tweet_lens) - 2):
            p_1 = set([k 
                       for k, v in tweet_lens[len(tweet_lens) - 2].items() 
                       if v[0] == ''])
            p_2 = set()
        if itercounter == (len(tweet_lens) - 1):
            p_1 = set()
            p_2 = set()
                            
        '''
        Check matched groups versus comparison window. The similarity
        calculation is the same as in Comparison 1, it just doesn't easily
        fit on one/two/three lines.
        '''
                            
        for id1 in matched_list[itercounter]:
            # comparison for minus 2
            m_2_list = list(m_2)
            
            comp_scores = [[id2, (np.dot(leng[id1][2], 
                                     tweet_lens[itercounter - 2][id2][2]) /
                                 (leng[id1][3] * 
                                     tweet_lens[itercounter - 2][id2][3]))] 
                                         for id2 in m_2_list]
        
            ids_comp = [k 
                        for k, v in comp_scores 
                        if v > sim_amt]
            # update "matched group" in new matches (minus 2)
            for id2 in ids_comp:
                tweet_lens[itercounter - 2][id2][0] = id1
                m_2.difference_update(ids_comp)
            
            
            # comparison for minus 1
            m_1_list = list(m_1)
            comp_scores = [[id2, (np.dot(leng[id1][2], 
                                     tweet_lens[itercounter - 1][id2][2]) / 
                                 (leng[id1][3] *
                                      tweet_lens[itercounter - 1][id2][3]))] 
                                          for id2 in m_1_list]
        
            ids_comp = [k 
                        for k, v in comp_scores 
                        if v > sim_amt]
            # update "matched group" in new matches (minus 1)
            for id2 in ids_comp:
                tweet_lens[itercounter - 1][id2][0] = id1
                m_1.difference_update(ids_comp)
            
            
            # comparison for plus 1
            p_1_list = list(p_1)
            comp_scores = [[id2, (np.dot(leng[id1][2], 
                                     tweet_lens[itercounter + 1][id2][2]) / 
                                 (leng[id1][3] *
                                     tweet_lens[itercounter + 1][id2][3]))] 
                                         for id2 in p_1_list]
                
            ids_comp = [k 
                        for k, v in comp_scores 
                        if v > sim_amt]
            # update "matched group" in new matches (plus 1)
            for id2 in ids_comp:
                tweet_lens[itercounter + 1][id2][0] = id1
                p_1.difference_update(ids_comp)
            
            
            # comparison for plus 2
            p_2_list = list(p_2)
            comp_scores = [[id2, (np.dot(leng[id1][2], 
                                     tweet_lens[itercounter + 2][id2][2]) / 
                                 (leng[id1][3] *
                                     tweet_lens[itercounter + 2][id2][3]))] 
                                         for id2 in p_2_list]
            ids_comp = [k 
                        for k, v in comp_scores 
                        if v > sim_amt]
            # update "matched group" in new matches (plus 2)
            for id2 in ids_comp:
                tweet_lens[itercounter + 2][id2][0] = id1
                p_2.difference_update(ids_comp)
        
        # logging
        now = time.time() - itertime
        hours = int(np.floor(now / 60 / 60))
        minutes = int(np.floor((now - (hours * 60 * 60)) / 60)) 
        seconds = int(np.floor(now - ((hours * 60 * 60) + (minutes * 60))))    
        logger.info("Comparison 2: Done with round " 
                    + repr(itercounter + 1) + " of " 
                    + repr(len(tweet_lens)) + " at "
                    + repr(hours) + " hours, " 
                    + repr(minutes) + " minutes, and " 
                    + repr(seconds) + " seconds.")
            
    '''
    Comparison 3: Matches vs matches
    '''
                
    # update/replace tweet_vector_dict with new match definitions
    tweet_vector_dict = {}
    for leng in tweet_lens:
        tweet_vector_dict.update(leng)
        
    # create list of all ids for "matched groups"
    matches_set = set([v[0] for k, v in tweet_vector_dict.items()])
    matches_set.discard('')
    
    matches_list = list(matches_set)
    
    itercounter = 0
    itertime = time.time()
    
    for match in matches_list:
        matches_set.discard(set([match])) # don't check same-vs-same
        matches_set_list = list(matches_set) # updated list of to-check ids
        
        # make comparisons, as before
        comp_scores = [[id2, (np.dot(tweet_vector_dict[match][2], 
                                 tweet_vector_dict[id2][2]) /
                             (tweet_vector_dict[match][3] *
                                 tweet_vector_dict[id2][3]))]
                                     for id2 in matches_set_list]
        ids_comp = [k 
                    for k, v in comp_scores 
                    if v > sim_amt]
                
        # update tweet_vector_dict for matched values
        for id2 in ids_comp:
            tweet_vector_dict[id2][0] = match
            for k, v in tweet_vector_dict.items():
                if v[0] == id2:
                    v[0] = match
        matches_set.difference_update(ids_comp)
        
        # logging
        itercounter += 1                   
        now = time.time() - itertime
        hours = int(np.floor(now / 60 / 60))
        minutes = int(np.floor((now - (hours * 60 * 60)) / 60)) 
        seconds = int(np.floor(now - ((hours * 60 * 60) + (minutes * 60))))    
        logger.info("Comparison 3: Done with round " 
                    + repr(itercounter) + " of " 
                    + repr(len(matches_list)) + " at "
                    + repr(hours) + " hours, " 
                    + repr(minutes) + " minutes, and " 
                    + repr(seconds) + " seconds.")
        
    '''
    Post-comparison: Match up ids in original DataFrame

    We'll create a dictionary now with the following parameters:
    key: tweet id
        0: group id
        1: tweet text
        2: tweet created at date
        3: boolean for "is first date in group"
    This dictionary consists of all tweets that have groups
    '''
    has_match = set([k 
                     for k, v in tweet_vector_dict.items() 
                     if v[0] != ''])
    is_match = list(set([v[0] 
                    for k, v in tweet_vector_dict.items() 
                    if v[0] != '']))
    match_dict = {k : [v[0], 
                       data.loc[k, tweet_text], 
                       data.loc[k, tweet_created_at],
                       False]
                  for k, v in tweet_vector_dict.items() 
                  if k in has_match}
    
    '''
    From here, we're just looping through each of the matched groups and making
    a list of two groups for each item: [id, date]. We're then sorting that 
    group by its date and adding the first item (first date) to a list of 
    tweets to keep.
    '''
    
    by_group = list()
    first_dates = list()
    
    for group in is_match:
        group_list = [[k, v[2]] 
                      for k, v in match_dict.items() 
                      if v[0] == group]
        by_group.append([k[0] for k in group_list])
        min_date = sorted(group_list, key = lambda x: x[1], reverse = False)[0]
        first_dates.append(min_date[0])

    tweets_to_remove = has_match.difference(set(first_dates))
    
    if remove_zeroes == True:
        tweets_to_remove.update(set(zeroes))
    
    tweets_to_keep = set(id_list).difference(tweets_to_remove)
    
    reduced_df = data.loc[tweets_to_keep, :]
    removed_df = data.loc[tweets_to_remove, :]
    
    '''
    The last item is the dup_df, which is a pandas DataFrame of tweet IDs,
    text, and group-identifiers. This can be used to assess sim_amt.
    '''
    
    dup_list = list()
    for group in range(len(by_group)):
        dup_df = pd.DataFrame(data.loc[by_group[group],
                                     [tweet_id, tweet_text]])
        dup_df['group'] = group
        dup_list.append(dup_df)
        
    dup_df = pd.concat(dup_list)
    dup_df.set_index('group', inplace=True)
        
    return list([reduced_df, removed_df, dup_df])
    
'''
LDA Models
'''

def thing_to_remove(token):
    """ 
    checks to see if token is punction, space, or URL
        token: spaCy word token
            
    returns boolean
    """
    return (token.is_punct 
            or token.is_space 
            or token.like_url
            or token.is_digit
            or token.like_num
            or token.lemma_ == "RT")

def lemmatized_sentence_corpus(data, 
                               parsed_col = 'parsed'):
    """
    generator used to take parsed tweets, lemmatize them, and give sentences
    
    dataframe: (pandas DataFrame)
    
    parsed_col: (str)
        column name where spaCy-parsed data is located. Defaults to 'parsed'.
    
    returns generator
    """
    for doc in data.get(parsed_col):
        yield(u' '.join([token.lemma_ 
                         for token in doc
                         if not thing_to_remove(token)]))


def phrase_model(data,
                 parsed_col = 'parsed'):

    """
    Creates unigram, bigram, and trigram models and sentences.
    Created files:
        phrase_model_unigram.txt : sentences based on lemmas
        phrase_model_bigram.txt : sentences based on lemmas and bigrams
        phrase_model_trigram.txt : sentences based on lemmas, bigrams, and trigrams
    
    Created models:
        bigram_model
        trigram_model
        
    These models are used later to create the dictionary.    

    """
    from gensim.models import Phrases
    from gensim.models.phrases import Phraser
    from gensim.models.word2vec import LineSentence
    from pandas import concat
    
    # shuffle data before starting --
        # Train/Test/Holdout data is taken from this order in a later step
    data_ids = set(data.index)
    train = data.sample(frac = .5)
    non_train_ids = data_ids.difference(set(train.index))
    test = data.loc[non_train_ids, :].sample(frac = .5)
    holdout_ids = non_train_ids.difference(set(test.index))
    holdout = data.loc[holdout_ids, :]
    data_shuffled = concat([train, test, holdout])
    
    train_test_hold = {'train':list(train.index), 
                       'test': list(test.index),
                       'hold': list(holdout.index)}
    
    
    with open('phrase_model_unigram.txt', 'w', encoding = 'utf_8') as file:
        for sentence in lemmatized_sentence_corpus(data = data_shuffled,
                                                   parsed_col = parsed_col):
            if sentence[0] == '.':
                file.write(sentence[1:] + '\n')
            else:
                file.write(sentence + '\n')
    
    unigram_sentences = LineSentence('phrase_model_unigram.txt')
    bigram_model = Phrases(unigram_sentences)
    bigram_phraser = Phraser(bigram_model)
    
#    bigram_model.save('phrase_model_bigram_save.txt')
    bigram_phraser.save('phrase_model_bigram_save.txt')
    
    with open('phrase_model_bigram_output.txt', 'w', encoding = 'utf_8') as file:
        for sent in unigram_sentences:
            bigram_sentence = u' '.join(bigram_phraser[sent])
            file.write(bigram_sentence + '\n')
            
    bigram_sentences = LineSentence('phrase_model_bigram_output.txt')
    trigram_model = Phrases(bigram_sentences)
    trigram_phraser = Phraser(trigram_model)
    
#    trigram_model.save('phrase_model_trigram_save.txt')
    trigram_phraser.save('phrase_model_trigram_save.txt')
    
    with open('phrase_model_trigram_output.txt', 'w', encoding = 'utf_8') as file:
        for sent in bigram_sentences:
            trigram_sentence = u' '.join(trigram_phraser[sent])
            file.write(trigram_sentence + '\n')
    
    return train_test_hold

def stop_word_update(stoplist = None, stop_add = [], stop_remove = []):
    """
    Updates stopword list:
        stoplist: set of words to change
        add: set or list of words to add to stoplist
        remove: set or list of words to remove from stoplist
    
    returns set "stoplist"
    """
    
    if stoplist == None:
        from spacy.lang.en.stop_words import STOP_WORDS
        stoplist = STOP_WORDS
        
    stoplist = set(stoplist)            
    stoplist.difference_update(set(list(stop_remove)))
    stoplist.update(set(list(stop_add)))
            
    return stoplist
    
    
def trigram_transform(data,
                      stoplist,
                      stop_remove = [],
                      parsed_col = 'parsed',
                      trigram_outfile = 'trigram_transformed.txt',
                      trigram_col = 'trigram_review'):
    """
    Takes parsed data in (dataframe.parsed) and lemmatizes the unigram tokens.
    These tokens are passed through the bigram and trigram models to combine
    two- and three-word phrases into single tokens. From here, the tokens are
    sorted to remove stopwords and then written to the outfile, line by line.
        dataframe: pandas dataframe where dataframe.parsed is the 
        bigram_model: bigram model from phrase_model() output
        trigram_model: trigram model from phrase_model() output
        stoplist: list of stopwords (perhaps from stop_list_update() output)
        trigram_outfile: text document output destination
        
    *writes output file of lemmatized, stopword-removed, trigram_updated text:
        trigram_transformed.txt
    *writes trigram_review for each document to its corresponding location in
        the dataframe under dataframe['trigram_review']

    returns nothing
    """
    from gensim.models import Phrases
    
    bigram_model = Phrases.load('phrase_model_bigram_save.txt')
    trigram_model = Phrases.load('phrase_model_trigram_save.txt')
    
    with open(trigram_outfile, 'w', encoding = 'utf_8') as file:
        item_list = list(data.index)
        trigram_list = list()
        for doc in item_list:
            unigram_review = [token.lemma_ for token 
                              in data.loc[doc, parsed_col]
                              if not thing_to_remove(token)]
            bigram_review = bigram_model[unigram_review]
            trigram_review = trigram_model[bigram_review]
            trigram_review = [term for term in trigram_review
                              if (term not in stoplist  # no stoplist words
                              and not (len(term) < 2  # no short words
                                       and term not in set(stop_remove)
                                           # unless they're in stop_remove
                                   ))]
            trigram_list.append(trigram_review)
            trigram_review_str = u' '.join(trigram_review)
            file.write(trigram_review_str + '\n')
        data.loc[:, trigram_col] = trigram_list    
        

def make_trigram(data, 
                 parsed_col = 'parsed',
                 stoplist = None,
                 stop_add = [],
                 stop_remove = [],
                 trigram_outfile = 'trigram_transformed.txt',
                 trigram_col = 'trigram_review'):
    """
    Performs phrase modeling with the end goal of creating a text doc of 
    the original documents transformed: lemmatized, stopword-removed, and
    trigram-updated. This is used in the next step.
        dataframe: pandas dataframe where data is stored
        text_column: quoted column name where the raw text is stored in dataframe
        stoplist: base list of stopwords to be removed, e.g., spacy.lang.en.stop_words
        stop_add: words to add to stoplist
        stop_remove: words to remove from stoplist
        trigram_outfile: custom text document where the trigram-transform text should be saved.
            
    returns nothing
    """    
    
    # create bigram and trigram phrase models:
    train_test_hold = phrase_model(data = data,
                                   parsed_col = parsed_col)
    
    # update that stoplist! Defaults to spacy.en.STOP_WORDS with no changes
    stoplist_out = stop_word_update(stoplist = stoplist, 
                                stop_add = stop_add, 
                                stop_remove = stop_remove)
    
    # create text file of updated document(s):
    trigram_transform(data = data,
                      stoplist = stoplist_out,
                      stop_remove = stop_remove,
                      parsed_col = parsed_col,
                      trigram_outfile = trigram_outfile,
                      trigram_col = trigram_col)
    
    return train_test_hold
    

    
def trigram_bow_generator(trigram_transformed_filepath, 
                          trigram_dictionary,
                          train_test_hold_lens,
                          train_test_hold_iter):
    """
    Creates generator for each line (doc) in the trigram-transformed text 
    document, using the trigram dictionary specified. Used in trigram_dict_bow()
    
        trigram_transformed_filepath: (variable)
            trigram-transformed doc from phrase_modeling()
            
        trigram_dictionary: (variable)
            actual trigram dictionary from lda_by_num()
            
    returns nothing
    """  
    
    from gensim.models.word2vec import LineSentence

    counter = 0
    train = train_test_hold_lens['train']
    test = train + train_test_hold_lens['test']
    
    if train_test_hold_iter == 'train':
        for doc in LineSentence(trigram_transformed_filepath):
            if counter < train:
                yield trigram_dictionary.doc2bow(doc)
            counter += 1
    if train_test_hold_iter == 'test':
        for doc in LineSentence(trigram_transformed_filepath):
            if train <= counter < test:
                yield trigram_dictionary.doc2bow(doc)
            counter += 1        
    if train_test_hold_iter == 'hold':
        for doc in LineSentence(trigram_transformed_filepath):
            if test <= counter:
                yield trigram_dictionary.doc2bow(doc)
            counter += 1    
    



def trigram_dict_bow(extremes,
                     train_test_hold,
                     keep_tokens = [],
                     trigram_transformed_filepath = 'trigram_transformed.txt',
                     trigram_dictionary_filepath = 'trigram_dict.dict',
                     trigram_bow_filepath = 'trigram_bow_corpus.mm'):
    """
    Prepares and saves dictionary and bag-of-words corpus from 
    trigrammed/normalized text. This step is separated from the LDA creation
    for looping purposes.
    
    extremes: (list of numbers) 
        low/high values for dictionary extremes
            [0]: (int) 
                minimum documents token must appear in to be counted
            [1]: (float between 0 and 1) 
                maximum portion of documents token can appear in to be counted
            e.g., extremes = [10, 0.4]
            
    keep_tokens: (list of strings) 
        must-keep tokens, irrespective of extremes
            e.g., keep_tokenx = ['no', 'not']
       
    trigram_transformed_filepath: (string) 
        trigram-transformed doc from phrase_modeling()
    
    trigram_dictionary_filepath: (string)
        where the dictionary should be saved
        
    trigram_bow_filepath: (string)
        where the bag of words corpus should be saved
    
    returns list of strings:
        trigram_dictionary_filepath,
        trigram_bow_filepath
        
    """
    from gensim.models.word2vec import LineSentence
    from gensim.corpora import Dictionary, MmCorpus
    
    trigram_reviews = LineSentence(trigram_transformed_filepath)
        
    trigram_dictionary = Dictionary(trigram_reviews)
    trigram_dictionary.filter_extremes(no_below = extremes[0], 
                                       no_above = extremes[1],
                                       keep_tokens = keep_tokens)
    trigram_dictionary.compactify()
    trigram_dictionary.save(trigram_dictionary_filepath)
    
    train_test_hold_lens = {k:len(v) for k, v in train_test_hold.items()}
    
    trigram_bow_filepaths = list()
    for k, v in train_test_hold_lens.items():
        MmCorpus.serialize(k + '_' + trigram_bow_filepath,
                           trigram_bow_generator(trigram_transformed_filepath,
                                                 trigram_dictionary,
                                                 train_test_hold_lens,
                                                 k))
        trigram_bow_filepaths.append(k + '_' + trigram_bow_filepath)
    
    return list([trigram_dictionary_filepath, trigram_bow_filepaths])


####################

def lda_k_finder(k,
                 data,
                 train_test_hold,
                 trigram_colname = 'trigram_review',
                 trigram_dictionary_filepath = 'trigram_dict.dict',
                 trigram_bow_filepath_stem = 'trigram_bow_corpus.mm',
                 lda_model_filepath_stem = 'lda_model',
                 iterations = 50,
                 chunksize = 1500,
                 passes = 20,
                 workers = None):
    """
    """
    from gensim.corpora import MmCorpus, Dictionary  
    from gensim.models import ldamulticore, coherencemodel

    dictionary = Dictionary.load('trigram_dict.dict')

    # corpuses
    holdout_corpus = MmCorpus('hold_trigram_bow_corpus.mm')
    test_corpus = MmCorpus('test_trigram_bow_corpus.mm')
    training_corpus = MmCorpus('train_trigram_bow_corpus.mm')
    
    # texts
    training_texts = data.loc[train_test_hold['train'], trigram_colname].tolist()

    # make a model
    model = ldamulticore.LdaMulticore(corpus = training_corpus,
                                      num_topics = k,
                                      id2word = dictionary,
                                      passes = passes,
                                      chunksize = chunksize,
                                      iterations = iterations,
                                      alpha = 'asymmetric',
                                      workers = workers,
                                      eval_every = None)

    # test a model
    pl_test = model.bound(test_corpus)
    pl_holdout = model.bound(holdout_corpus)
    ch_umass = coherencemodel.CoherenceModel(model = model,
                                             corpus = training_corpus,
                                             coherence = 'u_mass').get_coherence()
    ch_cv = coherencemodel.CoherenceModel(model = model,
                                          corpus = training_corpus,
                                          dictionary = dictionary,
                                          coherence = 'c_v',
                                          texts = training_texts).get_coherence()
    
    # save a model
    filename = "k_" + str(k) + "_" + lda_model_filepath_stem
    model.save(filename)
    
    model_info = {"model_stem" : filename,
                  "perplexity_test" : pl_test,
                  "perplexity_holdout" : pl_holdout,
                  "coherence_umass" : ch_umass,
                  "coherence_cv" : ch_cv}
    
    # return a model
    return model_info





####################
"""
# So this works if you want to visualize it, but when you try to do this from
# the server and just get results, it overloads the memory and somehow
# tells you that you need a billion gigs of RAM to complete. Work on this
# so that you can do this one-by-one if you just want to see how a model looks
# but if you want to search for k in a range, use the newer option.
def lda_k_finder(k_list,
                 data,
                 train_test_hold,
                 trigram_colname = 'trigram_review',
                 trigram_dictionary_filepath = 'trigram_dict.dict',
                 trigram_bow_filepath_stem = 'trigram_bow_corpus.mm',
                 lda_model_filepath_stem = 'lda_model',
                 iterations = 50,
                 chunksize = 1500,
                 passes = 20):
    
    from gensim.models.callbacks import CoherenceMetric, DiffMetric, PerplexityMetric, ConvergenceMetric
    from gensim.corpora import MmCorpus, Dictionary
    from gensim.models import ldamodel

    dictionary = Dictionary.load('trigram_dict.dict')

    # corpuses
    holdout_corpus = MmCorpus('hold_trigram_bow_corpus.mm')
    test_corpus = MmCorpus('test_trigram_bow_corpus.mm')
    training_corpus = MmCorpus('train_trigram_bow_corpus.mm')
    
    model_list = list()

    # texts
    training_texts = data.loc[train_test_hold['train'], trigram_colname].tolist()

    for k in k_list:
        print("Current k: ", k)                    
        # define perplexity callback for hold_out and test corpus
        pl_holdout = PerplexityMetric(corpus=holdout_corpus, 
                                      logger="visdom", 
                                      title="Perplexity (hold_out): k=" + str(k))
        pl_test = PerplexityMetric(corpus=test_corpus, 
                                   logger="visdom", 
                                   title="Perplexity (test): k=" + str(k))
        
        # define other remaining metrics available
        ch_umass = CoherenceMetric(corpus=training_corpus, 
                                   coherence="u_mass", 
                                   logger="visdom", 
                                   title="Coherence (u_mass): k=" + str(k))
        ch_cv = CoherenceMetric(corpus=training_corpus, 
                                texts=training_texts, 
                                coherence='c_v', 
                                logger="visdom", 
                                title="Coherence (c_v): k=" + str(k))
        diff_kl = DiffMetric(distance="kullback_leibler", 
                             logger="visdom", 
                             title="Diff (kullback_leibler): k=" + str(k))
        convergence_kl = ConvergenceMetric(distance="jaccard", 
                                           logger="visdom", 
                                           title="Convergence (jaccard): k=" + str(k))
        
        callbacks = [pl_holdout, pl_test, ch_cv, ch_umass, convergence_kl, diff_kl]
        
        # training LDA model
        model = ldamodel.LdaModel(corpus=training_corpus, 
                                  id2word=dictionary, 
                                  num_topics=k, 
                                  passes=passes, 
                                  chunksize=chunksize, 
                                  iterations=iterations, alpha='auto', callbacks=callbacks)
        filename = "k_" + str(k) + "_" + lda_model_filepath_stem
        model.save(filename)
        
        callback_values = dict()
        for callback in model.callbacks:
            if "Coherence" in callback.title:
                callback_values[callback.title[0:-(4+len(str(k)))]] = callback.get_value(model=model)
            else:
                callback_values[callback.title[0:-(4+len(str(k)))]] = callback.get_value()
            
        model_list.append([filename, model.metrics, callback_values])
        
        print("u_mass coherence: ", CoherenceMetric(corpus=training_corpus, coherence="u_mass").get_value(model=model))
        print("c_v coherence: ", CoherenceMetric(corpus=training_corpus, texts = training_texts, coherence="c_v").get_value(model=model))

    return model_list
"""

"""
This isn't needed any more. It's superceded by the above function.
def lda_maker(k_list,
              data,
              trigram_dictionary_filepath = 'trigram_dict.dict',
              trigram_bow_filepath = 'trigram_bow_corpus.mm',
              lda_model_filepath_stem = 'lda_model.txt',
              cpu_cores_minus_one = None,
              iterations = 50):

    Iterates through list of k topics, creating LDA models for each, using
    the lda_by_num() function, whose values can be passed in.
    
        k_list: (list of integers)
            number of k topics desired
                e.g., [20, 50, 200]
        
        trigram_dictionary_filepath: (string)
            where the dictionary should be saved
        
        trigram_bow_filepath: (string)
            where the bag of words corpus should be saved
        
        cpu_cores_minus_one: (integer)
            used for multithreading, defaulting to single-core (None)
            
        iterations: (integer)
            
    returns list of strings of lda file save locations

    from gensim.corpora import Dictionary, MmCorpus
    from gensim.models.ldamulticore import LdaMulticore
    from gensim.models.coherencemodel import CoherenceModel
    import warnings
    
    # load up dictionary and bag-of-word corpuses from file
    trigram_dictionary = Dictionary.load(trigram_dictionary_filepath)
    trigram_bow_corpus = MmCorpus(trigram_bow_filepath)
    
    lda_filepath_list = list()
    lda_coherence_list = list()
    
    # iterate through k_list and create unique LDA models for each k    
    for k in k_list:
        lda_model_filepath = 'k_' + str(k) + '_' + lda_model_filepath_stem
        with warnings.catch_warnings():
            lda = LdaMulticore(trigram_bow_corpus,
                               num_topics = k,
                               id2word = trigram_dictionary,
                               workers = cpu_cores_minus_one,
                               iterations = iterations,
                               passes = 20,
                               chunksize = 2000,
                               eval_every = 10)
            
            lda_coherence = CoherenceModel(model = lda,
                                           texts = data.trigram_review,
                                           corpus = trigram_bow_corpus,
                                           dictionary = trigram_dictionary,
                                           coherence = 'c_v',
                                           topn = k)
            
            lda_coherence_list.append([k, lda_coherence.get_coherence()])
            
            lda.save(lda_model_filepath)
            
        lda_filepath_list.append(lda_model_filepath)
    
    # return list of strings of LDA filepaths
    return list([lda_filepath_list, lda_coherence_list])

"""

def lda_assign(trigram_review,
               lda_model,
               trigram_dictionary):
    """
    Assign each individual tweet's 'trigram_review' to a specific LDA model's
    topic.
    """
    
    review_bow = trigram_dictionary.doc2bow(trigram_review)
    review_lda = lda_model[review_bow]
    review_lda = sorted(review_lda, key = lambda x: -x[1])
    
    return review_lda

def lda_assign_pandas(data,
                      topic_num,
                      lda_model_filepath_stem = 'lda_model',
                      trigram_review_col = 'trigram_review',
                      trigram_dictionary_filepath = 'trigram_dict.dict'):

    """
    """
    from gensim.corpora import Dictionary
    from gensim.models import LdaMulticore
    
    # load model
    lda_model = LdaMulticore.load("k_" + str(topic_num) + "_" + lda_model_filepath_stem)
    
    # load dictionary
    trigram_dictionary = Dictionary.load(trigram_dictionary_filepath)
    
    # assign to topic
    data.loc[:, 'k_' + str(topic_num) + '_topic_dist'] = data.loc[:, 
                 trigram_review_col].apply(
                     lambda x: 
                         lda_assign(trigram_review = x,
                                    lda_model = lda_model,
                                    trigram_dictionary = trigram_dictionary))
    data.loc[:, 'k_' + str(topic_num) + '_top_topic'] = data.loc[:,
                 'k_' + str(topic_num) + '_topic_dist'].apply(
                     lambda x:
                         x[0][0])
    
def lda_assign_pandas_list(data,
                           k_list,
                           lda_model_filepath_stem = 'lda_model',
                           trigram_review_col = 'trigram_review',
                           trigram_dictionary_filepath = 'trigram_dict.dict'):
    """
    """
    
    for k in k_list:
        lda_assign_pandas(data = data,
                          trigram_review_col = trigram_review_col,
                          topic_num = str(k),
                          lda_model_filepath_stem = lda_model_filepath_stem,
                          trigram_dictionary_filepath = trigram_dictionary_filepath)

def ldavis_to_html(k_list,
                   lda_model_filepath_stem = 'lda_model',
                   html_out_filepath_stem = 'ldavis.html',
                   trigram_dictionary_filepath = 'trigram_dict.dict',
                   trigram_bow_filepath = 'trigram_bow_corpus.mm'):
   
    """
    Create HTML output for each in a list of LDA models
    """    
    from gensim.corpora import Dictionary, MmCorpus
    from gensim.models import LdaMulticore
    from itertools import chain

    trigram_dictionary = Dictionary.load(trigram_dictionary_filepath)      
    trigram_bow_corpus = chain(chain(MmCorpus('train_' + trigram_bow_filepath),
                               MmCorpus('test_' + trigram_bow_filepath)),
                               MmCorpus('hold_' + trigram_bow_filepath))
    MmCorpus.serialize(trigram_bow_filepath, trigram_bow_corpus)
    trigram_bow_corpus = MmCorpus(trigram_bow_filepath)
    
    import pyLDAvis
    import pyLDAvis.gensim
    
    html_filepath_list = list()
    
    for k in k_list:
        lda_model = LdaMulticore.load('k_' + str(k) + '_' + lda_model_filepath_stem)
        lda_prepared = pyLDAvis.gensim.prepare(topic_model = lda_model, 
                                               corpus = trigram_bow_corpus, 
                                               dictionary = trigram_dictionary,
                                               sort_topics = False)
        
        html_filename = 'k_' + str(k) + '_' + html_out_filepath_stem
        pyLDAvis.save_html(data = lda_prepared, fileobj = html_filename)
        html_filepath_list.append(html_filename)
        
    return html_filepath_list

def top_tweets(data,
               k,
               top_n = 10,
               tweet_text = 'tweet_text',
               tweet_id = 'tweet_id'):
    colname = 'k_' + str(k) + '_topic_dist'
    all_scores = list()
    for topic in range(0, int(k)):
        all_scores.append(list())
    
    data.apply(lambda x: topic_to_dict(topic_dist = x.loc[colname],
                                       all_scores = all_scores,
                                       tweet_id = x.loc[tweet_id]),
               axis = 1)

    final_scores = list()
    for topic in all_scores:
        topic = sorted(topic, key = lambda x: -x[1])
        final_scores.append(topic[0:top_n])
    
    top_tweets = list()
    for topic in final_scores:
        
        top_tweets.append([[data.loc[i[0], 'tweet_id'], data.loc[i[0], 'tweet_text']] for i in topic])
    
    return list([final_scores, top_tweets])
        
def topic_to_dict(topic_dist, 
                  all_scores,
                  tweet_id):
    
    # sort by topic num
    topic_sort = sorted(topic_dist, key = lambda x: x[0])
    
    for topic in topic_sort:
        all_scores[int(topic[0])].append([tweet_id, topic[1]])

    
#    data.loc[:, colname].apply(lambda x:
 #       )

def top_words(lda_model_filepath):
    import re
    from gensim.models.ldamulticore import LdaMulticore
    
    lda_model = LdaMulticore.load(lda_model_filepath)
    
    for topic in lda_model.show_topics():
        print("Topic " + str(topic[0]) + ':')
        words = re.findall(r'"([^"]*)"', topic[1])
        pcts = re.findall(r'0.\d+', topic[1])
        for pair in range(0, 10):
            print(words[pair] + ': ' + pcts[pair])
        print('')
            

## add perplexity model



