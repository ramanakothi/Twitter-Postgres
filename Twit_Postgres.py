#!/usr/bin/env python

import psycopg2
import tweepy 
import json
from keys import *



def autorize_twitter_api():
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    return auth



def create_tweets_table(term_to_search):
    #Connect to Twitter Database created in Postgres
    conn_twitter = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)

    #Create a cursor to perform database operations
    cursor_twitter = conn_twitter.cursor()

    #with the cursor now, create two tables, users twitter and the corresponding table according to the selected topic
    cursor_twitter.execute("CREATE TABLE IF NOT EXISTS twitter_users (user_id VARCHAR PRIMARY KEY, user_name VARCHAR);")
    
    query_create = "CREATE TABLE IF NOT EXISTS %s (id SERIAL, created_at timestamp, tweet text NOT NULL, user_id VARCHAR,                     retweetcount int, PRIMARY KEY(id), FOREIGN KEY(user_id) REFERENCES twitter_users(user_id));" %("tweets_"+term_to_search)
    
    cursor_twitter.execute(query_create)
    
    #Commit changes
    conn_twitter.commit()
    
    #Close cursor and the connection
    cursor_twitter.close()
    conn_twitter.close()
    return

def store_tweets_in_table(term_to_search, user_id, created_at, tweet, user_name, retweetcount):
    #Connect to Twitter Database created in Postgres
    conn_twitter = psycopg2.connect(dbname=dbname, user=user, password=password, host=host, port=port)
    
    #Create a cursor to perform database operations
    cursor_twitter = conn_twitter.cursor()

    #with the cursor now, insert tweet into table
    cursor_twitter.execute("INSERT INTO twitter_users (user_id, user_name) VALUES (%s, %s) ON CONFLICT(user_id) DO NOTHING;", (user_id, user_name))
    
    cursor_twitter.execute("INSERT INTO %s (created_at, tweet, user_id, retweetcount) VALUES (%%s, %%s, %%s, %%s);" %('tweets_'+term_to_search), 
                           (created_at, tweet, user_id, retweetcount))
    
    #Commit changes
    conn_twitter.commit()
    
    #Close cursor and the connection
    cursor_twitter.close()
    conn_twitter.close()
    return

class MyStreamListener(tweepy.StreamListener):
    def on_data(self, raw_data):

        try:
            global term_to_search
            
            data = json.loads(raw_data)            
            
            #Obtain all the variables to store in each column
            user_id = data['user']['id_str']
            created_at = data['created_at']
            tweet = data['text']
            user_name = data['user']['screen_name']
            retweetcount = data['retweet_count']
            
            #Store them in the corresponding table in the database
            store_tweets_in_table(term_to_search, user_id, created_at, tweet, user_name, retweetcount)
            
        except Exception as e:
            #e = sys.exc_info()[1]
            #print(e.args[0])
            pass
    
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_error disconnects the stream
            return False


if __name__ == "__main__": 
    #Creates the table for storing the tweets
    term_to_search = "coronavirus"
    create_tweets_table(term_to_search)
    
    #Connect to the streaming twitter API
    api = tweepy.API(wait_on_rate_limit_notify=True)
    
    #Stream the tweets
    streamer = tweepy.Stream(auth=autorize_twitter_api(), listener=MyStreamListener(api=api))
    streamer.filter(languages=["en"], track=[term_to_search])
