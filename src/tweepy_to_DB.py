from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
import time
import pandas as pd
import json
import sqlalchemy
import datetime
from sqlalchemy import create_engine
import schedule
import numpy as np
import os
import pymysql
from apscheduler.schedulers.blocking import BlockingScheduler


class MyStreamListener(StreamListener):
    def __init__(self, time_limit=60):
        self.start_time = time.time()
        self.limit = time_limit
        self.tweets = []
        self.saveFile = open('current_tweets.json', 'a')
        super(MyStreamListener, self).__init__()

    def on_status(self, data):
        if (time.time() - self.start_time) < self.limit:
            tweet = data._json
            # print(tweet)
            self.saveFile.write(json.dumps(tweet) + '\n')
            return True
        else:
            self.saveFile.close()
            return False


def tweepy_to_df():
    auth = OAuthHandler(CONSUMER_KEY, CONSUMER_SECRET)
    auth.set_access_token(ACCESS_TOKEN, ACCESS_TOKEN_SECRET)
    myStream = Stream(auth, listener=MyStreamListener(time_limit=180))
    myStream.filter(track=outdoor_words, languages=["en"])
    
    # Initialize empty ist to store tweets
    tweets_data = []

    # Open connection to file
    with open('current_tweets.json', "r+") as tweets_file:
        # Read in tweets and store in list
        for line in tweets_file:
            line.replace('\n', '')
            if line.startswith('{"created_at"'):
                tweet = json.loads(line)
                if not type(tweet) == str:
                    tweets_data.append(tweet)
        tweets_file.truncate(0)
        tweets_file.close
                
    cols = ['coordinates', 'created_at', 'extended_tweet', 'place', 'possibly_sensitive', 'text', 'user']
    df = pd.DataFrame(tweets_data, columns=cols)
    df = df[~df['text'].str.startswith("RT @")]
    df = pd.concat([df.drop(['extended_tweet'], axis=1), df['extended_tweet'].apply(pd.Series)], axis=1)
    df = pd.concat([df.drop(['user'], axis=1), df['user'].apply(pd.Series)], axis=1)
    cols_to_keep = ['coordinates', 'created_at', 'place', 'possibly_sensitive', 'full_text', 'id_str', 'name',
                    'screen_name', 'location', 'description']
    if set(cols_to_keep).issubset(df.columns):
        df = df.loc[:, ~df.columns.duplicated()]
        df = df[cols_to_keep]
        df['coordinates'] = df['coordinates'].astype('str')
        return df
    else:
        return None


def sql_push():
    print(datetime.datetime.now(), " : Start time")
    current_df = tweepy_to_df()
    if current_df is not None:
        print("pushing")
        current_df.to_csv('testing.csv', index=False)
        testing_df = pd.read_csv('testing.csv')
        testing_df.to_sql('Stream', con=engine, index=False, if_exists='append')
    else:
        print("noting to see here")


#DB connection
engine = create_engine("mysql+pymysql://user:pass@167.99.172.188:3306/Tweets")

#Twitter Credentials to access Twitter API
ACCESS_TOKEN = ""
ACCESS_TOKEN_SECRET = ""
CONSUMER_KEY = ""
CONSUMER_SECRET = ""

outdoor_words = ['outdoors', 'camping', 'hiking', 'camping', 'wilderness', 'wildlife', 'o-zone', 'solar system',
                 'mountaineering', 'nationalpark', 'statepark', 'countypark', 'canyoneering', 'cycling', 'canoeing',
                 'caving', 'rafting', 'fishing', 'kayaking', 'rafting', 'rockclimbing', 'sailing', 'boating', 'skiing',
                 'snowboarding', 'snowshoeing', 'surfing', 'hunting', 'horsebackriding', 'trekking', 'mountainbiking',
                 'diving', 'iceclimbing', 'birdwatching', 'mushroomhunting', 'orienteering', 'parasailing',
                 'scubadiving', 'snorkeling', 'windsuring', 'waterskiing', 'flyfishing', 'parasailing', 'paragliding',
                 'picnicing', 'skydiving', 'backcountry', 'snorkeling', 'plogging', 'climatechange', 'globalwarming',
                 'deforestation', 'naturalresources', 'greenhouseeffect', 'renewableenergy', 'pollution', 'recycling',
                 'tsunami', 'volcano', 'rainforest', 'alternativeenergy', 'carbonfootprint', 'solarenergy',
                 'windenergy', 'oceanenergy', 'biofuel', 'mothernature', 'planetearth', 'landscape']

sched = BlockingScheduler()

# Schedule job_function to be called every two hours
sched.add_job(sql_push, 'interval', minutes=30)

sched.start()


