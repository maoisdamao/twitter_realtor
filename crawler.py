__author__ = 'xinyu'


import tweepy
import time
from pymongo import MongoClient

'''
    Variables that contains the user credentials to access Twitter API
    Use your own token & key to replace
'''
access_token = "752969209-kYFzZn4VuEA1VvMyB9ShmMdWDKrp9bxXGGSAAPzQ"
access_token_secret = "YYFcHDUpfdSrJGynkaPQLoJh6MMmWOPiqdVxcJecCzHhH"
consumer_key = "vHmZy6gjoxrOwXYdDpGhngwJd"
consumer_secret = "Wxc6tPaPub2oTLwA5yWd5tJASIKTYzQFaALOwETojPfEdRCM0i"

auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)

#Connect to mongodb
client = MongoClient('localhost', 27017)
db = client.trealtor
coll = db.user
tweets_coll = db.tweet

def insert_user(name, profile_image_url, id_str, description):
    coll.insert_one(
        {
            "username": name,
            "avatar": profile_image_url,
            "profile": description,
            "_id": id_str
        }
    )
    return

def insert_tweet(user, id_str, date, text):
    tweets_coll.insert_one(
        {
            "user": user,
            "date": date,
            "text": text,
            "_id": id_str
        }
    )


def base_users():
    #set 'a' used to hash to avoid duplicated twitter users
    a = set([])
    #download 1,000 user returned by twitter user search
    '''
    result = coll.delete_many({})
    print "delete from trealtor.user", result.deleted_count
    '''
    page_i = 1
    while (page_i < 51):
        users = api.search_users("realtor", 20, page_i)
        page_i += 1
        for who in users:
            if who.id_str not in a:
                a.add(who.id_str)
                insert_user(who.name, who.profile_image_url, who.id_str, who.description)
    for doc in coll.find():
        print(doc)
    print(coll.count())


def more_users():
    f = open("user-crawled.txt", 'wb')

    a = set([])
    for doc in coll.find(projection={'_id': 1}):
        a.add(doc['_id'])
    b = a.copy()
    count = 1
    count_follower_id = 1
    #crawlling other users according to users in db
    start_time = time.clock()
    for user_id in a:
        try:
            if count_follower_id < 15:
                followers = api.followers_ids(user_id)
                count_follower_id += 1
                f.write(user_id)
                f.write('\n')
                for j in range(0, len(followers), 100):
                    if count < 180:
                        infolist = api.lookup_users(followers[j:j+100])
                        count += 1
                        for follower in infolist:
                            if "realtor" in follower.description:
                                if follower.id_str not in b:
                                    print follower.name, "  ", follower.id_str
                                    print follower.description
                                    b.add(follower.id_str)
                                    insert_user(follower.name, follower.profile_image_url, follower.id_str, follower.description)
                    else:
                        count_follower_id = 1
                        count = 1
                        print "user list length:", len(b)
                        print "limit approaching, waiting for 900 seconds..."
                        time.sleep(900)
                        print "keep going..."
            else:
                count_follower_id = 1
                count = 1
                print "user list length:", len(b)
                print "limit approaching, waiting for 900 seconds..."
                time.sleep(900)
                print "keep going..."
        except tweepy.TweepError, error:
            print type(error)
            if str(error) == "Not authorized.":
                print "protected user +1"
                continue
            else:
                print error

    end_time = time.clock()
    print "total time:", end_time - start_time
    print "user list length:", len(b)
    print "count_follower_ids:", count_follower_id
    print "count_lookup_users:", count
    f.close()
    return

def get_timeline():
    count_timeline = 0
    #result = tweets_coll.delete_many({})
    print "delete from trealtor.user", result.deleted_count
    for doc in coll.find(projection={'_id': 1}):
        try:
            if count_timeline < 150:
                count_timeline += 1
                timeline = api.user_timeline(doc['_id'])
                for status in timeline:
                    insert_tweet(doc['_id'],status.id_str,status.created_at,status.text)
            else:
                count_timeline = 1
                print "limit approaching, waiting for 900 seconds..."
                time.sleep(900)
                print "amount of tweets now...", tweets_coll.count()
        except tweepy.TweepError, error:
            if str(error) == "Not authorized.":
                print "protected user +1", error
                continue
            else:
                print error

    return


if __name__ == '__main__':
    #base_users()
    more_users()
    #get_timeline()
