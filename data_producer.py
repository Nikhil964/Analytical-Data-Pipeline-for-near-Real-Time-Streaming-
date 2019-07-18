import tweepy
from tweepy import Stream
from tweepy.streaming import StreamListener
import time
import numpy as np
import pandas as pd
from google.cloud import language
import json
import boto3
import os
import uuid
import credentials
import sys
from google.cloud import language
from google.cloud.language import enums
from google.cloud.language import types

#Twitter Keys
consumer_key = 'Ws7Nfe9kXXXXXXXXXXX'
consumer_secret = 'z4u5fbQP0XQ6AeXXXXXXXXXXXXXXXXXXXXXX'
access_token = '393475636-SLXXXXXXXXXXXXXXXXXXXX'
access_token_secret = 'S8F67EDea21XXXXXXXXXXXXXX'

#aws keys
aws_key_id = 'AKIAJXXXXXXXXXXXXXXX'
aws_key = 'prU5Z1JNaOXXXXXXXXXXXXXXXXXXXXXXXXXXXXX'


def sentimentAnalysis(text):
    client = language.LanguageServiceClient.from_service_account_json("/home/ubuntu/gcloud.json")
    
    document = types.Document(
                              content=text,
                              type=enums.Document.Type.PLAIN_TEXT)
        
                              sent_analysis = client.analyze_sentiment(document)
                              
                              sentiment = sent_analysis.document_sentiment
                              
    return sentiment



class StreamListener(tweepy.StreamListener):
    def __init__(self, boto_client, search_list):
        super(tweepy.StreamListener, self).__init__()
        self.kinesis = boto_client
        self.search_list = search_list
    
    def on_status(self, status):
        print status.txt
    def on_data(self, data):
        try:
            all_data = json.loads(data)
            tw_data = {}
            status_sent_entity = {}
            retweet_status_sent_entity = {}
            print "Collecting Tweet"
            
            if 'lang' in all_data and (all_data['lang'] == "en"):
                tw_data['status_created_at']   = str(all_data["created_at"])
                tw_data['status_id']           = str(all_data["id"])
                tw_data['rt_status_id']                    = str(all_data['retweeted_status']['id'])
                tw_data['rt_status_retweet_count']         = str(all_data['retweeted_status']['retweet_count'])
                tw_data['rt_status_favorite_count']        = str(all_data['retweeted_status']['favorite_count'])
                tw_data['rt_status_text']                  = str(all_data['retweeted_status']['text'].encode('ascii', 'ignore').decode('ascii'))
                tw_data['rt_status_retweeted']             = all_data['retweeted_status']['retweeted']
                tw_data['rt_status_created_at']            = str(all_data['retweeted_status']['created_at'])
                tw_data['rt_status_favorited']             = all_data['retweeted_status']['favorited']
                tw_data['rt_status_user_friends_count']    = str(all_data['retweeted_status']['user']['friends_count'])
                tw_data['rt_status_user_location']         = str(all_data['retweeted_status']['user']['location'])
                tw_data['rt_status_user_id']               = str(all_data['retweeted_status']['user']['id'])
                tw_data['rt_status_user_favourites_count'] = str(all_data['retweeted_status']['user']['favourites_count'])
                tw_data['rt_status_user_screen_name']      = str(all_data['retweeted_status']['user']['screen_name'])
                tw_data['rt_status_user_profile_image']    = all_data['retweeted_status']['user']['profile_image_url']
                tw_data['rt_status_user_name']             = all_data['retweeted_status']['user']['name']
                tw_data['rt_status_user_followers_count']  = str(all_data['retweeted_status']['user']['followers_count'])
                tw_data['rt_status_user_listed_count']     = str(all_data['retweeted_status']['user']['listed_count'])
                tw_data['rt_status_user_statuses_count']   = str(all_data['retweeted_status']['user']['statuses_count'])
                tw_data['searched_names'] =  self.search_list[0]
                try:
                    rt_sentiment                          = sentimentAnalysis(tw_data['rt_status_text'])
                    tw_data['rt_status_sentScore']              = rt_sentiment.score
                except:
                    tw_data['rt_status_sentScore']              = 'None'
                
                try:
                    self.kinesis.put_record(DeliveryStreamName=self.search_list[0],
                                            Record={'Data': json.dumps(tw_data) + '\n'})
                    pass
                
                except Exception, e:
                    print"Failed Kinesis Put Record {}".format(str(e))
    
    except BaseException, e:
        print "failed on data ", str(e)
        time.sleep(5)
    
    def on_error(self, status):
        if status_code == 420:
            return False

def create_stream(client, stream_name):
    return client.create_delivery_stream(
                                         DeliveryStreamName=stream_name ,
                                         S3DestinationConfiguration={
                                         'RoleARN': 'arn:aws:iam::XXXXXXXXXXXXXXXX:role/firehouse',
                                         'BucketARN': 'arn:aws:s3:::nikhil1234567890123',
                                         'Prefix': stream_name+'/'
                                         }
                                         )

def main(search_name):
    stream_name = search_name[0]
    client = boto3.client('firehose', region_name='us-east-1',
                          aws_access_key_id=aws_key_id,
                          aws_secret_access_key=aws_key
                          )
                          try:
                              create_stream(client,stream_name)
                              print 'Creating Kinesis stream... Please wait...'
                                  time.sleep(60)
                              except:
                                  pass

stream_status = client.describe_delivery_stream(DeliveryStreamName=stream_name)
if stream_status['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
    print "\n ==== KINESES ONLINE ===="
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    searched_list = search_name
    
    streamListener = StreamListener(client, searched_list)
    stream = tweepy.Stream(auth=api.auth, listener=streamListener)
    
    while True:
        try:
            stream.filter(track=searched_list)
        except:
            pass

stream_status = client.describe_delivery_stream(DeliveryStreamName=stream_name)
if stream_status['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
    print "\n ==== KINESES ONLINE ===="
    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)
    api = tweepy.API(auth)
    
    searched_list = search_name
    
    streamListener = StreamListener(client, searched_list)
    stream = tweepy.Stream(auth=api.auth, listener=streamListener)
    
    while True:
        try:
            stream.filter(track=searched_list)
        except:
            time.sleep(5)
            continue

if __name__ == '__main__':
    main(sys.argv[1:])


