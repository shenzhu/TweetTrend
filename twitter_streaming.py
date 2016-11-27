# Important the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream 
#from elasticsearch import Elasticsearch
import requests
import json
import boto3


# Variables that contains the user credentials to access Twitter API
access_token = ""
access_token_secret = ""
consumer_key = ""
consumer_secret = ""


# This is a basic listener that just prints received tweets to stdout
class StdOutListener(StreamListener):

	def on_error(self, status):
		#print status
		pass


	def on_status(self, status):
		try:
			if status.coordinates:
				#print status
				tweet = {}
				tweet['user'] = status.user.screen_name
				tweet['text'] = status.text
				tweet['location'] = status.coordinates['coordinates']
				tweet['time'] = str(status.created_at)

				# Store twitter data into elasticsearch
				# es.index(index = 'twitter', doc_type = 'tweet', body = {
				# 	'user': tweet['user'],
				# 	'text': tweet['text'],
				# 	'location': tweet['location'],
				# 	'time': tweet['time']
				# 	})

				# print "EXECUTED"
				# postURL = 'http://search-tweetmap-hozfp5wv6wvf7ajfcenijhcmmu.us-west-2.es.amazonaws.com/twitter/tweet'
				# # postURL = 'http://search-tweetmap-5epb2bd6uqpvgt3zk6iu5ytit4.us-west-2.es.amazonaws.com/tweetmap/tweet'
				# r = requests.post(postURL, json = tweet)

				# Send message using AWS SQS
				response = queue.send_message(MessageBody = tweet['text'], MessageAttributes = {
					'Time': {
						'StringValue': tweet['time'],
						'DataType': 'String'
					},
					'User': {
						'StringValue': tweet['user'],
						'DataType': 'String'
					},
					'Longitude': {
						'StringValue': str(tweet['location'][0]),
						'DataType': 'String'
					},
					'Latitude': {
						'StringValue': str(tweet['location'][1]),
						'DataType': 'String'
					}
				})

				print response.get('MessageId')
				print tweet
		except Exception as e:
			print 'Error! {0}: {1}'.format(type(e), str(e))

if __name__ == '__main__':
	# Create elasticsearch instance
	#es = Elasticsearch([{'host': 'localhost', 'port': 9201}])

	# Use the already exiting queue tweet
	sqs = boto3.resource('sqs')
	queue = sqs.get_queue_by_name(QueueName = 'tweet')

	# This handles Twitter authetification and the connection to Twitter Streaming API
	l = StdOutListener()
	auth = OAuthHandler(consumer_key, consumer_secret)
	auth.set_access_token(access_token, access_token_secret)
	stream = Stream(auth, l)

	stream.filter(track = ['Trump', 'Hillary', 'Sanders', 'Facebook', 'LinkedIn',
                             'Amazon', 'Google', 'Uber', 'Columbia', 'New York'])