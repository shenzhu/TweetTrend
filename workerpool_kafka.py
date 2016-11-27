from kafka import KafkaConsumer
import json
import boto3
from watson_developer_cloud import AlchemyLanguageV1
from threading import Thread


class Worker(Thread):
    def __init__(self, message):
        Thread.__init__(self)
        self.message = message


    def run(self):
        message = self.message
        tweet = {'text': message.value['text'], 'user': message.value['user'],
             'time': message.value['time'], 'longitude': message.value['location'][0],
             'latitude': message.value['location'][1]}

        try:
            response = alchemy_language.sentiment(text = tweet['text'])
            tweet['sentiment'] = response['docSentiment']['type']
        except:
            pass

        # Publish result
        publishResponse = client.publish(TopicArn = topicArn, Message = json.dumps(tweet))

class WorkerPool(Thread):
    def __init__(self):
        Thread.__init__(self)

    def run(self):
        for message in consumer:
            worker = Worker(message)
            worker.start()


if __name__ == '__main__':
    # Get AWS SNS 
    client = boto3.client('sns')
    response = client.create_topic(Name = 'tweets')
    topicArn = response['TopicArn']

    # Subscribe
    subscribeResponse = client.subscribe(TopicArn = topicArn, Protocol = 'http', Endpoint = 'http://flask-env.pj5s5sxjmc.us-west-2.elasticbeanstalk.com/')

    # Get alchemy language 
    alchemy_language = AlchemyLanguageV1(api_key = '7a454d32972b85fd2599536ba9063e4d1530014b')

    consumer = KafkaConsumer('tweet', group_id = 'tweet-stream', bootstrap_servers = ['localhost:9092'], value_deserializer = lambda m: json.loads(m.decode('utf-8')))

    pool = WorkerPool()
    pool.start()



