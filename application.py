from flask import Flask, render_template, request
from flask_socketio import SocketIO, send, emit
import json
import requests

application = Flask(__name__)

socketio = SocketIO(application)

socketConnected = False


@application.route('/', methods=['GET', 'POST'])
def hello_world():
    global socketConnected

    if request.method == 'POST':

        # AWS sends JSON with text/plain
        try:
            js = json.loads(request.data)
        except:
            pass

        hdr = request.headers.get('X-Amz-Sns-Message-Type')

        # Subscribe to the SNS topic
        if hdr == 'SubscriptionConfirmation' and 'SubscribeURL' in js:
            r = requests.get(js['SubscribeURL'])

        if hdr == 'Notification':
            tweet = js['Message']

            # Send this tweet to elastic search
            # postURL = 'http://localhost:9201/tweetmap/tweet'
            postURL = 'http://search-tweetmap-hozfp5wv6wvf7ajfcenijhcmmu.us-west-2.es.amazonaws.com/tweetmap/tweet'
            # r = requests.post(postURL, json = tweet)

            # Send this tweet to front-end
            if socketConnected:
                socketio.emit('realTimeResponse', tweet)

    return render_template('TwitterMap.html')


@application.route('/search')
def handle_search():
    return render_template('search.html')


@socketio.on('realTime')
def handle_realtime_event(message):
    global socketConnected
    socketConnected = True

    # Fetch tweets in elastic search
    # queryURL = 'http://localhost:9201/tweetmap/_search?q=*:*&size=1000'
    queryURL = 'http://search-tweetmap-hozfp5wv6wvf7ajfcenijhcmmu.us-west-2.es.amazonaws.com/tweetmap/_search?q=*:*&size=10000'
    response = requests.get(queryURL)
    results = json.loads(response.text)

    tweets = []
    for result in results['hits']['hits']:
        tweet = {'sentiment': result['_source']['sentiment'], 'longitude': result['_source']['longitude'],
                 'latitude': result['_source']['latitude']}
        tweets.append(tweet)

    send(json.dumps(tweets))

@socketio.on('message')
def handle_message(message):
    if message == 'Init':
        # Run local elastic search
        queryURL = 'http://search-tweetmap-hozfp5wv6wvf7ajfcenijhcmmu.us-west-2.es.amazonaws.com/tweetmap/_search?q=*:*&size=10000'
        # queryURL = 'http://localhost:9201/tweetmap/_search?q=*:*&size=1000'
        response = requests.get(queryURL)
        results = json.loads(response.text)

        print("INIT MAP")
    else:

        queryKeyWord = message.replace(' ', '%20')
        queryURL = 'http://search-tweetmap-hozfp5wv6wvf7ajfcenijhcmmu.us-west-2.es.amazonaws.com/tweetmap/_search?q=' + queryKeyWord + '&size=10000'
        # queryURL = 'http://localhost:9201/tweetmap/_search?q=' + queryKeyWord + '&size=1000'
        response = requests.get(queryURL)
        results = json.loads(response.text)
        print("SEARCH" + str(message))

    # Find locations of each tweet
    tweets = []
    for result in results['hits']['hits']:
        tweet = {'sentiment': result['_source']['sentiment'], 'longitude': result['_source']['longitude'],
                 'latitude': result['_source']['latitude']}
        tweets.append(tweet)

    send(json.dumps(tweets))


if __name__ == '__main__':
    socketio.run(application, host='0.0.0.0')
