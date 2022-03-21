from datetime import time

from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
import sys
import socket
import requests
import requests_oauthlib
import json


# Include your Twitter account details
ACCESS_TOKEN = 'ffmbfmnfmknm'
ACCESS_SECRET = 'EWM0cMtuidM6fubj7QthnfklblkbmflbkfkmnlfmsfC8Ie1AgUXNK3W4ZPIdY3wX3KNmfnbnksjirTJTs'
CONSUMER_KEY = '1140720915677929473-'
CONSUMER_SECRET = '###############################################'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)


def get_tweets():
    url = 'https://api.twitter.com/2/tweets/search/recent?query=MALARIA&max_results=100&expansions=author_id&tweet.fields=id,created_at,author_id&user.fields=description'
    response = requests.get(url, auth=my_auth, stream=True)
    print(response)
    return response


def send_tweets_to_spark(http_resp, tcp_connection):
    for line in http_resp.iter_lines():
        full_tweet = json.loads(line)
        tweet_text = full_tweet['text']
        print("Tweet Text: " + tweet_text)
        print("------------------------------------------")
        tcp_connection.send(tweet_text + '\n')




TCP_IP = 'localhost'
TCP_PORT = 5252
# create a socket object
serversocket = socket.socket()



# bind to the port
serversocket.bind((TCP_IP, TCP_PORT))

# queue up to 5 requests
serversocket.listen(5)

print("----SERVER LISTENING------ " + str(TCP_PORT))

# establish a connection
clientsocket, addr = serversocket.accept()

print("Got a connection from %s" % str(addr))


while True:
    text=get_tweets()
    # text = "hello hello " + str(randrange(100))
    print(text)
    send_tweets_to_spark(bytes("{}\n".format(text), clientsocket))
    time.sleep(3)

# resp = get_tweets()





















#
# class TweetsListener(StreamListener):
#   # tweet object listens for the tweets
#   def __init__(self, csocket):
#     self.client_socket = csocket
#   def on_data(self, data):
#     try:
#       msg = json.loads( data )
#       print("new message")
#       # if tweet is longer than 140 characters
#       if "extended_tweet" in msg:
#         # add at the end of each tweet "t_end"
#         self.client_socket\
#             .send(str(msg['extended_tweet']['full_text']+"t_end")\
#             .encode('utf-8'))
#         print(msg['extended_tweet']['full_text'])
#       else:
#         # add at the end of each tweet "t_end"
#         self.client_socket\
#             .send(str(msg['text']+"t_end")\
#             .encode('utf-8'))
#         print(msg['text'])
#       return True
#     except BaseException as e:
#         print("Error on_data: %s" % str(e))
#     return True
#   def on_error(self, status):
#     print(status)
#     return True
#
#
# def sendData(c_socket, keyword):
#       print('start sending data from Twitter to socket')
#       # authentication based on the credentials
#       auth = OAuthHandler(consumer_key, consumer_secret)
#       auth.set_access_token(access_token, access_secret)
#       # start sending data from the Streaming API
#       twitter_stream = Stream(auth, TweetsListener(c_socket))
#       twitter_stream.filter(track=keyword, languages=["en"])
#       print(twitter_stream.filter(track=keyword, languages=["en"]))
#
# if __name__ == "__main__":
#     # server (local machine) creates listening socket
#     s = socket.socket()
#     host = "0.0.0.0"
#     port = 5555
#     s.bind((host, port))
#     print('socket is ready')
#     # server (local machine) listens for connections
#     s.listen(4)
#     print('socket is listening')
#     # return the socket and the address on the other side of the connection (client side)
#     c_socket, addr = s.accept()
#     print("Received request from: " + str(addr))
#     # select here the keyword for the tweet data
#     sendData(c_socket, keyword = ['MALARIA'])