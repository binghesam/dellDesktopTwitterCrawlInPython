#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

#by bing,setting the error time happens 
import time

import sys

#Variables that contains the user credentials to access Twitter API
consumer_key = 'MjHnsmzPgRvd0YlpvQmxr9Gte'
consumer_secret = 'Zl3qBfY4V7AYPUDj16P7dys22MlJD5ClrJLx2jcMDd1beTwpxb'
access_token = '3249467239-ZqWYfLL7NWtday0gJvXxhwRu6zcLTFYdOAUsdt3'
access_token_secret = 'IaaCXwPcr2SDFEf6bJmtGQ8cy6aKFFfurem3HNGfme2KO'

print("Enter the output file :{0}".format( sys.argv[1]) )
output_file = str(sys.argv[1])



#This is a basic listener that just prints received tweets to stdout.
#by bing, the streamListener should be the super class 
#then, the stdOut is the child class
#data should be the transformed files of the data.
class StdOutListener(StreamListener):

    def on_data(self, data):
        #print(data) # by bing, i ignore the print processing. 
        with open(output_file, 'a+')as f:
            f.write(data+'\n')
        return True

    def on_error(self, status):
        print(status)
        print("the is the https error")


if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        listener = StdOutListener()
        stream = Stream(auth, listener, timeout=60)
        # by bing, what is the use for the timeout variable.
        #  The maximum amount of time to wait for a response from Twitter


        try:
            #Twitter checks if coordatinates matches your locations filter. If that fails Twitter checks place.
            #bbox = left,bottom,right,top
            #stream.filter(track=['#Seattle', 'Seattle'], locations=[-122.53, 47.46, -122.20,47.74]) #the target region
            stream.filter(locations=[-122.53, 47.46, -122.20,47.74]) #the target region
            #stream.filter(track=['#us'])


        except Exception as e:
            print("Error. Restarting Stream.... Error: ")
            print(e.__doc__)
            print(e.message)
            # by bing ,testing for print some message from the original day time
            print time.gmtime()