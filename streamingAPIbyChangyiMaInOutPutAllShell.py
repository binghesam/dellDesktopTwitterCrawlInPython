#Import the necessary methods from tweepy library
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream

# the source for the original authorization.
####by bing, in fact,if you use ony one config key
#you should also use the only one location string.

import config0
import config1
import config2
import config3

import sys
import time

###########by bing optimize it
# if (sys.argv[1]==1):
#     config=config1
# elif sys.argv[1]==2:
#     config=config2
# leif

configList=[config0,config1,config2,config3]
#by bing testing for the different configs 
#configList=[config2,config3,config0,config1]

config=configList[int(sys.argv[1])] #by bing,just for starting from zero    
#Variables that contains the user credentials to access Twitter API

consumer_key = config.consumer_key
consumer_secret = config.consumer_secret
access_token = config.access_token
access_token_secret = config.access_token_secret
print("the config key used is :{0}".format((sys.argv[1])))



######by bing place setting
#bbox = left,bottom,right,top
Seattle=[-122.53, 47.46, -122.20,47.74]
La=[-118.65 ,33.71 ,-118.16, 34.19]
# in la, we get four locations: 
'''
top 34.331483, -118.505161;
left 34.190744, -118.652103
right 34.095393, -118.163657
bottom 33.711280, -118.296866
'''

#in ny
'''
top 40.911119, -73.909380
left  40.627184, -74.034349
right 40.749019, -73.707506
bottom 40.557315, -73.912126

the second one
top 40.643859, -74.083788
left 40.542707, -74.238970
right 40.603208, -74.063188
bottom  40.506693, -74.238970
'''
Ny=[-74.03, 40.50, -73.70, 40.91]

'''
in chicago:
top 42.021861, -87.672966
left 41.985929, -87.937369
right 41.715510, -87.529747
bottom 41.647402, -87.572879

'''
Chicago=[-87.93,41.64,-87.52,42.02]
locationList=[Seattle,La,Ny,Chicago]
locationListName=["Seattle","La","Ny","Chicago"]

##in the same file processing pipeline for the future development
print("Enter the output file :{0}".format(locationListName[int(sys.argv[1])]+sys.argv[2]))

####################using the specific time for naming the system
# output_file="testForVolumeInRam.txt"

# #output_file = str(locationListName[int(sys.argv[1])]+sys.argv[2])
# output_error_file=str(locationListName[int(sys.argv[1])]+sys.argv[2]+"ErrorFile")
# print("use the specific time to name the file")

####################using the real time to naming the whole system
year=time.gmtime().tm_year
month=time.gmtime().tm_mon
day=time.gmtime().tm_mday
hour=time.gmtime().tm_hour
minu=time.gmtime().tm_min
sec=time.gmtime().tm_sec
output_file = str(locationListName[int(sys.argv[1])]+"Day"+str(month)+str(day)+"Time"+str(hour)+str(minu))
output_error_file=str(locationListName[int(sys.argv[1])]+"Day"+str(month)+str(day)+"Time"+str(hour)+str(minu)+"ErrorFile")
print("use the system time to name the file, so we just use one parameter to name the whole system")

#output_file = "data.txt"
#This is a basic listener that just prints received tweets to stdout.
#by bing, the streamListener should be the super class 
#then, the stdOut is the child class
#data should be the transformed files of the data.
class StdOutListener(StreamListener):

    def on_data(self, data):
        #print(data) # by bing, i ignore the print processing. 
        # by bing,by talking with yimng Gao, we should use the a not a+ to process the file system
        with open(output_file, 'a')as f:
            f.write(data+"\n")
            #f.write(data+'\n')
        return True

    def on_error(self, status):
        print(status)
        print("the is the error in the inner StreamListener of StdOutListener functionrate")
        print time.gmtime()
        with open(output_error_file,"a") as err:
            err.write(str(status)+"\n")
            err.write("the is the error in the inner StreamListener of StdOutListener functionrate:"+"\n")
            err.write(str(time.gmtime())+"\n")
            err.write("****************************************************************************"+"\n")

if __name__ == '__main__':

    #This handles Twitter authetification and the connection to Twitter Streaming API
    auth = OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_token_secret)

    while True:
        listener = StdOutListener()
        stream = Stream(auth, listener, timeout=60)
        # by bing, what is the use for the timeout variable.
        # The maximum amount of time to wait for a response from Twitter
        # the gap should be measured by  seconds.


        try:
            #Twitter checks if coordatinates matches your locations filter. If that fails Twitter checks place.
            #bbox = left,bottom,right,top
            #stream.filter(track=['#Seattle', 'Seattle'], locations=[-122.53, 47.46, -122.20,47.74]) #the target region
            locSpec=locationList[int(sys.argv[1])] #just use  two locations.
            print("the location used is :{0}".format(locationListName[int(sys.argv[1])]))
            #stream.filter(locations=[-122.53, 47.46, -122.20,47.74]) #the target region
            stream.filter(locations=locSpec)
            #stream.filter(track=['#us'])


        except Exception as e:
            print("Error. Restarting Stream.... Error: ")
            print(e.__doc__)
            print(e.message)
            print("this is the error in the while true loop for stream getting and filtering")
            #by bing for printing the error time
            print time.gmtime()
            with open(output_error_file,"a") as err:
                err.write( "Error. Restarting Stream.... Error: "+"\n")               
                err.write(str(e.__doc__)+"\n")
                err.write(str(e.message)+"\n")
                err.write("this is the error in the while true loop for stream getting and filtering"+"\n")
                err.write(str(time.gmtime())+"\n")
                err.write("****************************************************************************"+"\n")