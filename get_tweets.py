# -*- coding: utf-8 -*-
#!/usr/bin/python

import MySQLdb
import urllib
import urllib2
import time
import datetime
import base64
import oauth2 as oauth
import json
import re

FAIL = -1
SUCCESS = 1

topic_dict = {}
class HealthTweets:

#==========================================================
#       __init__
#==========================================================
    def __init__(self):
        self.db = None
        self.cursor = None
        self.server_id = "10.1.1.141" #"129.105.15.141"
        self.server_user = <db_username> # in string format 
        self.server_pwd = <db_password>  # in string format 
        self.dbname = <db_name> # in string format 


	self.consumer_key = <twitter_consumer_key> # in string format 
	self.consumer_secret = <twitter_consumer_secret> # in string format 
	self.oauth_token = <twitter_token> # in string format 
	self.oauth_token_secret = <twitter_token_secret> # in string format 



#==========================================================
#	connect_db
#==========================================================
    def connect_db(self):


        while (1) :
            try :
                self.db = MySQLdb.connect(self.server_id, self.server_user, self.server_pwd, self.dbname)
                print "Got connection to the database " + self.dbname + "\n"
                break
            except MySQLdb.Error, e:
		print "  could not connect to database " + self.dbname
		print "Error %d: %s" % (e.args[0], e.args[1])
		sys.exit(1)
		#if e.args[0] == 1040 :  # Exit if too many connections error occured
		#    sys.exit(1)

        self.cursor = self.db.cursor()

	query = "set names 'utf8'"
        try:
            self.cursor.execute(query)
	    print "successfully set set names utf8"
        except MySQLdb.Error, e:
            print "Error %d: %s" % (e.args[0], e.args[1])

	return self.db, self.cursor

#==========================================================
#	close_db
#==========================================================
    def close_db(self):
        # close the cursor and connection
        self.cursor.close()
        self.db.commit()
	self.db.close()

#==========================================================
#	build_authorization_header
#==========================================================
    def build_authorization_header(self, url):


        access_token = oauth.Token(key=self.oauth_token, secret=self.oauth_token_secret)
        consumer = oauth.Consumer(key=self.consumer_key, secret=self.consumer_secret)
        client = oauth.Client(consumer, access_token)

	#url = "https://stream.twitter.com/1/statuses/sample.json"
	params = {
	    'oauth_version': "1.0",
	    'oauth_nonce': oauth.generate_nonce(),
	    'oauth_timestamp': int(time.time()),
	    'oauth_token': self.oauth_token,
	    'oauth_consumer_key': self.consumer_key
	}

	# Sign the request.
	# For some messed up reason, we need to specify is_form_encoded to prevent
	# the oauth2 library from setting oauth_body_hash which Twitter doesn't like.
	req = oauth.Request(method="GET", url=url, parameters=params)
	req.sign_request(oauth.SignatureMethod_HMAC_SHA1(), consumer, access_token)

	# Grab the Authorization header
	header = req.to_header()['Authorization'].encode('utf-8')

	return header


#==========================================================
#	streamAllHealthTweets()
#==========================================================
    def streamAllHealthTweets(self):
        self.connect_db()

	topic_list = self.get_topic_list()

        #topic_str = "flu"

        base_url = "https://stream.twitter.com/1.1/statuses/filter.json"

	topics_str = ""
        parameters = []
	for topic in topic_list:
	    words = re.split(' AND | ',topic)
	    for w in words:
		topics_str = topics_str + " " + w
        topics_str.replace("#", "&#35;").replace ("'", "&#39;")
        re.sub("\s+", " ", topics_str)
        topics_str = topics_str.strip()
        topics_str = topics_str.replace(" ", ",")
        topics_str = topics_str.replace("&", " ")

	parameters.append(("track", topics_str))
	url_ = urllib.urlencode(parameters).replace("+", "%20")
        search_url = base_url + "?" + url_
        print 'Search url = ' , search_url

	connect = False
        connect_count = 1

        while not connect:
	    try:
	    	req = urllib2.Request(search_url)
	    	req.add_header("Authorization", self.build_authorization_header(search_url))
	    	handle = urllib2.urlopen(req)
	    	connect = True
            	print "connection attemp " + str(connect_count) + " was successful"
            except urllib2.HTTPError, e:
            	print e.code , "\n Connection HTTP Error ... sleeping for " + str(connect_count*90) + " seconds"
            	print "connection attemp " + str(connect_count) + " was unsuccessful"
	    	connect_count = connect_count + 1
            	time.sleep(90)
            except urllib2.URLError, e:
            	print "\nConnection URL ERROR ... sleeping for " + str(connect_count*90) + " seconds"
            	print "connection attemp " + str(connect_count) + " was unsuccessful"
            	connect_count = connect_count + 1
            	time.sleep(90)

        backoff_time = 10
        numerrors = 0
        num_new_tweets = 0

        print "Start streaming.......\n"
        start_time = time.time()
        json_list = []
	start_time = datetime.datetime.now()
        while(1):

	    if num_new_tweets%10 == 0:
                print "Got ", num_new_tweets, " new tweets...."

                json_list = []

                #current_time = time.time()
		current_time = datetime.datetime.now()
		print '***********************************************'
		print "Current time = ",  time.ctime()
		print '***********************************************'
            current_time = datetime.datetime.now()
	    if ((current_time-start_time).seconds / 60 > 59):#if (int(current_time - start_time) > 55 * 60): # if more than 55 minutes
		break
            #if num_new_tweets == 10:
            #    print "Got 10 new tweets...exit"
            #    break

            if numerrors > 10:
                handle.close()
		print "Too many errors ... breaking "
                break

            try:
                # Read a line from the stream
                json_msg = handle.readline()
                if len(json_msg.strip()) != 0:
                    num_new_tweets += 1


                    try:
                        #print "********************"
                        #print json_msg
                        #print "********************"
                        result = json.loads(json_msg)

			# Check if tweet contains any topic
                        if (result.has_key('text') and result.has_key('id')):
                            tid = result['id']
			    #print "tid:", tid

			    for topic in topic_dict.keys():
				if (result['text'].lower().find(topic) >= 0) and (topic.lower().find("&") == -1):
				    print result['text']
				    if self.insertRawTweet(topic, tid, json_msg) == SUCCESS:
					json_list.append(result)
				elif list(set(topic.lower().split("&"))) == list(set(topic.lower().split("&")) & set(result['text'].lower().split())):
                                    if self.insertRawTweet(topic, tid, json_msg) == SUCCESS:
                                        json_list.append(result)
                    except:
                        #break
                        print "Failed to read json"
                        pass

            except urllib2.HTTPError, e:
                print e.code , " HTTP ERROR\n"
                time.sleep(backoff_time)
                numerrors += 1
                backoff_time *= 2
                if backoff_time > 240:
                    backoff_time = 240
            except urllib2.URLError, e:
                print "URLError\n"
                time.sleep(10)
                numerrors +=1

        handle.close()
        self.close_db()

#==========================================================
#	insertRawTweet()
#==========================================================
    def insertRawTweet(self, topic_str, tid, json_msg):
        table_name = topic_dict[topic_str] + "_raw_tweets"
	print "*******************************"
	print "Inserting raw data into ", table_name
	print "*******************************"
        query = "insert into " + table_name + " (tweet_id, raw_tweet) values ('" +\
                str(tid) + "','" + self.db.escape_string(json_msg) + "')"

	try :
	    #print query
            self.cursor.execute(query)
        except MySQLdb.Error, e:
            print e
            print "Failed to insert raw tweet for ",topic_str
            return FAIL

        return SUCCESS


#==========================================================
#	get_topic_list
#==========================================================
    def get_topic_list(self):

	query = "select symptom_org_name, symptom_name from symptom"
	print query
	self.cursor.execute(query)
	result = self.cursor.fetchall()

	topic_list = []
	for x in result:
	    topic_list.append(x[0])
	    topic_dict[x[0]] = x[1]
	#print topic_list
	#for x in topic_dict.keys():
	    #print x, '   ' , topic_dict[x]
	return topic_list


#==========================================================
#	create_tables
#==========================================================
    def create_tables(self):
	self.connect_db()
	query = "select symptom_name from symptom"
	self.cursor.execute(query)
	result = self.cursor.fetchall()

	for x in result:
	    query = "create table " + str(x[0]) + "_raw_tweets like haze_raw_tweets"
	    print query
	    try:
	        self.cursor.execute(query)
	    except:
		continue
	self.close_db()
#==========================================================
#	main
#==========================================================
def main():
    ht = HealthTweets()
    ht.create_tables()
    ht.streamAllHealthTweets()


if __name__ == "__main__":
	main()

