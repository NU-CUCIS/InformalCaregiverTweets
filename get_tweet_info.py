import MySQLdb
import pandas as pd
import json
import re

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

class get_tweet_info:
#==========================================================
#       __init__
#==========================================================
    def __init__(self):
        # Initialization
        self.server_id = "10.1.1.141"
        self.server_user = <db_username> # in string format 
        self.server_pwd = <db_password>  # in string format 
        self.dbname = <db_name> # in string format 
        self.cursor = None
        self.db = None

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
    			print "Error %d: %s" % (e.args[0], e.args[1])
    			print "  could not connect to database " + self.dbname + "\n"
    			sys.exit(0)
    			
    	self.cursor = self.db.cursor()
    	query = "set names 'utf8'"
    	try:
    		self.cursor.execute(query)
    		print "successfully set set names utf8"
    	except MySQLdb.Error, e:
    		print "Error %d: %s" % (e.args[0], e.args[1])
    	
    	query = "set time_zone='+00:00'"
    	try:
    		self.cursor.execute(query)
    		print "successfully set time_zone to UTC"
    	except MySQLdb.Error, e:
    		print "Error %d: %s" % (e.args[0], e.args[1])
	return self.db, self.cursor

#==========================================================
#	close_db
#==========================================================
    def close_db(self):
        self.cursor.close()
        self.db.commit()
        self.db.close()

#==========================================================
#	create_tables
#==========================================================
    def create_tables(self):
    	self.connect_db()
    	connection, cursor = self.connect_db()
    	df_mysql = pd.read_sql('select symptom_name from symptom;', con=connection)
    	symptom_list = list(df_mysql['symptom_name'])
    	self.close_db()
    	for symptom in symptom_list:
    		query = "create table " + symptom + "_tweets like caregiver_tweets"
    		connection, cursor = self.connect_db()
    		try:
    			cursor.execute(query)
    		except:
    			continue
    		self.close_db()

#==========================================================
#	drop_tables
#==========================================================
    def drop_tables(self):
    	self.connect_db()
    	connection, cursor = self.connect_db()
    	df_mysql = pd.read_sql('select symptom_name from symptom;', con=connection)
    	symptom_list = list(df_mysql['symptom_name'])
    	self.close_db()
    	for symptom in symptom_list:
    		query = "drop table " + symptom + "_tweets;"
    		print query

#==========================================================
#	get_tweet_text
#==========================================================
    def get_tweet_text(self, tweet_json):
        return tweet_json['text'].encode('utf-8')

#==========================================================
#	insert_tweets_to_db
#==========================================================
    def insert_tweets_to_db(self, df, table_name):
        connection, cursor = self.connect_db()
        if len(df) > 10000:
        	indices = range(0, len(df), 10000)
        	indices.append(len(df))
        	print 'more than 10,000', indices
        else:
        	indices = [0, len(df)]
        	print 'less than 10,000', indices
	
		for i in range(len(indices)-1):
			print indices[i], indices[i+1]
			df.iloc[indices[i]:indices[i+1],:].to_sql(name=table_name, con=connection, schema='haze', if_exists='append', flavor='mysql', index=False)
		self.close_db()

#==========================================================
#	main
#==========================================================
def main():
	fm = get_tweet_info()
	connection, cursor = fm.connect_db()
	df_mysql = pd.read_sql('select id, symptom_name from symptom;', con=connection)
	symptom_list = list(df_mysql['symptom_name'])
	symptom_dict = df_mysql.set_index('symptom_name').to_dict()['id']
	print symptom_dict['grandma_alz']
	
	fm.close_db()
	fm.create_tables()
	#symptom_list = ['caregiving']
	for symptom in symptom_list:
		connection, cursor = fm.connect_db()
		print 'select max(tweet_id_str) from cg_tweets where symptom_id = ' + str(symptom_dict[symptom]) + ';'
		df_mysql = pd.read_sql('select max(tweet_id_str) from cg_tweets where symptom_id = ' + str(symptom_dict[symptom]) + ';', con=connection)
		last_processed = df_mysql['max(tweet_id_str)'].ix[0]
		print last_processed
		if last_processed == None:
			last_processed = str(0)
		print 'loaded dataframe from MySQL. records:', len(df_mysql)
		fm.close_db()
				
		connection, cursor = fm.connect_db()
		print 'select * from ' + symptom + '_raw_tweets where tweet_id > ' + last_processed + ';'
		df_mysql = pd.read_sql('select * from ' + symptom + '_raw_tweets where tweet_id > ' + last_processed + ';', con=connection)
		print 'loaded dataframe from MySQL. records:', len(df_mysql)
		#fm.close_db()
		df_raw_tweets = df_mysql['raw_tweet']
# 		for i in range(len(df_raw_tweets)):
# 			print(i)
# 			print(df_raw_tweets.ix[i])
# 			json.loads(df_raw_tweets.ix[i])
		f = lambda x: json.loads(x)
		df_json_tweets = pd.DataFrame(df_raw_tweets.map(f))
		df_tweet_info = pd.DataFrame(columns=['tweet_id_str', 'symptom_id', 'created_at', 'tweet_text', 'tweet_source', 'user_id_str'])
		df_place_info = pd.DataFrame(columns=['tweet_id_str', 'place_type', 'place_name', 'full_name', 'country_code'])
		df_user_info = pd.DataFrame(columns=['user_id_str', 'user_name', 'screen_name', 'location', 'description', 'verified', 'time_zone'])
		
		# get old_user_ids, and old_tweet_ids
		connection, cursor = fm.connect_db()
		user_ids = pd.read_sql('select user_id_str from cg_users;', con=connection)
		old_user_ids = list(user_ids['user_id_str'])
		
		df_new_user_info = df_user_info[~df_user_info['user_id_str'].isin(old_user_ids)]
		df_new_user_info = df_new_user_info.drop_duplicates()
		
		
		tweet_ids = pd.read_sql('select tweet_id_str from cg_places;', con=connection)
		old_tweet_ids = list(tweet_ids['tweet_id_str'])
		#fm.close_db()
		
		count = 0
		count_1000 = 0
		for i in range(len(df_json_tweets)):
			tmp = df_json_tweets.ix[i]['raw_tweet']
			# tweet info
			tweet_id_str = str(tmp['id_str'])
			symptom_id = symptom_dict[symptom]
			created_at = str(tmp['created_at'])
			tweet_text = re.sub(r'[^\x00-\x7F]+',' ', tmp['text'].encode('utf-8')).replace('"', '').replace('\\', '')
			if str(tmp['source']).strip() != '':
				tweet_source = str(tmp['source']).split('>')[1].split('</')[0]
			else:
				tweet_source = None
			# user info
			user_id_str = tmp['user']['id_str']
			user_name = tmp['user']['name'].encode('utf-8').replace('"', '').replace('\\', '')
			screen_name = tmp['user']['screen_name'].encode('utf-8')
			if tmp['user']['location'] != None:
				location = tmp['user']['location'].encode('utf-8').replace('"', '').replace('\\', '')
			else:
				location = None
			if tmp['user']['description'] != None:
				description = tmp['user']['description'].encode('utf-8').replace('"', '').replace('\\', '')
			else:
				description = None
			verified = tmp['user']['verified']
			if tmp['user']['time_zone'] != None:
				time_zone = tmp['user']['time_zone']
			else:
				time_zone = None
			
			if user_id_str in old_user_ids:
				pass
			else:
				user_values_dict = {'user_id_str': user_id_str, 'user_name': user_name, 'screen_name': screen_name, 'location': location, 'description': description, 'verified': verified, 'time_zone': time_zone}
				user_columns = list(df_user_info.columns)
				user_values_list = []
				for c in user_columns:
					user_values_list.append('"' + str(user_values_dict[c]) + '"')
				try:
					query = 'insert into cg_users (' + ','.join(user_columns) + ') values (' + ','.join(list(user_values_list)) + ');'
					cursor.execute(query)
				except:
					print "Unexpected error:", sys.exc_info()[0]
					print query
					pass
			
			
			if tweet_id_str in old_tweet_ids:
				pass
			else:
				tweet_values_dict = {'tweet_id_str': tweet_id_str, 'symptom_id': symptom_id, 'created_at': created_at, 'tweet_text': tweet_text, 'user_id_str': user_id_str, 'tweet_source': tweet_source}
				tweet_columns = list(df_tweet_info.columns)
				tweet_values_list = []
				for c in tweet_columns:
					tweet_values_list.append('"' + str(tweet_values_dict[c]) + '"')
				try:
					query = 'insert into cg_tweets (' + ','.join(tweet_columns) + ') values (' + ','.join(list(tweet_values_list)) + ');'
					cursor.execute(query)
				except:
					print "Unexpected error:", sys.exc_info()[0]
					print query
					pass
			
				# place info
				if tmp['place'] != None:
					place_type = tmp['place']['place_type'].encode('utf-8')
					place_name = tmp['place']['name'].encode('utf-8')
					full_name = tmp['place']['full_name'].encode('utf-8')
					country_code = tmp['place']['country_code'].encode('utf-8')
					place_values_dict = {'tweet_id_str': tweet_id_str, 'place_type': place_type, 'place_name': place_name, 'full_name': full_name, 'country_code': country_code}
					df_place_info = df_place_info.append({'tweet_id_str': tweet_id_str, 'place_type': place_type, 'place_name': place_name, 'full_name': full_name, 'country_code': country_code}, ignore_index=True)
					place_columns = list(df_place_info.columns)
					place_values_list = []
					for c in place_columns:
						place_values_list.append('"' + str(place_values_dict[c]) + '"')
					try:
						query = 'insert into cg_places (' + ','.join(place_columns) + ') values (' + ','.join(list(place_values_list)).replace('None', '') + ');'
						cursor.execute(query)
					except:
						print query
						print "Unexpected error:", sys.exc_info()[0]
						pass
				
			df_tweet_info = df_tweet_info.append({'tweet_id_str': tweet_id_str, 'symptom_id': symptom_id, 'created_at': created_at, 'tweet_text': tweet_text, 'user_id_str': user_id_str, 'tweet_source': tweet_source}, ignore_index=True)
			df_user_info = df_user_info.append({'user_id_str': user_id_str, 'user_name': user_name, 'screen_name': screen_name, 'description': description, 'verified': verified, 'time_zone': time_zone}, ignore_index=True)
			count = count + 1
			if count % 1000 == 0:
				count_1000 = count_1000 + 1000
				print count_1000
		fm.close_db()

		df_new_place_info = df_place_info[~df_place_info['tweet_id_str'].isin(old_tweet_ids)]
		print '*****'
		print len(df_tweet_info), len(df_new_user_info), len(df_new_place_info)
		#fm.insert_tweets_to_db(df_new_user_info, 'cg_users')
		#fm.insert_tweets_to_db(df_new_place_info, 'cg_places')
		#fm.insert_tweets_to_db(df_tweet_info, 'cg_tweets')

if __name__ == "__main__":
	main()