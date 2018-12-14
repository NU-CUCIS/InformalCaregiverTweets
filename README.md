# Informal Caregiver Tweets
This repo is for hosting code used to collect and perform initial preprocessing of informal caregiver tweets

# Steps
- Update database username and password, and Twitter account information in both <code>get_tweets.py</code> and <code>get_tweet_info.py</code>.
- To collect tweets run the <code>get_tweets.py</code> script
- It can be run in the background by updating the crontab to run at the top of every hour by adding an entry as follows.
    - Edit the crontab file by running <code>crontab -e</code> and adding the following line e.g. <code> 0 * * * * python /home/rav650/care_project/get_tweets.py </code>
- To pre-process the tweets and seperate the information in different tables: tweet_info, user_info, and user_location run the <code>get_tweet_info.py</code> script.

Once the above scripts are done a program can be developed to download the tweets from the database server or a GUI interface can be used to export the data in csv or json formats.
