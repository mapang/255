
# coding: utf-8

# Name: Michelle Pang
# Email: mapang@eng.ucsd.edu
# PID: A10660264

# # Homework 6
# 
# In this homework, we are going to play with Twitter data.
# 
# The data is represented as rows of of [JSON](https://en.wikipedia.org/wiki/JSON#Example) strings.
# It consists of [tweets](https://dev.twitter.com/overview/api/tweets), [messages](https://dev.twitter.com/streaming/overview/messages-types), and a small amount of broken data (cannot be parsed as JSON).
# 
# For this homework, we will only focus on tweets and ignore all other messages.
# 
# 
# ## Tweets
# 
# A tweet consists of many data fields. [Here is an example](https://gist.github.com/arapat/03d02c9b327e6ff3f6c3c5c602eeaf8b). You can learn all about them in the Twitter API doc. We are going to briefly introduce only the data fields that will be used in this homework.
# 
# * `created_at`: Posted time of this tweet (time zone is included)
# * `id_str`: Tweet ID - we recommend using `id_str` over using `id` as Tweet IDs, becauase `id` is an integer and may bring some overflow problems.
# * `text`: Tweet content
# * `user`: A JSON object for information about the author of the tweet
#     * `id_str`: User ID
#     * `name`: User name (may contain spaces)
#     * `screen_name`: User screen name (no spaces)
# * `retweeted_status`: A JSON object for information about the retweeted tweet (i.e. this tweet is not original but retweeteed some other tweet)
#     * All data fields of a tweet except `retweeted_status`
# * `entities`: A JSON object for all entities in this tweet
#     * `hashtags`: An array for all the hashtags that are mentioned in this tweet
#     * `urls`: An array for all the URLs that are mentioned in this tweet
# 
# 
# ## Data source
# 
# All tweets are collected using the [Twitter Streaming API](https://dev.twitter.com/streaming/overview).
# 
# 
# ## Users partition
# 
# Besides the original tweets, we will provide you with a Pickle file, which contains a partition over 452,743 Twitter users. It contains a Python dictionary `{user_id: partition_id}`. The users are partitioned into 7 groups.

# # Part 0: Load data to a RDD

# The tweets data is stored on AWS S3. We have in total a little over 1 TB of tweets. We provide 10 MB of tweets for your local development. For the testing and grading on the homework server, we will use different data.
# 
# ## Testing on the homework server
# In the Playground, we provide input size of 4 GB. To test, read file list from `../../Data/hw6-files-4gb.txt`. More input files will be added soon. 
# 
# For final submission, make sure to read files list from `../../Data/hw6-files-final.txt`. Otherwise your program will receive no points.
# 
# ## Local test
# 
# For local testing, read files list from `../../Data/hw6-files-10mb.txt`.
# Now let's see how many lines there are in the input files.
# 
# 1. Make RDD from the list of files in `hw6-files-10mb.txt`.
# 2. Mark the RDD to be cached (so in next operation data will be loaded in memory) 
# 3. call the `print_count` method to print number of lines in all these files
# 
# It should print
# ```
# Number of elements: 2193
# ```

# In[1]:

def print_count(rdd):
    print 'Number of elements:', rdd.count()


# In[2]:


from pyspark import SparkContext
sc = SparkContext()
# Your program here





# In[3]:



with open("../../Data/hw6-files-10mb.txt","r") as f:
    lines = f.readlines()
#     for line in lines:

#         if(line != '\n'):
#             line = line[:-1]
 
            #rdd= sc.textFile(line.encode('utf-8')).cache()  #Orig, but doesnt handle multiple files
            #rdd = sc.textFile(','.join(lines)) #doesnt work
    rdd = sc.union([sc.textFile( line[:-1]) for line in lines]).cache()  # with this change time is 132 sec
    print_count(rdd)


# # Part 1: Parse JSON strings to JSON objects

# Python has built-in support for JSON.

# In[ ]:

# import json

# json_example = '''
# {
#     {"id": 1,
#     "name": "A green door",
#     "price": 12.50,
#     "tags": ["home", "green"]},
#     {"id": 2,
#     "name": "A blue door",
#     "price": 2.50,
#     "tags": ["home", "blue"]}
# }
# '''
# json_example = '''
# [
# {"created_at":"Thu Mar 10 06:45:40 +0000 2016","id":707819694410043393,"id_str":"707819694410043393","text":"@DemDefender @BernieSanders @LordAlmighty12  @HillaryClinton what's Your all not getting it. All politicians are corrupt even the good ones!","source":"\u003ca href=\"http:\/\/twitter.com\/download\/android\" rel=\"nofollow\"\u003eTwitter for Android\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":707818860485992449,"in_reply_to_status_id_str":"707818860485992449","in_reply_to_user_id":4855831668,"in_reply_to_user_id_str":"4855831668","in_reply_to_screen_name":"DemDefender","user":{"id":2914597730,"id_str":"2914597730","name":"MKTG \/ PROMO","screen_name":"pwexec","location":"Vancouver, Canada ","url":null,"description":"**PERSONAL ACCOUNT** #Communications #RadioPromo\/#PR\/#Marketing MGMT\/#Publishing\/#MakeupArtist\/#MusicPromo\/#Sports\/#Politics\/#Philosophy\/#History\/#Archaeology","protected":false,"verified":false,"followers_count":2845,"friends_count":2565,"listed_count":115,"favourites_count":18366,"statuses_count":31081,"created_at":"Sun Nov 30 10:04:11 +0000 2014","utc_offset":-28800,"time_zone":"Pacific Time (US & Canada)","geo_enabled":true,"lang":"en","contributors_enabled":false,"is_translator":false,"profile_background_color":"022330","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme15\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme15\/bg.png","profile_background_tile":false,"profile_link_color":"0084B4","profile_sidebar_border_color":"A8C7F7","profile_sidebar_fill_color":"C0DFEC","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/707710189789536257\/zCTdDvqM_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/707710189789536257\/zCTdDvqM_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/2914597730\/1457504776","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[],"user_mentions":[{"screen_name":"DemDefender","name":"DemDefender","id":4855831668,"id_str":"4855831668","indices":[0,12]},{"screen_name":"BernieSanders","name":"Bernie Sanders","id":216776631,"id_str":"216776631","indices":[13,27]},{"screen_name":"LordAlmighty12","name":"Just a Patrio\u0366\u0366\u0366\u0366t","id":632372819,"id_str":"632372819","indices":[28,43]},{"screen_name":"HillaryClinton","name":"Hillary Clinton","id":1339835893,"id_str":"1339835893","indices":[45,60]}],"symbols":[]},"favorited":false,"retweeted":false,"filter_level":"low","lang":"en","timestamp_ms":"1457592340458"},
# {"created_at":"Thu Mar 10 06:46:01 +0000 2016","id":707819784742817792,"id_str":"707819784742817792","text":"The NCAA needs to suspend this bitch Grayson Allen  https:\/\/t.co\/sV9gVvEbE5","source":"\u003ca href=\"http:\/\/twitter.com\/#!\/download\/ipad\" rel=\"nofollow\"\u003eTwitter for iPad\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":1634250012,"id_str":"1634250012","name":"Andy Mac","screen_name":"AndyMac300","location":"Bullcity, NC","url":null,"description":"RIP Lil brother #919 #UNCG","protected":false,"verified":false,"followers_count":627,"friends_count":367,"listed_count":11,"favourites_count":18129,"statuses_count":37278,"created_at":"Wed Jul 31 01:41:46 +0000 2013","utc_offset":-25200,"time_zone":"Arizona","geo_enabled":true,"lang":"en","contributors_enabled":false,"is_translator":false,"profile_background_color":"000000","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme1\/bg.png","profile_background_tile":false,"profile_link_color":"183945","profile_sidebar_border_color":"000000","profile_sidebar_fill_color":"000000","profile_text_color":"000000","profile_use_background_image":false,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/699328050270466048\/Ng6RjCnf_normal.jpg","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/699328050270466048\/Ng6RjCnf_normal.jpg","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/1634250012\/1455641030","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"quoted_status_id":707762363752173573,"quoted_status_id_str":"707762363752173573","quoted_status":{"created_at":"Thu Mar 10 02:57:51 +0000 2016","id":707762363752173573,"id_str":"707762363752173573","text":"While you were all watching the #DemDebate, this happened over on Fox News. https:\/\/t.co\/YJDScryXd0","source":"\u003ca href=\"https:\/\/about.twitter.com\/products\/tweetdeck\" rel=\"nofollow\"\u003eTweetDeck\u003c\/a\u003e","truncated":false,"in_reply_to_status_id":null,"in_reply_to_status_id_str":null,"in_reply_to_user_id":null,"in_reply_to_user_id_str":null,"in_reply_to_screen_name":null,"user":{"id":18071942,"id_str":"18071942","name":"Andrew Kirell","screen_name":"AndrewKirell","location":"Brooklyn, NY","url":"http:\/\/www.andrewkirell.com\/","description":"@thedailybeast senior editor \/\/ also make music \/\/ opinions are my own unless I've been hacked.","protected":false,"verified":true,"followers_count":6506,"friends_count":987,"listed_count":297,"favourites_count":5763,"statuses_count":22608,"created_at":"Fri Dec 12 06:54:41 +0000 2008","utc_offset":-18000,"time_zone":"Eastern Time (US & Canada)","geo_enabled":true,"lang":"en","contributors_enabled":false,"is_translator":false,"profile_background_color":"131516","profile_background_image_url":"http:\/\/abs.twimg.com\/images\/themes\/theme14\/bg.gif","profile_background_image_url_https":"https:\/\/abs.twimg.com\/images\/themes\/theme14\/bg.gif","profile_background_tile":false,"profile_link_color":"009999","profile_sidebar_border_color":"EEEEEE","profile_sidebar_fill_color":"EFEFEF","profile_text_color":"333333","profile_use_background_image":true,"profile_image_url":"http:\/\/pbs.twimg.com\/profile_images\/697152771007647744\/Sbc22YBs_normal.png","profile_image_url_https":"https:\/\/pbs.twimg.com\/profile_images\/697152771007647744\/Sbc22YBs_normal.png","profile_banner_url":"https:\/\/pbs.twimg.com\/profile_banners\/18071942\/1453677098","default_profile":false,"default_profile_image":false,"following":null,"follow_request_sent":null,"notifications":null},"geo":null,"coordinates":null,"place":null,"contributors":null,"is_quote_status":false,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[{"text":"DemDebate","indices":[32,42]}],"urls":[{"url":"https:\/\/t.co\/YJDScryXd0","expanded_url":"https:\/\/vine.co\/v\/iHelJ9u9t67","display_url":"vine.co\/v\/iHelJ9u9t67","indices":[76,99]}],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"en"},"is_quote_status":true,"retweet_count":0,"favorite_count":0,"entities":{"hashtags":[],"urls":[{"url":"https:\/\/t.co\/sV9gVvEbE5","expanded_url":"https:\/\/twitter.com\/andrewkirell\/status\/707762363752173573","display_url":"twitter.com\/andrewkirell\/s\u2026","indices":[52,75]}],"user_mentions":[],"symbols":[]},"favorited":false,"retweeted":false,"possibly_sensitive":false,"filter_level":"low","lang":"en","timestamp_ms":"1457592361995"}
# ]
# '''

# json_obj = json.loads(json_example)
# json_obj


# ## Broken tweets and irrelevant messages
# 
# The data of this assignment may contain broken tweets (invalid JSON strings). So make sure that your code is robust for such cases.
# 
# In addition, some lines in the input file might not be tweets, but messages that the Twitter server sent to the developer (such as [limit notices](https://dev.twitter.com/streaming/overview/messages-types#limit_notices)). Your program should also ignore these messages.
# 
# *Hint:* [Catch the ValueError](http://stackoverflow.com/questions/11294535/verify-if-a-string-is-json-in-python)
# 
# 
# (1) Parse raw JSON tweets to obtain valid JSON objects. From all valid tweets, construct a pair RDD of `(user_id, text)`, where `user_id` is the `id_str` data field of the `user` dictionary (read [Tweets](#Tweets) section above), `text` is the `text` data field.

# In[4]:

import json

def safe_parse(raw_json):
    # your code here
    try:
        json_object = json.loads(raw_json)
    except ValueError, e: # this outputs None
        pass
    if 'user' in json_object and 'text' in json_object and 'recipient' not in json_object:
        return (json_object['user']['id_str'],json_object['text'].encode('utf-8'))
        
    
    return None #return False


# def safe_parse(raw_json):   # THE T/F CASE
#     # your code here
#     try:
#         json_object = json.loads(raw_json)
#         #tweets = sc.parallelize(json_object, 4)  # WHAT number should this be?
#         #tweets = sc.read.json(json_object).rdd
#         #tweets = sc.read.json(raw_json)
#         #print type(tweets)
#         #json_filtered = tweets.filter(lambda x: x['limit'])
#         #if 'id_str' in json_object:
        
#             #return (json_object['user']['id_str'],json_object['text'].encode('utf-8'))#json_filtered.map(lambda x: (x['id_str'].encode('utf-8'),x['text'].encode('utf-8')))#.collect()   # should collect the output?
        
#     except ValueError, e: # this outputs None
#         return None#return False
#         #pass
#     if 'user' in json_object and 'text' in json_object and 'recipient' not in json_object:
#         return json_object#return True #return json_object
        
    
#     return None #return False





    


# In[ ]:

# tweet_tuple = rdd.filter(lambda x: safe_parse(x)).map(lambda x: json.loads(x)).map(lambda x:(x['user']['id_str'], x['text'].encode('utf-8')) ) #use this if safe_parse returns T/F
# #print type(tweet_tuple)


# (2) Count the number of different users in all valid tweets (hint: [the `distinct()` method](https://spark.apache.org/docs/latest/programming-guide.html#transformations)).
# 
# It should print
# ```
# The number of unique users is: 2083
# ```

# In[5]:

def print_users_count(count):
    print 'The number of unique users is:', count


# In[6]:

# your code here

tweet_tuple = rdd.map(lambda x: safe_parse(x)).filter(lambda x: x != None).cache()
count = tweet_tuple.map(lambda x: x[0]).distinct().count() # this is correct if safe_parse returns a tuple
#count = tweet_tuple.map(lambda x: x[0]).distinct().count() #.filter(lambda x: x != None) # use this if safe_parse returns T/F
print_users_count(count)
# print tweet_tuple.take(1)


# # Part 2: Number of posts from each user partition

# Load the Pickle file `../../Data/users-partition.pickle`, you will get a dictionary which represents a partition over 452,743 Twitter users, `{user_id: partition_id}`. The users are partitioned into 7 groups. For example, if the dictionary is loaded into a variable named `partition`, the partition ID of the user `59458445` is `partition["59458445"]`. These users are partitioned into 7 groups. The partition ID is an integer between 0-6.
# 
# Note that the user partition we provide doesn't cover all users appear in the input data.

# (1) Load the pickle file.

# In[7]:

# your code here

import pickle
filename = "../../Data/users-partition.pickle"
pd=pickle.load(open(filename,'rb'))
#print type(List.keys()[0])
# schemaString = "user_id partition_id"

# fields = [StructField(field_name, IntegerType(), True) for field_name in schemaString.split()]
# schema = StructType(fields)
#pf=sqlc.createDataFrame(List)
#print List


# (2) Count the number of posts from each user partition
# 
# Count the number of posts from group 0, 1, ..., 6, plus the number of posts from users who are not in any partition. Assign users who are not in any partition to the group 7.
# 
# Put the results of this step into a pair RDD `(group_id, count)` that is sorted by key.

# In[8]:

# your code here
#group_count = {}
#pickle_rdd = sc.parallelize(pd.keys(), pd.values())
# for item in pd:
#     print item[1]
#     if(group_count[item[1]] == None):
#         group_count[7] = group_count[7] +1
    
#     else:
#         group_count[item[1]] = group_count[item[1]]+1
#pd = k = user_id:v = group_id
group = tweet_tuple.map(lambda x: (pd[x[0]], 1) if x[0]  in pd else (7,1) ).reduceByKey(lambda x,y: x+y).sortByKey().collect()
#print  group.take(9)


# (3) Print the post count using the `print_post_count` function we provided.
# 
# It should print
# 
# ```
# Group 0 posted 81 tweets
# Group 1 posted 199 tweets
# Group 2 posted 45 tweets
# Group 3 posted 313 tweets
# Group 4 posted 86 tweets
# Group 5 posted 221 tweets
# Group 6 posted 400 tweets
# Group 7 posted 798 tweets
# ```

# In[9]:

def print_post_count(counts):
    for group_id, count in counts:
        print 'Group %d posted %d tweets' % (group_id, count)


# In[10]:

# your code here

print_post_count(group)


# # Part 3:  Tokens that are relatively popular in each user partition

# In this step, we are going to find tokens that are relatively popular in each user partition.
# 
# We define the number of mentions of a token $t$ in a specific user partition $k$ as the number of users from the user partition $k$ that ever mentioned the token $t$ in their tweets. Note that even if some users might mention a token $t$ multiple times or in multiple tweets, a user will contribute at most 1 to the counter of the token $t$.
# 
# Please make sure that the number of mentions of a token is equal to the number of users who mentioned this token but NOT the number of tweets that mentioned this token.
# 
# Let $N_t^k$ be the number of mentions of the token $t$ in the user partition $k$. Let $N_t^{all} = \sum_{i=0}^7 N_t^{i}$ be the number of total mentions of the token $t$.
# 
# We define the relative popularity of a token $t$ in a user partition $k$ as the log ratio between $N_t^k$ and $N_t^{all}$, i.e. 
# 
# \begin{equation}
# p_t^k = \log \frac{C_t^k}{C_t^{all}}.
# \end{equation}
# 
# 
# You can compute the relative popularity by calling the function `get_rel_popularity`.

# (0) Load the tweet tokenizer.

# In[11]:

# %load happyfuntokenizing.py
#!/usr/bin/env python

"""
This code implements a basic, Twitter-aware tokenizer.

A tokenizer is a function that splits a string of text into words. In
Python terms, we map string and unicode objects into lists of unicode
objects.

There is not a single right way to do tokenizing. The best method
depends on the application.  This tokenizer is designed to be flexible
and this easy to adapt to new domains and tasks.  The basic logic is
this:

1. The tuple regex_strings defines a list of regular expression
   strings.

2. The regex_strings strings are put, in order, into a compiled
   regular expression object called word_re.

3. The tokenization is done by word_re.findall(s), where s is the
   user-supplied string, inside the tokenize() method of the class
   Tokenizer.

4. When instantiating Tokenizer objects, there is a single option:
   preserve_case.  By default, it is set to True. If it is set to
   False, then the tokenizer will downcase everything except for
   emoticons.

The __main__ method illustrates by tokenizing a few examples.

I've also included a Tokenizer method tokenize_random_tweet(). If the
twitter library is installed (http://code.google.com/p/python-twitter/)
and Twitter is cooperating, then it should tokenize a random
English-language tweet.


Julaiti Alafate:
  I modified the regex strings to extract URLs in tweets.
"""

__author__ = "Christopher Potts"
__copyright__ = "Copyright 2011, Christopher Potts"
__credits__ = []
__license__ = "Creative Commons Attribution-NonCommercial-ShareAlike 3.0 Unported License: http://creativecommons.org/licenses/by-nc-sa/3.0/"
__version__ = "1.0"
__maintainer__ = "Christopher Potts"
__email__ = "See the author's website"

######################################################################

import re
import htmlentitydefs

######################################################################
# The following strings are components in the regular expression
# that is used for tokenizing. It's important that phone_number
# appears first in the final regex (since it can contain whitespace).
# It also could matter that tags comes after emoticons, due to the
# possibility of having text like
#
#     <:| and some text >:)
#
# Most imporatantly, the final element should always be last, since it
# does a last ditch whitespace-based tokenization of whatever is left.

# This particular element is used in a couple ways, so we define it
# with a name:
emoticon_string = r"""
    (?:
      [<>]?
      [:;=8]                     # eyes
      [\-o\*\']?                 # optional nose
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth      
      |
      [\)\]\(\[dDpP/\:\}\{@\|\\] # mouth
      [\-o\*\']?                 # optional nose
      [:;=8]                     # eyes
      [<>]?
    )"""

# The components of the tokenizer:
regex_strings = (
    # Phone numbers:
    r"""
    (?:
      (?:            # (international)
        \+?[01]
        [\-\s.]*
      )?            
      (?:            # (area code)
        [\(]?
        \d{3}
        [\-\s.\)]*
      )?    
      \d{3}          # exchange
      [\-\s.]*   
      \d{4}          # base
    )"""
    ,
    # URLs:
    r"""http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+"""
    ,
    # Emoticons:
    emoticon_string
    ,    
    # HTML tags:
     r"""<[^>]+>"""
    ,
    # Twitter username:
    r"""(?:@[\w_]+)"""
    ,
    # Twitter hashtags:
    r"""(?:\#+[\w_]+[\w\'_\-]*[\w_]+)"""
    ,
    # Remaining word types:
    r"""
    (?:[a-z][a-z'\-_]+[a-z])       # Words with apostrophes or dashes.
    |
    (?:[+\-]?\d+[,/.:-]\d+[+\-]?)  # Numbers, including fractions, decimals.
    |
    (?:[\w_]+)                     # Words without apostrophes or dashes.
    |
    (?:\.(?:\s*\.){1,})            # Ellipsis dots. 
    |
    (?:\S)                         # Everything else that isn't whitespace.
    """
    )

######################################################################
# This is the core tokenizing regex:
    
word_re = re.compile(r"""(%s)""" % "|".join(regex_strings), re.VERBOSE | re.I | re.UNICODE)

# The emoticon string gets its own regex so that we can preserve case for them as needed:
emoticon_re = re.compile(regex_strings[1], re.VERBOSE | re.I | re.UNICODE)

# These are for regularizing HTML entities to Unicode:
html_entity_digit_re = re.compile(r"&#\d+;")
html_entity_alpha_re = re.compile(r"&\w+;")
amp = "&amp;"

######################################################################

class Tokenizer:
    def __init__(self, preserve_case=False):
        self.preserve_case = preserve_case

    def tokenize(self, s):
        """
        Argument: s -- any string or unicode object
        Value: a tokenize list of strings; conatenating this list returns the original string if preserve_case=False
        """        
        # Try to ensure unicode:
        try:
            s = unicode(s)
        except UnicodeDecodeError:
            s = str(s).encode('string_escape')
            s = unicode(s)
        # Fix HTML character entitites:
        s = self.__html2unicode(s)
        # Tokenize:
        words = word_re.findall(s)
        # Possible alter the case, but avoid changing emoticons like :D into :d:
        if not self.preserve_case:            
            words = map((lambda x : x if emoticon_re.search(x) else x.lower()), words)
        return words

    def tokenize_random_tweet(self):
        """
        If the twitter library is installed and a twitter connection
        can be established, then tokenize a random tweet.
        """
        try:
            import twitter
        except ImportError:
            print "Apologies. The random tweet functionality requires the Python twitter library: http://code.google.com/p/python-twitter/"
        from random import shuffle
        api = twitter.Api()
        tweets = api.GetPublicTimeline()
        if tweets:
            for tweet in tweets:
                if tweet.user.lang == 'en':            
                    return self.tokenize(tweet.text)
        else:
            raise Exception("Apologies. I couldn't get Twitter to give me a public English-language tweet. Perhaps try again")

    def __html2unicode(self, s):
        """
        Internal metod that seeks to replace all the HTML entities in
        s with their corresponding unicode characters.
        """
        # First the digits:
        ents = set(html_entity_digit_re.findall(s))
        if len(ents) > 0:
            for ent in ents:
                entnum = ent[2:-1]
                try:
                    entnum = int(entnum)
                    s = s.replace(ent, unichr(entnum))	
                except:
                    pass
        # Now the alpha versions:
        ents = set(html_entity_alpha_re.findall(s))
        ents = filter((lambda x : x != amp), ents)
        for ent in ents:
            entname = ent[1:-1]
            try:            
                s = s.replace(ent, unichr(htmlentitydefs.name2codepoint[entname]))
            except:
                pass                    
            s = s.replace(amp, " and ")
        return s


# In[12]:

from math import log

tok = Tokenizer(preserve_case=False)

def get_rel_popularity(c_k, c_all):
    return log(1.0 * c_k / c_all) / log(2)


def print_tokens(tokens, gid = None):
    group_name = "overall"
    if gid is not None:
        group_name = "group %d" % gid
    print '=' * 5 + ' ' + group_name + ' ' + '=' * 5
    for t, n in tokens:
        print "%s\t%.4f" % (t, n)
    print


# (1) Tokenize the tweets using the tokenizer we provided above named `tok`. Count the number of mentions for each tokens regardless of specific user group.
# 
# Call `print_count` function to show how many different tokens we have.
# 
# It should print
# ```
# Number of elements: 8949
# ```

# In[13]:

# your code here





user_tweet_tok = tweet_tuple.map(lambda x: (x[0],tok.tokenize(x[1]))).reduceByKey(lambda x,y: x+y)# group all tweets of a user, thre are duplicate tokens
#print user_tweet_tok.take(3)
#tokens = user_tweet_tok.map(lambda x: tok.tokenize(x[1])).flatMap(lambda x: list(set(x))) # this worked before
#print tokens.take(2)

#---- pairs it up as (user, user's tokens)
tokens = user_tweet_tok.map(lambda x: (x[0],list(set(x[1])))).cache()# get rid of duplicate tokens per user and combine all users into 1 list
flat_tok = tokens.flatMap(lambda x: x[1])
distinct_tok = flat_tok.distinct()# gets distinct tokens over all users  #.reduce(lambda x, y: print_count(x)+ print_count(y))




#------ orig  wokrs -------
# tokens = user_tweet_tok.flatMap(lambda x: list(set(x[1])))# get rid of duplicate tokens per user and combine all users into 1 list
# distinct_tok = tokens.distinct()# gets distinct tokens over all users  #.reduce(lambda x, y: print_count(x)+ print_count(y))



print_count(distinct_tok)


# (2) Tokens that are mentioned by too few users are usually not very interesting. So we want to only keep tokens that are mentioned by at least 100 users. Please filter out tokens that don't meet this requirement.
# 
# Call `print_count` function to show how many different tokens we have after the filtering.
# 
# Call `print_tokens` function to show top 20 most frequent tokens.
# 
# It should print
# ```
# Number of elements: 44
# ===== overall =====
# :	1388.0000
# rt	1237.0000
# .	826.0000
# …	673.0000
# the	623.0000
# trump	582.0000
# to	499.0000
# ,	489.0000
# a	404.0000
# is	376.0000
# in	297.0000
# of	292.0000
# and	288.0000
# for	281.0000
# !	269.0000
# ?	210.0000
# on	195.0000
# i	192.0000
# you	191.0000
# this	190.0000
# ```

# In[14]:

# your code here
# orig: #tok_used_100 = tokens.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] >= 100).map(lambda (c,v): (v,c)).sortByKey(False).map(lambda (c,v): (v,c))


tok_used_100 = flat_tok.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y).filter(lambda x: x[1] >= 100).sortBy(lambda x: x[1], False).cache()#.map(lambda (c,v): (v,c)).sortByKey(False).map(lambda (c,v): (v,c))
#print tok_used_100.take(2)
print_count(tok_used_100)
print_tokens(tok_used_100.take(20))
#print_tokens(tok_used_100.collect()) #ORIG 


# (3) For all tokens that are mentioned by at least 100 users, compute their relative popularity in each user group. Then print the top 10 tokens with highest relative popularity in each user group. In case two tokens have same relative popularity, break the tie by printing the alphabetically smaller one.
# 
# **Hint:** Let the relative popularity of a token $t$ be $p$. The order of the items will be satisfied by sorting them using (-p, t) as the key.
# 
# It should print
# ```
# ===== group 0 =====
# ...	-3.5648
# at	-3.5983
# hillary	-4.0875
# i	-4.1255
# bernie	-4.1699
# not	-4.2479
# https	-4.2695
# he	-4.2801
# in	-4.3074
# are	-4.3646
# 
# ===== group 1 =====
# #demdebate	-2.4391
# -	-2.6202
# &	-2.7472
# amp	-2.7472
# clinton	-2.7570
# ;	-2.7980
# sanders	-2.8838
# ?	-2.9069
# in	-2.9664
# if	-3.0138
# 
# ===== group 2 =====
# are	-4.6865
# and	-4.7105
# bernie	-4.7549
# at	-4.7682
# sanders	-4.9542
# that	-5.0224
# in	-5.0444
# donald	-5.0618
# a	-5.0732
# #demdebate	-5.1396
# 
# ===== group 3 =====
# #demdebate	-1.3847
# bernie	-1.8480
# sanders	-2.1887
# of	-2.2356
# that	-2.3785
# the	-2.4376
# …	-2.4403
# clinton	-2.4467
# hillary	-2.4594
# be	-2.5465
# 
# ===== group 4 =====
# hillary	-3.7395
# sanders	-3.9542
# of	-4.0199
# clinton	-4.0790
# at	-4.1832
# in	-4.2143
# a	-4.2659
# on	-4.2854
# .	-4.3681
# the	-4.4251
# 
# ===== group 5 =====
# cruz	-2.3861
# he	-2.6280
# are	-2.7796
# will	-2.7829
# the	-2.8568
# is	-2.8822
# for	-2.9250
# that	-2.9349
# of	-2.9804
# this	-2.9849
# 
# ===== group 6 =====
# @realdonaldtrump	-1.1520
# cruz	-1.4532
# https	-1.5222
# !	-1.5479
# not	-1.8904
# …	-1.9269
# will	-2.0124
# it	-2.0345
# this	-2.1104
# to	-2.1685
# 
# ===== group 7 =====
# donald	-0.6422
# ...	-0.7922
# sanders	-1.0282
# trump	-1.1296
# bernie	-1.2106
# -	-1.2253
# you	-1.2376
# clinton	-1.2511
# if	-1.2880
# i	-1.2996
# ```

# In[41]:

# your code here
#use get_rel_popularity
#group = tweet_tuple.map(lambda x: (pd[x[0]], 1) if x[0]  in pd else (7,1) ).reduceByKey(lambda x,y: x+y).sortByKey().collect()
# this one contains (group,(user,tok)) #grouped_users = user_tweet_tok.map(lambda x: (pd[x[0]], [x]) if x[0]  in pd else (7,x) ).reduceByKey(lambda x,y: x+y)#.sortByKey().collect()

#grouped_users = user_tweet_tok.map(lambda x: (pd[x[0]], x[1]) if x[0]  in pd else (7,x) ).reduceByKey(lambda x,y: x+y)#.sortByKey().collect()


        #grouped_users = user_tweet_tok.map(lambda x: (pd[x[0]], x[1]) if x[0]  in pd else (7,x) ).reduceByKey(lambda x,y: x+y)#.sortByKey().collect()
#count_grouped_tok = grouped_users.map(lambda x: (x[0], (x[1].map(lambda  y: (y, 1)))))#.reduceByKey(lambda x,y: x[1]+y[1])
#group_tok = grouped_users.countByValue()
#print group_tok



#print user_tweet_tok.countByKey()

# createCombiner = (lambda x: (x,1)) # like a map to convert x[0] to int type
# mergeValue = (lambda X, V: (X[0], X[1])) # like a map to calc avg
# mergeCombiners = (lambda x, y: (x[0]+y[0], x[1]+y[1])) # like a reduce

# group_tok_count = group_tok.combineByKey(createCombiner, mergeValue, mergeCombiners)
# print group_tok_count.take(1)
#filtered_100 = grouped_users.filter(lambda x: x[1] in tok_used_100[0]) # contains only tokens in top 100 # maybe need nested filter
#count_filtered_100 = filtered_100.map()
#print count_grouped_tok.take(1)

#filtred_100 = grouped_users.reduceByKey(lambda x,y: x/y)

#user_tweet_tok.filter(lambda x: x[1] in )

#     createCombiner = (lambda x: (x,1)) # like a map to convert x[0] to int type
#     mergeValue = (lambda X, V: (X[0]+ V, X[1]+1)) # like a map to calc avg
#     mergeCombiners = (lambda x, y: (x[0]+y[0], x[1]+y[1])) # like a reduce
    
#     avgRDD = RDD1.mapValues(lambda x: int(x[0])).combineByKey(createCombiner, mergeValue, mergeCombiners )

grouped_users = tokens.map(lambda x: (pd[x[0]], x[1]) if x[0]  in pd else (7,x[1]) ).reduceByKey(lambda x,y: x+y)#.sortByKey().collect()
grouped_users_count = grouped_users.mapValues(lambda x: map ((lambda y: (y,1) ), x)).cache()
#group_0 = grouped_users.filter(lambda x: x[0] == 0 ).flatMap(lambda x: x[1] ).countByValue()# this returns a defaultdict obj

#print grouped_users.take(1)
#         for i in range(8):
#             group_i = grouped_users.filter(lambda x: x[0] == i )\
#             .flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)\
#             .join(tok_used_100)\
#             .mapValues(lambda x: get_rel_popularity (x[0],x[1]))\
#             #.sortBy(lambda x: x[1], False)

for i in range(8):
    group_i = grouped_users_count.filter(lambda x: x[0] == i ).flatMap(lambda x: x[1] )            .reduceByKey(lambda x,y: x+y)            .join(tok_used_100)            .mapValues(lambda x: get_rel_popularity (x[0],x[1]))            .sortByKey(True)            .sortBy(lambda x: x[1], False).take(10)
    print_tokens(group_i, i)

#print group_i.collect()
    #.keyBy(lambda x: (x[1],x[0])).sortByKey(False)
    
    #keyBy(keyfunc).sortByKey(ascending, numPartitions).values()

# group_1 = grouped_users_count.filter(lambda x: x[0] == 1 ).flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)
# group_2 = grouped_users_count.filter(lambda x: x[0] == 2 ).flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)
# group_3 = grouped_users_count.filter(lambda x: x[0] == 3 ).flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)
# group_4 = grouped_users_count.filter(lambda x: x[0] == 4 ).flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)
# group_5 = grouped_users_count.filter(lambda x: x[0] == 5 ).flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)
# group_6 = grouped_users_count.filter(lambda x: x[0] == 6 ).flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)
# group_7 = grouped_users_count.filter(lambda x: x[0] == 7 ).flatMap(lambda x: x[1] ).reduceByKey(lambda x,y: x+y)

#grouped_users_count_sum = grouped_users_count.mapValues(lambda x: )#.mapValues(lambda x: (t[0], t[1]/c[1]) for t in x if t[0] in tok_used_100)
                                    # grouped_users_count_tot = grouped_users_count.mapValues(lambda x: ( ))
#print group_i.collect()

# .reduceByKey(add)
#     .map(lambda kv: (kv[0][0], (kv[0][1], kv[1])))
#     .groupByKey()
#     .mapValues(lambda xs: (list(xs), sum(x[1] for x in xs))))
                                    # .mapValues(lambda xs: (list(xs), sum(x[1] for x in xs))))
#d = defaultdict(list)
# for tag, num in my_list:
#     d[tag].append(num)




#grouped_users_count_tot = grouped_users_count.mapValues(lambda x: reduce((lambda z,y: z[1]+y[1]), x))
        # grouped_users_count_tot = grouped_users_count.map(lambda x: x[1]).reduceByKey(lambda x,y: x+y)
        # print grouped_users_count_tot.take(3)

# grouped_users_count_tot = grouped_users_count
# print grouped_users_count_tot.take(1)    
#print grouped_users_count.foldByKey(0,lambda x,y: x+y).take(1)
# grouped_users_count_tot = grouped_users.mapValues(lambda x: reduce ((lambda x,y: x[1]+y[1] ), x))
# print grouped_users_count_tot.take(1)

                            # createCombiner = (lambda x: (x,1)) # like a map to initialize all v in (k,v) to 1 -> (k,(v,1))
                            # mergeValue = (lambda X, V: (X[0], X[1]+V)) # like a map to calc avg
                            # mergeCombiners = (lambda x, y: (x[0]+y[0], x[1]+y[1])) # like a reduce
                            # group_tok_count = grouped_users_count.combineByKey(createCombiner, mergeValue, mergeCombiners)

                            # print group_tok_count.take(1)







# (4) (optional, not for grading) The users partition is generated by a machine learning algorithm that tries to group the users by their political preferences. Three of the user groups are showing supports to Bernie Sanders, Ted Cruz, and Donald Trump. 
# 
# If your program looks okay on the local test data, you can try it on the larger input by submitting your program to the homework server. Observe the output of your program to larger input files, can you guess the partition IDs of the three groups mentioned above based on your output?

# In[ ]:

# Change the values of the following three items to your guesses
users_support = [
    (-1, "Bernie Sanders"),
    (-1, "Ted Cruz"),
    (-1, "Donald Trump")
]

for gid, candidate in users_support:
    print "Users from group %d are most likely to support %s." % (gid, candidate)

