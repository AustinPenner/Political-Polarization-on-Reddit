import numpy as np
import pandas as pd
import pymongo
from pymongo import MongoClient
from textblob import TextBlob
import nltk
from nltk.sentiment.vader import SentimentIntensityAnalyzer as SIA
import pprint

from datetime import datetime
import os
import time
import subprocess


def calculate_polarity(month, analyzer='sia'):
    """Calculate the polarity of each comment within a month. Update mongodb 
    collection with this data point."""
    
    client = MongoClient()
    db = client['myreddit']
    collection_name = 'comments-' + month
    collection = db[collection_name]
    
    cursor = collection.find({'$and': 
                                   [{'$expr': {'$ne': ['$body', '[deleted]']}},
                                    {'$expr': {'$gt': ['$score', 0]}}]
                           })

    if analyzer == 'sia':
        sia = SIA()
        for doc in cursor:
            _id = doc['_id']
            body = doc['body']
            sentiment = sia.polarity_scores(body)['compound']
            collection.update_one({"_id": _id}, {"$set": {"vader_sentiment": sentiment}})
    elif analyzer == 'textblob':
        for doc in cursor:
            _id = doc['_id']
            body = doc['body']
            sentiment = TextBlob(body).sentiment.polarity
            collection.update_one({"_id": _id}, {"$set": {"textblob_sentiment": sentiment}})
    else:
        raise Error('Invalid analyzer selected')

    client.close()


def monthly_stats(month, subreddit=None, analyzer='vader'):
    """Returns the average absolute sentiment polarity of a comment within a given month."""

    client = MongoClient()
    db = client['myreddit']
    collection_name = 'comments-' + month
    collection = db[collection_name]

    if subreddit == None:
        cursor = collection.find({'$and': 
                                       [{'$expr': {'$ne': ['$body', '[deleted]']}},
                                        {'$expr': {'$gt': ['$score', 0]}}]
                                })
    else:
        cursor = collection.find({'$and': 
                                       [{'$expr': {'$ne': ['$body', '[deleted]']}},
                                        {'$expr': {'$gt': ['$score', 0]}},
                                        {'subreddit': subreddit}]
                                })
    
    total_n_words = 0
    total_abs_polarity = 0
    total_abs_weighted_polarity = 0
    total_score = 0
    comment_count = 0
    for doc in cursor:
        try:
            total_n_words += len(doc['body'].split())
        except:
            pass
        if analyzer == 'vader':
            total_abs_weighted_polarity += doc['score']*abs(doc['vader_sentiment'])
            total_abs_polarity += abs(doc['vader_sentiment'])
        else:
            total_abs_weighted_polarity += doc['score']*abs(doc['textblob_sentiment'])
            total_abs_polarity += abs(doc['textblob_sentiment'])
        total_score += doc['score']
        comment_count += 1

    try:
        avg_abs_weighted_polarity = total_abs_weighted_polarity/total_score
    except ZeroDivisionError:
        avg_abs_weighted_polarity = None

    try:
        avg_abs_polarity = total_abs_polarity/comment_count
    except ZeroDivisionError:
        avg_abs_polarity = None

    try:
        avg_wordcount = total_n_words/comment_count
    except ZeroDivisionError:
        avg_wordcount = None

    client.close()

    return {'avg_abs_wght_pol': avg_abs_weighted_polarity, 
            'avg_abs_pol': avg_abs_polarity,
            'comment_count': comment_count, 
            'avg_wordcount': avg_wordcount}


def get_sentiment(m_start, m_end, df, subreddit=None, analyzer='vader'):

    d = {}
    for idx, row in df.loc[m_start:m_end].iterrows():
        month = row['month']
        if idx % 10 == 0:
            print(idx, month)
        d[month] = monthly_stats(month, subreddit=subreddit, analyzer=analyzer)

    return d


def monthly_stats_top_posts(yearmonths, subreddit, post_limit=100, analyzer='vader'):
    """Gets specific stats for all comments among the top (post_limit) posts."""
    
    client = MongoClient()
    db = client['myreddit']
    posts_all = db['posts_all']

    cursor = posts_all.find({'yearmonth': yearmonths[0], 'sub': subreddit}).sort('score', pymongo.DESCENDING).limit(post_limit)
    link_ids = [post['link_id'] if post['link_id'].startswith('t3_') else 't3_' + post['link_id'] for post in cursor]

    total_n_words = 0
    total_abs_weighted_polarity = 0
    total_abs_polarity = 0
    total_score = 0
    total_wordcount = 0
    comment_count = 0

    for ym in yearmonths:
        coll_name = 'comments-' + ym
        coll = db[coll_name]
        cursor = coll.find({'$and': 
                                [{'$expr': {'$ne': ['$body', '[deleted]']}},
                                 {'$expr': {'$gt': ['$score', 0]}},
                                 {'link_id': {'$in': link_ids}}]
                            })
        for doc in cursor:
            try:
                total_n_words += len(doc['body'].split())
            except:
                pass
            if analyzer == 'vader':
                total_abs_weighted_polarity += doc['score']*abs(doc['vader_sentiment'])
                total_abs_polarity += abs(doc['vader_sentiment'])
            else:
                total_abs_weighted_polarity += doc['score']*abs(doc['textblob_sentiment'])
                total_abs_polarity += abs(doc['textblob_sentiment'])
            total_score += doc['score']
            comment_count += 1
    
    client.close()

    try:
        avg_abs_weighted_polarity = total_abs_weighted_polarity/total_score
    except ZeroDivisionError:
        avg_abs_weighted_polarity = None

    try:
        avg_abs_polarity = total_abs_polarity/comment_count
    except ZeroDivisionError:
        avg_abs_polarity = None

    try:
        avg_wordcount = total_n_words/comment_count
    except ZeroDivisionError:
        avg_wordcount = None
    
    return {'avg_abs_wght_pol': avg_abs_weighted_polarity, 
            'avg_abs_pol': avg_abs_polarity,
            'comment_count': comment_count, 
            'avg_wordcount': avg_wordcount}