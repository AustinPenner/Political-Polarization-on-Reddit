import pandas as pd
from pymongo import MongoClient
import pprint
import praw

import pprint
import bs4 as bs
import urllib.request
import bz2,shutil

import boto3
import os
import time
import subprocess


def get_download_links():
    """Uses BeautifulSoup to parse "https://files.pushshift.io/reddit/comments/", which has all reddit comments by month. 
    Gets download links to the data and returns in a pandas dataframe"""

    url_source = 'https://files.pushshift.io/reddit/comments/'

    source = urllib.request.urlopen(url_source).read()
    soup = bs.BeautifulSoup(source,'lxml')
    table = soup.table

    url_dict = {'month': [], 'link': []}
    for i in table.find_all('tr', class_='file'):
        rel_url = i.find('a').text
        if rel_url[:2] == 'RC':
            dot = rel_url.find('.')
            url_dict['month'].append(rel_url[3:dot])
            url_dict['link'].append(url_source + rel_url)

    links_df = pd.DataFrame(url_dict)
    links_df['downloaded'] = False
    links_df['size_in_bytes'] = 0
    links_df['comment_count'] = 0

    return links_df

def download_file(download_url, s3_bucket = None, remove_file=True):
    """For a given file, this downloads the data and calls extract_file"""

    filename = download_url[download_url.rfind('/') + 1:]
    filepath = './data/comment_files/' + filename
    print('Downloading file...')

    if s3_bucket is None:
        urllib.request.urlretrieve(download_url, filepath)
    else:
        s3 = boto3.client('s3')
        s3.download_file(s3_bucket, filename, filepath)

    return extract_file(filepath, remove_file)

def extract_file(fp, remove_file):
    """Extracts the contents of a download file"""

    print('Extracting file...')
    fileout = fp[:fp.rfind('.')] + '.json'
    extension = fp[fp.rfind('.'):]

    if extension == '.bz2':
        with bz2.BZ2File(fp) as fr, open(fileout, 'wb') as fw:
            shutil.copyfileobj(fr,fw)
#     elif extension == '.xz':
#         pass
    else:
        raise Exception('Cannot decompress files of type {}'.format(extension))

    if remove_file:
        print('Removing file...')
        try:
            os.remove(fp)
        except:
            print("Error while deleting file ", fp)

    filesize = os.path.getsize(fileout)
    return fileout, filesize

def mongo_import(month, fp):
    """Takes .json file and imports into a mongo database. Used mongoimport command because it's faster
    than pymongo. Used subprocess to run bash command within python.

    Returns mongo database info"""

    db_name = 'reddit'
    collection_name = 'comments-{}'.format(month)
    #cmd = ['mongoimport', '-d', db_name, '-c', collection_name, '--file', fp]
    cmd = ['mongoimport', '-d', db_name, '-c', collection_name, '--file', fp, '--numInsertionWorkers', '8']
    print('Loading to mongodb...')
    subprocess.run(cmd, check=True, text=True)

    try:
        os.remove(fp)
    except:
        print("Error while deleting file ", fp)

    return {'db_name': db_name, 'collection_name': collection_name, 'filepath': fp, 'month': month}

def filter_comments(mongoinfo):
    """Takes mongoinfo dict containing database information for one month and runs a query to save only the
    relevant comments for our purposes. We want only top-level comments in one of the specified subreddits."""

    client = MongoClient()
    db_name, collection_name = mongoinfo['db_name'], mongoinfo['collection_name']
    db = client[db_name]
    comments = db[collection_name]
    comment_count = comments.count()

    cursor = comments.find({'subreddit': 
                                {'$in': ['politics', 'sports', 'worldnews', 'The_Donald']}, 
                            '$expr': 
                                {'$eq': ['$link_id', '$parent_id']}}, 
                           {'_id': 1, 'author': 1, 'body': 1, 'created_utc': 1, 'id': 1, 'link_id': 1, 
                                'parent_id': 1, 'score': 1, 'subreddit': 1})

    my_db = client['myreddit']
    my_comments = my_db[collection_name]

    print('Saving relevant comments...')
    for doc in cursor:
        my_comments.insert_one(doc)

    print('{} comments saved.'.format(my_comments.count()))
    comments.drop()
    client.close()

    return comment_count

def get_posts(praw_reddit, month):
    """We need not only comments for these subreddits, but post information as well. This function looks
    at all distinct posts ids within a month and pulls the post information from the reddit API using PRAW."""

    client = MongoClient()
    collection_name = 'comments-{}'.format(month)
    db = client['myreddit']
    comments = db[collection_name]
    post_ids = comments.distinct('link_id')
    distinct_posts = len(post_ids)
    print('There are {} distinct posts for the month'.format(distinct_posts))

    collection_name = 'posts-{}'.format(month)
    posts = db[collection_name]

    praw_generator = praw_reddit.info(post_ids)
    print('Getting post data...')
    for submission in praw_generator:
        d = {'link_id': submission.id, 
             'title': submission.title, 
             'score': submission.score, 
             'is_self': submission.is_self, 
             'datetime': submission.created_utc, 
             'sub': submission.subreddit.display_name, 
             'permalink': submission.permalink}
        posts.insert_one(d)

    client.close()

def main(praw_reddit, links_df=None, df_slice=None):
    if links_df is None:
        links_df = get_download_links()

    if df_slice is not None:
        links_df_iter = links_df[df_slice]
    else:
        links_df_iter = links_df

    for idx, row in links_df_iter.iterrows():
        start = time.time()
        month = row['month']
        url = row['link']
        print('Reddit comments month: {}'.format(month))

        filename = url[url.rfind('/') + 1:]
        s3_url = 's3://aust-galv-aust-finalcap/' + filename
        fp, filesize = download_file(s3_url, s3_bucket='aust-galv-aust-finalcap')
        mongoinfo = mongo_import(month, fp)

        comment_count = filter_comments(mongoinfo)
    #     get_posts(reddit, month)

        links_df.loc[idx, 'downloaded'] = True
        links_df.loc[idx, 'size_in_bytes'] = filesize
        links_df.loc[idx, 'comment_count'] = comment_count
        links_df.to_csv('data/comment_files/links_dataframe.csv', index=False)

        print('Done! Time elapsed: {:1.2f}'.format(time.time() - start))
        print('')

if __name__ == "__main__":
    with open('../keys/reddit_appid.txt') as f_id, open('../keys/reddit_secret.txt') as f_sec:
        APP_ID = f_id.read().rstrip()
        APP_SECRET = f_sec.read().rstrip()

    reddit = praw.Reddit(client_id=APP_ID, client_secret=APP_SECRET,
                        user_agent='script:my.project:v1.0.0 (by /u/Someone')

    links_df = get_download_links()

    main(reddit, links_df=links_df, df_slice=slice(89, 144))
