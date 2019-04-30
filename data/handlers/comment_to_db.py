import sqlite3
import json
from datetime import datetime

"""
Reddit Comment Database Creation Script.
Filters and creates a sqlite3 database from a .JSON of Reddit comments.
@Author Afaq Anwar
@Version 04/02/2019
"""

# List of Reddit Corpus Comments saved as .JSON
data_files = ['2017-01', '2017-02', '2017-03', '2017-12', '2018-01', '2018-10', '2018-11', '2018-12']
# Final commit to sql database.
sql_commit = []

connection = sqlite3.connect('{}.db'.format("reddit_data"))
cursor = connection.cursor()

# Creates the initial table within the database.
def create_table():
    cursor.execute("""CREATE TABLE IF NOT EXISTS parent_reply
    (parent_id TEXT PRIMARY KEY, comment_id TEXT UNIQUE, parent TEXT, comment TEXT,
     subreddit TEXT, unix INT, score INT)""")

# Cleans the data to avoid tokenizing of unnecessary data.
def format_data(data):
    data = data.replace("\n", " newlinechar ")
    data = data.replace("\r", " newlinechar ")
    data = data.replace('"', "'")
    return data

# Finds the parent of a comment based on a parent id.
# This helps set up the from - to reply structure.
def find_parent(pid):
    try:
        sql = "SELECT comment FROM parent_reply WHERE comment_id  = '{}' LIMIT 1".format(pid)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print("find_parent", e)
        return False

# Checks if the data is acceptable before being pushed to the databse.
def check_if_acceptable(data):
    if len(data.split(' ')) > 105 or len(data) <= 1:
        return False
    elif len(data) > 500:
        return False
    # Blocks removed comments from being inserted.
    elif data == "[deleted]" or data == "[removed]":
        return False
    # Filters out all links (hopefully).
    elif "https://" in data or "http://" in data or "www." in data or ".com" in data:
        return False
    else:
        return True


# Finds the score of a comment that is linked to a parent.
def find_existing_score(pid):
    try:
        sql = "SELECT score FROM parent_reply WHERE parent_id  = '{}' LIMIT 1".format(pid)
        cursor.execute(sql)
        result = cursor.fetchone()
        if result != None:
            return result[0]
        else:
            return False
    except Exception as e:
        print("find_score_parent", str(e))
        return False

def commit_builder(sql):
    global sql_commit
    sql_commit.append(sql)
    if len(sql_commit) > 5000:
        cursor.execute("BEGIN TRANSACTION")
        for s in sql_commit:
            try:
                cursor.execute(s)
            except:
                pass
        connection.commit()
        sql_commit = []

# Replaces a comment with a parent.
def sql_insert_replace_comment(comment_id, parent_id, parent, comment, subreddit, time, score):
    try:
        sql = """UPDATE parent_reply SET parent_id = ?, comment_id = ?, parent = ?, comment = ?, subreddit = ?, unix = ?, score = ? WHERE parent_id =?;""".format(parent_id, comment_id, parent, comment, subreddit, int(time), score, parent_id)
        commit_builder(sql)
    except Exception as e:
        print("INSERT REPLACE COMMENT", str(e))

# Attaches a comment to a parent with no previous comment.
def sql_insert_has_parent(comment_id, parent_id, parent, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, parent, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}","{}",{},{});""".format(parent_id, comment_id, parent, comment, subreddit, int(time), score)
        commit_builder(sql)
    except Exception as e:
        print('INSERT HAS PARENT', str(e))

# Inserts a new comment as the parent, aka a new thread.
def sql_insert_no_parent(comment_id, parent_id, comment, subreddit, time, score):
    try:
        sql = """INSERT INTO parent_reply (parent_id, comment_id, comment, subreddit, unix, score) VALUES ("{}","{}","{}","{}",{},{});""".format(parent_id, comment_id, comment, subreddit, int(time), score)
        commit_builder(sql)
    except Exception as e:
        print("INSERT NO PARENT", str(e))

if __name__ == "__main__":
    create_table()
    row_count = 0
    pairs = 0
    none_scores = 0
    for file in data_files:
        print("FILE LOADED - " + str(file))
        with open("/home/afaq/Projects/Data/Datasets/RedditCommentCorpus/{}/RC_{}".format(file.split("-")[0], file), buffering=1000) as f:
            for row in f:
                row_count += 1
                row = json.loads(row)
                score = row['score']
                # To avoid type comparisons with some malformed data sets, the score is prechecked.
                if score is None:
                    none_scores += 1
                    continue
                parent_id = row['parent_id'].split('_')[1]
                comment_id = row['id']
                body = format_data(row['body'])
                created_utc = row['created_utc']
                subreddit = row['subreddit']
                parent_data = find_parent(parent_id)

                # Filters out type of comment before committing to database.
                if score >= 2:
                    if check_if_acceptable(body):
                        existing_comment_score = find_existing_score(parent_id)
                        if existing_comment_score:
                            if score > existing_comment_score:
                                sql_insert_replace_comment(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                        else:
                            if parent_data:
                                sql_insert_has_parent(comment_id, parent_id, parent_data, body, subreddit, created_utc, score)
                                pairs += 1
                            else:
                                sql_insert_no_parent(comment_id, parent_id, body, subreddit, created_utc, score)

                if row_count % 100000 == 0:
                    print("Total rows read: {}, Paired Rows: {}, Invalid Comments: {}, Time: {}, File: {}".format(row_count, pairs, str(none_scores), str(datetime.now()), str(file)))
                    none_scores = 0
