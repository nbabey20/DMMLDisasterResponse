import praw
import pandas as pd
import os
import json
from datetime import datetime
from prawcore.exceptions import Redirect


#API credentials
reddit = praw.Reddit(
    client_id="9Hzn7E0aeNSu9i_A0_VJCQ",
    client_secret="9x5eOtG8JVcgeIC7B9xzPiVZCm7ngA",
    user_agent="babey_disaster_crawler/1.0"
)

#read only true just to fetch data
reddit.read_only = True

#post-collection function ---
def collect_posts(query, limit=10):
    subreddit = reddit.subreddit("all")
    posts = []
    
    for submission in subreddit.search(query, sort="new", time_filter="all", limit=limit):
        posts.append(submission)
    
    return posts

#save comments to json
def save_post_and_comments_to_json(submission, json_dir="json_data"):
    #check for directory
    os.makedirs(json_dir, exist_ok=True)
    
    #retr;ieve all comments
    submission.comments.replace_more(limit=None)
    
    #push comment data to array
    comments_data = []
    for comment in submission.comments.list():
        # Some comments may have been deleted or removed
        comment_author = str(comment.author) if comment.author else "[deleted]"
        comments_data.append({
            "comment_id": comment.id,
            "author": comment_author,
            "body": comment.body,
            "score": comment.score,
            "created_utc": comment.created_utc
        })
    
    #json structure
    post_json = {
        "post_id": submission.id,
        "title": submission.title,
        "selftext": submission.selftext,
        "score": submission.score,
        "num_comments": submission.num_comments,
        "created_utc": submission.created_utc,
        "subreddit": str(submission.subreddit),
        "author": str(submission.author) if submission.author else "[deleted]",
        "url": submission.url,
        "comments": comments_data
    }
    
    #file path
    json_filename = f"{submission.id}.json"
    json_path = os.path.join(json_dir, json_filename)
    
    #write to local folder
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(post_json, f, ensure_ascii=False, indent=2)
    
    return json_path

def main():
    #only use keyword wildfire
    query = "wildfire"
    limit = 500 

    print(f"Searching Reddit for posts about '{query}'...")
    submissions = collect_posts(query, limit=limit)
    
    if not submissions:
        print("No posts found for the given query.")
        return
    
    #rows for csv
    rows = []
    
    for i, submission in enumerate(submissions, start=1):
        #post and comments saved to json file
        json_path = save_post_and_comments_to_json(submission, json_dir="json_data")
        
        #Build CSV rows with correct columns
        rows.append({
            "Index": i,
            "PostID": submission.id,
            "Title": submission.title,
            "Score": submission.score,
            "Subreddit": str(submission.subreddit),
            "Author": str(submission.author) if submission.author else "[deleted]",
            "CreatedUTC": datetime.utcfromtimestamp(submission.created_utc),
            "NumComments": submission.num_comments,
            "Permalink": f"https://www.reddit.com{submission.permalink}",
            "SelfText": submission.selftext,
            "JSONPath": json_path  # The path to the JSON file
        })
    
    #create dataframe for each row
    df = pd.DataFrame(rows, columns=[
        "Index",
        "PostID",
        "Title",
        "Score",
        "Subreddit",
        "Author",
        "CreatedUTC",
        "NumComments",
        "Permalink",
        "SelfText",
        "JSONPath"
    ])
    
    #save to csv
    csv_filename = "wildfire_posts.csv"
    df.to_csv(csv_filename, index=False)
    
    print(f"\nCollected {len(df)} posts. Data saved to '{csv_filename}'.")
    print("JSON files stored in the 'json_data' folder.")

if __name__ == "__main__":
    main()