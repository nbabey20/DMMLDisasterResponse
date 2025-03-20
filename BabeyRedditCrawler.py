import os
import json
import time
import praw
import pandas as pd
from datetime import datetime
from prawcore.exceptions import TooManyRequests

#API credentials
reddit = praw.Reddit(
    client_id="9Hzn7E0aeNSu9i_A0_VJCQ",
    client_secret="9x5eOtG8JVcgeIC7B9xzPiVZCm7ngA",
    user_agent="babey_disaster_crawler/1.0"
)

reddit.read_only = True

#function that serializes a comment thread
def serialize_comment(comment):
    #if the comment has replies, comment metadata is stored
    data = {
        "comment_id": comment.id,
        "author": str(comment.author) if comment.author else "[deleted]",
        "body": comment.body,
        "score": comment.score,
        "created_utc": comment.created_utc
    }
    #each comment has an id, if a comment has responses, it is given a parent id
    #if the comment has no replies, only the basic data fields are included
    replies = []
    if hasattr(comment, "replies"):
        for reply in comment.replies:
            if isinstance(reply, praw.models.Comment):
                serialized_reply = serialize_comment(reply)
                replies.append(serialized_reply)
                
    if replies:
        data["parent_id"] = comment.parent_id
        data["replies"] = replies
        
    return data

#function that deals with TooManyRequests
def safe_replace_more(comments, limit=None):
    """
    Repeatedly attempts to call comments.replace_more(limit=limit).
    If TooManyRequests is encountered, waits and retries.
    """
    while True:
        try:
            comments.replace_more(limit=limit)
            break
        except TooManyRequests as e:
            #wait and try again
            wait_time = getattr(e, "retry_after", None)
            if wait_time is None:
                wait_time = 60  #default wait time if retry_after is None
            print(f"Too many requests. Sleeping for {wait_time + 1} seconds...")
            time.sleep(wait_time + 1)

#collect posts from input list of subreddits
def collect_posts_from_subreddits(subreddits, query, limit=10):
    combined_subreddit_str = "+".join(subreddits)
    subreddit_obj = reddit.subreddit(combined_subreddit_str)
    
    posts = []
    for submission in subreddit_obj.search(query, sort="new", time_filter="all", limit=limit):
        posts.append(submission)
    
    return posts

#save posts and comments as threads in JSON structure
def save_post_and_comments_to_json(submission, directory="LA_Wildfire_2025"):
    os.makedirs(directory, exist_ok=True)
    
    #handle limit rates
    safe_replace_more(submission.comments, limit=None)
    
    #only serialize top level comments
    comments_data = [serialize_comment(comment)
                     for comment in submission.comments
                     if isinstance(comment, praw.models.Comment)]
    
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
    
    json_filename = f"wildfire_{submission.id}.json"
    json_path = os.path.join(directory, json_filename)
    
    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(post_json, f, ensure_ascii=False, indent=2)
    
    return json_path

#determine county info for csv data
def get_county_info(subreddit_name):
    sub_lower = subreddit_name.lower()
    if sub_lower == "losangeles":
        return "06037", "los angeles"
    elif sub_lower == "sandiego":
        return "06073", "san diego"
    elif sub_lower in ["ventura", "venturacounty"]:
        return "06111", "ventura"
    else:
        return "", ""

#collect attached images if present
def get_attached_images(submission):
    image_extensions = ('.jpg', '.jpeg', '.png', '.gif')
    if not submission.is_self:
        url_lower = submission.url.lower()
        if url_lower.endswith(image_extensions) or (hasattr(submission, "post_hint") and submission.post_hint == "image"):
            return submission.url
    return ""

#main script to build csv and store json paths
def main():
    subreddits_to_search = ["LosAngeles", "sandiego", "ventura", "venturacounty"]
    query = "wildfire"
    limit = 200  #adjust
    
    print(f"Searching subreddits {subreddits_to_search} for '{query}'...")
    submissions = collect_posts_from_subreddits(subreddits_to_search, query, limit=limit)
    
    if not submissions:
        print("No posts found for the given query.")
        return
    
    rows = []
    for i, submission in enumerate(submissions, start=1):
        json_path = save_post_and_comments_to_json(submission, directory="LA_Wildfire_2025")
        
        sub_name = submission.subreddit.display_name
        county_fips, county_subreddit_name = get_county_info(sub_name)
        attached_images = get_attached_images(submission)
        #csv data creation
        rows.append({
            "Index": i,
            "EvtName": "LA Wildfires",
            "CountyFIPS": county_fips,
            "CountySubredditName": county_subreddit_name,
            "JSONPath": json_path,
            "TitleOfThePost": submission.title,
            "Author": str(submission.author) if submission.author else "[deleted]",
            "CreatedUTC": datetime.utcfromtimestamp(submission.created_utc),
            "Score": submission.score,
            "NumComments": submission.num_comments,
            "Subreddit": sub_name,
            "Permalink": f"https://www.reddit.com{submission.permalink}",
            "SelfText": submission.selftext,
            "AttachedImages": attached_images,
            "SHELDUS_Event_Name": "Wildfires_2025_CA",
            "matched_terms": "wildfire",
            "match_found": "yes"
        })
    
    columns = [
        "Index",
        "EvtName",
        "CountyFIPS",
        "CountySubredditName",
        "JSONPath",
        "TitleOfThePost",
        "Author",
        "CreatedUTC",
        "Score",
        "NumComments",
        "Subreddit",
        "Permalink",
        "SelfText",
        "AttachedImages",
        "SHELDUS_Event_Name",
        "matched_terms",
        "match_found"
    ]
    
    df = pd.DataFrame(rows, columns=columns)
    csv_filename = "la_wildfire_posts.csv"
    df.to_csv(csv_filename, index=False)
    
    print(f"\nCollected {len(df)} posts. Data saved to '{csv_filename}'.")
    print("JSON files (with hierarchical comment threads) stored in the 'LA_Wildfire_2025' folder.")

if __name__ == "__main__":
    main()
