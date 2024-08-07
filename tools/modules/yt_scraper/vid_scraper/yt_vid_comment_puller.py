from googleapiclient.discovery import build
from utils.auth.secret_squirrel import SecretSquirrel
import json



class YouTubeVidCommentPuller():

    def __init__(self, video_id='mlUub4X__74'):
        self.creds = SecretSquirrel('').stash
        api_key = self.creds['YOUTUBE_API_KEY']
        self.video_id = video_id  # Replace with the YouTube video ID

        # Build a YouTube client
        self.youtube = build('youtube', 'v3', developerKey=api_key)



    # Function to get video comments
    def get_video_comments(self, youtube, video_id, max_results=100):
        comments = []
        response = youtube.commentThreads().list(
            part='snippet',
            videoId=video_id,
            maxResults=max_results,
            textFormat='plainText'
        ).execute()

        while response:
            for item in response['items']:
                comment = item['snippet']['topLevelComment']['snippet']['textDisplay']
                comments.append(comment)

            # Check if there are more comments to fetch
            if 'nextPageToken' in response:
                response = youtube.commentThreads().list(
                    part='snippet',
                    videoId=video_id,
                    pageToken=response['nextPageToken'],
                    maxResults=max_results,
                    textFormat='plainText'
                ).execute()
            else:
                break

        return comments

    def execute(self):
        # Fetch comments
        comments = self.get_video_comments(self.youtube, self.video_id)

        # Save comments to a JSON file
        with open(f"tools/modules/yt_scraper/vid_scraper/output/{self.video_id}.json", 'w', encoding='utf-8') as f:
            json.dump(comments, f, ensure_ascii=False, indent=4)

        print(f"Comments saved to 'output/{self.video_id}.json'")