import os

from tweepy import Client


class TweepyExtractor:
    def __init__(self, path):
        self.raw_path = path

    def get_client(self):
        client = Client(
            bearer_token=os.getenv("TWITTER_BEARER_TOKEN"),
            access_token=os.getenv("TWITTER_API_KEY"),
            access_token_secret=os.getenv("TWITTER_API_KEY_SECRET"),
        )
        return client

    def fetch(self, topic, max_results, **kwargs):
        client = self.get_client()
        cursor = client.search_all_tweets(
            query=topic,
            max_results=max_results,
        )
        for tweet in cursor:
            print(tweet)
