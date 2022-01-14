import gzip
import json
import logging
import os
import uuid
from datetime import datetime

import tweepy


class TweepyExtractor:
    def __init__(self, path):
        self.path = path

    def auth(self):
        logging.info("Checking bearer token")
        return os.getenv("TWITTER_BEARER_TOKEN")

    def get_client(self, auth_token):
        logging.info("Getting Tweepy client")
        client = tweepy.Client(bearer_token=auth_token)
        return client

    def fetch(self, topic, max_results, **kwargs):
        auth_token = self.auth()
        client = self.get_client(auth_token)

        logging.info(f"Searching for {max_results} with {topic} keyword")
        params = {
            "expansions": ["author_id"],
            "max_results": max_results,
            "query": topic,
            "tweet_fields": ["lang", "created_at", "public_metrics"],
            "start_time": kwargs["data_interval_start"],
            "end_time": kwargs["data_interval_end"],
        }
        response = client.search_recent_tweets(**params)

        tweet_list = [tweet.data for tweet in response.data]
        tweet_list = self.add_watermark(tweet_list, params)
        self.save_file(tweet_list, kwargs["data_interval_start"])

    def add_watermark(self, tweet_list, watermark):
        for key, value in watermark.items():
            if isinstance(value, datetime):
                watermark[key] = value.isoformat()

        for tweet in tweet_list:
            tweet["watermark"] = watermark
        return tweet_list

    def save_file(self, data, execution_date):
        filepath = f"{self.path}/{execution_date}"
        filename = f"{uuid.uuid4()}.json.gz"
        logging.info(f"Saving file at {filepath}/{filename} as JSON with GZIP")

        json_str = json.dumps(data) + "\n"
        json_bytes = json_str.encode("utf-8")
        os.makedirs(filepath, exist_ok=True)
        with gzip.open(f"{filepath}/{filename}", "wb") as stream:
            stream.write(json_bytes)
        return f"{filepath}/{filename}"
