import os
import shutil
import unittest

import tweepy

from include import TweepyExtractor


class TestTweepyExtractor(unittest.TestCase):
    def setUp(self):
        self.tmp_path = ".tmp/landing"
        os.makedirs(self.tmp_path, exist_ok=True)
        self.extractor = TweepyExtractor(path=self.tmp_path)

    def tearDown(self):
        shutil.rmtree("/".join(self.tmp_path.split("/")[:-1]))

    def test_auth(self):
        token = self.extractor.auth()
        self.assertIsInstance(token, str)

    def test_get_client(self):
        auth_token = self.extractor.auth()
        client = self.extractor.get_client(auth_token)
        self.assertIsInstance(client, tweepy.Client)

        tweet_id = 1293593516040269825
        response = None
        try:
            response = client.get_tweet(id=tweet_id)
        finally:
            self.assertIsNotNone(response)

    def test_add_watermark(self):
        tweet_list = [{"author_id": 123456, "text": "My text #python"}]
        watermark = {"max_results": 5, "query": "#test"}
        tweet_watermarked = self.extractor.add_watermark(tweet_list, watermark)

        self.assertIsInstance(tweet_watermarked, list)
        for tweet in tweet_watermarked:
            self.assertIsInstance(tweet, dict)
            self.assertIsNotNone(tweet.get("watermark"))
            self.assertIsInstance(tweet.get("watermark"), dict)

    def test_save_file(self):
        tweet_list = [{"author_id": 123456, "text": "My text #python"}]
        watermark = {"max_results": 5, "query": "#test"}
        tweet_watermarked = self.extractor.add_watermark(tweet_list, watermark)

        filename = self.extractor.save_file(tweet_watermarked, "1970-01-01")
        self.assertTrue(os.path.exists(filename))
        self.assertTrue(os.path.isfile(filename))


if __name__ == "__main__":
    unittest.main()
