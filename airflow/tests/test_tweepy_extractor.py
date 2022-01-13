import os
import unittest

import tweepy

from include import TweepyExtractor


class TestTweepyExtractor(unitest.TestCase):
    def setUp(self):
        tmp_path = "~/.tmp/landing"
        os.makedirs(tmp_path, exist_ok=True)
        self.extractor = TweepyExtractor(path=tmp_path)

    def test_auth(self):
        token = self.extractor.auth()
        self.assertIsInstance(token, str)

    def test_get_client(self):
        token = self.extractor.auth()
        client = self.get_client(auth_token)
        self.assertIsInstance(client, tweepy.Client)

        tweet_id = 1293593516040269825
        response = None
        try:
            response = self.client.get_tweet(id=tweet_id)
        finally:
            self.assertIsNotNone(response)


if __name__ == "__main__":
    unittest.main()
