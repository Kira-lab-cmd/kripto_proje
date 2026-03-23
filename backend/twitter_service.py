import tweepy
from textblob import TextBlob
from dotenv import load_dotenv
import os

load_dotenv()

class TwitterService:
    def __init__(self):
        auth = tweepy.OAuth1UserHandler(
            os.getenv('X_API_KEY'),
            os.getenv('X_API_SECRET'),
            os.getenv('X_ACCESS_TOKEN'),
            os.getenv('X_ACCESS_SECRET')
        )
        self.api = tweepy.API(auth)

    def search_tweets(self, query='BTC', count=50):
        try:
            tweets = self.api.search_tweets(q=query, count=count, lang='en', tweet_mode='extended')
            return [tweet.full_text for tweet in tweets]
        except Exception as e:
            return str(e)

    def get_sentiment(self, texts):
        sentiments = []
        for text in texts:
            analysis = TextBlob(text)
            sentiments.append(analysis.sentiment.polarity)  # -1 negatif, +1 pozitif
        avg_sentiment = sum(sentiments) / len(sentiments) if sentiments else 0
        return avg_sentiment  # Ortalama sentiment skoru

