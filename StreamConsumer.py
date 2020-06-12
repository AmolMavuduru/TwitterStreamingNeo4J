from kafka import KafkaConsumer
import json
from py2neo import Graph, Node, Relationship, Database
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
from googletrans import Translator
import re
import string

def remove_punct(text):
    text  = "".join([char for char in text if char not in string.punctuation])
    text = re.sub('[0-9]+', '', text)
    return text

def get_hashtag_list(tweet_dict):

    return [hash['text'] for hash in tweet_dict['entities']['hashtags']]


def get_sentiment_data(tweet, english=True):

    sentiment_analyzer = SentimentIntensityAnalyzer()
    translator = Translator()

    if not english:
        tweet = translator.translate(tweet).text

    analysis = sentiment_analyzer.polarity_scores(remove_punct(tweet))

    polarity = analysis['compound']


    if polarity > 0:

        sentiment = 'positive'

    elif polarity < 0:

        sentiment = 'negative'

    else:

        sentiment = 'neutral'


    return {'sentiment': sentiment, 'polarity': polarity}



def main():
    '''
    main function initiates a kafka consumer, initialize the tweetdata database.
    Consumer consumes tweets from producer extracts features, cleanses the tweet text,
    and loads the data into a Neo4j database
    '''
    # set-up a Kafka consumer
    consumer = KafkaConsumer("twitter")
    db = Database("bolt://localhost:7687", user="neo4j", password="abc123")
    g = Graph("bolt://localhost:7687", user="neo4j", password="abc123")


    for msg in consumer:

        dict_data = json.loads(msg.value.decode('utf-8'))
        tweet_text = dict_data["text"]
        try:
            sentiment_data = get_sentiment_data(dict_data["text"], english=False)
        except:
            sentiment_data = get_sentiment_data(dict_data["text"], english=True)
        tweet_id = dict_data["id_str"]
        location = dict_data["user"]["location"]
        screen_name = dict_data["user"]["screen_name"]

        #print(dict_data)
        hashtags = get_hashtag_list(dict_data)
        print("id: ", tweet_id)
        print("screen name: ", screen_name)
        print("location: ", location)
        print("sentiment: ", sentiment_data['sentiment'])
        print("text: ", tweet_text)
        #print("Hashtags: ", hashtags)
        # add text and sentiment info to neo4j

        tx = g.begin()
        tweet_node = Node("Tweet", id = tweet_id,
            screen_name = screen_name, 
            location = location, 
            text = tweet_text,
            sentiment = sentiment_data['sentiment'],
            polarity = sentiment_data['polarity'])

        try:
            tx.merge(tweet_node, "Tweet", "id")
        except:
            continue

        #print(dict(tweet_node))

        for hashtag in hashtags:

            hashtag_node = Node("Hashtag", name=hashtag)
            #print(dict(hashtag_node))
            tx.merge(hashtag_node, "Hashtag", "name")
            tweet_hashtag_rel = Relationship(tweet_node, "INCLUDES", hashtag_node)
            tx.merge(tweet_hashtag_rel, "Hashtag", "name")

        tx.commit()
        
        print('\n')

if __name__ == "__main__":
    main()