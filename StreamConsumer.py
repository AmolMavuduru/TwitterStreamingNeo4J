from kafka import KafkaConsumer
import json
from py2neo import Graph, Node, Relationship, Database
from textblob import TextBlob
import re
import string

def remove_punct(text):
    text  = "".join([char for char in text if char not in string.punctuation])
    text = re.sub('[0-9]+', '', text)
    return text

def get_hashtag_list(tweet_dict):

    return [hash['text'] for hash in tweet_dict['entities']['hashtags']]


def get_sentiment_data(tweet):

    analysis = TextBlob(remove_punct(tweet))

    polarity = analysis.sentiment.polarity


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
        text = TextBlob(dict_data["text"])
        tweet_text = dict_data["text"]
        sentiment_data = get_sentiment_data(dict_data["text"])
        tweet_id = dict_data["id_str"]
        location = dict_data["user"]["location"]
        screen_name = dict_data["user"]["screen_name"]

        #print(dict_data)
        hashtags = get_hashtag_list(dict_data)
        print("id: ", tweet_id)
        print("screen name: ", screen_name)
        print("location: ", location)
        print("text: ", tweet_text)
        print("Hashtags: ", hashtags)
        # add text and sentiment info to neo4j

        tx = g.begin()
        tweet_node = Node("Tweet", id=tweet_id,
            screen_name=screen_name, 
            location=location, 
            text=tweet_text)
        tx.create(tweet_node)

        print(dict(tweet_node))

        for hashtag in hashtags:

            hashtag_node = Node("Hashtag", name=hashtag)
            #INCLUDES = Relationship.type("INCLUDES")
            print(dict(hashtag_node))
            tweet_hashtag_rel = Relationship(tweet_node, "INCLUDES", hashtag_node)
            tx.create(tweet_hashtag_rel)
            #g.merge(INCLUDES(tweet_node, hashtag_node), "Tweet", "id")

        tx.commit()


        
        print('\n')

if __name__ == "__main__":
    main()