import os
import subprocess
import sys
import numpy as np
import pandas as pd
import base64
from io import BytesIO
from time import sleep
import atexit
from collections import defaultdict
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly
import plotly.graph_objs as go
import plotly.express as px
from collections import defaultdict
from wordcloud import WordCloud, STOPWORDS, ImageColorGenerator
from py2neo import Graph, Node, Relationship, Database
from dash.dependencies import Input, Output

#KAFKA_DIR = "~/Downloads/kafka_2.12-2.5.0"

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)


class Neo4JDataExtractor(object):

    def __init__(self, user, password):

        self.database = Database("bolt://localhost:7687", user="neo4j", password="abc123")
        self.graph = Graph("bolt://localhost:7687", user="neo4j", password="abc123")
        self.hashtag_counts_df = None
        self.tweets = None


    def get_tweets_df(self):

        self.tweets = self.graph.run("MATCH (t: Tweet) RETURN t.screen_name AS user, t.sentiment AS sentiment, t.polarity AS polarity, t.text AS text").to_data_frame()
        return self.tweets

    def get_hashtags_df(self):

        hashtags = self.graph.run("MATCH (t: Tweet) --> (h: Hashtag) RETURN h.name AS hashtag").to_data_frame()
        counts = hashtags['hashtag'].value_counts()
        hashtag_counts_df = pd.DataFrame({'hashtag': list(counts.index), 'count': counts}).reset_index(drop=True)
        self.hashtag_counts_df = hashtag_counts_df.sort_values(by=['count'], ascending=False)
        return self.hashtag_counts_df

    def get_top5_tweets_df(self):

        self.get_tweets_df()
        return self.tweets.tail(5)

    def plot_wordcloud(self):

        self.get_hashtags_df()
        self.hashtag_counts_dict = dict(zip(self.hashtag_counts_df['hashtag'], self.hashtag_counts_df['count']))
        wc = WordCloud(background_color='white', width=720, height=480)
        wc.fit_words(self.hashtag_counts_dict)
        return wc.to_image()

    def get_sentiment_counts(self, hashtag):

        pos_query = "MATCH (h:Hashtag {name: '" + hashtag + "'}) <-- (t:Tweet) WHERE t.sentiment = 'positive' RETURN count(t) AS count"
        neut_query = "MATCH (h:Hashtag {name: '" + hashtag + "'}) <-- (t:Tweet) WHERE t.sentiment = 'neutral' RETURN count(t) AS count"
        neg_query = "MATCH (h:Hashtag {name: '" + hashtag + "'}) <-- (t:Tweet) WHERE t.sentiment = 'negative' RETURN count(t) AS count"
        
        positive_count = self.graph.run(pos_query).to_data_frame()['count'][0]
        neutral_count = self.graph.run(neut_query).to_data_frame()['count'][0]
        negative_count = self.graph.run(neg_query).to_data_frame()['count'][0]
        
        return {'positive': positive_count, 'neutral': neutral_count, 'negative': negative_count}

    def get_sentiment_counts_dict(self, hashtags: list):

        hashtag_sentiment_dict = {'positive': defaultdict(), 'neutral': defaultdict(), 'negative': defaultdict()}
        for hashtag in hashtags:
            sentiment_counts = self.get_sentiment_counts(hashtag)
            for sentiment, count in sentiment_counts.items():
                hashtag_sentiment_dict[sentiment][hashtag] = count
        
        return hashtag_sentiment_dict



data_extractor = Neo4JDataExtractor("neo4j", "abc123")

colors = {
    'background': 'white',
    'text': 'black'
}

app.layout = html.Div(style={'backgroundColor': colors['background'], 'textAlign': 'center'}, children=[
    html.H1(
        children='Twitter Live Streaming',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='Built with Kafka, Tweepy, Neo4J, and Dash', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    html.Br(),

   	html.H4(children='Most Recent Tweets',
        style={
            'textAlign': 'center',
            'color': colors['text']
    }),

    dash_table.DataTable(
    	id='live-tweet-table',
    	columns=([{'id': 'user', 'name': 'user'},
    			  {'id': 'text', 'name': 'text'},
    			  {'id': 'sentiment', 'name': 'sentiment'},
    			  {'id': 'polarity', 'name': 'polarity'}]),
    	),

    dcc.Graph(id='live-hashtag-graph'),

    dcc.Graph(id='live-sentiment-pie'),

    html.H4(children='Hashtag Word Cloud',
        style={
            'textAlign': 'center',
            'color': colors['text']
    }),

    html.Img(id='word-cloud'),

    dcc.Interval(
            id='interval-component',
            interval=3*1000, # in milliseconds
            n_intervals=0
        ),

    dcc.Interval(
    		id='pie-chart-interval',
    		interval=3*1000,
    		n_intervals=0),

    dcc.Interval(
            id='word-cloud-interval',
            interval=4*1000,
            n_intervals=0)
])

@app.callback(Output('live-hashtag-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):

    hashtag_counts_df = data_extractor.get_hashtags_df()
    hashtags = list(hashtag_counts_df['hashtag'][:10])
    """
    counts = list(hashtag_counts_df['count'][:10])
    fig = go.Figure(
    	data=[go.Bar(
            x=hashtags, y=counts,
            text=counts,
            textposition='auto',
        )])
    """

    sentiment_counts = data_extractor.get_sentiment_counts_dict(hashtags)
    fig = go.Figure(data=[
    go.Bar(name='positive', x=hashtags, y=list(sentiment_counts['positive'].values()), marker_color='green'),
    go.Bar(name='neutral', x=hashtags, y=list(sentiment_counts['neutral'].values()), marker_color='blue'),
    go.Bar(name='negative', x=hashtags, y=list(sentiment_counts['negative'].values()), marker_color='red')
    ])


    fig.update_layout(title={
        'text': "Top 10 Hashtags",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
    	xaxis_title="Hashtag",
    	yaxis_title="Number of Tweets",
        barmode='stack')

    return fig



@app.callback(Output('live-sentiment-pie', 'figure'),
			  [Input('pie-chart-interval', 'n_intervals')])
def update_sentiment_pie(n):

	tweets = data_extractor.get_tweets_df()
	sentiment_counts = tweets['sentiment'].value_counts()
	sentiment_counts = pd.DataFrame({'sentiment': list(sentiment_counts.index), 'count': sentiment_counts}).reset_index(drop=True)
	trace = go.Pie(labels = sentiment_counts['sentiment'], values = sentiment_counts['count'])
	data = [trace]
	fig = go.Figure(data = data)

	fig.update_layout(title={
        'text': "Overall Tweet Sentiment",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'
        })

	return fig


@app.callback(Output('word-cloud', 'src'), 
              [Input('word-cloud-interval', 'n_intervals')])
def make_image(n):
    img = BytesIO()
    data_extractor.plot_wordcloud().save(img, format='PNG')
    return 'data:image/png;base64,{}'.format(base64.b64encode(img.getvalue()).decode())



@app.callback(Output('live-tweet-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_tweets_live(n):

	top5_tweets = data_extractor.get_top5_tweets_df()

	return top5_tweets.to_dict('records')



if __name__ == '__main__':
	app.run_server(debug=True)
