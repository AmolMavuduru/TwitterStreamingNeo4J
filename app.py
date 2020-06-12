import os
import subprocess
import sys
import numpy as np
import pandas as pd
from time import sleep
import atexit
from collections import defaultdict
import dash
import dash_table
import dash_core_components as dcc
import dash_html_components as html
import plotly
import plotly.graph_objs as go
from py2neo import Graph, Node, Relationship, Database
from dash.dependencies import Input, Output

KAFKA_DIR = "~/Downloads/kafka_2.12-2.5.0"

global child_processes
global status_dict

child_processes = defaultdict()
status_dict = defaultdict()

external_stylesheets = ['https://codepen.io/chriddyp/pen/bWLwgP.css']

app = dash.Dash(__name__, external_stylesheets=external_stylesheets)

"""
def kill_subprocesses():

	for key, process in child_processes.items():
		process.terminate()


def wait_subprocesses():

	for key, process in child_processes.items():
		status_dict[key] = process.wait()


atexit.register(kill_subprocesses)


def start_zookeeper(kafka_dir):

	pass


def start_kafka_server(kafka_dir):

	pass

def start_kafka_producer():

	child_processes['stream_producer'] = subprocess.Popen(["python", "StreamProducer.py"])


def start_kafka_consumer():

	child_processes['stream_consumer'] = subprocess.Popen(["python", "StreamConsumer.py"])
"""

class Neo4JDataExtractor(object):

	def __init__(self, user, password):

		self.database = Database("bolt://localhost:7687", user="neo4j", password="abc123")
		self.graph = Graph("bolt://localhost:7687", user="neo4j", password="abc123")


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

		self.tweets = self.graph.run("MATCH (t: Tweet) RETURN t.screen_name AS user, t.sentiment AS sentiment, t.text AS text LIMIT 5").to_data_frame()
		return self.tweets

data_extractor = Neo4JDataExtractor("neo4j", "abc123")

colors = {
    'background': 'white',
    'text': 'black'
}

app.layout = html.Div(style={'backgroundColor': colors['background']}, children=[
    html.H1(
        children='Twitter Live Streaming',
        style={
            'textAlign': 'center',
            'color': colors['text']
        }
    ),

    html.Div(children='Built with Kafka, Tweepy, Neo4J, and Dask', style={
        'textAlign': 'center',
        'color': colors['text']
    }),

    dash_table.DataTable(
    	id='live-tweet-table',
    	columns=([{'id': 'user', 'name': 'user'},
    			  {'id': 'text', 'name': 'text'},
    			  {'id': 'sentiment', 'name': 'sentiment'}]),
    	),

    dcc.Graph(id='live-hashtag-graph'),
    dcc.Interval(
            id='interval-component',
            interval=1*1000, # in milliseconds
            n_intervals=0
        )
])

@app.callback(Output('live-hashtag-graph', 'figure'),
              [Input('interval-component', 'n_intervals')])
def update_graph_live(n):

	hashtag_counts_df = data_extractor.get_hashtags_df()
	hashtags = list(hashtag_counts_df['hashtag'][:10])
	counts = list(hashtag_counts_df['count'][:10])
	fig = go.Figure(
		data=[go.Bar(
	        x=hashtags, y=counts,
	        text=counts,
	        textposition='auto',
	    )])

	fig.update_layout(title={
        'text': "Top 10 Hashtags",
        'y':0.9,
        'x':0.5,
        'xanchor': 'center',
        'yanchor': 'top'},
		xaxis_title="Hashtag",
		yaxis_title="Number of Tweets")

	return fig

@app.callback(Output('live-tweet-table', 'data'),
              [Input('interval-component', 'n_intervals')])
def update_tweets_live(n):

	top5_tweets = data_extractor.get_top5_tweets_df()

	return top5_tweets.to_dict('records')



if __name__ == '__main__':
	app.run_server(debug=True)
