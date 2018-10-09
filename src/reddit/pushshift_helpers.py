import json
import time
import pandas
import requests
import datetime

import helpers


def give_me_intervals(start_at, number_of_days_per_interval = 3):
	full_day = 86400
	period = (full_day * number_of_days_per_interval)

	end_at = int(datetime.datetime.utcnow().timestamp())

	end = start_at + period
	yield (int(start_at), int(end))

	padding = 1
	while end <= end_at:
		start_at = end + padding
		end = (start_at - padding) + period
		yield (int(start_at), int(end))


def write_to_csv(output_directory, posts, start_at, end_at):
	posts_csv_path = r'{}/posts_{}_{}.csv'.format(output_directory, start_at, end_at)
	if len(posts) > 0:
		df_posts = pandas.DataFrame(posts)
		df_posts.index.names = ['index']
		df_posts.to_csv(posts_csv_path, header=True)
	else:
		with open(posts_csv_path, 'w') as posts_csv_file:
			pass


def pull_posts_for(subreddit, start_at, end_at):
	uri = r'https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}'.format(subreddit, start_at, end_at)
	return map_posts(make_request(uri)['data'])
	
	
def pull_comments_for(subreddit, start_at, end_at):
	uri = r'https://api.pushshift.io/reddit/search/comment?subreddit={}&after={}&before={}'.format(subreddit, start_at, end_at)
	return map_comments(make_request(uri)['data'])
	
	
def make_request(uri, max_retries = 5):
	def fire_away(uri):
		response = requests.get(uri)
		assert response.status_code == 200
		return json.loads(response.content)

	current_tries = 1
	while current_tries < max_retries:
		try:
			response = fire_away(uri)
			return response
		except:
			time.sleep(.150)
			current_tries += 1

	return fire_away(uri)

	
def map_posts(posts):
	mapped_collection = []
	if len(posts) == 0:
		return mapped_collection

	for post in posts:
		mapped_collection.append({
			'author': post['author'],
			'is_self': post['is_self'],
			'selftext': helpers.remove_new_lines(getattr(post, 'selftext', '')),
			'url': getattr(post, 'url', ''),
			'title': helpers.remove_new_lines(getattr(post, 'title', '')),
			'created_utc': post['created_utc'],
			'id': post['id'],
			'score': post['score'],
			'subreddit': post['subreddit'],
			'num_comments': post['num_comments'],
			'prefix': 't4_'
		})

	return mapped_collection
	
	
def map_comments(comments):
	mapped_collection = []
	if len(comments) == 0:
		return mapped_collection

	for comment in comments:
		mapped_collection.append({
			'author': comment['author'],
			'body': helpers.remove_new_lines(comment['body']),
			'created_utc': comment['created_utc'],
			'id': comment['id'],
			'link_id': comment['link_id'],
			'score': comment['score'],
			'subreddit': comment['subreddit'],
			'prefix': 't1_'
		})

	return mapped_collection