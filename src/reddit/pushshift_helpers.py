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
	size = 500
	uri = r'https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}&size={}'.format(subreddit, start_at, end_at, size)
	post_collections = map_posts(make_request(uri)['data'])

	n = len(post_collections)
	while n >= 500:
		last = post_collections[-1]
		new_start_at = last['created_utc']
		
		uri = r'https://api.pushshift.io/reddit/search/submission?subreddit={}&after={}&before={}&size={}'.format(subreddit, new_start_at, end_at, size)
		more_posts = map_posts(make_request(uri)['data'])
		post_collections.extend(more_posts)

		n = len(more_posts)

	return post_collections

	
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
	return list(map(lambda post: {
		'id': post['id'],
		'created_utc': post['created_utc'],
		'prefix': 't4_'
	}, posts))

