import os
import time
import numpy
import pandas
import datetime

import helpers
import pushshift_helpers
import scrapper_task

import luigi

class AggregateTask(luigi.Task):

	is_complete = False

	subreddit = luigi.Parameter('subreddit')
	start_at = luigi.IntParameter('start-at')

	def requires(self):
		for interval in pushshift_helpers.give_me_intervals(self.start_at):
			yield scrapper_task.ScrapperTask(self.subreddit, interval[0], interval[1])

			
	def run(self):
    		
		helpers.verify_directories([
			r'./data/reddit/{}/by_day/posts'.format(self.subreddit),
			r'./data/reddit/{}/by_day/comments'.format(self.subreddit)
		])

		for row in self.input():
			for input_file in row:
			
				fn = input_file.fn
				if helpers.is_file_empty(fn):
					input_file.remove()
					continue
    			
				df = pandas.read_csv(fn, index_col='index', low_memory=False)

				if 'comments_' in fn:
					
					df = df.loc[:, ['author','body','id','created_utc','submissionId','score']]
					df.columns = ['author', 'body', 'commentId', 'created_utc', 'postId', 'score']
					df.body = df.body.fillna('').astype(str)

					df['by_day'] = df.created_utc.map(lambda cu: time.strftime("%Y_%m_%d", time.gmtime(cu)))
					for day in numpy.unique(df.by_day.values):
						
						flag = 'w'
						header = True
						file_path = r'./data/reddit/{}/by_day/comments/{}.csv'.format(self.subreddit, day)
						if os.path.exists(file_path):
							flag = 'a'
							header = False
				
						q1 = df.by_day == day
						with open(file_path, flag) as output:
							df.loc[q1, ['author', 'body', 'commentId', 'created_utc', 'postId', 'score']].to_csv(output, header=header, index=False)

				else:

					df = df.loc[:, ['author','selftext','id','title','created_utc','score','view_count', 'url']]
					df.columns = ['author', 'selftext', 'postId', 'title', 'created_utc', 'score', 'view_count', 'url']
					df['by_day'] = df.created_utc.map(lambda cu: time.strftime("%Y_%m_%d", time.gmtime(cu)))

					for day in numpy.unique(df.by_day.values):
						
						flag = 'w'
						header = True
						
						file_path = r'./data/reddit/{}/by_day/posts/{}.csv'.format(self.subreddit, day)
						if os.path.exists(file_path):
							flag = 'a'
							header = False

						q1 = df.by_day == day
						with open(file_path, flag) as output:
							df.loc[q1, ['author', 'created_utc', 'postId', 'score', 'selftext', 'title', 'url', 'view_count']].to_csv(output, header=header, index=False)

				input_file.remove()
			
		self.is_complete = True
	
		
	def complete(self):
		return self.is_complete


if __name__ == '__main__':
    luigi.run()